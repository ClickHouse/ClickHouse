#pragma once

#include <memory> // for std::unique_ptr
#include <cmath>
#include <stdexcept>
#include <limits>
#include <iostream>
#include <base/types.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <AggregateFunctions/Mapping.h>
#include <AggregateFunctions/Store.h>
#include <AggregateFunctions/DDSketchEncoding.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class BaseQuantileDDSketch
{
public:
    BaseQuantileDDSketch(std::unique_ptr<KeyMapping> mapping_, std::unique_ptr<DenseStore> store_,
                         std::unique_ptr<DenseStore> negative_store_, Float64 zero_count_)
        : mapping(std::move(mapping_)), store(std::move(store_)), negative_store(std::move(negative_store_)),
          zero_count(zero_count_),
          count(static_cast<Float64>(negative_store->count + zero_count_ + store->count)) {}

    void add(Float64 val, Float64 weight = 1.0)
    {
        if (weight <= 0.0)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "weight must be a positive Float64");
        }

        if (val > mapping->getMinPossible())
        {
            store->add(mapping->key(val), weight);
        }
        else if (val < -mapping->getMinPossible())
        {
            negative_store->add(mapping->key(-val), weight);
        }
        else
        {
            zero_count += weight;
        }

        count += weight;
    }

    Float64 get(Float64 quantile) const
    {
        if (quantile < 0 || quantile > 1 || count == 0)
        {
            return std::numeric_limits<Float64>::quiet_NaN(); // Return NaN if the conditions are not met
        }

        Float64 rank = quantile * (count - 1);
        Float64 quantile_value;
        if (rank < negative_store->count)
        {
            Float64 reversed_rank = negative_store->count - rank - 1;
            int key = negative_store->keyAtRank(reversed_rank, false);
            quantile_value = -mapping->value(key);
        }
        else if (rank < zero_count + negative_store->count)
        {
            quantile_value = 0;
        }
        else
        {
            int key = store->keyAtRank(rank - zero_count - negative_store->count, true);
            quantile_value = mapping->value(key);
        }
        return quantile_value;
    }

    void copy(const BaseQuantileDDSketch& other)
    {
        Float64 rel_acc = (other.mapping->getGamma() - 1) / (other.mapping->getGamma() + 1);
        mapping = std::make_unique<LogarithmicMapping>(rel_acc);
        store = std::make_unique<DenseStore>();
        negative_store = std::make_unique<DenseStore>();
        store->copy(other.store.get());
        negative_store->copy(other.negative_store.get());
        zero_count = other.zero_count;
        count = other.count;
    }

    void merge(const BaseQuantileDDSketch& other)
    {
        if (mapping->getGamma() != other.mapping->getGamma())
        {
            // modify the one with higher precision to match the one with lower precision
            if (mapping->getGamma() > other.mapping->getGamma())
            {
                BaseQuantileDDSketch new_sketch = other.changeMapping(mapping->getGamma());
                this->merge(new_sketch);
                return;
            }
            else
            {
                BaseQuantileDDSketch new_sketch = changeMapping(other.mapping->getGamma());
                copy(new_sketch);
            }
        }

        // If the other sketch is empty, do nothing
        if (other.count == 0)
        {
            return;
        }

        // If this sketch is empty, copy the other sketch
        if (count == 0)
        {
            copy(other);
            return;
        }

        count += other.count;
        zero_count += other.zero_count;

        store->merge(other.store.get());
        negative_store->merge(other.negative_store.get());
    }

    void serialize(WriteBuffer& buf) const
    {
        // Write the mapping
        writeBinary(enc.FlagIndexMappingBaseLogarithmic.byte, buf);
        mapping->serialize(buf);

        // Write the positive and negative stores
        writeBinary(enc.FlagTypePositiveStore, buf);
        store->serialize(buf);

        writeBinary(enc.FlagTypeNegativeStore, buf);
        negative_store->serialize(buf);

        // Write the zero count
        writeBinary(enc.FlagZeroCountVarFloat.byte, buf);
        writeBinary(zero_count, buf);
    }

    void deserialize(ReadBuffer& buf)
    {
        // Read the mapping
        UInt8 flag = 0;
        readBinary(flag, buf);
        if (flag != enc.FlagIndexMappingBaseLogarithmic.byte)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid flag for mapping");
        }
        mapping->deserialize(buf);

        // Read the positive and negative stores
        readBinary(flag, buf);
        if (flag != enc.FlagTypePositiveStore)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid flag for positive store");
        }
        store->deserialize(buf);

        readBinary(flag, buf);
        if (flag != enc.FlagTypeNegativeStore)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid flag for negative store");
        }
        negative_store->deserialize(buf);

        // Read the zero count
        readBinary(flag, buf);
        if (flag != enc.FlagZeroCountVarFloat.byte)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid flag for zero count");
        }
        readBinary(zero_count, buf);
        count = static_cast<Float64>(negative_store->count + zero_count + store->count);
    }

private:
    std::unique_ptr<KeyMapping> mapping;
    std::unique_ptr<DenseStore> store;
    std::unique_ptr<DenseStore> negative_store;
    Float64 zero_count;
    Float64 count;
    DDSketchEncoding enc;
    Poco::Logger * log = &Poco::Logger::get("DDSketch");


    BaseQuantileDDSketch changeMapping(Float64 new_gamma) const
    {
        Float64 old_gamma = mapping->getGamma();

        // Create new Stores to hold the remapped bins
        auto new_store = std::make_unique<DenseStore>();
        auto new_negative_store = std::make_unique<DenseStore>();

        // Change mapping for the positive store
        for (int i = 0; i < store->length(); ++i)
        {
            int old_key = i + store->offset;
            int new_key = static_cast<int>(std::round(old_key * std::log(new_gamma) / std::log(old_gamma)));
            new_store->add(new_key, store->bins[i]);
        }

        // Change mapping for the negative store
        for (int i = 0; i < negative_store->length(); ++i)
        {
            int old_key = i + negative_store->offset;
            int new_key = static_cast<int>(std::round(old_key * std::log(new_gamma) / std::log(old_gamma)));
            new_negative_store->add(new_key, negative_store->bins[i]);
        }

        auto new_mapping = std::make_unique<LogarithmicMapping>((new_gamma - 1) / (new_gamma + 1));

        // Construct a new BaseQuantileDDSketch object with the new components
        return BaseQuantileDDSketch(std::move(new_mapping), std::move(new_store), std::move(new_negative_store), zero_count);
    }
};

class DDSketch : public BaseQuantileDDSketch
{
public:
    explicit DDSketch(Float64 relative_accuracy = 0.01)
        : BaseQuantileDDSketch(std::make_unique<LogarithmicMapping>(relative_accuracy),
                               std::make_unique<DenseStore>(),
                               std::make_unique<DenseStore>(), 0.0) {}
};

}
