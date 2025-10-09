#pragma once

#include <memory> // for std::unique_ptr
#include <cmath>
#include <stdexcept>
#include <limits>
#include <iostream>
#include <base/types.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <AggregateFunctions/DDSketch/Mapping.h>
#include <AggregateFunctions/DDSketch/Store.h>
#include <AggregateFunctions/DDSketch/DDSketchEncoding.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

class DDSketchDenseLogarithmic
{
public:
    explicit DDSketchDenseLogarithmic(Float64 relative_accuracy = 0.01)
        : mapping(std::make_unique<DDSketchLogarithmicMapping>(relative_accuracy)),
          store(std::make_unique<DDSketchDenseStore>()),
          negative_store(std::make_unique<DDSketchDenseStore>()),
          zero_count(0.0),
          count(0.0)
    {
    }

    DDSketchDenseLogarithmic(std::unique_ptr<DDSketchLogarithmicMapping> mapping_,
             std::unique_ptr<DDSketchDenseStore> store_,
             std::unique_ptr<DDSketchDenseStore> negative_store_,
             Float64 zero_count_)
        : mapping(std::move(mapping_)),
          store(std::move(store_)),
          negative_store(std::move(negative_store_)),
          zero_count(zero_count_),
          count(store->count + negative_store->count + zero_count_)
    {
    }

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

    void copy(const DDSketchDenseLogarithmic& other)
    {
        Float64 rel_acc = (other.mapping->getGamma() - 1) / (other.mapping->getGamma() + 1);
        mapping = std::make_unique<DDSketchLogarithmicMapping>(rel_acc);
        store = std::make_unique<DDSketchDenseStore>();
        negative_store = std::make_unique<DDSketchDenseStore>();
        store->copy(other.store.get());
        negative_store->copy(other.negative_store.get());
        zero_count = other.zero_count;
        count = other.count;
    }

    void merge(const DDSketchDenseLogarithmic& other)
    {
        if (mapping->getGamma() != other.mapping->getGamma())
        {
            // modify the one with higher precision to match the one with lower precision
            if (mapping->getGamma() > other.mapping->getGamma())
            {
                DDSketchDenseLogarithmic new_sketch = other.changeMapping(mapping->getGamma());
                this->merge(new_sketch);
                return;
            }

            DDSketchDenseLogarithmic new_sketch = changeMapping(other.mapping->getGamma());
            copy(new_sketch);
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

    /// NOLINTBEGIN(readability-static-accessed-through-instance)

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
        count = negative_store->count + zero_count + store->count;
    }

    /// NOLINTEND(readability-static-accessed-through-instance)

private:
    std::unique_ptr<DDSketchLogarithmicMapping> mapping;
    std::unique_ptr<DDSketchDenseStore> store;
    std::unique_ptr<DDSketchDenseStore> negative_store;
    Float64 zero_count;
    Float64 count;
    DDSketchEncoding enc;


    DDSketchDenseLogarithmic changeMapping(Float64 new_gamma) const
    {
        auto new_mapping = std::make_unique<DDSketchLogarithmicMapping>((new_gamma - 1) / (new_gamma + 1));

        auto new_positive_store = std::make_unique<DDSketchDenseStore>();
        auto new_negative_store = std::make_unique<DDSketchDenseStore>();

        auto remap_store = [this, &new_mapping](DDSketchDenseStore& old_store, std::unique_ptr<DDSketchDenseStore>& target_store)
        {
            for (int i = 0; i < old_store.length(); ++i)
            {
                int old_index = i + old_store.offset;
                Float64 old_bin_count = old_store.bins[i];

                Float64 in_lower_bound = this->mapping->lowerBound(old_index);
                Float64 in_upper_bound = this->mapping->lowerBound(old_index + 1);
                Float64 in_size = in_upper_bound - in_lower_bound;

                int new_index = new_mapping->key(in_lower_bound);
                // Distribute counts to new bins
                for (; new_mapping->lowerBound(new_index) < in_upper_bound; ++new_index)
                {
                    Float64 out_lower_bound = new_mapping->lowerBound(new_index);
                    Float64 out_upper_bound = new_mapping->lowerBound(new_index + 1);
                    Float64 lower_intersection_bound = std::max(out_lower_bound, in_lower_bound);
                    Float64 higher_intersection_bound = std::min(out_upper_bound, in_upper_bound);
                    Float64 intersection_size = higher_intersection_bound - lower_intersection_bound;
                    Float64 proportion = intersection_size / in_size;
                    target_store->add(new_index, proportion * old_bin_count);
                }
            }
        };

        remap_store(*store, new_positive_store);
        remap_store(*negative_store, new_negative_store);

        return DDSketchDenseLogarithmic(std::move(new_mapping), std::move(new_positive_store), std::move(new_negative_store), zero_count);
    }
};

}
