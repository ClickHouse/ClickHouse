#pragma once

#include <cmath>
#include <limits>
#include <memory> // for std::unique_ptr
#include <base/types.h>

#include <AggregateFunctions/DDSketch/DDSketchEncoding.h>
#include <AggregateFunctions/DDSketch/Mapping.h>
#include <AggregateFunctions/DDSketch/Store.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Common/Exception.h>

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
        : mapping(std::make_unique<DDSketchLogarithmicMapping>(relative_accuracy))
        , store(std::make_unique<DDSketchDenseStore>())
        , negative_store(std::make_unique<DDSketchDenseStore>())
        , zero_count(0.0)
        , count(0.0)
    {
    }

    DDSketchDenseLogarithmic(
        std::unique_ptr<DDSketchLogarithmicMapping> mapping_,
        std::unique_ptr<DDSketchDenseStore> store_,
        std::unique_ptr<DDSketchDenseStore> negative_store_,
        Float64 zero_count_)
        : mapping(std::move(mapping_))
        , store(std::move(store_))
        , negative_store(std::move(negative_store_))
        , zero_count(zero_count_)
        , count(store->count + negative_store->count + zero_count_)
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
        Float64 quantile_value = 0;
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

    Float64 getGamma() const { return mapping->getGamma(); }

    Float64 getCount() const { return count; }

    void copy(const DDSketchDenseLogarithmic & other)
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

    void merge(const DDSketchDenseLogarithmic & other)
    {
        if (*mapping != *other.mapping)
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

    void serialize(WriteBuffer & buf) const
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

    void deserialize(ReadBuffer & buf)
    {
        // Read the mapping
        UInt8 flag = 0;
        readBinary(flag, buf);
        if (flag != enc.FlagIndexMappingBaseLogarithmic.byte)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid flag for mapping");
        }
        mapping->deserialize(buf);

        /// reject bins with impossible keys now, otherwise they only blow up later during a merge
        const auto [min_valid_key, max_valid_key] = mapping->validKeyRange();

        // Read the positive and negative stores
        readBinary(flag, buf);
        if (flag != enc.FlagTypePositiveStore)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid flag for positive store");
        }
        store->deserialize(buf, min_valid_key, max_valid_key);

        readBinary(flag, buf);
        if (flag != enc.FlagTypeNegativeStore)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid flag for negative store");
        }
        negative_store->deserialize(buf, min_valid_key, max_valid_key);

        // Read the zero count
        readBinary(flag, buf);
        if (flag != enc.FlagZeroCountVarFloat.byte)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid flag for zero count");
        }
        readBinary(zero_count, buf);
        if (!std::isfinite(zero_count) || zero_count < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid zero_count in DDSketch: {}", zero_count);

        count = negative_store->count + zero_count + store->count;
        if (!std::isfinite(count))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid total count in DDSketch: {}", count);
    }

    /// NOLINTEND(readability-static-accessed-through-instance)

private:
    std::unique_ptr<DDSketchLogarithmicMapping> mapping;
    std::unique_ptr<DDSketchDenseStore> store;
    std::unique_ptr<DDSketchDenseStore> negative_store;
    Float64 zero_count;
    Float64 count;
    DDSketchEncoding enc;


    /// Only remaps onto a coarser mapping (`merge` remaps the finer sketch), so an old bin overlaps at most two new bins
    DDSketchDenseLogarithmic changeMapping(Float64 new_gamma) const
    {
        auto new_mapping = std::make_unique<DDSketchLogarithmicMapping>((new_gamma - 1) / (new_gamma + 1));

        auto new_positive_store = std::make_unique<DDSketchDenseStore>();
        auto new_negative_store = std::make_unique<DDSketchDenseStore>();

        /// Pieces below this share of an old bin are rounding slivers at coinciding bin boundaries: `lowerBound` is off by
        /// up to ~2e-13 relative (`exp` of an argument up to ~709), and an old bin is `gamma - 1` wide relative to its lower
        /// bound. Skip them, otherwise `keyAtRank` can return the neighbouring bin. The cap is not reached for the accepted
        /// relative accuracies (above 1e-6)
        const Float64 min_remap_proportion = std::min(1e-9 / (mapping->getGamma() - 1), 0.25);

        /// The highest old bins can end above the highest new bin, and a key above it would make the state unreadable.
        /// Their pieces above it go to the last kept piece, as the values below the lowest new bin go one bin up in `key`
        const int max_new_key = static_cast<int>(new_mapping->validKeyRange().second);

        auto remap_store = [this, &new_mapping, min_remap_proportion, max_new_key](
                               DDSketchDenseStore & old_store, std::unique_ptr<DDSketchDenseStore> & target_store)
        {
            for (int i = 0; i < old_store.length(); ++i)
            {
                int old_index = i + old_store.offset;
                Float64 old_bin_count = old_store.bins[i];

                if (old_bin_count == 0)
                    continue;

                Float64 in_lower_bound = this->mapping->lowerBound(old_index);
                Float64 in_upper_bound = this->mapping->lowerBound(old_index + 1);

                /// The lowest and highest old bins can start outside the range of the coarser mapping
                int new_index = new_mapping->key(std::clamp(in_lower_bound, new_mapping->getMinPossible(), new_mapping->getMaxPossible()));

                /// `exp` overflows for the upper bound of the highest old bin with some gammas, and bins of a gamma very close to 1
                /// can have no width in floating point. There is nothing to split by, so the whole bin goes where `key` puts it
                if (!std::isfinite(in_upper_bound) || !(in_upper_bound > in_lower_bound))
                {
                    target_store->add(new_index, old_bin_count);
                    continue;
                }
                Float64 in_size = in_upper_bound - in_lower_bound;
                // Distribute counts to new bins
                static constexpr int max_remap_iterations = 100000;
                /// The last kept piece gets the rest of the count, including skipped slivers, so the pieces sum exactly
                /// to the old count, otherwise `get` can compute a negative rank
                Float64 remaining_count = old_bin_count;
                bool has_last = false;
                /// An old bin below the lowest new bin overlaps none, it goes where `key` puts its values
                int last_index = new_index;
                Float64 last_count = 0;
                int iterations = 0;
                for (; new_index <= max_new_key && new_mapping->lowerBound(new_index) < in_upper_bound; ++new_index)
                {
                    if (++iterations > max_remap_iterations)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Too many iterations in DDSketch changeMapping");

                    Float64 out_lower_bound = new_mapping->lowerBound(new_index);
                    Float64 out_upper_bound = new_mapping->lowerBound(new_index + 1);
                    Float64 lower_intersection_bound = std::max(out_lower_bound, in_lower_bound);
                    Float64 higher_intersection_bound = std::min(out_upper_bound, in_upper_bound);
                    Float64 intersection_size = higher_intersection_bound - lower_intersection_bound;
                    Float64 proportion = intersection_size / in_size;
                    if (proportion < min_remap_proportion)
                        continue;
                    if (has_last)
                    {
                        target_store->add(last_index, last_count);
                        remaining_count -= last_count;
                    }
                    has_last = true;
                    last_index = new_index;
                    last_count = proportion * old_bin_count;
                }
                target_store->add(last_index, remaining_count);
            }
        };

        remap_store(*store, new_positive_store);
        remap_store(*negative_store, new_negative_store);

        return DDSketchDenseLogarithmic(std::move(new_mapping), std::move(new_positive_store), std::move(new_negative_store), zero_count);
    }
};

}
