#pragma once

#include <algorithm>
#include <cmath>
#include <limits>
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
        : mapping(relative_accuracy)
        , zero_count(0.0)
        , count(0.0)
    {
    }

    DDSketchDenseLogarithmic(
        const DDSketchLogarithmicMapping & mapping_,
        DDSketchDenseStore && store_,
        DDSketchDenseStore && negative_store_,
        Float64 zero_count_)
        : mapping(mapping_)
        , store(std::move(store_))
        , negative_store(std::move(negative_store_))
        , zero_count(zero_count_)
        , count(store.count + negative_store.count + zero_count_)
    {
    }

    void add(Float64 val, Float64 weight = 1.0)
    {
        if (weight <= 0.0)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "weight must be a positive Float64");
        }

        if (val > mapping.getMinPossible())
        {
            addValue(store, val, weight);
        }
        else if (val < -mapping.getMinPossible())
        {
            addValue(negative_store, -val, weight);
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
        if (rank < negative_store.count)
        {
            Float64 reversed_rank = negative_store.count - rank - 1;
            int key = negative_store.keyAtRank(reversed_rank, false);
            quantile_value = -mapping.value(key);
        }
        else if (rank < zero_count + negative_store.count)
        {
            quantile_value = 0;
        }
        else
        {
            int key = store.keyAtRank(rank - zero_count - negative_store.count, true);
            quantile_value = mapping.value(key);
        }
        return quantile_value;
    }

    /// `get` of all levels in one pass over the bins: `result[i]` is for `levels[i]`. The levels are in [0, 1], and
    /// `indices` sorts them in increasing order
    template <typename T>
    void getMany(const Float64 * levels, const size_t * indices, size_t num_levels, T * result) const
    {
        if (count == 0)
        {
            for (size_t i = 0; i < num_levels; ++i)
                result[i] = toResult<T>(std::numeric_limits<Float64>::quiet_NaN());
            return;
        }

        /// Levels by increasing rank. The order flips for a total weight below 1: `count = 0.5` gives rank -0.5 for level 1
        const Float64 max_rank = count - 1;
        const auto index = [&](size_t i) { return max_rank >= 0 ? indices[i] : indices[num_levels - 1 - i]; };

        size_t num_negative = 0;
        while (num_negative < num_levels && levels[index(num_negative)] * max_rank < negative_store.count)
            ++num_negative;

        /// The negative store is by magnitude: level 0 is its highest key, so go in reverse
        DDSketchDenseStore::KeyAtRankCursor negative_cursor(negative_store);
        for (size_t i = num_negative; i-- > 0;)
        {
            const Float64 rank = levels[index(i)] * max_rank;
            const int key = negative_cursor.next(negative_store.count - rank - 1, false);
            result[index(i)] = toResult<T>(-mapping.value(key));
        }

        DDSketchDenseStore::KeyAtRankCursor positive_cursor(store);
        for (size_t i = num_negative; i < num_levels; ++i)
        {
            const Float64 rank = levels[index(i)] * max_rank;
            if (rank < zero_count + negative_store.count)
            {
                result[index(i)] = static_cast<T>(0.0);
                continue;
            }
            const int key = positive_cursor.next(rank - zero_count - negative_store.count, true);
            result[index(i)] = toResult<T>(mapping.value(key));
        }
    }

    /// Casting NaN or an out-of-range value to an integer is undefined. NaN of an empty sketch gives 0, as in the other
    /// quantile functions, and the rest is clamped: e.g. the bin of the largest `Date` has a value above it
    template <typename T>
    static T toResult(Float64 value)
    {
        if constexpr (std::is_integral_v<T>)
        {
            if (std::isnan(value))
                return 0;
            if (value <= static_cast<Float64>(std::numeric_limits<T>::lowest()))
                return std::numeric_limits<T>::lowest();
            /// The largest value of a 64-bit type rounds up to a power of two, which is out of its range
            if (value >= static_cast<Float64>(std::numeric_limits<T>::max()))
                return std::numeric_limits<T>::max();
        }
        return static_cast<T>(value);
    }

    Float64 getGamma() const { return mapping.getGamma(); }

    Float64 getCount() const { return count; }

    Float64 getZeroCount() const { return zero_count; }

    /// Makes the sketch empty, with the given mapping
    void reset(const DDSketchLogarithmicMapping & mapping_)
    {
        mapping = mapping_;
        store.clear();
        negative_store.clear();
        zero_count = 0;
        count = 0;
    }

    /// For building a sketch from bucketed data. Does not downscale: the caller makes all the bins fit first
    void addToBin(bool negative, Int64 key, Float64 bin_count)
    {
        chassert(key >= mapping.validKeyRange().first && key <= mapping.validKeyRange().second);
        (negative ? negative_store : store).add(static_cast<int>(key), bin_count);
        count += bin_count;
    }

    void addZeroCount(Float64 weight)
    {
        zero_count += weight;
        count += weight;
    }

    /// Calls `f(key, bin_count)` for the non-empty bins of the positive or the negative store, in increasing key order
    template <typename F>
    void forEachBin(bool negative, F && f) const
    {
        (negative ? negative_store : store).forEachBin(std::forward<F>(f));
    }

    void copy(const DDSketchDenseLogarithmic & other)
    {
        /// Not rebuilt from the relative accuracy: the gamma could move by an ulp and stop matching the buckets of a scale
        mapping = other.mapping;
        store.copy(other.store);
        negative_store.copy(other.negative_store);
        zero_count = other.zero_count;
        count = other.count;
    }

    void merge(const DDSketchDenseLogarithmic & other)
    {
        /// No bins, no resolution to keep: e.g. a data point with only zeros at scale -5 must not lower the resolution
        if (!other.hasBins())
        {
            zero_count += other.zero_count;
            count += other.count;
            return;
        }

        /// Without bins, this is a new state (merges go into new states) with the relative accuracy of the function. That
        /// caps the resolution: a coarser one gets `other` remapped below, a finer one copies `other`
        const bool is_coarser = mapping != other.mapping && mapping.getGamma() > other.mapping.getGamma();
        if (!hasBins() && !is_coarser)
        {
            const Float64 this_zero_count = zero_count;
            copy(other);
            zero_count += this_zero_count;
            count += this_zero_count;
            return;
        }

        /// Remap the finer sketch and merge again: `changeMapping` can return an even coarser mapping
        if (mapping != other.mapping)
        {
            if (is_coarser)
            {
                this->merge(other.changeMapping(mapping));
            }
            else
            {
                *this = changeMapping(other.mapping);
                this->merge(other);
            }
            return;
        }

        /// Too far apart: downscale, as OpenTelemetry does. An exception would fail every background merge of these states
        const int shift = std::max(
            store.shiftToInclude(other.store.min_key, other.store.max_key),
            negative_store.shiftToInclude(other.negative_store.min_key, other.negative_store.max_key));
        if (shift == 0)
        {
            store.merge(other.store);
            negative_store.merge(other.negative_store);
        }
        else
        {
            downscale(shift);
            const auto [min_valid_key, max_valid_key] = mapping.validKeyRange();
            store.mergeDownscaled(other.store, shift, min_valid_key, max_valid_key);
            negative_store.mergeDownscaled(other.negative_store, shift, min_valid_key, max_valid_key);
        }

        count += other.count;
        zero_count += other.zero_count;
    }

    /// NOLINTBEGIN(readability-static-accessed-through-instance)

    void serialize(WriteBuffer & buf) const
    {
        // Write the mapping
        writeBinary(enc.FlagIndexMappingBaseLogarithmic.byte, buf);
        mapping.serialize(buf);

        // Write the positive and negative stores
        writeBinary(enc.FlagTypePositiveStore, buf);
        store.serialize(buf);

        writeBinary(enc.FlagTypeNegativeStore, buf);
        negative_store.serialize(buf);

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
        mapping.deserialize(buf);

        /// reject bins with impossible keys now, otherwise they only blow up later during a merge
        const auto [min_valid_key, max_valid_key] = mapping.validKeyRange();

        /// Versions 26.2 and earlier can write bins too far apart: `addKey` downscales
        store.clear();
        negative_store.clear();
        int shift = 0;
        const auto key_of = [&](Int64 data_key) { return static_cast<int>(mapping.downscaledKey(data_key, shift)); };
        const auto read_store = [&](DDSketchDenseStore & target)
        {
            DDSketchDenseStore::deserialize(
                buf,
                min_valid_key,
                max_valid_key,
                [&](Int64 first, Int64 last) { target.reserveKeys(key_of(first), key_of(last)); },
                [&](Int64 data_key, Float64 bin_count) { shift += addKey(target, key_of(data_key), bin_count); });
        };

        // Read the positive and negative stores
        readBinary(flag, buf);
        if (flag != enc.FlagTypePositiveStore)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid flag for positive store");
        }
        read_store(store);

        readBinary(flag, buf);
        if (flag != enc.FlagTypeNegativeStore)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid flag for negative store");
        }
        read_store(negative_store);

        // Read the zero count
        readBinary(flag, buf);
        if (flag != enc.FlagZeroCountVarFloat.byte)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid flag for zero count");
        }
        readBinary(zero_count, buf);
        if (!std::isfinite(zero_count) || zero_count < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid zero_count in DDSketch: {}", zero_count);

        count = negative_store.count + zero_count + store.count;
        if (!std::isfinite(count))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid total count in DDSketch: {}", count);
    }

    /// NOLINTEND(readability-static-accessed-through-instance)

private:
    DDSketchLogarithmicMapping mapping;
    DDSketchDenseStore store;
    DDSketchDenseStore negative_store;
    Float64 zero_count;
    Float64 count;
    DDSketchEncoding enc;

    bool hasBins() const { return store.count != 0 || negative_store.count != 0; }

    void addValue(DDSketchDenseStore & target, Float64 value, Float64 weight)
    {
        /// A downscale lowers `getMaxPossible`, so a value such as 1.78e308 must not fail only because it is added after a downscale
        const int key = mapping.key(std::isfinite(value) ? std::min(value, mapping.getMaxPossible()) : value);
        /// `addKey` downscales as `merge` does, so the result does not depend on how the values are split between states. The exception
        /// is values near `DBL_MIN`: a downscale raises `getMinPossible`, so a value below the new minimum goes to `zero_count` if it
        /// is added after the downscale, but stays in the lowest bin if it is added before it or merged from another state
        addKey(target, key, weight);
    }

    /// Adds at `key` of the current mapping, downscaling first if it does not fit into `target`. Returns the shift
    int addKey(DDSketchDenseStore & target, int key, Float64 weight)
    {
        if (key < target.min_key || key > target.max_key) [[unlikely]]
        {
            if (const int shift = target.shiftToInclude(key, key))
            {
                downscale(shift);
                target.add(static_cast<int>(mapping.downscaledKey(key, shift)), weight);
                return shift;
            }
        }
        target.add(key, weight);
        return 0;
    }

    /// Makes the bins 2^shift times wider: see `DDSketchLogarithmicMapping::downscaled`
    void downscale(int shift)
    {
        mapping = mapping.downscaled(shift);
        const auto [min_valid_key, max_valid_key] = mapping.validKeyRange();
        store.downscale(shift, min_valid_key, max_valid_key);
        negative_store.downscale(shift, min_valid_key, max_valid_key);
    }

    /// Only remaps onto a coarser mapping (`merge` remaps the finer sketch), so an old bin overlaps at most two new bins
    /// The result can be coarser than `target_mapping`: remapping adds a bin at each end, which may not fit
    DDSketchDenseLogarithmic changeMapping(const DDSketchLogarithmicMapping & target_mapping) const
    {
        /// The bins nest, e.g. scale 6 onto scale 5: downscale without splitting the counts
        if (const auto shift = mapping.shiftTo(target_mapping))
        {
            DDSketchDenseLogarithmic result;
            result.copy(*this);
            result.downscale(*shift);
            return result;
        }

        DDSketchLogarithmicMapping new_mapping = target_mapping;
        while (const int shift = std::max(shiftToRemap(store, new_mapping), shiftToRemap(negative_store, new_mapping)))
            new_mapping = new_mapping.downscaled(shift);

        DDSketchDenseStore new_positive_store;
        DDSketchDenseStore new_negative_store;

        /// Pieces below this share of an old bin are rounding slivers at coinciding bin boundaries: `lowerBound` is off by
        /// up to ~2e-13 relative (`exp` of an argument up to ~709), and an old bin is `gamma - 1` wide relative to its lower
        /// bound. Skip them, otherwise `keyAtRank` can return the neighbouring bin. The cap is not reached for the accepted
        /// relative accuracies (above 1e-6)
        const Float64 min_remap_proportion = std::min(1e-9 / (mapping.getGamma() - 1), 0.25);

        /// The highest old bins can end above the highest new bin, and a key above it would make the state unreadable.
        /// Their pieces above it go to the last kept piece, as the values below the lowest new bin go one bin up in `key`
        const int max_new_key = static_cast<int>(new_mapping.validKeyRange().second);

        auto remap_store = [this, &new_mapping, min_remap_proportion, max_new_key](
                               const DDSketchDenseStore & old_store, DDSketchDenseStore & target_store)
        {
            for (int i = 0; i < old_store.length(); ++i)
            {
                int old_index = i + old_store.offset;
                Float64 old_bin_count = old_store.bins[i];

                if (old_bin_count == 0)
                    continue;

                Float64 in_lower_bound = this->mapping.lowerBound(old_index);
                Float64 in_upper_bound = this->mapping.lowerBound(old_index + 1);

                /// The lowest and highest old bins can start outside the range of the coarser mapping
                int new_index = new_mapping.key(std::clamp(in_lower_bound, new_mapping.getMinPossible(), new_mapping.getMaxPossible()));

                /// `exp` overflows for the upper bound of the highest old bin with some gammas, and bins of a gamma very close to 1
                /// can have no width in floating point. There is nothing to split by, so the whole bin goes where `key` puts it
                if (!std::isfinite(in_upper_bound) || !(in_upper_bound > in_lower_bound))
                {
                    target_store.add(new_index, old_bin_count);
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
                for (; new_index <= max_new_key && new_mapping.lowerBound(new_index) < in_upper_bound; ++new_index)
                {
                    if (++iterations > max_remap_iterations)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Too many iterations in DDSketch changeMapping");

                    Float64 out_lower_bound = new_mapping.lowerBound(new_index);
                    Float64 out_upper_bound = new_mapping.lowerBound(new_index + 1);
                    Float64 lower_intersection_bound = std::max(out_lower_bound, in_lower_bound);
                    Float64 higher_intersection_bound = std::min(out_upper_bound, in_upper_bound);
                    Float64 intersection_size = higher_intersection_bound - lower_intersection_bound;
                    Float64 proportion = intersection_size / in_size;
                    if (proportion < min_remap_proportion)
                        continue;
                    if (has_last)
                    {
                        target_store.add(last_index, last_count);
                        remaining_count -= last_count;
                    }
                    has_last = true;
                    last_index = new_index;
                    last_count = proportion * old_bin_count;
                }
                target_store.add(last_index, remaining_count);
            }
        };

        remap_store(store, new_positive_store);
        remap_store(negative_store, new_negative_store);

        return DDSketchDenseLogarithmic(new_mapping, std::move(new_positive_store), std::move(new_negative_store), zero_count);
    }

    /// The `shift` after which the bins of `old_store` remapped by `changeMapping` onto `new_mapping` fit into a store
    int shiftToRemap(const DDSketchDenseStore & old_store, const DDSketchLogarithmicMapping & new_mapping) const
    {
        if (old_store.min_key > old_store.max_key)
            return 0;
        const auto new_key = [&](Float64 value)
        { return new_mapping.key(std::clamp(value, new_mapping.getMinPossible(), new_mapping.getMaxPossible())); };
        /// The new keys of the outer bounds of the old bins, plus a bin at each end for rounding
        return DDSketchDenseStore::shiftToFit(
            static_cast<Int64>(new_key(mapping.lowerBound(old_store.min_key))) - 1,
            static_cast<Int64>(new_key(mapping.lowerBound(old_store.max_key + 1))) + 1);
    }
};

}
