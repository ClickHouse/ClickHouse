#pragma once

#include <algorithm>
#include <cmath>
#include <limits>
#include <vector>
#include <base/types.h>
#include <base/arithmeticOverflow.h>

#include <AggregateFunctions/DDSketch/DDSketchEncoding.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/VectorWithMemoryTracking.h>


/// We grow the number of bins by a multiple of 128
/// each time we need to extend the range of the bins.
/// This is done to avoid reallocating the bins vector too often.
constexpr UInt32 CHUNK_SIZE = 128;
/// The first allocation is a multiple of 16 bins
constexpr UInt32 INITIAL_CHUNK_SIZE = 16;

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
}

class DDSketchDenseStore
{
public:
    static constexpr Int64 max_key_range = 65536;

    Float64 count = 0;
    int min_key = std::numeric_limits<int>::max();
    int max_key = std::numeric_limits<int>::min();
    int offset = 0;
    VectorWithMemoryTracking<Float64> bins;

    explicit DDSketchDenseStore(UInt32 chunk_size_ = CHUNK_SIZE)
        : chunk_size(chunk_size_)
    {
    }

    void copy(const DDSketchDenseStore & other)
    {
        bins = other.bins;
        count = other.count;
        min_key = other.min_key;
        max_key = other.max_key;
        offset = other.offset;
    }

    void clear()
    {
        count = 0;
        min_key = std::numeric_limits<int>::max();
        max_key = std::numeric_limits<int>::min();
        offset = 0;
        bins.clear();
    }

    int length() const { return static_cast<int>(bins.size()); }

    void add(int key, Float64 weight)
    {
        int idx = getIndex(key);
        bins[idx] += weight;
        count += weight;
    }

    /// `keyAtRank` for non-decreasing ranks, in one pass over the bins
    class KeyAtRankCursor
    {
    public:
        explicit KeyAtRankCursor(const DDSketchDenseStore & store_)
            : store(store_)
            , key(store.min_key)
            , running_ct(store.min_key <= store.max_key ? store.bins[store.min_key - store.offset] : 0.0)
        {
        }

        int next(Float64 rank, bool lower)
        {
            while (key <= store.max_key)
            {
                if ((lower && running_ct > rank) || (!lower && running_ct >= rank + 1))
                    return key;
                ++key;
                if (key <= store.max_key)
                    running_ct += store.bins[key - store.offset];
            }
            return store.max_key;
        }

    private:
        const DDSketchDenseStore & store;
        int key;
        /// The sum of the bins from `min_key` to `key`
        Float64 running_ct;
    };

    int keyAtRank(Float64 rank, bool lower) const { return KeyAtRankCursor(*this).next(rank, lower); }

    void merge(const DDSketchDenseStore & other)
    {
        if (other.count == 0)
            return;

        if (count == 0)
        {
            copy(other);
            return;
        }

        if (other.min_key < min_key || other.max_key > max_key)
        {
            extendRange(other.min_key, other.max_key);
        }

        for (int key = other.min_key; key <= other.max_key; ++key)
        {
            bins[key - offset] += other.bins[key - other.offset];
        }

        count += other.count;
    }

    /// The smallest `shift` for `downscale` after which the keys from `key_min` to `key_max` fit into `max_key_range` bins
    static int shiftToFit(Int64 key_min, Int64 key_max)
    {
        int shift = 0;
        /// k >> s = ⌊k / 2^s⌋
        while ((key_max >> shift) - (key_min >> shift) + 1 > max_key_range)
            ++shift;
        return shift;
    }

    /// The smallest `shift` after which this store also fits the keys from `key_min` to `key_max`. An empty store adds
    /// nothing: its `min_key > max_key`
    int shiftToInclude(Int64 key_min, Int64 key_max) const
    {
        return shiftToFit(std::min<Int64>(min_key, key_min), std::max<Int64>(max_key, key_max));
    }

    /// Calls `f(key, bin_count)` for the non-empty bins, in increasing key order
    template <typename F>
    void forEachBin(F && f) const
    {
        for (int key = min_key; key <= max_key; ++key)
        {
            const Float64 bin_count = bins[key - offset];
            if (bin_count != 0)
                f(key, bin_count);
        }
    }

    /// Merges `other` into 2^shift times wider bins: bin `k` goes to bin `k >> shift`, clamped to the valid keys, which
    /// cover a bit less at the edges
    void mergeDownscaled(const DDSketchDenseStore & other, int shift, Int64 min_valid_key, Int64 max_valid_key)
    {
        if (other.min_key > other.max_key)
            return;

        const auto new_key = [&](int key) { return static_cast<int>(std::clamp<Int64>(key >> shift, min_valid_key, max_valid_key)); };
        if (new_key(other.min_key) < min_key || new_key(other.max_key) > max_key)
            extendRange(new_key(other.min_key), new_key(other.max_key));

        other.forEachBin([&](int key, Float64 bin_count) { bins[new_key(key) - offset] += bin_count; });
        count += other.count;
    }

    void downscale(int shift, Int64 min_valid_key, Int64 max_valid_key)
    {
        DDSketchDenseStore result(chunk_size);
        result.mergeDownscaled(*this, shift, min_valid_key, max_valid_key);
        *this = std::move(result);
    }

    /// NOLINTBEGIN(readability-static-accessed-through-instance)

    void serialize(WriteBuffer & buf) const
    {
        // Calculate the size of the dense and sparse encodings to choose the smallest one
        UInt64 num_bins = 0;
        UInt64 num_non_empty_bins = 0;
        if (count != 0)
        {
            num_bins = max_key - min_key + 1;
        }

        size_t sparse_encoding_overhead = 0;
        for (int index = min_key; index <= max_key; ++index)
        {
            if (bins[index - offset] != 0)
            {
                num_non_empty_bins++;
                sparse_encoding_overhead += 2; // 2 bytes for index delta
            }
        }

        size_t dense_encoding_overhead = (num_bins - num_non_empty_bins) * estimatedFloatSize(0.0);

        // Choose the smallest encoding and write to buffer
        if (dense_encoding_overhead <= sparse_encoding_overhead)
        {
            // Write the dense encoding
            writeBinary(enc.BinEncodingContiguousCounts, buf); // Flag for dense encoding
            writeVarUInt(num_bins, buf);
            writeVarInt(min_key, buf);
            writeVarInt(1, buf); // indexDelta in dense encoding
            for (int index = min_key; index <= max_key; ++index)
            {
                writeFloatBinary(bins[index - offset], buf);
            }
        }
        else
        {
            // Write the sparse encoding
            writeBinary(enc.BinEncodingIndexDeltasAndCounts, buf); // Flag for sparse encoding
            writeVarUInt(num_non_empty_bins, buf);
            int previous_index = 0;
            for (int index = min_key; index <= max_key; ++index)
            {
                Float64 bin_count = bins[index - offset];
                if (bin_count != 0)
                {
                    writeVarInt(index - previous_index, buf);
                    writeFloatBinary(bin_count, buf);
                    previous_index = index;
                }
            }
        }
    }

    /// NOLINTEND(readability-static-accessed-through-instance)

    /// Reads bins written by `serialize` or another DDSketch implementation: `add_bin(key, bin_count)` for each nonempty
    /// bin, after `reserve(first, last)` for a dense encoding that fits into a store. The keys may not fit into a store,
    /// e.g. from versions 26.2 and earlier
    template <typename Reserve, typename AddBin>
    static void deserialize(ReadBuffer & buf, Int64 min_valid_key, Int64 max_valid_key, Reserve && reserve, AddBin && add_bin)
    {
        /// Keys/deltas are varint64; keep them in Int64 and only narrow to int once check_key has
        /// accepted the real value. A narrowing read would let a huge key wrap into the valid range
        const auto check_key = [&](Int64 key)
        {
            if (key < min_valid_key || key > max_valid_key)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "DDSketch bin key {} is out of the valid range [{}, {}] for this mapping",
                    key, min_valid_key, max_valid_key);
        };
        const auto advance = [](Int64 & key, Int64 delta)
        {
            if (common::addOverflow(key, delta, key))
                throw Exception(ErrorCodes::INCORRECT_DATA, "DDSketch bin key overflow while decoding");
        };

        UInt8 encoding_mode = 0;
        readBinary(encoding_mode, buf);
        if (encoding_mode == DDSketchEncoding::BinEncodingContiguousCounts)
        {
            UInt64 num_bins = 0;
            readVarUInt(num_bins, buf);

            Int64 key = 0;
            readVarInt(key, buf);
            Int64 index_delta = 0;
            readVarInt(index_delta, buf);

            /// Only if the bins fit and their keys are valid: `num_bins` can be corrupted, and an invalid key must reach
            /// `check_key`
            if (index_delta == 1 && num_bins != 0 && num_bins <= static_cast<UInt64>(max_key_range) && key >= min_valid_key
                && key <= max_valid_key && key + static_cast<Int64>(num_bins) - 1 <= max_valid_key)
                reserve(key, key + static_cast<Int64>(num_bins) - 1);

            for (UInt64 i = 0; i < num_bins; ++i)
            {
                Float64 bin_count = 0;
                readFloatBinary(bin_count, buf);
                if (!std::isfinite(bin_count) || bin_count < 0)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid bin count in DDSketch dense store: {}", bin_count);
                if (bin_count > 0)
                {
                    check_key(key);
                    add_bin(key, bin_count);
                }
                if (i + 1 < num_bins)
                    advance(key, index_delta);
            }
        }
        else if (encoding_mode == DDSketchEncoding::BinEncodingIndexDeltasAndCounts)
        {
            UInt64 num_non_empty_bins = 0;
            readVarUInt(num_non_empty_bins, buf);

            Int64 key = 0;
            for (UInt64 i = 0; i < num_non_empty_bins; ++i)
            {
                Int64 index_delta = 0;
                readVarInt(index_delta, buf);
                Float64 bin_count = 0;
                readFloatBinary(bin_count, buf);
                if (!std::isfinite(bin_count) || bin_count < 0)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid bin count in DDSketch sparse store: {}", bin_count);
                advance(key, index_delta);
                if (bin_count > 0)
                {
                    check_key(key);
                    add_bin(key, bin_count);
                }
            }
        }
        else
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid flag for encoding mode");
        }
    }

    /// Allocates the bins for the keys from `first` to `last` in an empty store, so that adding them does not move the bins
    void reserveKeys(int first, int last)
    {
        const Int64 range = static_cast<Int64>(last) - static_cast<Int64>(first) + 1;
        if (min_key <= max_key || !bins.empty() || range > max_key_range) [[unlikely]]
            throw Exception(ErrorCodes::LOGICAL_ERROR, "DDSketch store reserves {} bins, but it is not empty or they are too many", range);
        const UInt32 new_length = getNewLength(first, last);
        bins.assign(new_length, 0.0);
        offset = first - static_cast<int>((static_cast<Int64>(new_length) - range) / 2);
    }

private:
    UInt32 chunk_size;
    DDSketchEncoding enc;

    int getIndex(int key)
    {
        if (key < min_key || key > max_key)
        {
            extendRange(key, key);
        }
        return key - offset;
    }

    UInt32 getNewLength(int new_min_key, int new_max_key) const
    {
        Int64 desired_length = static_cast<Int64>(new_max_key) - static_cast<Int64>(new_min_key) + 1;
        const UInt32 chunk = bins.empty() ? std::min(chunk_size, INITIAL_CHUNK_SIZE) : chunk_size;
        return static_cast<UInt32>(chunk * std::ceil(static_cast<Float64>(desired_length) / chunk));
    }

    void extendRange(int key, int second_key)
    {
        const int new_min_key = std::min(key, min_key);
        const int new_max_key = std::max(second_key, max_key);

        /// The callers downscale first (`shiftToFit`)
        const Int64 range = static_cast<Int64>(new_max_key) - static_cast<Int64>(new_min_key) + 1;
        if (range > max_key_range) [[unlikely]]
            throw Exception(ErrorCodes::LOGICAL_ERROR, "DDSketch store needs {} bins, but supports at most {}", range, max_key_range);

        if (length() != 0 && new_min_key >= offset && new_max_key < offset + length())
        {
            min_key = new_min_key;
            max_key = new_max_key;
            return;
        }

        size_t new_length = getNewLength(new_min_key, new_max_key);
        if (new_length > bins.size())
            new_length = std::max(new_length, std::min<size_t>(2 * bins.size(), max_key_range));
        else
            new_length = bins.size();

        const int new_offset = new_min_key - static_cast<int>((static_cast<Int64>(new_length) - range) / 2);
        if (new_length > bins.size())
        {
            VectorWithMemoryTracking<Float64> new_bins(new_length, 0.0);
            if (min_key <= max_key)
                std::copy(bins.begin() + (min_key - offset), bins.begin() + (max_key - offset + 1), new_bins.begin() + (min_key - new_offset));
            bins = std::move(new_bins);
        }
        else if (min_key <= max_key)
        {
            /// The keys fit at another offset: move the bins within the store and clear what they leave
            const auto first = bins.begin() + (min_key - offset);
            const auto last = bins.begin() + (max_key - offset + 1);
            const auto new_first = bins.begin() + (min_key - new_offset);
            const auto new_last = new_first + (last - first);
            if (new_first < first)
            {
                std::copy(first, last, new_first);
                std::fill(std::max(new_last, first), last, 0.0);
            }
            else
            {
                std::copy_backward(first, last, new_last);
                std::fill(first, std::min(new_first, last), 0.0);
            }
        }
        offset = new_offset;
        min_key = new_min_key;
        max_key = new_max_key;
    }

    size_t estimatedFloatSize(Float64 value) const
    {
        // Assuming IEEE 754 double-precision binary floating-point format: binary64
        return sizeof(value);
    }
};

}
