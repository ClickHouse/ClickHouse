#pragma once

#include <base/types.h>
#include <vector>
#include <cmath>
#include <limits>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <AggregateFunctions/DDSketch/DDSketchEncoding.h>


// We start with 128 bins and grow the number of bins by 128
// each time we need to extend the range of the bins.
// This is done to avoid reallocating the bins vector too often.
constexpr UInt32 CHUNK_SIZE = 128;

namespace DB
{

class DDSketchDenseStore
{
public:
    Float64 count = 0;
    int min_key = std::numeric_limits<int>::max();
    int max_key = std::numeric_limits<int>::min();
    int offset = 0;
    std::vector<Float64> bins;

    explicit DDSketchDenseStore(UInt32 chunk_size_ = CHUNK_SIZE) : chunk_size(chunk_size_) {}

    void copy(DDSketchDenseStore* other)
    {
        bins = other->bins;
        count = other->count;
        min_key = other->min_key;
        max_key = other->max_key;
        offset = other->offset;
    }

    int length() const
    {
        return static_cast<int>(bins.size());
    }

    void add(int key, Float64 weight)
    {
        int idx = getIndex(key);
        bins[idx] += weight;
        count += weight;
    }

    int keyAtRank(Float64 rank, bool lower)
    {
        Float64 running_ct = 0.0;
        for (size_t i = 0; i < bins.size(); ++i)
        {
            running_ct += bins[i];
            if ((lower && running_ct > rank) || (!lower && running_ct >= rank + 1))
            {
                return static_cast<int>(i) + offset;
            }
        }
        return max_key;
    }

    void merge(DDSketchDenseStore* other)
    {
        if (other->count == 0) return;

        if (count == 0)
        {
            copy(other);
            return;
        }

        if (other->min_key < min_key || other->max_key > max_key)
        {
            extendRange(other->min_key, other->max_key);
        }

        for (int key = other->min_key; key <= other->max_key; ++key)
        {
            bins[key - offset] += other->bins[key - other->offset];
        }

        count += other->count;
    }

    /// NOLINTBEGIN(readability-static-accessed-through-instance)

    void serialize(WriteBuffer& buf) const
    {

        // Calculate the size of the dense and sparse encodings to choose the smallest one
        UInt64 num_bins = 0, num_non_empty_bins = 0;
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

    void deserialize(ReadBuffer& buf)
    {
        UInt8 encoding_mode;
        readBinary(encoding_mode, buf);
        if (encoding_mode == enc.BinEncodingContiguousCounts)
        {
            UInt64 num_bins;
            readVarUInt(num_bins, buf);
            int start_key;
            readVarInt(start_key, buf);
            int index_delta;
            readVarInt(index_delta, buf);

            for (UInt64 i = 0; i < num_bins; ++i)
            {
                Float64 bin_count;
                readFloatBinary(bin_count, buf);
                add(start_key, bin_count);
                start_key += index_delta;
            }
        }
        else
        {
            UInt64 num_non_empty_bins;
            readVarUInt(num_non_empty_bins, buf);
            int previous_index = 0;
            for (UInt64 i = 0; i < num_non_empty_bins; ++i)
            {
                int index_delta;
                readVarInt(index_delta, buf);
                Float64 bin_count;
                readFloatBinary(bin_count, buf);
                previous_index += index_delta;
                add(previous_index, bin_count);
            }
        }
    }

    /// NOLINTEND(readability-static-accessed-through-instance)

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
        int desired_length = new_max_key - new_min_key + 1;
        return static_cast<UInt32>(chunk_size * std::ceil(static_cast<Float64>(desired_length) / chunk_size)); // Fixed float conversion
    }

    void extendRange(int key, int second_key)
    {
        int new_min_key = std::min({key, min_key});
        int new_max_key = std::max({second_key, max_key});

        if (length() == 0)
        {
            bins = std::vector<Float64>(getNewLength(new_min_key, new_max_key), 0.0);
            offset = new_min_key;
            adjust(new_min_key, new_max_key);
        }
        else if (new_min_key >= offset && new_max_key < offset + length())
        {
            min_key = new_min_key;
            max_key = new_max_key;
        }
        else
        {
            UInt32 new_length = getNewLength(new_min_key, new_max_key);
            if (new_length > bins.size())
            {
                bins.resize(new_length);
                bins.resize(bins.capacity());
            }
            adjust(new_min_key, new_max_key);
        }
    }

    void adjust(int new_min_key, int new_max_key)
    {
        centerBins(new_min_key, new_max_key);
        min_key = new_min_key;
        max_key = new_max_key;
    }

    void shiftBins(int shift)
    {
        int new_offset = offset - shift;
        if (new_offset > offset)
            std::rotate(bins.begin(), bins.begin() + (new_offset - offset) % bins.size(), bins.end());
        else
            std::rotate(bins.begin(), bins.end() - (offset - new_offset) % bins.size(), bins.end());
        offset = new_offset;
    }

    void centerBins(int new_min_key, int new_max_key)
    {
        int margins = length() - (new_max_key - new_min_key + 1);
        int new_offset = new_min_key - margins / 2;
        shiftBins(offset - new_offset);
    }

    size_t estimatedFloatSize(Float64 value) const
    {
        // Assuming IEEE 754 double-precision binary floating-point format: binary64
        return sizeof(value);
    }
};

}
