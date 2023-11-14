#pragma once

#include <base/types.h>
#include <vector>
#include <cmath>
#include <limits>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <AggregateFunctions/DDSketchEncoding.h>


// We start with 128 bins and grow the number of bins by 128
// each time we need to extend the range of the bins.
// This is done to avoid reallocating the bins vector too often.
constexpr int CHUNK_SIZE = 128;

namespace DB
{

class Store
{
public:
    virtual ~Store() = default;
    Float64 count = 0;
    int min_key = std::numeric_limits<int>::max();
    int max_key = std::numeric_limits<int>::min();
    int offset = 0;
    std::vector<Float64> bins;

    virtual void copy(Store* store) = 0;
    virtual int length() = 0;
    virtual void add(int key, Float64 weight) = 0;
    virtual int keyAtRank(Float64 rank, bool lower) = 0;
    virtual void merge(Store* store) = 0;
};

class DenseStore : public Store
{
public:
    explicit DenseStore(int chunk_size_ = CHUNK_SIZE) : chunk_size(chunk_size_) {}

    void copy(Store* other) override
    {
        bins = other->bins;
        count = other->count;
        min_key = other->min_key;
        max_key = other->max_key;
        offset = other->offset;
    }

    int length() override
    {
        return static_cast<int>(bins.size());
    }

    void add(int key, Float64 weight) override
    {
        int idx = getIndex(key);
        bins[idx] += weight;
        count += weight;
    }

    int keyAtRank(Float64 rank, bool lower) override
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

    void merge(Store* other) override
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

    void serialize(WriteBuffer& buf) const
    {

        // Calculate the size of the dense and sparse encodings to choose the smallest one
        size_t dense_encoding_size = 0, sparse_encoding_size = 0;
        UInt64 num_bins = 0, num_non_empty_bins = 0;
        if (count != 0)
        {
            num_bins = max_key - min_key + 1;
        }

        dense_encoding_size += estimatedVarIntSize(num_bins);
        dense_encoding_size += estimatedVarIntSize(min_key);
        dense_encoding_size += estimatedVarIntSize(1); // indexDelta in dense encoding

        int previous_index = min_key;
        for (int index = min_key; index <= max_key; ++index)
        {
            Float64 count = bins[index - offset];
            size_t count_varfloat64_size = estimatedFloatSize(count);
            dense_encoding_size += count_varfloat64_size;
            if (count != 0)
            {
                num_non_empty_bins++;
                sparse_encoding_size += estimatedFloatSize(index - previous_index);
                sparse_encoding_size += count_varfloat64_size;
                previous_index = index;
            }
        }
        sparse_encoding_size += estimatedVarIntSize(num_non_empty_bins);

        // Choose the smallest encoding and write to buffer
        if (dense_encoding_size <= sparse_encoding_size)
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
            previous_index = 0;
            for (int index = min_key; index <= max_key; ++index)
            {
                Float64 count = bins[index - offset];
                if (count != 0)
                {
                    writeVarInt(index - previous_index, buf);
                    writeFloatBinary(count, buf);
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
            int min_key;
            readVarInt(min_key, buf);
            int index_delta;
            readVarInt(index_delta, buf);

            for (UInt64 i = 0; i < num_bins; ++i)
            {
                Float64 count;
                readFloatBinary(count, buf);
                add(min_key, count);
                min_key += index_delta;
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
                Float64 count;
                readFloatBinary(count, buf);
                previous_index += index_delta;
                add(previous_index, count);
            }
        }
    }

private:
    int chunk_size;
    DDSketchEncoding enc;

    int getIndex(int key)
    {
        if (key < min_key || key > max_key)
        {
            extendRange(key, key);
        }
        return key - offset;
    }

    int getNewLength(int new_min_key, int new_max_key) const
    {
        int desired_length = new_max_key - new_min_key + 1;
        return static_cast<int>(chunk_size * std::ceil(static_cast<Float64>(desired_length) / chunk_size)); // Fixed float conversion
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
            int new_length = getNewLength(new_min_key, new_max_key);
            bins.resize(new_length, 0.0);
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
        if (shift > 0)
        {
            bins.insert(bins.begin(), shift, 0.0);
            bins.resize(bins.size() - shift);
        }
        else
        {
            bins.erase(bins.begin(), bins.begin() + std::abs(shift));
            bins.resize(bins.size() + std::abs(shift), 0.0);
        }
        offset -= shift;
    }

    void centerBins(int new_min_key, int new_max_key)
    {
        int middle_key = new_min_key + (new_max_key - new_min_key + 1) / 2;
        shiftBins(offset + length() / 2 - middle_key);
    }

    // This is a rough estimate for the size of a VarInt/VarUInt.
    // The actual size depends on the encoding and the value.
    size_t estimatedVarIntSize(Int64 value) const
    {
        // This is a very rough estimate.
        // Some encodings (like LEB128 or VarInt) use 1 byte for small values and more bytes for larger values.
        return 1 + (sizeof(value) * 8) / 7;
    }

    size_t estimatedFloatSize(Float64 value) const
    {
        // Assuming IEEE 754 double-precision binary floating-point format: binary64
        return sizeof(value);
    }
};

}
