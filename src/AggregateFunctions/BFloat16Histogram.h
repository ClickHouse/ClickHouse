#pragma once

#include <IO/ReadBuffer.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <Common/HashTable/HashMap.h>
#include <Common/PODArray.h>
#include <common/types.h>
#include <ext/bit_cast.h>

namespace DB
{

template <typename Value>
struct BFloat16Histogram
{
    using bfloat16 = UInt16;
    using Data = HashMap<bfloat16, size_t>;
    using Array = PODArrayWithStackMemory<Float32, 64>;
    Data data;
    Array array;

    void add(const Value & x, size_t to_add = 1)
    {
        if (isNaN(x))
            return;

        bfloat16 val = to_bfloat16(x);
        if (!data.find(val))
        {
            sorted = false;
            array.push_back(to_Float32(val));
        }
        count += to_add;
        data[val] += to_add;
    }

    void merge(const BFloat16Histogram & rhs)
    {
        for (const Float32 & value : rhs.array)
        {
            add(value, rhs.data.find(to_bfloat16(value))->getMapped());
        }
    }

    void write(WriteBuffer & buf) const
    {
        writeVarUInt(count, buf);
        data.write(buf);
        size_t size = array.size();
        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const char *>(array.data()), size * sizeof(array[0]));
    }

    void read(ReadBuffer & buf)
    {
        count = 0;
        readVarUInt(count, buf);
        data.read(buf);
        size_t size = 0;
        readVarUInt(size, buf);
        array.resize(size);
        buf.read(reinterpret_cast<char *>(array.data()), size * sizeof(array[0]));
    }

    template <typename ResultType>
    ResultType quantile(const Float64 & level)
    {
        if (array.empty())
        {
            return onEmpty<ResultType>();
        }
        sortIfNeeded();

        size_t sum = 0;
        size_t need = level * count;
        for (const Float32 & value : array)
        {
            sum += data[to_bfloat16(value)];
            if (sum >= need)
                return value;
        }

        return array[array.size() - 1];
    }

    // levels[indices[i]] must be sorted
    template <typename T>
    void quantilesManySorted(const Float64 * levels, const size_t * indices, size_t size, T * result)
    {
        if (array.empty())
        {
            for (size_t i = 0; i < size; ++i)
            {
                result[indices[i]] = onEmpty<T>();
            }
            return;
        }
        sortIfNeeded();

        size_t sum = 0;
        size_t it = 0;
        for (const Float32 & value : array)
        {
            sum += data[to_bfloat16(value)];
            while (it < size && sum >= static_cast<size_t>(levels[indices[it]] * count))
            {
                result[indices[it++]] = value;
            }
        }

        for (size_t i = it; i < size; ++i)
        {
            result[indices[i]] = array[array.size() - 1];
        }
    }

    template <typename T>
    void quantilesMany(const Float64 * levels, const size_t * indices, size_t size, T * result)
    {
        if (is_sorted_r(levels, indices, size))
        {
            quantilesManySorted(levels, indices, size, result);
        }
        else
        {
            for (size_t i = 0; i < size; ++i)
                result[indices[i]] = quantile<T>(levels[indices[i]]);
        }
    }

private:
    size_t count = 0;
    bool sorted = false;

    bfloat16 to_bfloat16(const Value & x) const { return ext::bit_cast<UInt32>(static_cast<Float32>(x)) >> 16; }

    Float32 to_Float32(const bfloat16 & x) const { return ext::bit_cast<Float32>(x << 16); }

    void sortIfNeeded()
    {
        if (sorted)
            return;
        sorted = true;
        std::sort(array.begin(), array.end());
    }

    bool is_sorted_r(const Float64 * levels, const size_t * indices, size_t size) const
    {
        for (size_t i = 0; i < size - 1; ++i)
        {
            if (levels[indices[i]] > levels[indices[i + 1]])
                return false;
        }

        return true;
    }

    template <typename ResultType>
    ResultType onEmpty() const
    {
        return std::numeric_limits<ResultType>::quiet_NaN();
    }
};

}
