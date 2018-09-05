#pragma once

#include <Common/PODArray.h>
#include <Common/NaNUtils.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <Core/Types.h>
#include <IO/VarInt.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/** Calculates quantile by collecting all values into array
  *  and applying n-th element (introselect) algorithm for the resulting array.
  *
  * It use O(N) memory and it is very inefficient in case of high amount of identical values.
  * But it is very CPU efficient for not large datasets.
  */
template <typename Value>
struct QuantileExact
{
    /// The memory will be allocated to several elements at once, so that the state occupies 64 bytes.
    static constexpr size_t bytes_in_arena = 64 - sizeof(PODArray<Value>);

    using Array = PODArray<Value, bytes_in_arena, AllocatorWithStackMemory<Allocator<false>, bytes_in_arena>>;
    Array array;

    void add(const Value & x)
    {
        /// We must skip NaNs as they are not compatible with comparison sorting.
        if (!isNaN(x))
            array.push_back(x);
    }

    template <typename Weight>
    void add(const Value &, const Weight &)
    {
        throw Exception("Method add with weight is not implemented for QuantileExact", ErrorCodes::NOT_IMPLEMENTED);
    }

    void merge(const QuantileExact & rhs)
    {
        array.insert(rhs.array.begin(), rhs.array.end());
    }

    void serialize(WriteBuffer & buf) const
    {
        size_t size = array.size();
        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const char *>(&array[0]), size * sizeof(array[0]));
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t size = 0;
        readVarUInt(size, buf);
        array.resize(size);
        buf.read(reinterpret_cast<char *>(&array[0]), size * sizeof(array[0]));
    }

    /// Get the value of the `level` quantile. The level must be between 0 and 1.
    Value get(Float64 level)
    {
        if (!array.empty())
        {
            size_t n = level < 1
                ? level * array.size()
                : (array.size() - 1);

            std::nth_element(array.begin(), array.begin() + n, array.end());    /// NOTE You can think of the radix-select algorithm.
            return array[n];
        }

        return std::numeric_limits<Value>::quiet_NaN();
    }

    /// Get the `size` values of `levels` quantiles. Write `size` results starting with `result` address.
    /// indices - an array of index levels such that the corresponding elements will go in ascending order.
    void getMany(const Float64 * levels, const size_t * indices, size_t size, Value * result)
    {
        if (!array.empty())
        {
            size_t prev_n = 0;
            for (size_t i = 0; i < size; ++i)
            {
                auto level = levels[indices[i]];

                size_t n = level < 1
                    ? level * array.size()
                    : (array.size() - 1);

                std::nth_element(array.begin() + prev_n, array.begin() + n, array.end());

                result[indices[i]] = array[n];
                prev_n = n;
            }
        }
        else
        {
            for (size_t i = 0; i < size; ++i)
                result[i] = Value();
        }
    }

    /// The same, but in the case of an empty state, NaN is returned.
    Float64 getFloat(Float64) const
    {
        throw Exception("Method getFloat is not implemented for QuantileExact", ErrorCodes::NOT_IMPLEMENTED);
    }

    void getManyFloat(const Float64 *, const size_t *, size_t, Float64 *) const
    {
        throw Exception("Method getManyFloat is not implemented for QuantileExact", ErrorCodes::NOT_IMPLEMENTED);
    }
};

}
