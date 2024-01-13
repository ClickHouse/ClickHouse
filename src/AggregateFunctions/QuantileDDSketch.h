#pragma once

#include <base/types.h>
#include <base/sort.h>
#include <AggregateFunctions/DDSketch.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>


namespace DB
{
template <typename Value>
class QuantileDDSketch
{
public:
    using Weight = UInt64;

    QuantileDDSketch() = default;

    explicit QuantileDDSketch(Float64 relative_accuracy) : data(relative_accuracy) { }

    void add(const Value & x)
    {
        add(x, 1);
    }

    void add(const Value & x, Weight w)
    {
        if (!isNaN(x))
            data.add(x, w);
    }

    void merge(const QuantileDDSketch &other)
    {
        data.merge(other.data);
    }

    void serialize(WriteBuffer & buf) const
    {
        data.serialize(buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        data.deserialize(buf);
    }

    Value get(Float64 level) const
    {
        return getImpl<Value>(level);
    }

    void getMany(const Float64 * levels, const size_t * indices, size_t size, Value * result) const
    {
        getManyImpl(levels, indices, size, result);
    }

    Float64 getFloat(Float64 level) const
    {
        return getImpl<Float64>(level);
    }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t size, Float64 * result) const
    {
        getManyImpl(levels, indices, size, result);
    }

private:
    DDSketchDenseLogarithmic data;

    template <typename T>
    T getImpl(Float64 level) const
    {
        return static_cast<T>(data.get(level));
    }

    template <typename T>
    void getManyImpl(const Float64 * levels, const size_t *, size_t num_levels, T * result) const
    {
        for (size_t i = 0; i < num_levels; ++i)
            result[i] = getImpl<T>(levels[i]);
    }
};

}
