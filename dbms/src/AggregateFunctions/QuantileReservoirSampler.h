#pragma once

#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/ReservoirSampler.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/** */
template <typename Value>
struct QuantileReservoirSampler
{
    using Data = ReservoirSampler<Value, ReservoirSamplerOnEmpty::RETURN_NAN_OR_ZERO>;
    Data data;

    void add(const Value & x)
    {
        data.insert(x);
    }

    template <typename Weight>
    void add(const Value & x, const Weight & weight)
    {
        throw Exception("Method add with weight is not implemented for ReservoirSampler", ErrorCodes::NOT_IMPLEMENTED);
    }

    void merge(const QuantileReservoirSampler & rhs)
    {
        data.merge(rhs);
    }

    void serialize(WriteBuffer & buf) const
    {
        data.write(buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        data.read(buf);
    }

    /// Get the value of the `level` quantile. The level must be between 0 and 1.
    Value get(Float64 level) const
    {
        return data.quantileInterpolated(level);
    }

    /// Get the `size` values of `levels` quantiles. Write `size` results starting with `result` address.
    /// indices - an array of index levels such that the corresponding elements will go in ascending order.
    void getMany(const Float64 * levels, const size_t * indices, size_t size, Value * result) const
    {
        for (size_t i = 0; i < size; ++i)
            result[indices[i]] = data.quantileInterpolated(levels[indices[i]]);
    }

    /// The same, but in the case of an empty state, NaN is returned.
    float getFloat(Float64 level) const
    {
        return data.quantileInterpolated(level);
    }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t size, float * result) const
    {
        for (size_t i = 0; i < size; ++i)
            result[indices[i]] = data.quantileInterpolated(levels[indices[i]]);
    }
};

}
