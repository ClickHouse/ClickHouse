#pragma once

#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/ReservoirSamplerDeterministic.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/** */
template <typename Value>
struct QuantileReservoirSamplerDeterministic
{
    using Data = ReservoirSamplerDeterministic<Value, ReservoirSamplerDeterministicOnEmpty::RETURN_NAN_OR_ZERO>;
    Data data;

    void add(const Value &)
    {
        throw Exception("Method add without determinator is not implemented for ReservoirSamplerDeterministic", ErrorCodes::NOT_IMPLEMENTED);
    }

    template <typename Determinator>
    void add(const Value & x, const Determinator & determinator)
    {
        data.insert(x, determinator);
    }

    void merge(const QuantileReservoirSamplerDeterministic & rhs)
    {
        data.merge(rhs.data);
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
    Value get(Float64 level)
    {
        return data.quantileInterpolated(level);
    }

    /// Get the `size` values of `levels` quantiles. Write `size` results starting with `result` address.
    /// indices - an array of index levels such that the corresponding elements will go in ascending order.
    void getMany(const Float64 * levels, const size_t * indices, size_t size, Value * result)
    {
        for (size_t i = 0; i < size; ++i)
            result[indices[i]] = data.quantileInterpolated(levels[indices[i]]);
    }

    /// The same, but in the case of an empty state, NaN is returned.
    float getFloat(Float64 level)
    {
        return data.quantileInterpolated(level);
    }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t size, float * result)
    {
        for (size_t i = 0; i < size; ++i)
            result[indices[i]] = data.quantileInterpolated(levels[indices[i]]);
    }
};

}
