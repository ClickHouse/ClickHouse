#pragma once

#include <AggregateFunctions/Bfloat16Histogram.h>

namespace DB 
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

template<typename Value>
struct QuantileBfloat16Histogram 
{
    using Hist = Bfloat16Histogram<Value>;
    Hist data;

    void add(const Value & x)
    {
        data.add(x);
    }

    template <typename Weight>
    void add(const Value &, const Weight &)
    {
        throw Exception("Method add with weight is not implemented for QuantileBfloat16Histogram", ErrorCodes::NOT_IMPLEMENTED);
    }

    void merge(const QuantileBfloat16Histogram & rhs)
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

    Value get(Float64 level)
    {
        return data.quantile(level);
    }

    void getMany(const Float64 * levels, const size_t * indices, size_t size, Value * result)
    {
        data.quantilesMany(levels, indices, size, result);
    }

    Float64 getFloat(Float64 level) {
        return data.quantile(level);
    }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t size, Float64 * result)
    {
        data.quantilesMany(levels, indices, size, result);
    }
};

}
