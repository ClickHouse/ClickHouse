#pragma once

#include <base/types.h>
#include <base/sort.h>
#include <AggregateFunctions/DDSketch.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>


namespace DB
{

/**
 * A DDSketch is a fully-mergeable quantile sketch with relative-error guarantees. That is, for any value x,
 * the value returned by the sketch is guaranteed to be in the (1 +- epsilon) * x range. The sketch is
 * parameterized by a relative accuracy epsilon, which is the maximum relative error of any quantile estimate.
 *
 * The sketch is implemented as a set of logarithmically-spaced bins. Each bin is a pair of a value and a count.
 *
 * The sketch is fully mergeable, meaning that the merge of two sketches is equivalent to the sketch of the
 * union of the input datasets. The memory size of the sketch depends on the range that is covered by
 * the input values: the larger that range, the more bins are needed to keep track of the input values.
 * As a rough estimate, if working on durations using DDSketches.unboundedDense(0.02) (relative accuracy of 2%),
 * about 2kB (275 bins) are needed to cover values between 1 millisecond and 1 minute, and about 6kB (802 bins)
 * to cover values between 1 nanosecond and 1 day.
 *
 * This implementation maintains the binary compatibility with the DDSketch ProtoBuf format
 * https://github.com/DataDog/sketches-java/blob/master/src/protobuf/proto/DDSketch.proto.
 * Which enables sending the pre-aggregated sketches to the ClickHouse server and calculating the quantiles
 * during the query time. See DDSketchEncoding.h for byte-level details.
 *
*/

template <typename Value>
class QuantileDD
{
public:
    using Weight = UInt64;

    QuantileDD() = default;

    explicit QuantileDD(Float64 relative_accuracy) : data(relative_accuracy) { }

    void add(const Value & x)
    {
        add(x, 1);
    }

    void add(const Value & x, Weight w)
    {
        if (!isNaN(x))
            data.add(x, w);
    }

    void merge(const QuantileDD &other)
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
