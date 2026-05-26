#pragma once

#include <kll_sketch.hpp>
#include <base/types.h>
#include <Common/PODArray.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/VarInt.h>


namespace DB
{

template <typename Value>
class QuantileKLL
{
public:
    using Sketch = datasketches::kll_sketch<double>;

    QuantileKLL() = default;

    explicit QuantileKLL(ssize_t k) : sketch(static_cast<uint16_t>(k)) {}

    void add(const Value & x)
    {
        sketch.update(static_cast<double>(x));
    }

    void merge(const QuantileKLL & rhs)
    {
        sketch.merge(rhs.sketch);
    }

    void serialize(WriteBuffer & buf) const
    {
        auto bytes = sketch.serialize();
        writeVarUInt(bytes.size(), buf);
        buf.write(reinterpret_cast<const char *>(bytes.data()), bytes.size());
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t size;
        readVarUInt(size, buf);
        PODArray<char> bytes(size);
        buf.readStrict(bytes.data(), size);
        sketch = Sketch::deserialize(bytes.data(), size);
    }

    Value get(Float64 level) const
    {
        return static_cast<Value>(getFloat(level));
    }

    void getMany(const Float64 * levels, const size_t * indices, size_t num, Value * result) const
    {
        for (size_t i = 0; i < num; ++i)
            result[indices[i]] = get(levels[indices[i]]);
    }

    Float64 getFloat(Float64 level) const
    {
        if (sketch.is_empty())
            return std::numeric_limits<Float64>::quiet_NaN();
        return sketch.get_quantile(level);
    }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t num, Float64 * result) const
    {
        for (size_t i = 0; i < num; ++i)
            result[indices[i]] = getFloat(levels[indices[i]]);
    }

private:
    Sketch sketch{datasketches::kll_constants::DEFAULT_K};
};

}
