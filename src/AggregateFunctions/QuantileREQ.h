#pragma once

#include <req_sketch.hpp>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/VarInt.h>

#include <stdexcept>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

template <typename Value>
class QuantileREQ
{
public:
    using Sketch = datasketches::req_sketch<double>;

    /// Upper bound on a single serialized REQ sketch state. The serialized size is dominated
    /// by the compactor levels (each containing up to `k` doubles). With `k <= 1024` and
    /// a generous allowance for header bytes and level metadata, 16 MiB is far above any
    /// legitimate sketch and prevents a malformed length prefix from forcing an arbitrary
    /// allocation during `deserialize`.
    static constexpr size_t MAX_SERIALIZED_SIZE = 16 * 1024 * 1024;

    QuantileREQ() = default;

    /// `k` must already have been validated to `[4, 1024]` by the factory; the cast to
    /// `uint16_t` is then well-defined. Accept `ssize_t` here only because the generic
    /// `AggregateFunctionQuantile<...>` passes its stored accuracy parameter through.
    explicit QuantileREQ(ssize_t k) : sketch(static_cast<uint16_t>(k))
    {
        chassert(k >= 4 && k <= 1024);
    }

    void add(const Value & x)
    {
        sketch.update(static_cast<double>(x));
    }

    void merge(const QuantileREQ & rhs)
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

        if (size > MAX_SERIALIZED_SIZE)
            throw Exception(
                ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                "Serialized REQ sketch size {} exceeds the maximum allowed {}",
                size,
                MAX_SERIALIZED_SIZE);

        PODArray<char> bytes(size);
        buf.readStrict(bytes.data(), size);

        /// `req_sketch::deserialize` reports malformed inputs via `std::invalid_argument`
        /// or `std::out_of_range`. Translate to `DB::Exception` so a corrupted state on
        /// the wire surfaces as a normal query error instead of an unhandled C++ exception.
        try
        {
            sketch = Sketch::deserialize(bytes.data(), size);
        }
        catch (const std::invalid_argument & e)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize REQ sketch state: {}", e.what());
        }
        catch (const std::out_of_range & e)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize REQ sketch state: {}", e.what());
        }
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
    Sketch sketch{12}; // k=12 ≈ 1% relative rank error at 95% confidence, hra=true
};

}
