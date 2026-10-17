#pragma once

#include <kll_sketch.hpp>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/VarInt.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

template <typename Value>
class QuantileKLL
{
public:
    using Sketch = datasketches::kll_sketch<double>;

    /// Upper bound on a sane serialized KLL sketch size. The serialized form is bounded by
    /// the number of retained items (proportional to k * log2(n/k)) plus a small header.
    /// Even with the largest legal k = 65535 and an astronomical number of inputs,
    /// the serialized state stays well under 64 MiB. The bound below is deliberately
    /// generous to avoid false positives, while still catching obviously malformed lengths
    /// that would otherwise force `PODArray<char>` to attempt an unbounded allocation.
    static constexpr size_t MAX_SERIALIZED_SIZE = 64 * 1024 * 1024;

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
        if (size > MAX_SERIALIZED_SIZE)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Serialized KLL sketch size {} exceeds the maximum allowed {}",
                size, MAX_SERIALIZED_SIZE);
        PODArray<char> bytes(size);
        buf.readStrict(bytes.data(), size);
        try
        {
            sketch = Sketch::deserialize(bytes.data(), size);
        }
        catch (const std::bad_alloc &)
        {
            /// Preserve allocation failures verbatim — they are not data-corruption errors.
            throw;
        }
        catch (const std::exception & e)
        {
            /// DataSketches signals malformed input via `std::invalid_argument` /
            /// `std::runtime_error`. Convert these to a `DB::Exception` so callers
            /// see a structured error code instead of an unrelated exception type.
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Cannot deserialize KLL sketch: {}", e.what());
        }
    }

    Value get(Float64 level) const
    {
        if (sketch.is_empty())
            return Value{};
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
