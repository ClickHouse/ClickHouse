#pragma once

#include "config.h"

#if USE_DATASKETCHES

#include <base/types.h>
#include <req_sketch.hpp>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

/** Relative Error Quantiles (REQ) sketch — a fully-mergeable streaming quantile sketch with a
  * *relative* rank-error guarantee, i.e. the rank error is proportional to the distance from the
  * nearer tail rather than a flat fraction of the stream size. This makes it accurate at extreme
  * quantiles (e.g. p99.9), where `QuantileTDigest` has no formal bound and `QuantileGK` only
  * guarantees a flat rank error.
  *
  * Reference: "Relative Error Streaming Quantiles", Graham Cormode, Zohar Karnin, Edo Liberty,
  * Justin Thaler, Pavel Veselý (PODS 2021; JACM 2023, https://dl.acm.org/doi/10.1145/3617891).
  *
  * Wraps `datasketches::req_sketch` from the bundled DataSketches library. The sketch is built in
  * high-rank-accuracy (HRA) mode, which favours accuracy near rank 1.0 — the tail that matters for
  * latency SLOs.
  */
template <typename Value>
class QuantileReq
{
public:
    using Item = Float64;
    using Sketch = datasketches::req_sketch<Item>;

    /// `k` controls accuracy and memory: larger means more accurate and larger. Must be even and
    /// at least 4; DataSketches enforces this. Default mirrors the DataSketches default of 12.
    QuantileReq() : sketch(default_k, /* hra */ true) { }

    explicit QuantileReq(ssize_t k) : sketch(toK(k), /* hra */ true) { }

    void add(const Value & x)
    {
        if (!isNaN(x))
            sketch.update(static_cast<Item>(x));
    }

    void merge(const QuantileReq & other)
    {
        sketch.merge(other.sketch);
    }

    void serialize(WriteBuffer & buf) const
    {
        auto bytes = sketch.serialize();
        writeVectorBinary(bytes, buf);
    }

    /// You can only call this for a freshly created object.
    void deserialize(ReadBuffer & buf)
    {
        typename Sketch::vector_bytes bytes;
        readVectorBinary(bytes, buf);
        if (bytes.empty())
            return;

        try
        {
            sketch = Sketch::deserialize(bytes.data(), bytes.size());
        }
        catch (const DB::Exception &)
        {
            throw;
        }
        catch (const std::bad_alloc &)
        {
            /// Memory pressure is not data corruption; let it propagate.
            throw;
        }
        catch (const std::exception & e)
        {
            /// `datasketches` throws `std::invalid_argument` / `std::out_of_range` on malformed
            /// input. These are not `DB::Exception`, so without translation they escape
            /// `SerializationAggregateFunction`'s `catch (...)`, reach the top level as
            /// `LOGICAL_ERROR` (code 1001), and abort the process. Translate to `CORRUPTED_DATA`
            /// so the bad input is rejected cleanly.
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Cannot deserialize REQ sketch state: {}", e.what());
        }
    }

    Value get(Float64 level)
    {
        return getImpl<Value>(level);
    }

    void getMany(const Float64 * levels, const size_t * indices, size_t size, Value * result)
    {
        getManyImpl(levels, indices, size, result);
    }

    Float64 getFloat(Float64 level)
    {
        return getImpl<Float64>(level);
    }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t size, Float64 * result)
    {
        getManyImpl(levels, indices, size, result);
    }

private:
    static constexpr uint16_t default_k = 12;

    Sketch sketch;

    static uint16_t toK(ssize_t k)
    {
        /// The aggregate-function layer only guarantees `k > 0`. Round up to an even value and
        /// clamp to the range DataSketches accepts, so a user-supplied odd or tiny `k` does not
        /// throw deep inside the sketch constructor.
        if (k < 4)
            return 4;
        if (k > 65534)
            return 65534;
        return static_cast<uint16_t>(k % 2 == 0 ? k : k + 1);
    }

    template <typename T>
    T getImpl(Float64 level)
    {
        if (sketch.is_empty())
            return std::numeric_limits<T>::quiet_NaN();
        return static_cast<T>(sketch.get_quantile(level));
    }

    template <typename T>
    void getManyImpl(const Float64 * levels, const size_t *, size_t num_levels, T * result)
    {
        for (size_t i = 0; i < num_levels; ++i)
            result[i] = getImpl<T>(levels[i]);
    }
};

}

#endif
