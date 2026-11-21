#pragma once

#include "config.h"

#if USE_DATASKETCHES

#include <boost/noncopyable.hpp>
#include <memory>
#include <tdigest.hpp>
#include <AggregateFunctions/SketchDataUtils.h>
#include <Core/Types.h>

namespace DB
{

const int compression = 100;

template <typename T>
class TDigestSketchData : private boost::noncopyable
{
private:
    std::unique_ptr<datasketches::tdigest<double>> tdigest;

    datasketches::tdigest<double> * getTDigest()
    {
        if (!tdigest)
            tdigest = std::make_unique<datasketches::tdigest<double>>(datasketches::tdigest<double>(compression));
        return tdigest.get();
    }

    /// Merge an imported (or partial-state) digest, preserving its compression `k` instead of
    /// degrading it to the default one. The largest `k` among the inputs is kept (`tdigest::merge`
    /// keeps the receiver's `k`), so the result does not depend on the merge order.
    void mergeDigest(datasketches::tdigest<double> && sk)
    {
        if (!tdigest)
        {
            tdigest = std::make_unique<datasketches::tdigest<double>>(std::move(sk));
            return;
        }
        if (sk.get_k() > tdigest->get_k())
        {
            sk.merge(*tdigest);
            *tdigest = std::move(sk);
            return;
        }
        tdigest->merge(sk);
    }

public:
    TDigestSketchData() = default;
    ~TDigestSketchData() = default;

    void insertOriginal(double value)
    {
        getTDigest()->update(value);
    }

    void insertSerialized(const uint8_t* data, size_t size)
    {
        if (data == nullptr || size == 0)
            return;

        mergeDigest(deserializeSketch<datasketches::tdigest<double>>(data, size));
    }

    typename datasketches::tdigest<double>::vector_bytes getSerializedData()
    {
        if (!tdigest)
            return {};
        tdigest->compress();
        auto bytes = tdigest->serialize();
        return bytes;
    }

    void merge(const TDigestSketchData & rhs)
    {
        if (!rhs.tdigest)
            return;
        mergeDigest(datasketches::tdigest<double>(*rhs.tdigest));
    }

    /// You can only call for an empty object.
    void read(DB::ReadBuffer & in)
    {
        typename datasketches::tdigest<double>::vector_bytes bytes;
        readVectorBinary(bytes, in);
        if (!bytes.empty())
        {
            mergeDigest(deserializeSketch<datasketches::tdigest<double>>(bytes.data(), bytes.size()));
        }
    }

    void write(DB::WriteBuffer & out) const
    {
        if (tdigest)
        {
            auto bytes = tdigest->serialize();
            writeVectorBinary(bytes, out);
        }
        else
        {
            typename datasketches::tdigest<double>::vector_bytes bytes;
            writeVectorBinary(bytes, out);
        }
    }
};

}

#endif
