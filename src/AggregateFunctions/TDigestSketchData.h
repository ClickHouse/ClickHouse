#pragma once

#include "config.h"

#if USE_DATASKETCHES

#include <boost/noncopyable.hpp>
#include <memory>
#include <tdigest.hpp>
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

        auto sk = datasketches::tdigest<double>::deserialize(data, size);
        getTDigest()->merge(sk);
    }

    std::vector<uint8_t> getSerializedData()
    {
        if (!tdigest)
        {
            std::vector<uint8_t> empty;
            return empty;
        }
        tdigest->compress();
        auto bytes = tdigest->serialize();
        return bytes;
    }

    void merge(const TDigestSketchData & rhs)
    {
        if (!rhs.tdigest)
            return;
        datasketches::tdigest<double> * u = getTDigest();
        u->merge(*rhs.tdigest);
    }

    /// You can only call for an empty object.
    void read(DB::ReadBuffer & in)
    {
        typename datasketches::tdigest<double>::vector_bytes bytes;
        readVectorBinary(bytes, in);
        if (!bytes.empty())
        {
            auto tdigest_local = datasketches::tdigest<double>::deserialize(bytes.data(), bytes.size());
            getTDigest()->merge(tdigest_local);
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

