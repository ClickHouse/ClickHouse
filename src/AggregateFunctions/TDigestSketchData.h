#pragma once

#include "config.h"

#if USE_DATASKETCHES

#include <boost/noncopyable.hpp>
#include <memory>
#include <tdigest.hpp>

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
    using value_type = T;

    TDigestSketchData() = default;
    ~TDigestSketchData() = default;

    void insertOriginal(double value)
    {
        getTDigest()->update(value);
    }

    void insert(T value)
    {
        getTDigest()->update(value);
    }

    std::map<Float64, Int64> getCentroids()
    {
        std::map<Float64, Int64> centroids;
        if (!tdigest)
        {
            return centroids;
        }
        tdigest->compress();
        for (const auto& centroid : tdigest->get_centroids())
        {
            centroids[centroid.get_mean()] = centroid.get_weight();
        }
        return centroids;
    }

    String serializedData()
    {
        if (!tdigest)
        {
            return "{}";
        }

        std::stringstream ss;
        ss << "{";
        bool first = true;
        tdigest->compress();
        for (const auto& centroid : tdigest->get_centroids())
        {
            double mean = centroid.get_mean();
            long long weight = centroid.get_weight();
            if (!first) {
                ss << ",";
            } else {
                first = false;
            }
            ss << "\"" << mean << "\":" << weight ;
        }
        ss << "}";
        return ss.str();
    }

    void merge(const TDigestSketchData & rhs)
    {
        datasketches::tdigest<double> * u = getTDigest();
        u->merge(*const_cast<TDigestSketchData &>(rhs).tdigest);
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

