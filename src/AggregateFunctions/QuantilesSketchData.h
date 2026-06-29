#pragma once

#include "config.h"

#if USE_DATASKETCHES

#include <boost/noncopyable.hpp>
#include <memory>
#include <quantiles_sketch.hpp>
#include <AggregateFunctions/SketchDataUtils.h>
#include <Core/Types.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

namespace DB
{

template <typename T>
class QuantilesSketchData : private boost::noncopyable
{
private:
    std::unique_ptr<datasketches::quantiles_sketch<double>> quantile_sketch;

    datasketches::quantiles_sketch<double> * getQuantilesSketch()
    {
        if (!quantile_sketch)
            quantile_sketch = std::make_unique<datasketches::quantiles_sketch<double>>(datasketches::quantiles_sketch<double>());
        return quantile_sketch.get();
    }

public:
    QuantilesSketchData() = default;
    ~QuantilesSketchData() = default;

    void insertOriginal(double value)
    {
        getQuantilesSketch()->update(value);
    }

    void insertSerialized(std::string_view serialized_data, bool base64_encoded = false)
    {
        if (serialized_data.empty())
            return;

        std::string decoded_storage;
        /// When merging internally-generated sketches (from serializedQuantiles),
        /// we know the data is raw binary, not base64. Use base64_encoded=false for performance.
        /// For external data sources that might send base64, set base64_encoded=true.
        auto [data_ptr, data_size] = decodeSketchData(serialized_data, decoded_storage, base64_encoded);

        if (data_ptr == nullptr || data_size == 0)
            return;

        /// Deserialize and merge the sketch
        auto sk = datasketches::quantiles_sketch<double>::deserialize(data_ptr, data_size);
        getQuantilesSketch()->merge(sk);
    }

    String serializedData()
    {
        if (!quantile_sketch)
        {
            return "";
        }
        auto bytes = quantile_sketch->serialize();
        return String(bytes.begin(), bytes.end());
    }

    String getValuesAndWeights()
    {
        if (!quantile_sketch)
        {
            return "{}";
        }

        WriteBufferFromOwnString buf;
        writeChar('{', buf);
        bool first = true;
        for (const auto&& node : *quantile_sketch)
        {
            double value = node.first;
            UInt64 weight = static_cast<UInt64>(node.second);
            if (!first)
            {
                writeChar(',', buf);
            }
            else
            {
                first = false;
            }
            writeChar('"', buf);
            writeText(value, buf);
            writeChar('"', buf);
            writeChar(':', buf);
            writeText(weight, buf);
        }
        writeChar('}', buf);
        return buf.str();
    }

    void merge(const QuantilesSketchData & rhs)
    {
        if (!rhs.quantile_sketch)
            return;
        datasketches::quantiles_sketch<double> * u = getQuantilesSketch();
        u->merge(*rhs.quantile_sketch);
    }

    void read(DB::ReadBuffer & in)
    {
        typename datasketches::quantiles_sketch<double>::vector_bytes bytes;
        readVectorBinary(bytes, in);
        if (!bytes.empty())
        {
            auto quantile_sketch_local = datasketches::quantiles_sketch<double>::deserialize(bytes.data(), bytes.size());
            getQuantilesSketch()->merge(quantile_sketch_local);
        }
    }

    void write(DB::WriteBuffer & out) const
    {
        if (quantile_sketch)
        {
            auto bytes = quantile_sketch->serialize();
            writeVectorBinary(bytes, out);
        }
        else
        {
            typename datasketches::quantiles_sketch<double>::vector_bytes bytes;
            writeVectorBinary(bytes, out);
        }
    }
};

}

#endif
