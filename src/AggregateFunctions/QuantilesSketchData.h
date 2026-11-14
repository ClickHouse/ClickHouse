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

    /// Merge an imported (or partial-state) sketch, preserving its configured precision `k`
    /// instead of degrading it to the default `k` of a freshly created sketch. The first sketch
    /// is adopted as-is. For exact-mode sketches (which the library streams item by item into
    /// the receiving sketch, keeping the receiver's `k`) the larger `k` is kept, so the result
    /// does not depend on the merge order. Estimation-mode sketches follow the library rules
    /// (the result takes the smallest `k` among the estimation-mode inputs).
    void mergeSketch(datasketches::quantiles_sketch<double> && sk)
    {
        if (!quantile_sketch)
        {
            quantile_sketch = std::make_unique<datasketches::quantiles_sketch<double>>(std::move(sk));
            return;
        }
        if (!quantile_sketch->is_estimation_mode() && sk.get_k() > quantile_sketch->get_k())
        {
            sk.merge(*quantile_sketch);
            *quantile_sketch = std::move(sk);
            return;
        }
        quantile_sketch->merge(sk);
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
        mergeSketch(deserializeSketch<datasketches::quantiles_sketch<double>>(data_ptr, data_size));
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
        mergeSketch(datasketches::quantiles_sketch<double>(*rhs.quantile_sketch));
    }

    void read(DB::ReadBuffer & in)
    {
        typename datasketches::quantiles_sketch<double>::vector_bytes bytes;
        readVectorBinary(bytes, in);
        if (!bytes.empty())
        {
            mergeSketch(deserializeSketch<datasketches::quantiles_sketch<double>>(bytes.data(), bytes.size()));
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
