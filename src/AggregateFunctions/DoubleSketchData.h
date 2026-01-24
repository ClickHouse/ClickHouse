#pragma once

#include "config.h"

#if USE_DATASKETCHES

#include <boost/noncopyable.hpp>
#include <memory>
#include <quantiles_sketch.hpp>
#include <Common/Base64.h>
#include <AggregateFunctions/SketchDataUtils.h>

namespace DB
{

template <typename T>
class DoubleSketchData : private boost::noncopyable
{
private:
    std::unique_ptr<datasketches::quantiles_sketch<double>> quantile_sketch;

    datasketches::quantiles_sketch<double> * getDoubleSketch()
    {
        if (!quantile_sketch)
            quantile_sketch = std::make_unique<datasketches::quantiles_sketch<double>>(datasketches::quantiles_sketch<double>());
        return quantile_sketch.get();
    }

public:
    using value_type = T;

    DoubleSketchData() = default;
    ~DoubleSketchData() = default;

    void insertOriginal(double value)
    {
        getDoubleSketch()->update(value);
    }

    void insert(T value)
    {
        getDoubleSketch()->update(value);
    }

    void insertSerialized(std::string_view serialized_data)
    {
        if (serialized_data.empty())
            return;

        std::string decoded_data;
        const uint8_t * data_ptr;
        size_t data_size;

        /// Fast check: only attempt base64 decode if data looks like base64
        /// This avoids expensive exception handling for raw binary data (the common case)
        if (looksLikeBase64(serialized_data))
        {
            try
            {
                decoded_data = base64Decode(std::string(serialized_data));
                data_ptr = reinterpret_cast<const uint8_t*>(decoded_data.data());
                data_size = decoded_data.size();
            }
            catch (...)
            {
                /// Looked like base64 but wasn't valid, use raw data
                data_ptr = reinterpret_cast<const uint8_t*>(serialized_data.data());
                data_size = serialized_data.size();
            }
        }
        else
        {
            /// Doesn't look like base64, use raw data directly
            data_ptr = reinterpret_cast<const uint8_t*>(serialized_data.data());
            data_size = serialized_data.size();
        }

        /// Deserialize and merge the sketch
        try
        {
            auto sk = datasketches::quantiles_sketch<double>::deserialize(data_ptr, data_size);
            getDoubleSketch()->merge(sk);
        }
        catch (...)
        {
            /// If deserialization fails (corrupted or invalid data), skip this value.
            /// This allows graceful handling of bad input data rather than failing the entire aggregation.
        }
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

        std::stringstream ss;
        ss << "{";
        bool first = true;
        for (const auto&& node : *(quantile_sketch.get()))
        {
            double value = node.first;
            long long weight = node.second;
            if (!first)
            {
                ss << ",";
            }
            else
            {
                first = false;
            }
            ss << "\"" << value << "\":" << weight;
        }
        ss << "}";
        return ss.str();
    }

    void merge(const DoubleSketchData & rhs)
    {
        if (!rhs.quantile_sketch)
            return;
        datasketches::quantiles_sketch<double> * u = getDoubleSketch();
        u->merge(*const_cast<DoubleSketchData &>(rhs).quantile_sketch);
    }

    void read(DB::ReadBuffer & in)
    {
        typename datasketches::quantiles_sketch<double>::vector_bytes bytes;
        readVectorBinary(bytes, in);
        if (!bytes.empty())
        {
            auto quantile_sketch_local = datasketches::quantiles_sketch<double>::deserialize(bytes.data(), bytes.size());
            getDoubleSketch()->merge(quantile_sketch_local);
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
