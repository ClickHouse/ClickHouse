#pragma once

#include "config.h"

#if USE_DATASKETCHES

#include <boost/noncopyable.hpp>
#include <memory>
#include <quantiles_sketch.hpp>

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
            if (!first) {
                ss << ",";
            } else {
                first = false;
            }
            ss << "\"" << value << "\":" << weight;
        }
        ss << "}";
        return ss.str();
    }

    void merge(const DoubleSketchData & rhs)
    {
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
