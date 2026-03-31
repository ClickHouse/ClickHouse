#pragma once

#include <array>
#include <string_view>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnString.h>
#include <Common/PODArray.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeString.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/arithmeticOverflow.h>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/** Aggregate function combinator -Sparkbar applies another aggregate function independently
  * to each bucket determined by the first (x-axis) argument, and renders the per-bucket
  * results as a Unicode sparkbar string.
  *
  * Usage:
  *   sumSparkbar(width, begin_x, end_x)(x_col, value_col)
  *   countSparkbar(width, begin_x, end_x)(x_col)
  *   uniqSparkbar(width, begin_x, end_x)(x_col, key_col)
  *
  * The first argument is the x-axis (bucket key) column; remaining arguments are forwarded
  * to the nested aggregate function.
  *
  * The explicit range [begin_x, end_x] is always required. Without it, bucketing cannot be
  * performed at aggregation time because the range is not known until all data is seen,
  * and the per-bucket states are irreversible once accumulated.
  */
template <typename Key>
class AggregateFunctionSparkbar final
    : public IAggregateFunctionHelper<AggregateFunctionSparkbar<Key>>
{
private:
    static constexpr size_t BAR_LEVELS = 8;
    static constexpr size_t MAX_WIDTH  = 1024;

    AggregateFunctionPtr nested_function;

    size_t width;
    Key    begin_x;
    Key    end_x;

    size_t align_of_data;
    size_t size_of_data;

    size_t updateFrame(ColumnString::Chars & frame, Float64 value) const
    {
        static constexpr std::array<std::string_view, BAR_LEVELS + 1> bars{" ", "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"};
        const auto & bar = (std::isnan(value) || value < 1 || value > BAR_LEVELS) ? bars[0] : bars[static_cast<UInt8>(value)];
        frame.insert(bar.begin(), bar.end());
        return bar.size();
    }

    void render(ColumnString & to_column, const PaddedPODArray<Float64> & values) const
    {
        auto & chars   = to_column.getChars();
        auto & offsets = to_column.getOffsets();

        Float64 y_max = 0;
        for (Float64 v : values)
        {
            if (!std::isnan(v) && v > 0)
                y_max = std::max(y_max, v);
        }

        if (y_max == 0)
        {
            offsets.push_back(chars.size());
            return;
        }

        for (Float64 v : values)
        {
            Float64 scaled = 0;
            if (!std::isnan(v) && v > 0)
                scaled = v / y_max * static_cast<Float64>(BAR_LEVELS - 1) + 1;
            updateFrame(chars, scaled);
        }

        offsets.push_back(chars.size());
    }

public:
    AggregateFunctionSparkbar(
        AggregateFunctionPtr nested_function_,
        size_t width_,
        Key begin_x_,
        Key end_x_,
        const DataTypes & arguments,
        const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionSparkbar<Key>>{arguments, params, std::make_shared<DataTypeString>()}
        , nested_function{nested_function_}
        , width{width_}
        , begin_x{begin_x_}
        , end_x{end_x_}
        , align_of_data{nested_function->alignOfData()}
        , size_of_data{(nested_function->sizeOfData() + align_of_data - 1) / align_of_data * align_of_data}
    {
        if (width < 2 || width > MAX_WIDTH)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Parameter width for aggregate function {} must be in range [2, {}]",
                getName(), MAX_WIDTH);

        if (begin_x >= end_x)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Parameter begin_x must be strictly less than end_x for aggregate function {}",
                getName());
    }

    String getName() const override
    {
        return nested_function->getName() + "Sparkbar";
    }

    bool isState() const override { return nested_function->isState(); }

    bool isVersioned() const override { return nested_function->isVersioned(); }

    size_t getVersionFromRevision(size_t revision) const override
    {
        return nested_function->getVersionFromRevision(revision);
    }

    size_t getDefaultVersion() const override { return nested_function->getDefaultVersion(); }

    bool allocatesMemoryInArena() const override { return nested_function->allocatesMemoryInArena(); }

    bool hasTrivialDestructor() const override { return nested_function->hasTrivialDestructor(); }

    size_t sizeOfData() const override { return width * size_of_data; }

    size_t alignOfData() const override { return align_of_data; }

    void create(AggregateDataPtr __restrict place) const override
    {
        for (size_t i = 0; i < width; ++i)
        {
            try
            {
                nested_function->create(place + i * size_of_data);
            }
            catch (...)
            {
                for (size_t j = 0; j < i; ++j)
                    nested_function->destroy(place + j * size_of_data);
                throw;
            }
        }
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        for (size_t i = 0; i < width; ++i)
            nested_function->destroy(place + i * size_of_data);
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        for (size_t i = 0; i < width; ++i)
            nested_function->destroyUpToState(place + i * size_of_data);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        Key key;
        if constexpr (static_cast<Key>(-1) < 0)
            key = static_cast<Key>(columns[0]->getInt(row_num));
        else
            key = static_cast<Key>(columns[0]->getUInt(row_num));

        if (key < begin_x || key > end_x)
            return;

        /// Use Float64 arithmetic to correctly map large ranges to a small number of buckets.
        /// Integer division (w / range) would truncate to 0 when range >> width.
        const Float64 range  = static_cast<Float64>(end_x) - static_cast<Float64>(begin_x);
        const Float64 offset = static_cast<Float64>(key)   - static_cast<Float64>(begin_x);
        const size_t pos = static_cast<size_t>(
            std::min<Float64>(offset / range * static_cast<Float64>(width), static_cast<Float64>(width - 1)));

        /// First argument (columns[0]) is the x-axis key; the rest go to the nested function.
        nested_function->add(place + pos * size_of_data, columns + 1, row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        for (size_t i = 0; i < width; ++i)
            nested_function->merge(place + i * size_of_data, rhs + i * size_of_data, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        for (size_t i = 0; i < width; ++i)
            nested_function->serialize(place + i * size_of_data, buf, version);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        for (size_t i = 0; i < width; ++i)
            nested_function->deserialize(place + i * size_of_data, buf, version, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        /// Collect the nested function result for each bucket into a temporary column.
        auto result_col = nested_function->getResultType()->createColumn();
        result_col->reserve(width);

        for (size_t i = 0; i < width; ++i)
            nested_function->insertResultInto(place + i * size_of_data, *result_col, arena);

        /// Convert results to Float64 using Field to handle all numeric types (UInt64, Int64,
        /// Float32, Float64, Decimal, …) without relying on getFloat64() which is not universally
        /// implemented.
        PaddedPODArray<Float64> values(width);
        for (size_t i = 0; i < width; ++i)
        {
            if (result_col->isNullAt(i))
            {
                values[i] = std::nan("");
                continue;
            }

            Field field;
            result_col->get(i, field);
            values[i] = applyVisitor(FieldVisitorConvertToNumber<Float64>(), field);
        }

        render(assert_cast<ColumnString &>(to), values);
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_function; }
};

}
