#pragma once

#include <array>
#include <string_view>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Common/PODArray.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** Aggregate function combinator -Sparkbar applies another aggregate function to values
  * falling into buckets by the first argument, and then renders the results as a sparkbar string.
  *
  * Example: sumSparkbar(10, 0, 100)(x, amount) - sums amounts for each bucket and renders as sparkbar
  *          countSparkbar(10, 0, 100)(x) - counts values in each bucket and renders as sparkbar
  */
template <typename Key>
class AggregateFunctionSparkbar final : public IAggregateFunctionHelper<AggregateFunctionSparkbar<Key>>
{
private:
    static constexpr size_t BAR_LEVELS = 8;
    static constexpr size_t MAX_WIDTH = 1024;

    AggregateFunctionPtr nested_function;

    size_t width;
    Key begin_x;
    Key end_x;

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
        auto & chars = to_column.getChars();
        auto & offsets = to_column.getOffsets();

        Float64 max_value = 0;
        for (const auto & v : values)
        {
            if (!std::isnan(v) && v > 0)
                max_value = std::max(max_value, v);
        }

        if (max_value == 0)
        {
            offsets.push_back(chars.size());
            return;
        }

        for (auto v : values)
        {
            Float64 scaled = 0;
            if (!std::isnan(v) && v > 0)
                scaled = v / max_value * (BAR_LEVELS - 1) + 1;
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
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter width must be in range [2, {}]", MAX_WIDTH);

        if (begin_x >= end_x)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter min_x must be less than max_x");

        auto result_type = removeNullable(nested_function->getResultType());
        if (!isNumber(result_type) && !isDecimal(result_type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function {} requires nested function with numeric result type, got {}",
                getName(), nested_function->getResultType()->getName());
    }

    String getName() const override
    {
        return nested_function->getName() + "Sparkbar";
    }

    bool isState() const override
    {
        return nested_function->isState();
    }

    bool isVersioned() const override
    {
        return nested_function->isVersioned();
    }

    size_t getVersionFromRevision(size_t revision) const override
    {
        return nested_function->getVersionFromRevision(revision);
    }

    size_t getDefaultVersion() const override
    {
        return nested_function->getDefaultVersion();
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_function->allocatesMemoryInArena();
    }

    bool hasTrivialDestructor() const override
    {
        return nested_function->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        /// We store `width` instances of the nested function state
        return width * size_of_data;
    }

    size_t alignOfData() const override
    {
        return align_of_data;
    }

    AggregateDataPtr getNestedPlace(AggregateDataPtr __restrict place, size_t bucket) const
    {
        return place + bucket * size_of_data;
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        for (size_t i = 0; i < width; ++i)
        {
            try
            {
                nested_function->create(getNestedPlace(place, i));
            }
            catch (...)
            {
                for (size_t j = 0; j < i; ++j)
                    nested_function->destroy(getNestedPlace(place, j));
                throw;
            }
        }
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        for (size_t i = 0; i < width; ++i)
            nested_function->destroy(getNestedPlace(place, i));
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        for (size_t i = 0; i < width; ++i)
            nested_function->destroyUpToState(getNestedPlace(place, i));
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        Key key;

        if constexpr (static_cast<Key>(-1) < 0)
            key = static_cast<Key>(columns[0]->getInt(row_num));
        else
            key = static_cast<Key>(columns[0]->getUInt(row_num));

        /// Skip out-of-range keys
        if (key < begin_x || key > end_x)
            return;

        /// Calculate bucket index
        size_t bucket = 0;
        Key delta = end_x - begin_x;
        if (delta < std::numeric_limits<Key>::max())
            delta = delta + 1;
        Float64 w = static_cast<Float64>(width);
        bucket = std::min<size_t>(
            static_cast<size_t>(w / static_cast<Float64>(delta) * static_cast<Float64>(key - begin_x)),
            width - 1);

        /// The nested function receives columns starting from index 1 (skip the key column)
        nested_function->add(getNestedPlace(place, bucket), columns + 1, row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        for (size_t i = 0; i < width; ++i)
            nested_function->merge(getNestedPlace(place, i), getNestedPlace(const_cast<AggregateDataPtr>(rhs), i), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        for (size_t i = 0; i < width; ++i)
            nested_function->serialize(getNestedPlace(const_cast<AggregateDataPtr>(place), i), buf, version);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        for (size_t i = 0; i < width; ++i)
            nested_function->deserialize(getNestedPlace(place, i), buf, version, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        auto & to_column = assert_cast<ColumnString &>(to);

        /// Extract values from each bucket
        PaddedPODArray<Float64> bucket_values(width, 0);

        /// Create a temporary column to receive nested results
        auto result_type = nested_function->getResultType();
        auto temp_column = result_type->createColumn();

        for (size_t i = 0; i < width; ++i)
        {
            nested_function->insertResultInto(getNestedPlace(place, i), *temp_column, arena);
        }

        /// Convert results to Float64 for rendering
        bool is_nullable = nested_function->getResultType()->isNullable();
        for (size_t i = 0; i < width; ++i)
        {
            if (is_nullable && temp_column->isNullAt(i))
                bucket_values[i] = std::nan("");
            else
                bucket_values[i] = temp_column->getFloat64(i);
        }

        render(to_column, bucket_values);
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_function; }
};

}
