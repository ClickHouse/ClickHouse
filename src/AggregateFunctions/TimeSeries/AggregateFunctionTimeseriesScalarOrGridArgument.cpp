#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesScalarOrGridArgument.h>

#include <algorithm>
#include <cmath>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/castTypeToEither.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{
    /// The argument may be NaN, and NaN is not equal to itself, so `==` alone would report the same NaN in two rows as different values.
    bool sameValue(Float64 lhs, Float64 rhs)
    {
        return (lhs == rhs) || (std::isnan(lhs) && std::isnan(rhs));
    }
}


void checkTimeseriesScalarOrGridArgumentType(TimeseriesScalarOrGridArgumentNames names, const DataTypePtr & type)
{
    const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
    if (!isNativeNumber(array_type ? array_type->getNestedType() : type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of the argument `{}` for aggregate function {}, expected a number or an array of numbers",
            type->getName(), names.argument_name, names.function_name);
}


void AggregateFunctionTimeseriesScalarOrGridArgument::captureOrCheck(
    TimeseriesScalarOrGridArgumentNames names,
    size_t grid_size,
    size_t row_begin,
    size_t row_end,
    const IColumn & column,
    const UInt8 * flags,
    bool flag_value_to_include)
{
    /// A row is included if its flag is non-zero and `flag_value_to_include` is true, or its flag is zero and
    /// `flag_value_to_include` is false. `flags` is nullptr when every row is included.
    auto is_excluded = [&](size_t row) { return flags && ((flags[row] != 0) != flag_value_to_include); };

    size_t row = row_begin;
    while (row < row_end && is_excluded(row))
        ++row;
    if (row == row_end)
        return;

    const auto * array_column = typeid_cast<const ColumnArray *>(&column);
    const IColumn & number_column = array_column ? array_column->getData() : column;

    /// `[begin, begin + size)` are the elements of the current `row` in the nested column (`offsets[-1]` is 0).
    auto array_begin = [&] { return array_column->getOffsets()[static_cast<ssize_t>(row) - 1]; };
    auto array_size = [&] { return array_column->getOffsets()[row] - array_column->getOffsets()[static_cast<ssize_t>(row) - 1]; };

    /// The argument holds any native number type (checked when the function is created).
    const bool dispatched = castTypeToEither<
        ColumnVector<UInt8>, ColumnVector<UInt16>, ColumnVector<UInt32>, ColumnVector<UInt64>,
        ColumnVector<Int8>, ColumnVector<Int16>, ColumnVector<Int32>, ColumnVector<Int64>,
        ColumnVector<Float32>, ColumnVector<Float64>>(&number_column, [&](const auto & number_column_typed)
    {
        const auto & data = number_column_typed.getData();

        if (values.empty())
        {
            if (array_column)
            {
                const size_t size = array_size();
                if (size != grid_size)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Aggregate function {} requires the array argument `{}` to have one value per grid point ({}), got {} values",
                        names.function_name, names.argument_name,
                        grid_size, size);

                const auto * row_values = data.data() + array_begin();
                values.resize(size);
                for (size_t i = 0; i < size; ++i)
                    values[i] = static_cast<Float64>(row_values[i]);

                /// An array with the same value at every grid point is kept as that single value.
                if (std::all_of(values.begin(), values.end(), [&](Float64 value) { return sameValue(value, values[0]); }))
                    values.resize(1);
            }
            else
            {
                values.push_back(static_cast<Float64>(data[row]));
            }
            ++row;
        }

        /// The other rows must carry the captured `values`.
        for (; row < row_end; ++row)
        {
            if (is_excluded(row))
                continue;

            bool same = false;
            if (!array_column)
            {
                same = (values.size() == 1) && sameValue(static_cast<Float64>(data[row]), values[0]);
            }
            else if (array_size() == grid_size)
            {
                const auto * row_values = data.data() + array_begin();
                if (values.size() == 1)
                    same = std::all_of(row_values, row_values + grid_size, [&](auto value) { return sameValue(static_cast<Float64>(value), values[0]); });
                else
                    same = std::equal(values.begin(), values.end(), row_values, [](Float64 lhs, auto rhs) { return sameValue(lhs, static_cast<Float64>(rhs)); });
            }

            if (!same)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Aggregate function {} requires the same value of the argument `{}` in every row", names.function_name, names.argument_name);
        }
        return true;
    });

    if (!dispatched)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} for the argument `{}` of aggregate function {}", number_column.getName(), names.argument_name, names.function_name);
}


void AggregateFunctionTimeseriesScalarOrGridArgument::merge(
    TimeseriesScalarOrGridArgumentNames names, const AggregateFunctionTimeseriesScalarOrGridArgument & other)
{
    if (values.empty())
    {
        values = other.values;
        return;
    }

    if (other.values.empty())
        return;

    if (values.size() != other.values.size() || !std::equal(values.begin(), values.end(), other.values.begin(), sameValue))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot merge states of aggregate function {} created with different values of the argument `{}`", names.function_name, names.argument_name);
}


void AggregateFunctionTimeseriesScalarOrGridArgument::serialize(WriteBuffer & buf) const
{
    writeBinaryLittleEndian(static_cast<UInt64>(values.size()), buf);
    for (const Float64 value : values)
        writeBinaryLittleEndian(value, buf);
}


void AggregateFunctionTimeseriesScalarOrGridArgument::deserialize(TimeseriesScalarOrGridArgumentNames names, ReadBuffer & buf, size_t grid_size)
{
    UInt64 size = 0;
    readBinaryLittleEndian(size, buf);
    if (size > 1 && size != grid_size)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Cannot deserialize data of aggregate function {} with {} values of the argument `{}`, expected 0, 1 or {}",
            names.function_name, size, names.argument_name, grid_size);

    values.resize(size);
    for (auto & value : values)
        readBinaryLittleEndian(value, buf);
}


Float64 AggregateFunctionTimeseriesScalarOrGridArgument::at(size_t grid_index) const
{
    /// Without rows every window is empty, so the returned value is not used.
    if (values.empty())
        return 0;

    if (values.size() == 1)
        return values[0];

    chassert(grid_index < values.size());
    return values[grid_index];
}

}
