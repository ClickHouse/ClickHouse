#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesQuantileToGrid.h>

#include <algorithm>
#include <cmath>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Functions/castTypeToEither.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{
    /// A NaN level is allowed (Prometheus defines `quantile_over_time(NaN, v)` as NaN), so NaN must compare equal to itself.
    bool samePhi(Float64 lhs, Float64 rhs)
    {
        return (lhs == rhs) || (std::isnan(lhs) && std::isnan(rhs));
    }
}


void AggregateFunctionTimeseriesQuantileToGridPhi::captureOrCheck(
    size_t grid_size, size_t row_begin, size_t row_end, const IColumn & column, const UInt8 * flags, bool flag_value_to_include)
{
    auto is_excluded = [&](size_t row) { return flags && ((flags[row] != 0) != flag_value_to_include); };

    size_t row = row_begin;
    while (row < row_end && is_excluded(row))
        ++row;
    if (row == row_end)
        return;

    const auto * array_column = typeid_cast<const ColumnArray *>(&column);
    const IColumn & number_column = array_column ? array_column->getData() : column;

    /// `[begin, begin + size)` are the elements of the current `row` in the nested column (`offsets[-1]` is 0).
    auto array_begin = [&] { return array_column->getOffsets()[row - 1]; };
    auto array_size = [&] { return array_column->getOffsets()[row] - array_column->getOffsets()[row - 1]; };

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
                        "Aggregate function timeSeriesQuantileToGrid requires the array argument `phi` to have one value per grid point ({}), got {} values",
                        grid_size, size);

                const auto * row_values = data.data() + array_begin();
                values.resize(size);
                for (size_t i = 0; i < size; ++i)
                    values[i] = static_cast<Float64>(row_values[i]);

                /// An array with the same value at every grid point is kept as that single value.
                if (std::all_of(values.begin(), values.end(), [&](Float64 value) { return samePhi(value, values[0]); }))
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
                same = (values.size() == 1) && samePhi(static_cast<Float64>(data[row]), values[0]);
            }
            else if (array_size() == grid_size)
            {
                const auto * row_values = data.data() + array_begin();
                if (values.size() == 1)
                    same = std::all_of(row_values, row_values + grid_size, [&](auto value) { return samePhi(static_cast<Float64>(value), values[0]); });
                else
                    same = std::equal(values.begin(), values.end(), row_values, [](Float64 lhs, auto rhs) { return samePhi(lhs, static_cast<Float64>(rhs)); });
            }

            if (!same)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Aggregate function timeSeriesQuantileToGrid requires the same value of the argument `phi` in every row");
        }
        return true;
    });

    if (!dispatched)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} for the argument `phi`", number_column.getName());
}


void AggregateFunctionTimeseriesQuantileToGridPhi::merge(const AggregateFunctionTimeseriesQuantileToGridPhi & other)
{
    if (values.empty())
    {
        values = other.values;
        return;
    }

    if (other.values.empty())
        return;

    if (values.size() != other.values.size() || !std::equal(values.begin(), values.end(), other.values.begin(), samePhi))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot merge states of aggregate function timeSeriesQuantileToGrid created with different values of the argument `phi`");
}


void AggregateFunctionTimeseriesQuantileToGridPhi::serialize(WriteBuffer & buf) const
{
    writeBinaryLittleEndian(static_cast<UInt64>(values.size()), buf);
    for (const Float64 value : values)
        writeBinaryLittleEndian(value, buf);
}


void AggregateFunctionTimeseriesQuantileToGridPhi::deserialize(ReadBuffer & buf, size_t grid_size)
{
    UInt64 size = 0;
    readBinaryLittleEndian(size, buf);
    if (size > 1 && size != grid_size)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Cannot deserialize data with {} values of the argument `phi`, expected 0, 1 or {}", size, grid_size);

    values.resize(size);
    for (auto & value : values)
        readBinaryLittleEndian(value, buf);
}


Float64 AggregateFunctionTimeseriesQuantileToGridPhi::at(size_t grid_index) const
{
    if (values.empty())
        return 0;
    return (values.size() == 1) ? values[0] : values[grid_index];
}

}
