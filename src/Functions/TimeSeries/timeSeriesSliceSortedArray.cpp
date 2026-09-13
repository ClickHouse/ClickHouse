#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SUPPORT_IS_DISABLED;
}

namespace Setting
{
    extern const SettingsBool enable_time_series_table;
}

/// Function timeSeriesSliceSortedArray(sorted_samples, min_time, max_time) returns the samples of a time series with timestamps
/// in the interval [min_time, max_time]. The samples are passed as an array of tuples (timestamp, value) sorted by timestamp,
/// the function finds the interval in the array with a binary search and returns a slice of the array.
class FunctionTimeSeriesSliceSortedArray final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesSliceSortedArray";

    static FunctionPtr create(ContextPtr context)
    {
        if (!context->getSettingsRef()[Setting::enable_time_series_table])
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Function {} is in private preview and disabled by default. Enable it with setting enable_time_series_table",
                name);

        return std::make_shared<FunctionTimeSeriesSliceSortedArray>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool isDeterministic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto & samples_type = arguments[0].type;
        const auto * array_type = typeid_cast<const DataTypeArray *>(samples_type.get());
        const auto * tuple_type = array_type ? typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get()) : nullptr;
        if (!tuple_type || (tuple_type->getElements().size() != 2))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument #1 (sorted_samples) of function {} has wrong type {}, it must be Array(Tuple(timestamp, value))",
                name, samples_type->getName());

        const auto & timestamp_type = tuple_type->getElements()[0];
        if (!(isDateTimeOrDateTime64(timestamp_type) || isUInt32(timestamp_type)))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument #1 (sorted_samples) of function {} has wrong type {}, the type of the timestamps must be DateTime64 or DateTime or UInt32",
                name, samples_type->getName());

        for (size_t i = 1; i < 3; ++i)
        {
            const auto & bound_type = arguments[i].type;
            if (!(isDateTimeOrDateTime64(bound_type) || isNumber(bound_type) || isDecimal(bound_type)))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument #{} ({}) of function {} has wrong type {}, it must be DateTime64 or DateTime or a number",
                    i + 1, (i == 1) ? "min_time" : "max_time", name, bound_type->getName());
        }

        /// This function can be called with `SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp_type, value_type)))`
        /// as the first argument, in this case it should return `Array(Tuple(timestamp_type, value_type))`.
        if (tuple_type->hasExplicitNames())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(tuple_type->getElements(), tuple_type->getElementNames()));
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(tuple_type->getElements()));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// The array can be constant while the bounds are not.
        ColumnPtr samples_column_holder = arguments[0].column->convertToFullColumnIfConst();
        const auto * samples_column = checkAndGetColumn<ColumnArray>(samples_column_holder.get());
        if (!samples_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument #1 (sorted_samples) of function {} must be an array, got {}",
                name, arguments[0].column->getName());

        const auto * tuples_column = checkAndGetColumn<ColumnTuple>(&samples_column->getData());
        if (!tuples_column || (tuples_column->tupleSize() != 2))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument #1 (sorted_samples) of function {} must be an array of tuples (timestamp, value), got {}",
                name, arguments[0].column->getName());

        const auto & timestamp_type = typeid_cast<const DataTypeTuple &>(*typeid_cast<const DataTypeArray &>(*result_type).getNestedType()).getElement(0);

        /// The bounds are converted to the type of the timestamps, so they can be compared with the timestamps as raw values.
        ColumnPtr min_time_column = castColumn(arguments[1], timestamp_type);
        ColumnPtr max_time_column = castColumn(arguments[2], timestamp_type);

        /// The bounds are usually constant (the selector passes them as literals), then they're read once instead of
        /// being materialized for every row.
        const bool bounds_are_const = isColumnConst(*min_time_column) && isColumnConst(*max_time_column);
        if (bounds_are_const)
        {
            min_time_column = assert_cast<const ColumnConst &>(*min_time_column).getDataColumnPtr();
            max_time_column = assert_cast<const ColumnConst &>(*max_time_column).getDataColumnPtr();
        }
        else
        {
            min_time_column = min_time_column->convertToFullColumnIfConst();
            max_time_column = max_time_column->convertToFullColumnIfConst();
        }

        const IColumn & timestamps = tuples_column->getColumn(0);

        auto execute = [&]<typename TimestampColumnType>()
        {
            if (bounds_are_const)
                return executeForTimestampType<TimestampColumnType, /* bounds_are_const = */ true>(
                    samples_column_holder, timestamps, *min_time_column, *max_time_column, input_rows_count);
            return executeForTimestampType<TimestampColumnType, /* bounds_are_const = */ false>(
                samples_column_holder, timestamps, *min_time_column, *max_time_column, input_rows_count);
        };

        if (isDateTime64(timestamp_type))
            return execute.template operator()<ColumnDecimal<DateTime64>>();
        return execute.template operator()<ColumnUInt32>();
    }

private:
    /// `samples_column_ptr` is a `ColumnArray`, it's returned as is if every slice is a whole array.
    /// If `bounds_are_const`, `min_time_column` and `max_time_column` contain one row used for all the arrays,
    /// otherwise they contain a row per array.
    template <typename TimestampColumnType, bool bounds_are_const>
    static ColumnPtr executeForTimestampType(
        const ColumnPtr & samples_column_ptr,
        const IColumn & timestamps,
        const IColumn & min_time_column,
        const IColumn & max_time_column,
        size_t input_rows_count)
    {
        const auto & samples_column = assert_cast<const ColumnArray &>(*samples_column_ptr);
        const auto * typed_timestamps = checkAndGetColumn<TimestampColumnType>(&timestamps);
        const auto * typed_min_time = checkAndGetColumn<TimestampColumnType>(&min_time_column);
        const auto * typed_max_time = checkAndGetColumn<TimestampColumnType>(&max_time_column);
        if (!typed_timestamps || !typed_min_time || !typed_max_time)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected columns {}, {}, {} of timestamps in function {}",
                timestamps.getName(), min_time_column.getName(), max_time_column.getName(), name);

        const auto & timestamps_data = typed_timestamps->getData();
        const auto & min_time_data = typed_min_time->getData();
        const auto & max_time_data = typed_max_time->getData();

        const auto & source_tuples = samples_column.getData();

        auto res_tuples = source_tuples.cloneEmpty();
        auto res_offsets = ColumnArray::ColumnOffsets::create();
        auto & res_offsets_data = res_offsets->getData();
        res_offsets_data.reserve(input_rows_count);

        typename TimestampColumnType::ValueType min_time{};
        typename TimestampColumnType::ValueType max_time{};
        if constexpr (bounds_are_const)
        {
            min_time = min_time_data[0];
            max_time = max_time_data[0];
        }

        /// The slices of consecutive rows are adjacent in the nested column if a slice reaches the end of its array
        /// and the next slice starts at the start of its array. That is the usual case when a time range covers
        /// whole buckets, so adjacent slices are accumulated in a pending range and copied together.
        size_t pending_begin = 0;
        size_t pending_end = 0;
        size_t res_size = 0;

        auto copy_pending = [&]
        {
            if (pending_begin < pending_end)
                res_tuples->insertRangeFrom(source_tuples, pending_begin, pending_end - pending_begin);
        };

        const auto & offsets = samples_column.getOffsets();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t begin = (i == 0) ? 0 : offsets[i - 1];
            size_t end = offsets[i];

            if constexpr (!bounds_are_const)
            {
                min_time = min_time_data[i];
                max_time = max_time_data[i];
            }

            size_t slice_begin = begin;
            size_t slice_end = end;
            if (begin < end)
            {
                const auto * first = timestamps_data.data() + begin;
                const auto * last = timestamps_data.data() + end;

                /// The samples are sorted by timestamp, so a slice is found with a binary search.
                /// A search is skipped if the bound is outside of the array, which is the usual case for whole buckets.
                if (timestamps_data[begin] < min_time)
                    slice_begin = std::lower_bound(first, last, min_time) - timestamps_data.data();
                if (max_time < timestamps_data[end - 1])
                    slice_end = std::upper_bound(first, last, max_time) - timestamps_data.data();

                /// `min_time` greater than `max_time` gives an empty slice.
                slice_end = std::max(slice_end, slice_begin);
            }

            if (slice_begin != pending_end)
            {
                copy_pending();
                pending_begin = slice_begin;
            }
            pending_end = slice_end;

            res_size += slice_end - slice_begin;
            res_offsets_data.push_back(res_size);
        }

        /// Every slice is a whole array: the result is the source column, nothing needs to be copied.
        /// A copy moves `pending_begin` past the copied range, so nothing has been copied if it's still zero.
        if ((pending_begin == 0) && (pending_end == source_tuples.size()))
        {
            chassert(res_tuples->empty());
            return samples_column_ptr;
        }

        copy_pending();
        return ColumnArray::create(std::move(res_tuples), std::move(res_offsets));
    }
};


REGISTER_FUNCTION(TimeSeriesSliceSortedArray)
{
    FunctionDocumentation::Description description = R"(
Returns the samples of a time series with timestamps in the interval `[min_time, max_time]`.

The samples are passed as an array of tuples `(timestamp, value)` sorted by timestamp, for example a value of the
`samples` column of the samples table of a `TimeSeries` table, or a result of the aggregate function `timeSeriesGroupArray`.
The function finds the requested interval in the array with a binary search, so the array must be sorted by timestamp.

If the array is not sorted, the result is not specified: the function returns some part of the array, which can
include samples outside of the interval and miss samples inside of it. The function doesn't check that the array is sorted.

The function returns an array of the same type as `sorted_samples`. If `min_time` is greater than `max_time`, the function returns an empty array.

<Note>
This function is in private preview, enable it by setting `enable_time_series_table = 1`.
</Note>
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesSliceSortedArray(sorted_samples, min_time, max_time)";
    FunctionDocumentation::Arguments arguments = {
        {"sorted_samples", "Samples of a time series sorted by timestamp. The result is not specified if the array is not sorted.", {"Array(Tuple(DateTime64, Float*))", "Array(Tuple(DateTime, Float*))", "Array(Tuple(UInt32, Float*))"}},
        {"min_time", "Start of the interval, inclusive. It is converted to the type of the timestamps.", {"DateTime64", "DateTime", "UInt32", "(U)Int*", "Float*", "Decimal*"}},
        {"max_time", "End of the interval, inclusive. It is converted to the type of the timestamps.", {"DateTime64", "DateTime", "UInt32", "(U)Int*", "Float*", "Decimal*"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the samples with timestamps in the interval `[min_time, max_time]`, in the same order as in `sorted_samples`.", {"Array(Tuple(T1, T2))"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT timeSeriesSliceSortedArray([(100, 1.), (110, 2.), (120, 3.), (130, 4.)]::Array(Tuple(UInt32, Float64)), 105, 120) AS result
        )",
        R"(
┌─result────────────┐
│ [(110,2),(120,3)] │
└───────────────────┘
        )"
    },
    {
        "Slicing samples with DateTime64 timestamps",
        R"(
SELECT timeSeriesSliceSortedArray(
    [('2025-06-01 00:00:00'::DateTime64(3), 1.), ('2025-06-01 00:00:30'::DateTime64(3), 2.), ('2025-06-01 00:01:00'::DateTime64(3), 3.)],
    '2025-06-01 00:00:15'::DateTime64(3), '2025-06-01 00:01:00'::DateTime64(3)) AS result
        )",
        R"(
┌─result────────────────────────────────────────────────────────┐
│ [('2025-06-01 00:00:30.000',2),('2025-06-01 00:01:00.000',3)] │
└───────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesSliceSortedArray>(documentation);
}

}
