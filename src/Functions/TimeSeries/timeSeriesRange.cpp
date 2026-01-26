#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/DecimalFunctions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function timeSeriesRange(start_timestamp, end_timestamp, step) returns a range of timestamps
/// [start_timestamp, start_timestamp + step, start_timestamp + 2 * step, ..., end_timestamp].
///
/// Function timeSeriesFromGrid(start_timestamp, end_timestamp, step, [value1, value2, value3, ..., valueN]) converts array of values [value1, value2, value3, ...]
/// to array of tuples [(start_timestamp, value1), (start_timestamp + step, value2), (start_timestamp + 2 * step, value3), ..., (end_timestamp, valueN)].
template <bool with_values>
class FunctionTimeSeriesRange : public IFunction
{
public:
    static constexpr auto name = with_values ? "timeSeriesFromGrid" : "timeSeriesRange";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesRange<with_values>>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return with_values ? 4 : 3; }
    bool isDeterministic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkDataTypes(arguments);

        const auto & start_timestamp_type = arguments[0].type;
        const auto & end_timestamp_type = arguments[1].type;
        const auto & step_type = arguments[2].type;

        UInt32 timestamp_scale = 0;
        if (isDateTime64(start_timestamp_type))
            timestamp_scale = std::max(timestamp_scale, getDecimalScale(*start_timestamp_type));
        if (isDateTime64(end_timestamp_type))
            timestamp_scale = std::max(timestamp_scale, getDecimalScale(*end_timestamp_type));
        if (isDecimal(step_type))
            timestamp_scale = std::max(timestamp_scale, getDecimalScale(*step_type));

        DataTypePtr timestamp_type;
        if (timestamp_scale > 0)
            timestamp_type = std::make_shared<DataTypeDateTime64>(timestamp_scale);
        else
            timestamp_type = start_timestamp_type;

        if constexpr (with_values)
        {
            const auto & values_type = arguments[3].type;
            auto value_type = removeNullable(typeid_cast<const DataTypeArray &>(*values_type).getNestedType());
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{timestamp_type, value_type}));
        }
        else
        {
            return std::make_shared<DataTypeArray>(timestamp_type);
        }
    }

    static void checkDataTypes(const ColumnsWithTypeAndName & arguments)
    {
        constexpr size_t expected_num_ars = with_values ? 4 : 3;
        if (arguments.size() != expected_num_ars)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with {} arguments", name, expected_num_ars);

        const auto & start_timestamp_type = arguments[0].type;
        const auto & end_timestamp_type = arguments[1].type;
        const auto & step_type = arguments[2].type;

        if (!(isDateTimeOrDateTime64(start_timestamp_type) || isUInt32(start_timestamp_type)))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} (start_timestamp) of function {} has wrong type {}, it must be {}",
                            1, name, start_timestamp_type, "DateTime64 or DateTime or UInt32");

        if (!(isDateTimeOrDateTime64(end_timestamp_type) || isUInt32(end_timestamp_type)))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} (end_timestamp) of function {} has wrong type {}, it must be {}",
                            2, name, end_timestamp_type, "DateTime64 or DateTime or UInt32");

        if (!(WhichDataType(step_type).isDecimal32() || WhichDataType(step_type).isDecimal64() || isNativeInteger(step_type)))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} (step) of function {} has wrong type {}, it must be {}",
                            3, name, step_type, "a number");

        if constexpr (with_values)
        {
            const auto & values_type = arguments[3].type;
            if (!isArray(values_type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                                4, name, values_type, "an array of values");
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        constexpr size_t expected_num_ars = with_values ? 4 : 3;
        chassert(arguments.size() == expected_num_ars);

        const auto & start_timestamp_type = arguments[0].type;
        const auto & end_timestamp_type = arguments[1].type;
        const auto & step_type = arguments[2].type;

        DataTypePtr result_timestamp_type;
        if constexpr (with_values)
            result_timestamp_type = typeid_cast<const DataTypeTuple &>(*typeid_cast<const DataTypeArray &>(*result_type).getNestedType()).getElement(0);
        else
            result_timestamp_type = typeid_cast<const DataTypeArray &>(*result_type).getNestedType();
        UInt32 result_timestamp_scale = isDateTime64(*result_timestamp_type) ? getDecimalScale(*result_timestamp_type) : 0;

        UInt32 start_timestamp_scale = 0;
        if (isDateTime64(*start_timestamp_type))
            start_timestamp_scale = getDecimalScale(*start_timestamp_type);
        chassert(start_timestamp_scale <= result_timestamp_scale);
        Int64 start_timestamp_multiplier = (start_timestamp_scale < result_timestamp_scale) ? DecimalUtils::scaleMultiplier<Int64>(result_timestamp_scale - start_timestamp_scale) : 1;

        UInt32 end_timestamp_scale = 0;
        if (isDateTime64(*end_timestamp_type))
            end_timestamp_scale = getDecimalScale(*end_timestamp_type);
        chassert(end_timestamp_scale <= result_timestamp_scale);
        Int64 end_timestamp_multiplier = (end_timestamp_scale < result_timestamp_scale) ? DecimalUtils::scaleMultiplier<Int64>(result_timestamp_scale - end_timestamp_scale) : 1;

        UInt32 step_scale = 0;
        if (isDecimal(*step_type))
            step_scale = getDecimalScale(*step_type);
        chassert(step_scale <= result_timestamp_scale);
        Int64 step_multiplier = (step_scale < result_timestamp_scale) ? DecimalUtils::scaleMultiplier<Int64>(result_timestamp_scale - step_scale) : 1;

        const auto & start_timestamp_column = *arguments[0].column;
        const auto & end_timestamp_column = *arguments[1].column;
        const auto & step_column = *arguments[2].column;

        const IColumn * values_column = nullptr;
        bool values_are_nullable = false;

        if constexpr (with_values)
        {
            const auto & values_type = arguments[3].type;
            values_are_nullable
                = (typeid_cast<const DataTypeArray *>(values_type.get())
                && typeid_cast<const DataTypeArray *>(values_type.get())->getNestedType()->isNullable());
            values_column = arguments[3].column.get();
        }

        if (isDateTime64(result_timestamp_type))
        {
            return doExecute<DateTime64, Decimal64>(start_timestamp_column, start_timestamp_multiplier,
                                                    end_timestamp_column, end_timestamp_multiplier,
                                                    step_column, step_multiplier,
                                                    values_column, values_are_nullable,
                                                    result_type, input_rows_count);
        }
        else if (isDateTime(result_timestamp_type) || isUInt32(result_timestamp_type))
        {
            return doExecute<UInt32, Int32>(start_timestamp_column, start_timestamp_multiplier,
                                            end_timestamp_column, end_timestamp_multiplier,
                                            step_column, step_multiplier,
                                            values_column, values_are_nullable,
                                            result_type, input_rows_count);
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Illegal result type {} of function {}, it must be {}",
                            result_type, name, "DateTime64 or DateTime or UInt32");
        }
    }

    template <typename TimestampType, typename IntervalType>
    static ColumnPtr doExecute(const IColumn & start_timestamp_column, Int64 start_timestamp_multiplier,
                               const IColumn & end_timestamp_column, Int64 end_timestamp_multiplier,
                               const IColumn & step_column, Int64 step_multiplier,
                               const IColumn * values_column, bool values_are_nullable,
                               const DataTypePtr & result_type,
                               size_t num_rows)
    {
        const IColumn * values = nullptr;
        const IColumn::Offsets * values_offsets = nullptr;
        const NullMap * null_map = nullptr;

        if constexpr (with_values)
        {
            const auto * array_column = checkAndGetColumn<ColumnArray>(values_column);
            if (!array_column || (values_are_nullable && !checkColumn<ColumnNullable>(array_column->getData())))
            {
                auto full_column = values_column->convertToFullIfNeeded();
                if (full_column.get() != values_column)
                {
                    return doExecute<TimestampType, IntervalType>(start_timestamp_column, start_timestamp_multiplier,
                                                                end_timestamp_column, end_timestamp_multiplier,
                                                                step_column, step_multiplier,
                                                                full_column.get(), values_are_nullable,
                                                                result_type, num_rows);
                }
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                                "Illegal column {} of argument #{} of function {}, it must be {}",
                                values_column->getName(), 1, name, "Array(Nullable(Float64)) or Array(Float64)");
            }
            values_offsets = &array_column->getOffsets();
            values = &array_column->getData();
            if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(values))
            {
                null_map = &nullable_column->getNullMapData();
                values = &nullable_column->getNestedColumn();
            }
        }

        DataTypePtr result_timestamp_type;
        DataTypePtr result_value_type;
        if constexpr (with_values)
        {
            const auto & result_tuple_type = typeid_cast<const DataTypeTuple &>(*typeid_cast<const DataTypeArray &>(*result_type).getNestedType());
            result_timestamp_type = result_tuple_type.getElement(0);
            result_value_type = result_tuple_type.getElement(1);
        }
        else
        {
            result_timestamp_type = typeid_cast<const DataTypeArray &>(*result_type).getNestedType();
        }

        auto res_timestamps = result_timestamp_type->createColumn();

        MutableColumnPtr res_values;
        if constexpr (with_values)
        {
            res_values = result_value_type->createColumn();
            res_values->reserve(values->size());
            res_timestamps->reserve(values->size());
        }

        auto res_offsets = ColumnArray::ColumnOffsets::create();
        res_offsets->reserve(num_rows);

        for (size_t i = 0; i != num_rows; ++i)
        {
            auto start_timestamp = static_cast<TimestampType>(start_timestamp_column.get64(i) * start_timestamp_multiplier);
            auto end_timestamp = static_cast<TimestampType>(end_timestamp_column.get64(i) * end_timestamp_multiplier);
            auto step = static_cast<IntervalType>(step_column.getInt(i) * step_multiplier);

            if (end_timestamp < start_timestamp)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "End timestamp is less than start timestamp");

            size_t num_steps = 1;
            if (start_timestamp != end_timestamp)
            {
                if (step <= static_cast<IntervalType>(0))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Step should be greater than zero");
                num_steps = (end_timestamp - start_timestamp) / step + 1;
            }

            size_t values_base_offset;
            if constexpr (with_values)
            {
                values_base_offset = (*values_offsets)[i - 1];
                size_t num_values = (*values_offsets)[i] - values_base_offset;
                if (num_values != num_steps)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Number of values ({}) doesn't match number of steps ({})", num_values, num_steps);
            }

            for (size_t j = 0; j != num_steps; ++j)
            {
                size_t offset;
                if constexpr (with_values)
                {
                    offset = j + values_base_offset;
                    if (null_map && (*null_map)[offset])
                        continue;
                }
                TimestampType timestamp = start_timestamp + j * step;
                res_timestamps->insert(timestamp);
                if constexpr (with_values)
                    res_values->insertFrom(*values, offset);
            }

            res_offsets->insert(res_timestamps->size());
        }

        if constexpr (with_values)
            return ColumnArray::create(ColumnTuple::create(Columns{std::move(res_timestamps), std::move(res_values)}), std::move(res_offsets));
        else
            return ColumnArray::create(std::move(res_timestamps), std::move(res_offsets));
    }
};


REGISTER_FUNCTION(TimeSeriesRange)
{
    FunctionDocumentation::Description description = R"(
Generates a range of timestamps [start_timestamp, start_timestamp + step, start_timestamp + 2 * step, ..., end_timestamp].

If `start_timestamp` is equal to `end_timestamp`, the function returns a 1-element array containing `[start_timestamp]`.

Function `timeSeriesRange()` is similar to function [range](../functions/array-functions.md#range).
)";
    FunctionDocumentation::Syntax syntax = "timeSeriesRange(start_timestamp, end_timestamp, step)";
    FunctionDocumentation::Arguments arguments = {{"start_timestamp", "Start of the range.", {"DateTime64", "DateTime", "UInt32"}},
                                                  {"end_timestamp", "End of the range.", {"DateTime64", "DateTime", "UInt32"}},
                                                  {"step", "Step of the range in seconds", {"UInt32/64", "Decimal32/64"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a range of timestamps.", {"Array(DateTime64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT timeSeriesRange('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:01:00'::DateTime64(3), 30)
        )",
        R"(
┌────────────────────────────────────result─────────────────────────────────────────┐
│ ['2025-06-01 00:00:00.000', '2025-06-01 00:00:30.000', '2025-06-01 00:01:00.000'] │
└───────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesRange<false>>(documentation);
}

REGISTER_FUNCTION(TimeSeriesFromGrid)
{
    FunctionDocumentation::Description description = R"(
Converts an array of values `[x1, x2, x3, ...]` to an array of tuples
`[(start_timestamp, x1), (start_timestamp + step, x2), (start_timestamp + 2 * step, x3), ...]`.

The current timestamp is increased by `step` until it becomes greater than `end_timestamp`
If the number of the values doesn't match the number of the timestamps, the function throws an exception.

NULL values in `[x1, x2, x3, ...]` are skipped but the current timestamp is still incremented.
For example, for `[value1, NULL, x2]` the function returns `[(start_timestamp, x1), (start_timestamp + 2 * step, x2)]`.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesFromGrid(start_timestamp, end_timestamp, step, values)";
    FunctionDocumentation::Arguments arguments = {
        {"start_timestamp", "Start of the grid.", {"DateTime64", "DateTime", "UInt32"}},
        {"end_timestamp", "End of the grid.", {"DateTime64", "DateTime", "UInt32"}},
        {"step", "Step of the grid in seconds", {"Decimal64", "Decimal32", "UInt32/64"}},
        {"values", "Array of values", {"Array(Float*)", "Array(Nullable(Float*))"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns values from the source array of values combined with timestamps on a regular time grid described by `start_timestamp` and `step`.", {"Array(Tuple(DateTime64, Float64))"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT timeSeriesFromGrid('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:01:30.000'::DateTime64(3), 30, [10, 20, NULL, 30]) AS result;
        )",
        R"(
┌─────────────────────────────────────────────result─────────────────────────────────────────────┐
│ [('2025-06-01 00:00:00.000',10),('2025-06-01 00:00:30.000',20),('2025-06-01 00:01:30.000',30)] │
└────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesRange<true>>(documentation);
}

}
