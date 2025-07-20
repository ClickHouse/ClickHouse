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

#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function timeSeriesGrid(start_timestamp, end_timestamp, step, [value1, value2, value3, ...]) converts array of values [value1, value2, value3, ...]
/// to array of tuples [(start_timestamp, value1), (start_timestamp + step, value2), (start_timestamp + 2 * step, value3), ...].
class FunctionTimeSeriesGrid : public IFunction
{
public:
    static constexpr auto name = "timeSeriesGrid";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesGrid>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 4; }

    bool isDeterministic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkDataTypes(arguments);

        const auto & start_timestamp_type = arguments[0].type;
        const auto & end_timestamp_type = arguments[1].type;
        const auto & step_type = arguments[2].type;
        const auto & values_type = arguments[3].type;

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

        auto value_type = removeNullable(typeid_cast<const DataTypeArray &>(*values_type).getNestedType());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{timestamp_type, value_type}));
    }

    static void checkDataTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 4)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with 4 arguments", name);

        const auto & start_timestamp_type = arguments[0].type;
        const auto & end_timestamp_type = arguments[1].type;
        const auto & step_type = arguments[2].type;
        const auto & values_type = arguments[3].type;

        if (!(isDateTimeOrDateTime64(start_timestamp_type) || isUInt32(start_timestamp_type)))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} (start_timestamp) of function {} has wrong type {}, it must be {}",
                            1, name, start_timestamp_type, "DateTime64 or DateTime or UInt32");

        if (!(isDateTimeOrDateTime64(end_timestamp_type) || isUInt32(end_timestamp_type)))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} (end_timestamp) of function {} has wrong type {}, it must be {}",
                            2, name, end_timestamp_type, "DateTime64 or DateTime or UInt32");

        if (!(WhichDataType(step_type).isDecimal32() || WhichDataType(step_type).isDecimal64() || isNativeInteger(step_type)))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} (step) of function {} has wrong type {}, it must be {}",
                            3, name, step_type, "a number");

        if (!isArray(values_type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                            4, name, values_type, "an array of values");
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        chassert(arguments.size() == 4);

        const auto & start_timestamp_type = arguments[0].type;
        const auto & end_timestamp_type = arguments[1].type;
        const auto & step_type = arguments[2].type;
        const auto & values_type = arguments[3].type;

        auto result_timestamp_type = typeid_cast<const DataTypeTuple &>(*typeid_cast<const DataTypeArray &>(*result_type).getNestedType()).getElement(0);
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

        bool values_are_nullable
            = (typeid_cast<const DataTypeArray *>(values_type.get())
               && typeid_cast<const DataTypeArray *>(values_type.get())->getNestedType()->isNullable());

        const auto & start_timestamp_column = *arguments[0].column;
        const auto & end_timestamp_column = *arguments[1].column;
        const auto & step_column = *arguments[2].column;
        const auto & values_column = *arguments[3].column;

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
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Illegal result type {} of function {}, it must be {}",
                            result_type, name, "DateTime64 or DateTime or UInt32");
        }
    }

    template <typename TimestampType, typename IntervalType>
    static ColumnPtr doExecute(const IColumn & start_timestamp_column, Int64 start_timestamp_multiplier,
                               const IColumn & end_timestamp_column, Int64 end_timestamp_multiplier,
                               const IColumn & step_column, Int64 step_multiplier,
                               const IColumn & values_column, bool values_are_nullable,
                               const DataTypePtr & result_type,
                               size_t num_rows)
    {
        LOG_INFO(getLogger("!!!"), "doExecute: start_timestamp_multiplier = {}, end_timestamp_multiplier = {}, step_multiplier = {}",
                 start_timestamp_multiplier, end_timestamp_multiplier, step_multiplier);

        const auto * array_column = checkAndGetColumn<ColumnArray>(&values_column);
        if (!array_column || (values_are_nullable && !checkColumn<ColumnNullable>(&array_column->getData())))
        {
            auto full_column = values_column.convertToFullIfNeeded();
            if (full_column.get() != &values_column)
            {
                return doExecute<TimestampType, IntervalType>(start_timestamp_column, start_timestamp_multiplier,
                                                              end_timestamp_column, end_timestamp_multiplier,
                                                              step_column, step_multiplier,
                                                              *full_column, values_are_nullable,
                                                              result_type, num_rows);
            }
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Illegal column {} of argument #{} of function {}, it must be {}",
                            values_column.getName(), 1, name, "Array(Nullable(Float64)) or Array(Float64)");
        }

        const auto & offsets = array_column->getOffsets();
        const IColumn * values = &array_column->getData();

        const NullMap * null_map = nullptr;
        if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(values))
        {
            null_map = &nullable_column->getNullMapData();
            values = &nullable_column->getNestedColumn();
        }

        const auto & result_tuple_type = typeid_cast<const DataTypeTuple &>(*typeid_cast<const DataTypeArray &>(*result_type).getNestedType());
        const auto & result_timestamp_type = *result_tuple_type.getElement(0);
        const auto & result_value_type = *result_tuple_type.getElement(1);

        auto res_timestamps = result_timestamp_type.createColumn();
        res_timestamps->reserve(values->size());

        auto res_values = result_value_type.createColumn();
        res_values->reserve(values->size());

        auto res_offsets = ColumnArray::ColumnOffsets::create();
        res_offsets->reserve(num_rows);

        for (size_t i = 0; i != num_rows; ++i)
        {
            auto start_timestamp = static_cast<TimestampType>(start_timestamp_column.get64(i) * start_timestamp_multiplier);
            auto end_timestamp = static_cast<TimestampType>(end_timestamp_column.get64(i) * end_timestamp_multiplier);
            auto step = static_cast<IntervalType>(step_column.get64(i) * step_multiplier);
            auto timestamp = start_timestamp;
            size_t start_offset = offsets[i - 1];
            size_t end_offset = offsets[i];
            for (size_t j = start_offset; j != end_offset; ++j)
            {
                if (timestamp > end_timestamp)
                    break;
                if (!null_map || !(*null_map)[j])
                {
                    res_timestamps->insert(timestamp);
                    res_values->insertFrom(*values, j);
                }
                timestamp += step;
            }
            res_offsets->insert(res_values->size());
        }

        return ColumnArray::create(ColumnTuple::create(Columns{std::move(res_timestamps), std::move(res_values)}), std::move(res_offsets));
    }
};


REGISTER_FUNCTION(TimeSeriesGrid)
{
    FunctionDocumentation::Description description = R"(
        Converts array of values [value1, value2, value3, ...] to array of tuples
        [(start_timestamp, value1), (start_timestamp + step, value2), (start_timestamp + 2 * step, value3), ...].
        If some of the values [value1, value2, value3, ...] are NULL then the function won't copy such null values to the result array
        but will still increase the current timestamp, e.g. for [value1, NULL, value2] the function will return
        [(start_timestamp, value1), (start_timestamp + 2 * step, value2)].
        The current timestamp is increased by step until it either becomes greater than end_timestamp or until there are no more values
        in the array of values.
        )";
    FunctionDocumentation::Syntax syntax = "timeSeriesGrid(start_timestamp, end_timestamp, step, values)";
    FunctionDocumentation::Arguments arguments = {{"start_timestamp", "Start of the grid.", {"DateTime64", "DateTime", "UInt32"}},
                                                  {"end_timestamp", "End of the grid.", {"DateTime64", "DateTime", "UInt32"}},
                                                  {"step", "Step of the grid in seconds", {"Decimal64", "Decimal32", "UInt64", "UInt32"}},
                                                  {"values", "array of values", {"Array(Nullable(Float64))", "Array(Float64)", "Array(Nullable(Float32))", "Array(Float32)"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns values from the source array of values combined with timestamps on a regular time grid described by start timestamp and step", {"Array(DateTime64, Float64"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesGrid('2025-06-01 00:00:00'::DateTime64(3), '2025-06-02 00:00:00'::DateTime64(3), 30, [10, 20, NULL, 30])", "[('2025-06-01 00:00:00.000',10),('2025-06-01 00:00:30.000',20),('2025-06-01 00:01:30.000',30)]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesGrid>(documentation);
}

}
