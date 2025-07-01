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
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function timeSeriesGrid(start_timestamp, step, [value1, value2, value3, ...]) converts array of values [value1, value2, value3, ...]
/// to array of tuples [(start_timestamp, value1), (start_timestamp + step, value2), (start_timestamp + 2 * step, value3), ...].
class FunctionTimeSeriesGrid : public IFunction
{
public:
    static constexpr auto name = "timeSeriesGrid";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesGrid>(); }
    explicit FunctionTimeSeriesGrid() {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    bool isDeterministic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkDataTypes(arguments);

        auto timestamp_type = arguments[0].type;
        auto value_type = removeNullable(typeid_cast<const DataTypeArray &>(*arguments[2].type).getNestedType());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{timestamp_type, value_type}));
    }

    static void checkDataTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be called with 4 arguments", name);

        const auto & start_timestamp_type = arguments[0].type;
        const auto & step_type = arguments[1].type;
        const auto & values_array_type = arguments[2].type;

        if (!(isDateTimeOrDateTime64(start_timestamp_type) || isUInt32(start_timestamp_type)))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} (start_timestamp) of function {} has wrong type {}, it must be {}",
                            1, name, start_timestamp_type, "DateTime64 or DateTime or UInt32");

        if (!(WhichDataType(step_type).isDecimal32() || WhichDataType(step_type).isDecimal64() || isNativeInteger(step_type)))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} (step) of function {} has wrong type {}, it must be {}",
                            2, name, step_type, "a number");

        if (isDecimal(step_type))
        {
            auto step_scale = getDecimalScale(*step_type);
            if (!isDateTime64(start_timestamp_type) || (getDecimalScale(*start_timestamp_type) < step_scale))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} (step) of function {} has a bigger scale {} than argument #{} (start_timestamp)",
                                2, name, step_scale, 1);
        }

        if (!isArray(values_array_type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} has wrong type {}, it must be {}",
                            3, name, values_array_type, "an array of values");
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        chassert(arguments.size() == 3);

        const auto & column_start_timestamp = *arguments[0].column;
        const auto & start_timestamp_type = arguments[0].type;
        const auto & column_step = *arguments[1].column;
        const auto & step_type = arguments[1].type;
        const auto & column_values_array = *arguments[2].column;
        const auto & values_array_type = arguments[2].type;

        bool values_are_nullable
            = (typeid_cast<const DataTypeArray *>(values_array_type.get())
               && typeid_cast<const DataTypeArray *>(values_array_type.get())->getNestedType()->isNullable());

        UInt32 step_extra_scale = 0;
        if (isDateTime64(start_timestamp_type))
        {
            auto timestamp_scale = getDecimalScale(*start_timestamp_type);
            UInt32 step_scale = isDecimal(step_type) ? getDecimalScale(*step_type) : 0;
            chassert(timestamp_scale >= step_scale);
            step_extra_scale = timestamp_scale - step_scale;
        }

        if (isDateTime64(start_timestamp_type))
            return doExecute<DateTime64, Decimal64>(column_start_timestamp, column_step, step_extra_scale, column_values_array, values_are_nullable, result_type, input_rows_count);
        else if (isDateTime(start_timestamp_type) || isUInt32(start_timestamp_type))
            return doExecute<UInt32, Int32>(column_start_timestamp, column_step, step_extra_scale, column_values_array, values_are_nullable, result_type, input_rows_count);
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Illegal column {} of argument #{} of function {}, it must be {}",
                            column_start_timestamp.getName(), 1, name, "DateTime64 or DateTime or UInt32");
    }

    template <typename TimestampType, typename IntervalType>
    static ColumnPtr doExecute(const IColumn & column_start_timestamp,
                               const IColumn & column_step, UInt32 step_extra_scale,
                               const IColumn & column_values_array, bool values_are_nullable,
                               const DataTypePtr & result_type,
                               size_t num_rows)
    {
        const auto * array_column = checkAndGetColumn<ColumnArray>(&column_values_array);
        if (!array_column || (values_are_nullable && !checkColumn<ColumnNullable>(&array_column->getData())))
        {
            auto full_column = column_values_array.convertToFullIfNeeded();
            if (full_column.get() != &column_values_array)
                return doExecute<TimestampType, IntervalType>(column_start_timestamp, column_step, step_extra_scale, *full_column, values_are_nullable, result_type, num_rows);
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Illegal column {} of argument #{} of function {}, it must be {}",
                            column_values_array.getName(), 1, name, "Array(Nullable(Float64)) or Array(Float64)");
        }

        const auto & offsets = array_column->getOffsets();

        Int64 step_multiplier = 1;
        if (step_extra_scale > 0)
            step_multiplier *= DecimalUtils::scaleMultiplier<Decimal64>(step_extra_scale);

        const auto & tuple_data_type = typeid_cast<const DataTypeTuple &>(*typeid_cast<const DataTypeArray &>(*result_type).getNestedType());
        const auto & timestamp_data_type = *tuple_data_type.getElement(0);
        const auto & value_data_type = *tuple_data_type.getElement(1);

        auto timestamps = timestamp_data_type.createColumn();
        timestamps->reserve(array_column->getData().size());

        if (!values_are_nullable)
        {
            for (size_t i = 0; i != num_rows; ++i)
            {
                auto start_timestamp = static_cast<TimestampType>(column_start_timestamp.get64(i));
                auto step = static_cast<IntervalType>(column_step.get64(i) * step_multiplier);
                size_t start_offset = offsets[i - 1];
                size_t end_offset = offsets[i];
                for (size_t j = start_offset; j != end_offset; ++j)
                {
                    TimestampType timestamp = start_timestamp + (j - start_offset) * step;
                    timestamps->insert(timestamp);
                }
            }
            return ColumnArray::create(ColumnTuple::create(Columns{std::move(timestamps), array_column->getData().convertToFullIfNeeded()}), array_column->getOffsetsPtr());
        }

        const auto & nullable_column = checkAndGetColumn<ColumnNullable>(array_column->getData());
        const auto & null_map = nullable_column.getNullMapData();
        const auto & values = nullable_column.getNestedColumn();

        auto new_values = value_data_type.createColumn();
        new_values->reserve(values.size());

        auto new_offsets = ColumnArray::ColumnOffsets::create();
        new_offsets->reserve(num_rows);

        for (size_t i = 0; i != num_rows; ++i)
        {
            auto start_timestamp = static_cast<TimestampType>(column_start_timestamp.get64(i));
            auto step = static_cast<IntervalType>(column_step.get64(i) * step_multiplier);
            size_t start_offset = offsets[i - 1];
            size_t end_offset = offsets[i];
            for (size_t j = start_offset; j != end_offset; ++j)
            {
                if (!null_map[j])
                {
                    TimestampType timestamp = start_timestamp + (j - start_offset) * step;
                    timestamps->insert(timestamp);
                    new_values->insertFrom(values, j);
                }
            }
            new_offsets->insert(new_values->size());
        }

        return ColumnArray::create(ColumnTuple::create(Columns{std::move(timestamps), std::move(new_values)}), std::move(new_offsets));
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
        )";
    FunctionDocumentation::Syntax syntax = "timeSeriesGrid(start_timestamp, step, values)";
    FunctionDocumentation::Arguments arguments = {{"start_timestamp", "Start of the grid.", {"DateTime64", "DateTime", "UInt32"}},
                                                  {"step", "Step of the grid in seconds", {"Decimal64", "Decimal32", "UInt64", "UInt32"}},
                                                  {"values", "array of values", {"Array(Nullable(Float64))", "Array(Float64)", "Array(Nullable(Float32))", "Array(Float32)"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns values from the source array of values combined with timestamps on a regular time grid described by start timestamp and step", {"Array(DateTime64, Float64"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT timeSeriesGrid('2025-06-01 00:00:00'::DateTime64(3), 30, [10, 20, NULL, 30])", "[('2025-06-01 00:00:00.000',10),('2025-06-01 00:00:30.000',20),('2025-06-01 00:01:30.000',30)]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesGrid>(documentation);
}

}
