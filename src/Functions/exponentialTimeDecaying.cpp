#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeExponentialTimeDecayingFloat64.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

#include <algorithm>
#include <cmath>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int UNKNOWN_FUNCTION;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_time_decay_aggregate_functions;
}

namespace
{

void assertExperimentalFeatureEnabled(const ContextPtr & context, const String & function_name)
{
    if (context && !context->getSettingsRef()[Setting::allow_experimental_time_decay_aggregate_functions])
        throw Exception(
            ErrorCodes::UNKNOWN_FUNCTION,
            "Function {} is experimental and disabled by default. Enable it with setting "
            "allow_experimental_time_decay_aggregate_functions",
            function_name);
}

void assertDecayingType(const DataTypePtr & type, const String & function_name, size_t argument)
{
    if (!isExponentialTimeDecayingFloat64(type))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument {} of function {} must be ExponentialTimeDecayingFloat64, got {}",
            argument,
            function_name,
            type->getName());
}

void assertTimeType(const DataTypePtr & type, const String & function_name)
{
    if (!isNumber(type) && !isDateTime(type) && !isDateTime64(type))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Time argument of function {} must be a number, DateTime, or DateTime64, got {}",
            function_name,
            type->getName());
}

struct DecayingColumnView
{
    const ColumnFloat64 & value;
    const ColumnFloat64 & time;
    const ColumnFloat64 & stored_decay_length;
    Float64 decay_length;
};

DecayingColumnView getDecayingColumnView(const ColumnPtr & column, const DataTypePtr & type)
{
    const auto decay_length = tryGetExponentialTimeDecayingFloat64DecayLength(type);
    chassert(decay_length);

    const auto & tuple = assert_cast<const ColumnTuple &>(*column);
    return {
        assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)),
        assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)),
        assert_cast<const ColumnFloat64 &>(tuple.getColumn(2)),
        *decay_length,
    };
}

bool isEmptyRow(const DecayingColumnView & input, size_t row)
{
    return input.value.getData()[row] == 0 && std::isnan(input.time.getData()[row]);
}

void assertValidRow(const DecayingColumnView & input, size_t row, const String & function_name)
{
    const Float64 stored_decay_length = input.stored_decay_length.getData()[row];
    if (!std::isfinite(stored_decay_length) || stored_decay_length != input.decay_length)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Stored decay length {} does not match type decay length {} in function {}",
            stored_decay_length,
            input.decay_length,
            function_name);

    if (isEmptyRow(input, row))
        return;

    const Float64 value = input.value.getData()[row];
    const Float64 time = input.time.getData()[row];
    if (!std::isfinite(value))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Value of function {} must be finite", function_name);
    if (!std::isfinite(time))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time of function {} must be finite", function_name);
}

Float64 valueAt(const DecayingColumnView & input, size_t row, Float64 target_time)
{
    if (isEmptyRow(input, row))
        return 0;

    const Float64 value = input.value.getData()[row];
    const Float64 time = input.time.getData()[row];

    if (target_time == time)
        return value;

    return value * std::exp((time - target_time) / input.decay_length);
}

struct DecayingColumnBuilder
{
    void append(Float64 value_value, Float64 time_value, Float64 decay_length_value)
    {
        value->insertValue(value_value);
        time->insertValue(time_value);
        decay_length->insertValue(decay_length_value);
    }

    ColumnPtr build()
    {
        return ColumnTuple::create(Columns{std::move(value), std::move(time), std::move(decay_length)});
    }

    ColumnFloat64::MutablePtr value = ColumnFloat64::create();
    ColumnFloat64::MutablePtr time = ColumnFloat64::create();
    ColumnFloat64::MutablePtr decay_length = ColumnFloat64::create();
};

class FunctionExponentialTimeDecayingAdd final : public IFunction
{
public:
    static constexpr auto name = "exponentialTimeDecayingAdd";
    static FunctionPtr create(ContextPtr context)
    {
        assertExperimentalFeatureEnabled(context, name);
        return std::make_shared<FunctionExponentialTimeDecayingAdd>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        assertDecayingType(arguments[0].type, getName(), 1);
        assertDecayingType(arguments[1].type, getName(), 2);

        const Float64 left_decay_length = *tryGetExponentialTimeDecayingFloat64DecayLength(arguments[0].type);
        const Float64 right_decay_length = *tryGetExponentialTimeDecayingFloat64DecayLength(arguments[1].type);
        if (left_decay_length != right_decay_length)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function {} cannot add values with different decay lengths: {} and {}",
                getName(),
                left_decay_length,
                right_decay_length);

        return createDataTypeExponentialTimeDecayingFloat64(left_decay_length);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto left_column = arguments[0].column->convertToFullColumnIfConst();
        auto right_column = arguments[1].column->convertToFullColumnIfConst();
        const auto left = getDecayingColumnView(left_column, arguments[0].type);
        const auto right = getDecayingColumnView(right_column, arguments[1].type);
        DecayingColumnBuilder result;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            assertValidRow(left, row, getName());
            assertValidRow(right, row, getName());

            if (isEmptyRow(left, row))
            {
                result.append(
                    right.value.getData()[row],
                    right.time.getData()[row],
                    left.decay_length);
                continue;
            }
            if (isEmptyRow(right, row))
            {
                result.append(
                    left.value.getData()[row],
                    left.time.getData()[row],
                    left.decay_length);
                continue;
            }

            const Float64 latest_time = std::max(left.time.getData()[row], right.time.getData()[row]);
            result.append(
                valueAt(left, row, latest_time) + valueAt(right, row, latest_time),
                latest_time,
                left.decay_length);
        }

        return result.build();
    }
};

class FunctionExponentialTimeDecayingValueAt final : public IFunction
{
public:
    static constexpr auto name = "exponentialTimeDecayingValueAt";
    static FunctionPtr create(ContextPtr context)
    {
        assertExperimentalFeatureEnabled(context, name);
        return std::make_shared<FunctionExponentialTimeDecayingValueAt>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        assertDecayingType(arguments[0].type, getName(), 1);
        assertTimeType(arguments[1].type, getName());
        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto input_column = arguments[0].column->convertToFullColumnIfConst();
        auto target_time_column = arguments[1].column->convertToFullColumnIfConst();
        const auto input = getDecayingColumnView(input_column, arguments[0].type);
        auto result = ColumnFloat64::create(input_rows_count, 0.0);
        auto & result_data = result->getData();

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            assertValidRow(input, row, getName());
            if (!std::isfinite(target_time_column->getFloat64(row)))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Target time of function {} must be finite", getName());
            result_data[row] = valueAt(input, row, target_time_column->getFloat64(row));
        }
        return result;
    }
};

class FunctionExponentialTimeDecayingDecayLength final : public IFunction
{
public:
    static constexpr auto name = "exponentialTimeDecayingDecayLength";
    static FunctionPtr create(ContextPtr context)
    {
        assertExperimentalFeatureEnabled(context, name);
        return std::make_shared<FunctionExponentialTimeDecayingDecayLength>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        assertDecayingType(arguments[0].type, getName(), 1);
        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const Float64 decay_length = *tryGetExponentialTimeDecayingFloat64DecayLength(arguments[0].type);
        return ColumnFloat64::create(input_rows_count, decay_length);
    }
};

}

REGISTER_FUNCTION(ExponentialTimeDecaying)
{
    factory.registerFunction<FunctionExponentialTimeDecayingAdd>(FunctionDocumentation{
        .description = R"(
Adds two exponentially time-decaying values at their greatest anchor time.
Both inputs must have identical decay lengths encoded in their types. The function rebases them to
`ct = greatest(A.time, B.time)` and returns `(A.value_at(ct) + B.value_at(ct), ct)`.
Because values are stored as `Float64`, large signed values that nearly cancel can be sensitive to
addition order and grouping. Normalize magnitudes or use a numerically stable method to pre-aggregate
sensitive inputs when stronger numerical reproducibility is required.
)",
        .syntax = "exponentialTimeDecayingAdd(a, b)",
        .arguments = {
            {"a", "First value of type `ExponentialTimeDecayingFloat64(decay_length)`.", {}},
            {"b", "Second value with the same parameterized type.", {}}},
        .returned_value = {"Returns the combined `ExponentialTimeDecayingFloat64(decay_length)` value.", {}},
        .examples = {{
            "Add values with the same decay length",
            "SELECT exponentialTimeDecayingAdd("
            "exponentialTimeDecayingFloat64(10)(2.718281828459045, toFloat64(0)), "
            "exponentialTimeDecayingFloat64(10)(4, toFloat64(10)))",
            "(5,10,10)"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});

    factory.registerFunction<FunctionExponentialTimeDecayingValueAt>(FunctionDocumentation{
        .description = R"(
Evaluates an exponentially time-decaying value at any target time by extrapolating the exponential curve from its anchor.
Numeric, DateTime, and DateTime64 targets are converted to seconds, so `now()` and `now64()` can be used.
)",
        .syntax = "exponentialTimeDecayingValueAt(value, target_time)",
        .arguments = {
            {"value", "Value of type `ExponentialTimeDecayingFloat64(decay_length)`.", {}},
            {"target_time", "Evaluation time; it may be before, at, or after the anchor.",
                {"(U)Int*", "Float*", "Decimal", "DateTime", "DateTime64"}}},
        .returned_value = {"Returns the decayed value at the target time.", {"Float64"}},
        .examples = {{
            "Evaluate one decay length later",
            "SELECT round(exponentialTimeDecayingValueAt(exponentialTimeDecayingFloat64(10)(8, toFloat64(0)), toFloat64(10)), 6)",
            "2.943036"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});

    factory.registerFunction<FunctionExponentialTimeDecayingDecayLength>(FunctionDocumentation{
        .description = "Returns the decay length encoded in an `ExponentialTimeDecayingFloat64` type.",
        .syntax = "exponentialTimeDecayingDecayLength(value)",
        .arguments = {{"value", "Value of type `ExponentialTimeDecayingFloat64(decay_length)`.", {}}},
        .returned_value = {"Returns the decay length.", {"Float64"}},
        .examples = {{
            "Read the decay length",
            "SELECT exponentialTimeDecayingDecayLength(exponentialTimeDecayingFloat64(10)(1, toFloat64(0)))",
            "10"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});
}

}
