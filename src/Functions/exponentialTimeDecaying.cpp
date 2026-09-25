#include <Columns/ColumnExponentialTimeDecaying.h>
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
            "Argument {} of function {} must be ExponentialTimeDecaying, got {}",
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
    const ColumnFloat64 & value_at_anchor;
    const ColumnFloat64 & anchor_time;
    Float64 decay_length;
};

DecayingColumnView getDecayingColumnView(const ColumnPtr & column, const DataTypePtr & type)
{
    const auto decay_length = tryGetExponentialTimeDecayingFloat64DecayLength(type);
    chassert(decay_length);

    const auto & decaying = assert_cast<const ColumnExponentialTimeDecaying &>(*column);
    const auto & tuple = decaying.getStorageTuple();
    return {
        assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)),
        assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)),
        *decay_length,
    };
}

bool isEmptyRow(const DecayingColumnView & input, size_t row)
{
    return input.value_at_anchor.getData()[row] == 0;
}

void assertValidRow(const DecayingColumnView & input, size_t row, const String & function_name)
{
    const Float64 value = input.value_at_anchor.getData()[row];
    const Float64 time = input.anchor_time.getData()[row];
    if (!std::isfinite(value) || !std::isfinite(time))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Argument of function {} is not a canonical ExponentialTimeDecaying value",
            function_name);
}

Float64 valueAt(const DecayingColumnView & input, size_t row, Float64 target_time)
{
    if (isEmptyRow(input, row))
        return 0;

    const Float64 value = input.value_at_anchor.getData()[row];
    const Float64 anchor_time = input.anchor_time.getData()[row];
    if (target_time == anchor_time)
        return value;

    return value * std::exp((anchor_time - target_time) / input.decay_length);
}

struct DecayingColumnBuilder
{
    explicit DecayingColumnBuilder(Float64 decay_length_)
        : decay_length(decay_length_)
    {
    }

    void append(Float64 value, Float64 time)
    {
        const auto normalized = normalizeExponentialTimeDecayingFloat64(value, time, decay_length);
        value_at_anchor->insertValue(normalized.value_at_anchor);
        anchor_time->insertValue(normalized.anchor_time);
    }

    ColumnPtr build()
    {
        auto tuple = ColumnTuple::create(
            Columns{std::move(value_at_anchor), std::move(anchor_time)});
        return ColumnExponentialTimeDecaying::create(tuple->assumeMutable(), decay_length);
    }

    const Float64 decay_length;
    ColumnFloat64::MutablePtr value_at_anchor = ColumnFloat64::create();
    ColumnFloat64::MutablePtr anchor_time = ColumnFloat64::create();
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

        return arguments[0].type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto left_column = arguments[0].column->convertToFullColumnIfConst();
        auto right_column = arguments[1].column->convertToFullColumnIfConst();
        const auto left = getDecayingColumnView(left_column, arguments[0].type);
        const auto right = getDecayingColumnView(right_column, arguments[1].type);
        DecayingColumnBuilder result(left.decay_length);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            assertValidRow(left, row, getName());
            assertValidRow(right, row, getName());

            if (isEmptyRow(left, row))
            {
                result.append(
                    right.value_at_anchor.getData()[row],
                    right.anchor_time.getData()[row]);
                continue;
            }
            if (isEmptyRow(right, row))
            {
                result.append(
                    left.value_at_anchor.getData()[row],
                    left.anchor_time.getData()[row]);
                continue;
            }

            const Float64 latest_time = std::max(
                left.anchor_time.getData()[row],
                right.anchor_time.getData()[row]);
            result.append(
                valueAt(left, row, latest_time) + valueAt(right, row, latest_time),
                latest_time);
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
Adds two exponentially time-decaying values using their direct value/anchor payloads.
Both inputs must have identical decay lengths encoded in their types. The function rebases the older direct payload to
`ct = greatest(A.anchor_time, B.anchor_time)` and stores `A.value_at(ct) + B.value_at(ct)` at that anchor.
Because values are stored as `Float64`, large signed values that nearly cancel can be sensitive to
addition order and grouping. Normalize magnitudes or use a numerically stable method to pre-aggregate
sensitive inputs when stronger numerical reproducibility is required.
)",
        .syntax = "exponentialTimeDecayingAdd(a, b)",
        .arguments = {
            {"a", "First value of type `ExponentialTimeDecaying(decay_length)`.", {}},
            {"b", "Second value with the same parameterized type.", {}}},
        .returned_value = {"Returns the combined `ExponentialTimeDecaying(decay_length)` value.", {}},
        .examples = {{
            "Add values with the same decay length",
            "SELECT round(exponentialTimeDecayingValueAt(exponentialTimeDecayingAdd("
            "exponentialTimeDecaying(10)(2.718281828459045, toFloat64(0)), "
            "exponentialTimeDecaying(10)(4, toFloat64(10))), toFloat64(10)), 6) "
            "SETTINGS allow_experimental_time_decay_aggregate_functions = 1",
            "5"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});

    factory.registerFunction<FunctionExponentialTimeDecayingValueAt>(FunctionDocumentation{
        .description = R"(
Evaluates an exponentially time-decaying value at any target time from its stored direct value and anchor.
Numeric, DateTime, and DateTime64 targets are converted to seconds, so `now()` and `now64()` can be used.
)",
        .syntax = "exponentialTimeDecayingValueAt(value, target_time)",
        .arguments = {
            {"value", "Value of type `ExponentialTimeDecaying(decay_length)`.", {}},
            {"target_time", "Evaluation time; it may be before, at, or after the normalization time.",
                {"(U)Int*", "Float*", "Decimal", "DateTime", "DateTime64"}}},
        .returned_value = {"Returns the decayed value at the target time.", {"Float64"}},
        .examples = {{
            "Evaluate one decay length later",
            "SELECT round(exponentialTimeDecayingValueAt(exponentialTimeDecaying(10)(8, toFloat64(0)), toFloat64(10)), 6) "
            "SETTINGS allow_experimental_time_decay_aggregate_functions = 1",
            "2.943036"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});

    factory.registerFunction<FunctionExponentialTimeDecayingDecayLength>(FunctionDocumentation{
        .description = "Returns the decay length encoded in an `ExponentialTimeDecaying` type.",
        .syntax = "exponentialTimeDecayingDecayLength(value)",
        .arguments = {{"value", "Value of type `ExponentialTimeDecaying(decay_length)`.", {}}},
        .returned_value = {"Returns the decay length.", {"Float64"}},
        .examples = {{
            "Read the decay length",
            "SELECT exponentialTimeDecayingDecayLength(exponentialTimeDecaying(10)(1, toFloat64(0))) "
            "SETTINGS allow_experimental_time_decay_aggregate_functions = 1",
            "10"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});
}

}
