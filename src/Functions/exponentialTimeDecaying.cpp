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

struct DecayingColumnView
{
    const ColumnFloat64 & value;
    const ColumnFloat64 & time;
    const ColumnFloat64 & decay_length;
};

DecayingColumnView getDecayingColumnView(const ColumnPtr & column)
{
    const auto & tuple = assert_cast<const ColumnTuple &>(*column);
    return {
        assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)),
        assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)),
        assert_cast<const ColumnFloat64 &>(tuple.getColumn(2)),
    };
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

void assertValidRow(const DecayingColumnView & input, size_t row, const String & function_name)
{
    const Float64 value = input.value.getData()[row];
    const Float64 time = input.time.getData()[row];
    const Float64 decay_length = input.decay_length.getData()[row];
    if (!std::isfinite(value))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Value of function {} must be finite", function_name);
    if (!std::isfinite(time))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time of function {} must be finite", function_name);
    if (!std::isfinite(decay_length) || decay_length <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Decay length of function {} must be finite and positive", function_name);
}

Float64 valueAt(const DecayingColumnView & input, size_t row, Float64 target_time)
{
    const Float64 value = input.value.getData()[row];
    const Float64 time = input.time.getData()[row];

    if (target_time == time)
        return value;

    return value * std::exp((time - target_time) / input.decay_length.getData()[row]);
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

class FunctionExponentialTimeDecayingFloat64 final : public IFunction
{
public:
    static constexpr auto name = "exponentialTimeDecayingFloat64";
    static FunctionPtr create(ContextPtr context)
    {
        assertExperimentalFeatureEnabled(context, name);
        return std::make_shared<FunctionExponentialTimeDecayingFloat64>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isNumber(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Value argument of function {} must be a number", getName());
        assertTimeType(arguments[1].type, getName());
        if (!isNumber(arguments[2].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Decay length argument of function {} must be a number", getName());

        return createDataTypeExponentialTimeDecayingFloat64();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto float64_type = std::make_shared<DataTypeFloat64>();
        auto value = castColumn(arguments[0], float64_type)->convertToFullColumnIfConst();
        auto decay_length = castColumn(arguments[2], float64_type)->convertToFullColumnIfConst();
        auto time = ColumnFloat64::create(input_rows_count, 0.0);

        const auto & value_data = assert_cast<const ColumnFloat64 &>(*value).getData();
        const auto & decay_length_data = assert_cast<const ColumnFloat64 &>(*decay_length).getData();
        auto & time_data = time->getData();
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            if (!std::isfinite(value_data[row]))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Value of function {} must be finite", getName());
            if (!std::isfinite(decay_length_data[row]) || decay_length_data[row] <= 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Decay length of function {} must be finite and positive", getName());
            time_data[row] = arguments[1].column->getFloat64(row);
            if (!std::isfinite(time_data[row]))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time of function {} must be finite", getName());
        }

        return ColumnTuple::create(Columns{std::move(value), std::move(time), std::move(decay_length)});
    }
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

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        assertDecayingType(arguments[0].type, getName(), 1);
        assertDecayingType(arguments[1].type, getName(), 2);
        return arguments[0].type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto left_column = arguments[0].column->convertToFullColumnIfConst();
        auto right_column = arguments[1].column->convertToFullColumnIfConst();
        const auto left = getDecayingColumnView(left_column);
        const auto right = getDecayingColumnView(right_column);
        DecayingColumnBuilder result;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            assertValidRow(left, row, getName());
            assertValidRow(right, row, getName());
            const Float64 left_decay_length = left.decay_length.getData()[row];
            const Float64 right_decay_length = right.decay_length.getData()[row];
            if (left_decay_length != right_decay_length)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Function {} cannot add values with different decay lengths: {} and {}",
                    getName(),
                    left_decay_length,
                    right_decay_length);

            const Float64 latest_time = std::max(left.time.getData()[row], right.time.getData()[row]);
            result.append(
                valueAt(left, row, latest_time) + valueAt(right, row, latest_time),
                latest_time,
                left_decay_length);
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
        const auto input = getDecayingColumnView(input_column);
        auto result = ColumnFloat64::create(input_rows_count, 0.0);
        auto & result_data = result->getData();

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            assertValidRow(input, row, getName());
            if (!std::isfinite(target_time_column->getFloat64(row)))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Target time of function {} must be finite", getName());
            if (target_time_column->getFloat64(row) < input.time.getData()[row])
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Target time of function {} must not precede the anchor time", getName());
            result_data[row] = valueAt(input, row, target_time_column->getFloat64(row));
        }
        return result;
    }
};

}

REGISTER_FUNCTION(ExponentialTimeDecaying)
{
    factory.registerFunction<FunctionExponentialTimeDecayingFloat64>(FunctionDocumentation{
        .description = R"(
Constructs an `ExponentialTimeDecayingFloat64` value anchored at `time`.
The value must be finite, and the decay length must be finite and positive.
The anchor is stored as `Float64`; DateTime and DateTime64 inputs are converted to seconds.
)",
        .syntax = "exponentialTimeDecayingFloat64(value, time, decay_length)",
        .arguments = {
            {"value", "Value at the anchor time.", {"(U)Int*", "Float*", "Decimal"}},
            {"time", "Anchor time.", {"(U)Int*", "Float*", "Decimal", "DateTime", "DateTime64"}},
            {"decay_length", "Time difference required for the value to decay to `1/e`; seconds for DateTime and DateTime64.",
                {"(U)Int*", "Float*", "Decimal"}}},
        .returned_value = {"Returns an exponentially time-decaying value.", {"ExponentialTimeDecayingFloat64"}},
        .examples = {{
            "Construct a value",
            "SELECT exponentialTimeDecayingFloat64(8, toFloat64(0), 10)",
            "(8,0,10)"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});

    factory.registerFunction<FunctionExponentialTimeDecayingAdd>(FunctionDocumentation{
        .description = R"(
Adds two exponentially time-decaying values at their greatest anchor time.
Both inputs must have identical decay lengths. The function rebases them to
`ct = greatest(A.time, B.time)` and returns `(A.value_at(ct) + B.value_at(ct), ct, A.decay_length)`.
Because values are stored as `Float64`, large signed values that nearly cancel can be sensitive to
addition order and grouping. Normalize magnitudes or use a numerically stable method to pre-aggregate
sensitive inputs when stronger numerical reproducibility is required.
)",
        .syntax = "exponentialTimeDecayingAdd(a, b)",
        .arguments = {
            {"a", "First decaying value.", {"ExponentialTimeDecayingFloat64"}},
            {"b", "Second decaying value with the same decay length.", {"ExponentialTimeDecayingFloat64"}}},
        .returned_value = {"Returns the combined decaying value.", {"ExponentialTimeDecayingFloat64"}},
        .examples = {{
            "Add values with the same decay length",
            "SELECT exponentialTimeDecayingAdd("
            "exponentialTimeDecayingFloat64(2.718281828459045, toFloat64(0), 10), "
            "exponentialTimeDecayingFloat64(4, toFloat64(10), 10))",
            "(5,10,10)"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});

    factory.registerFunction<FunctionExponentialTimeDecayingValueAt>(FunctionDocumentation{
        .description = R"(
Evaluates an exponentially time-decaying value at its anchor time or a later target time.
Numeric, DateTime, and DateTime64 targets are converted to seconds, so `now()` and `now64()` can be used.
)",
        .syntax = "exponentialTimeDecayingValueAt(value, target_time)",
        .arguments = {
            {"value", "Decaying value.", {"ExponentialTimeDecayingFloat64"}},
            {"target_time", "Evaluation time at or after the anchor.",
                {"Number", "DateTime", "DateTime64"}}},
        .returned_value = {"Returns the decayed value at the target time.", {"Float64"}},
        .examples = {{
            "Evaluate one decay length later",
            "SELECT round(exponentialTimeDecayingValueAt(exponentialTimeDecayingFloat64(8, toFloat64(0), 10), toFloat64(10)), 6)",
            "2.943036"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});
}

}
