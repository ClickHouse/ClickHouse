#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeExponentialTimeDecayingFloat64.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

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
    const IColumn & time;
    const ColumnFloat64 & half_life;
};

DecayingColumnView getDecayingColumnView(const ColumnPtr & column)
{
    const auto & tuple = assert_cast<const ColumnTuple &>(*column);
    return {
        assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)),
        tuple.getColumn(1),
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

void assertMatchingTimeType(const DataTypePtr & decaying_type, const DataTypePtr & target_type, const String & function_name)
{
    const auto & time_type = getExponentialTimeDecayingFloat64TimeType(decaying_type);
    if (!time_type->equals(*target_type))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Target time of function {} must have type {}, got {}",
            function_name,
            time_type->getName(),
            target_type->getName());
}

void assertValidRow(const DecayingColumnView & input, size_t row, const String & function_name)
{
    const Float64 value = input.value.getData()[row];
    const Float64 time = input.time.getFloat64(row);
    const Float64 half_life = input.half_life.getData()[row];
    if (!std::isfinite(value) || value < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Value of function {} must be finite and non-negative", function_name);
    if (!std::isfinite(time))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time of function {} must be finite", function_name);
    if (!std::isfinite(half_life) || half_life <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Half-life of function {} must be finite and positive", function_name);
}

Float64 valueAt(const DecayingColumnView & input, size_t row, Float64 target_time)
{
    return input.value.getData()[row]
        * std::exp2((input.time.getFloat64(row) - target_time) / input.half_life.getData()[row]);
}

struct DecayingColumnBuilder
{
    explicit DecayingColumnBuilder(const DataTypePtr & time_type)
        : time(time_type->createColumn())
        , value(ColumnFloat64::create())
        , half_life(ColumnFloat64::create())
    {
    }

    void append(Float64 value_value, const IColumn & source_time, size_t source_row, Float64 half_life_value)
    {
        value->insertValue(value_value);
        time->insertFrom(source_time, source_row);
        half_life->insertValue(half_life_value);
    }

    ColumnPtr build()
    {
        return ColumnTuple::create(Columns{std::move(value), std::move(time), std::move(half_life)});
    }

    MutableColumnPtr time;
    ColumnFloat64::MutablePtr value;
    ColumnFloat64::MutablePtr half_life;
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
        if (!isNumber(arguments[1].type) && !isDateTime(arguments[1].type) && !isDateTime64(arguments[1].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Time argument of function {} must be a number, DateTime, or DateTime64",
                getName());
        if (!isNumber(arguments[2].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Half-life argument of function {} must be a number", getName());

        return createDataTypeExponentialTimeDecayingFloat64(arguments[1].type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto float64_type = std::make_shared<DataTypeFloat64>();
        auto value = castColumn(arguments[0], float64_type)->convertToFullColumnIfConst();
        auto time = arguments[1].column->convertToFullColumnIfConst();
        auto half_life = castColumn(arguments[2], float64_type)->convertToFullColumnIfConst();

        const auto & value_data = assert_cast<const ColumnFloat64 &>(*value).getData();
        const auto & half_life_data = assert_cast<const ColumnFloat64 &>(*half_life).getData();
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            if (!std::isfinite(value_data[row]) || value_data[row] < 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Value of function {} must be finite and non-negative", getName());
            if (!std::isfinite(half_life_data[row]) || half_life_data[row] <= 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Half-life of function {} must be finite and positive", getName());
            if (!std::isfinite(time->getFloat64(row)))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time of function {} must be finite", getName());
        }

        return ColumnTuple::create(Columns{std::move(value), std::move(time), std::move(half_life)});
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
        const auto & left_time_type = getExponentialTimeDecayingFloat64TimeType(arguments[0].type);
        const auto & right_time_type = getExponentialTimeDecayingFloat64TimeType(arguments[1].type);
        if (!left_time_type->equals(*right_time_type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Arguments of function {} must use the same time type, got {} and {}",
                getName(),
                left_time_type->getName(),
                right_time_type->getName());
        return arguments[0].type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto left_column = arguments[0].column->convertToFullColumnIfConst();
        auto right_column = arguments[1].column->convertToFullColumnIfConst();
        const auto left = getDecayingColumnView(left_column);
        const auto right = getDecayingColumnView(right_column);
        const auto & time_type = getExponentialTimeDecayingFloat64TimeType(arguments[0].type);
        DecayingColumnBuilder result(time_type);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            assertValidRow(left, row, getName());
            assertValidRow(right, row, getName());
            const Float64 left_half_life = left.half_life.getData()[row];
            const Float64 right_half_life = right.half_life.getData()[row];
            if (left_half_life != right_half_life)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Function {} cannot add values with different half-lives: {} and {}",
                    getName(),
                    left_half_life,
                    right_half_life);

            const bool left_is_latest = left.time.compareAt(row, row, right.time, 1) >= 0;
            const IColumn & latest_time_column = left_is_latest ? left.time : right.time;
            const Float64 latest_time = latest_time_column.getFloat64(row);
            result.append(
                valueAt(left, row, latest_time) + valueAt(right, row, latest_time),
                latest_time_column,
                row,
                left_half_life);
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
        assertMatchingTimeType(arguments[0].type, arguments[1].type, getName());
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
            if (target_time_column->compareAt(row, row, input.time, 1) < 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Target time of function {} must not precede the anchor time", getName());
            result_data[row] = valueAt(input, row, target_time_column->getFloat64(row));
        }
        return result;
    }
};

class FunctionExponentialTimeDecayingRebase final : public IFunction
{
public:
    static constexpr auto name = "exponentialTimeDecayingRebase";
    static FunctionPtr create(ContextPtr context)
    {
        assertExperimentalFeatureEnabled(context, name);
        return std::make_shared<FunctionExponentialTimeDecayingRebase>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        assertDecayingType(arguments[0].type, getName(), 1);
        assertMatchingTimeType(arguments[0].type, arguments[1].type, getName());
        return arguments[0].type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto input_column = arguments[0].column->convertToFullColumnIfConst();
        auto target_time_column = arguments[1].column->convertToFullColumnIfConst();
        const auto input = getDecayingColumnView(input_column);
        const auto & time_type = getExponentialTimeDecayingFloat64TimeType(arguments[0].type);
        DecayingColumnBuilder result(time_type);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            assertValidRow(input, row, getName());
            if (!std::isfinite(target_time_column->getFloat64(row)))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Target time of function {} must be finite", getName());
            if (target_time_column->compareAt(row, row, input.time, 1) < 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Target time of function {} must not precede the anchor time", getName());
            result.append(
                valueAt(input, row, target_time_column->getFloat64(row)),
                *target_time_column,
                row,
                input.half_life.getData()[row]);
        }
        return result.build();
    }
};

}

REGISTER_FUNCTION(ExponentialTimeDecaying)
{
    factory.registerFunction<FunctionExponentialTimeDecayingFloat64>(FunctionDocumentation{
        .description = R"(
Constructs an `ExponentialTimeDecayingFloat64` value anchored at `time`.
The value must be finite and non-negative, and the half-life must be finite and positive.
)",
        .syntax = "exponentialTimeDecayingFloat64(value, time, half_life)",
        .arguments = {
            {"value", "Value at the anchor time.", {"(U)Int*", "Float*", "Decimal"}},
            {"time", "Anchor time.", {"(U)Int*", "Float*", "Decimal", "DateTime", "DateTime64"}},
            {"half_life", "Half-life in the time argument's units; seconds for DateTime and DateTime64.",
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
Both inputs must have identical half-lives. The function rebases them to
`ct = greatest(A.time, B.time)` and returns `(A.value_at(ct) + B.value_at(ct), ct, A.half_life)`.
)",
        .syntax = "exponentialTimeDecayingAdd(a, b)",
        .arguments = {
            {"a", "First decaying value.", {"ExponentialTimeDecayingFloat64"}},
            {"b", "Second decaying value with the same time type and half-life.", {"ExponentialTimeDecayingFloat64"}}},
        .returned_value = {"Returns the combined decaying value.", {"ExponentialTimeDecayingFloat64"}},
        .examples = {{
            "Add values with the same half-life",
            "SELECT exponentialTimeDecayingAdd("
            "exponentialTimeDecayingFloat64(8, toFloat64(0), 10), "
            "exponentialTimeDecayingFloat64(4, toFloat64(10), 10))",
            "(8,10,10)"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});

    factory.registerFunction<FunctionExponentialTimeDecayingValueAt>(FunctionDocumentation{
        .description = R"(
Evaluates an exponentially time-decaying value at its anchor time or a later target time.
For a value using `DateTime` as its time type, `now()` can be passed as the target.
For `DateTime64`, use `now64()` with the matching scale and time zone.
)",
        .syntax = "exponentialTimeDecayingValueAt(value, target_time)",
        .arguments = {
            {"value", "Decaying value.", {"ExponentialTimeDecayingFloat64"}},
            {"target_time", "Evaluation time at or after the anchor, with the type used by the decaying value.",
                {"Number", "DateTime", "DateTime64"}}},
        .returned_value = {"Returns the decayed value at the target time.", {"Float64"}},
        .examples = {{
            "Evaluate one half-life later",
            "SELECT exponentialTimeDecayingValueAt(exponentialTimeDecayingFloat64(8, toFloat64(0), 10), toFloat64(10))",
            "4"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});

    factory.registerFunction<FunctionExponentialTimeDecayingRebase>(FunctionDocumentation{
        .description = "Re-anchors a decaying value at its anchor time or a later time and recalculates its value.",
        .syntax = "exponentialTimeDecayingRebase(value, target_time)",
        .arguments = {
            {"value", "Decaying value.", {"ExponentialTimeDecayingFloat64"}},
            {"target_time", "New anchor at or after the current anchor, with the type used by the decaying value.",
                {"Number", "DateTime", "DateTime64"}}},
        .returned_value = {"Returns a decaying value anchored at the target time.", {"ExponentialTimeDecayingFloat64"}},
        .examples = {{
            "Rebase one half-life later",
            "SELECT exponentialTimeDecayingRebase(exponentialTimeDecayingFloat64(8, toFloat64(0), 10), toFloat64(10))",
            "(4,10,10)"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});
}

}
