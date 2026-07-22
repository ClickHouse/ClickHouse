#include <Columns/ColumnArray.h>
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
#include <limits>
#include <map>

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
    const IColumn & time;
    const ColumnArray & components;
    const ColumnFloat64 & component_value;
    const ColumnFloat64 & component_half_life;
};

DecayingColumnView getDecayingColumnView(const ColumnPtr & column)
{
    const auto & tuple = assert_cast<const ColumnTuple &>(*column);
    const auto & components = assert_cast<const ColumnArray &>(tuple.getColumn(3));
    const auto & component_tuple = assert_cast<const ColumnTuple &>(components.getData());
    return {
        tuple.getColumn(1),
        components,
        assert_cast<const ColumnFloat64 &>(component_tuple.getColumn(0)),
        assert_cast<const ColumnFloat64 &>(component_tuple.getColumn(1)),
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

std::pair<size_t, size_t> getComponentRange(const ColumnArray & components, size_t row)
{
    const auto & offsets = components.getOffsets();
    return {row == 0 ? 0 : offsets[row - 1], offsets[row]};
}

void addRebasedComponents(
    std::map<Float64, Float64> & values_by_half_life,
    const DecayingColumnView & input,
    size_t row,
    Float64 target_time)
{
    const Float64 source_time = input.time.getFloat64(row);
    const auto [begin, end] = getComponentRange(input.components, row);
    const auto & values = input.component_value.getData();
    const auto & half_lives = input.component_half_life.getData();

    for (size_t index = begin; index < end; ++index)
    {
        const Float64 half_life = half_lives[index];
        if (!std::isfinite(half_life) || half_life <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Decay component half-life must be finite and positive");
        if (!std::isfinite(values[index]) || values[index] < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Decay component value must be finite and non-negative");
        const Float64 value = values[index] * std::exp2((source_time - target_time) / half_life);
        values_by_half_life[half_life] += value;
    }
}

struct DecayingColumnBuilder
{
    explicit DecayingColumnBuilder(const DataTypePtr & time_type)
        : time(time_type->createColumn())
        , value(ColumnFloat64::create())
        , half_life(ColumnFloat64::create())
        , component_value(ColumnFloat64::create())
        , component_half_life(ColumnFloat64::create())
        , component_offsets(ColumnArray::ColumnOffsets::create())
    {
    }

    void append(const IColumn & source_time, size_t source_row, const std::map<Float64, Float64> & values_by_half_life)
    {
        Float64 total_value = 0;
        Float64 weighted_half_life = 0;
        for (const auto & [component_half_life_value, component_value_value] : values_by_half_life)
        {
            component_value->insertValue(component_value_value);
            component_half_life->insertValue(component_half_life_value);
            total_value += component_value_value;
            weighted_half_life += component_half_life_value * component_value_value;
        }

        value->insertValue(total_value);
        half_life->insertValue(
            total_value > 0
                ? weighted_half_life / total_value
                : std::numeric_limits<Float64>::quiet_NaN());
        time->insertFrom(source_time, source_row);
        component_offsets->insertValue(component_value->size());
    }

    ColumnPtr build()
    {
        auto component_tuple = ColumnTuple::create(
            Columns{std::move(component_value), std::move(component_half_life)});
        auto component_array = ColumnArray::create(std::move(component_tuple), std::move(component_offsets));
        return ColumnTuple::create(
            Columns{std::move(value), std::move(time), std::move(half_life), std::move(component_array)});
    }

    MutableColumnPtr time;
    ColumnFloat64::MutablePtr value;
    ColumnFloat64::MutablePtr half_life;
    ColumnFloat64::MutablePtr component_value;
    ColumnFloat64::MutablePtr component_half_life;
    ColumnArray::ColumnOffsets::MutablePtr component_offsets;
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

        auto offsets = ColumnArray::ColumnOffsets::create(input_rows_count);
        auto & offsets_data = offsets->getData();
        for (size_t row = 0; row < input_rows_count; ++row)
            offsets_data[row] = row + 1;

        auto components = ColumnArray::create(
            ColumnTuple::create(Columns{value, half_life}),
            std::move(offsets));
        return ColumnTuple::create(Columns{std::move(value), std::move(time), std::move(half_life), std::move(components)});
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
            if (!std::isfinite(left.time.getFloat64(row)) || !std::isfinite(right.time.getFloat64(row)))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Times of function {} must be finite", getName());
            const bool left_is_latest = left.time.compareAt(row, row, right.time, 1) >= 0;
            const IColumn & latest_time_column = left_is_latest ? left.time : right.time;
            const Float64 latest_time = latest_time_column.getFloat64(row);

            std::map<Float64, Float64> values_by_half_life;
            addRebasedComponents(values_by_half_life, left, row, latest_time);
            addRebasedComponents(values_by_half_life, right, row, latest_time);
            result.append(latest_time_column, row, values_by_half_life);
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
            if (!std::isfinite(target_time_column->getFloat64(row)))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Target time of function {} must be finite", getName());
            if (target_time_column->compareAt(row, row, input.time, 1) < 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Target time of function {} must not precede the anchor time", getName());
            std::map<Float64, Float64> values_by_half_life;
            addRebasedComponents(values_by_half_life, input, row, target_time_column->getFloat64(row));
            for (const auto & component : values_by_half_life)
                result_data[row] += component.second;
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
            if (!std::isfinite(target_time_column->getFloat64(row)))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Target time of function {} must be finite", getName());
            if (target_time_column->compareAt(row, row, input.time, 1) < 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Target time of function {} must not precede the anchor time", getName());
            std::map<Float64, Float64> values_by_half_life;
            addRebasedComponents(values_by_half_life, input, row, target_time_column->getFloat64(row));
            result.append(*target_time_column, row, values_by_half_life);
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
            "(8,0,10,[(8,10)])"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});

    factory.registerFunction<FunctionExponentialTimeDecayingAdd>(FunctionDocumentation{
        .description = R"(
Adds two exponentially time-decaying values at their greatest anchor time.
First rebase both inputs to `ct = greatest(A.time, B.time)`. If `Av` and `Bv` are their rebased
values and `Ad` and `Bd` are their rebased effective half-lives, the result has `value = Av + Bv`,
`time = ct`, and `half_life = (Ad * Av + Bd * Bv) / (Av + Bv)`.
Distinct half-lives remain separate internally so repeated additions remain independent of grouping and input order.
)",
        .syntax = "exponentialTimeDecayingAdd(a, b)",
        .arguments = {
            {"a", "First decaying value.", {"ExponentialTimeDecayingFloat64"}},
            {"b", "Second decaying value with the same time type.", {"ExponentialTimeDecayingFloat64"}}},
        .returned_value = {"Returns the combined decaying value.", {"ExponentialTimeDecayingFloat64"}},
        .examples = {{
            "Add values with different half-lives",
            "SELECT exponentialTimeDecayingAdd("
            "exponentialTimeDecayingFloat64(8, toFloat64(0), 10), "
            "exponentialTimeDecayingFloat64(4, toFloat64(10), 20))",
            "(8,10,15,[(4,10),(4,20)])"}},
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
        .returned_value = {"Returns the sum of all independently decayed components at the target time.", {"Float64"}},
        .examples = {{
            "Evaluate one half-life later",
            "SELECT exponentialTimeDecayingValueAt(exponentialTimeDecayingFloat64(8, toFloat64(0), 10), toFloat64(10))",
            "4"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});

    factory.registerFunction<FunctionExponentialTimeDecayingRebase>(FunctionDocumentation{
        .description = "Re-anchors a decaying value at its anchor time or a later time and recalculates its value and effective half-life.",
        .syntax = "exponentialTimeDecayingRebase(value, target_time)",
        .arguments = {
            {"value", "Decaying value.", {"ExponentialTimeDecayingFloat64"}},
            {"target_time", "New anchor at or after the current anchor, with the type used by the decaying value.",
                {"Number", "DateTime", "DateTime64"}}},
        .returned_value = {"Returns a decaying value anchored at the target time.", {"ExponentialTimeDecayingFloat64"}},
        .examples = {{
            "Rebase one half-life later",
            "SELECT exponentialTimeDecayingRebase(exponentialTimeDecayingFloat64(8, toFloat64(0), 10), toFloat64(10))",
            "(4,10,10,[(4,10)])"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});
}

}
