#include <Columns/ColumnExponentialTimeDecaying.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/assert_cast.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeExponentialTimeDecaying.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/exponentialTimeDecaying.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/Context.h>

#include <boost/multiprecision/cpp_bin_float.hpp>

#include <algorithm>
#include <cmath>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_FUNCTION;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_time_decay_aggregate_functions;
    extern const SettingsString exponential_time_decay_default_mode;
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

ExponentialTimeDecayingMode getDefaultExponentialTimeDecayingMode(const ContextPtr & context)
{
    return parseExponentialTimeDecayingMode(
        context
            ? context->getSettingsRef()[Setting::exponential_time_decay_default_mode].value
            : "compact");
}

void assertDecayingType(const DataTypePtr & type, const String & function_name, size_t argument)
{
    if (!isExponentialTimeDecaying(type))
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
    const ColumnExponentialTimeDecaying & column;
    Float64 decay_length;
    ExponentialTimeDecayingKeyWidth key_width;
    ExponentialTimeDecayingMode mode;
};

DecayingColumnView getDecayingColumnView(const ColumnPtr & column, const DataTypePtr & type)
{
    const auto decay_length = tryGetExponentialTimeDecayingDecayLength(type);
    chassert(decay_length);

    const auto & decaying = assert_cast<const ColumnExponentialTimeDecaying &>(*column);
    return {
        decaying,
        *decay_length,
        decaying.getKeyWidth(),
        decaying.getMode(),
    };
}

bool isEmptyRow(const DecayingColumnView & input, size_t row)
{
    return input.column.getCanonicalValueAt(row).value_at_anchor == 0;
}

void assertValidRow(const DecayingColumnView & input, size_t row, const String & function_name)
{
    const auto canonical = input.column.getCanonicalValueAt(row);
    if (!std::isfinite(canonical.value_at_anchor) || !std::isfinite(canonical.anchor_time))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Argument of function {} is not a canonical exponential-time-decaying value",
            function_name);
}

Float64 valueAt(
    const DecayingColumnView & input, size_t row, Float64 target_time, bool at_unit_time = false)
{
    if (isEmptyRow(input, row))
        return 0;

    const auto canonical = input.column.getCanonicalValueAt(row);
    const Float64 value = canonical.value_at_anchor;
    const Float64 anchor_time = canonical.anchor_time;
    if (target_time == anchor_time)
        return value;

    if (at_unit_time)
    {
        using ExtendedTimeDecayFloat = boost::multiprecision::cpp_bin_float_quad;
        const ExtendedTimeDecayFloat extended_value(value);
        const ExtendedTimeDecayFloat extended_anchor(anchor_time);
        const ExtendedTimeDecayFloat extended_target(target_time);
        const ExtendedTimeDecayFloat extended_decay_length(input.decay_length);
        const ExtendedTimeDecayFloat time_delta = extended_anchor - extended_target;
        const ExtendedTimeDecayFloat exponent = time_delta / extended_decay_length;
        const ExtendedTimeDecayFloat factor = boost::multiprecision::exp(exponent);
        const ExtendedTimeDecayFloat result = extended_value * factor;
        return static_cast<Float64>(result);
    }

    return value * std::exp((anchor_time - target_time) / input.decay_length);
}

Float64 unitTime(const DecayingColumnView & input, size_t row)
{
    return input.column.getUnitTimestampAt(row);
}

struct DecayingColumnBuilder
{
    explicit DecayingColumnBuilder(
        Float64 decay_length_,
        ExponentialTimeDecayingKeyWidth key_width_ = ExponentialTimeDecayingKeyWidth::Bits64,
        ExponentialTimeDecayingMode mode_ = ExponentialTimeDecayingMode::Compact)
        : decay_length(decay_length_)
        , key_width(key_width_)
        , mode(mode_)
    {
    }

    void append(Float64 value, Float64 time)
    {
        if (!isFiniteExponentialTimeDecayingCurve(value, time, decay_length))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "exponential-time-decaying value does not define a finite decay curve");

        value_at_anchor->insertValue(value);
        anchor_time->insertValue(time);
    }

    ColumnPtr build()
    {
        auto tuple = ColumnTuple::create(
            Columns{std::move(value_at_anchor), std::move(anchor_time)});
        return materializeExponentialTimeDecayingStorageColumn(
            *tuple,
            decay_length,
            "function result",
            key_width,
            mode);
    }

    const Float64 decay_length;
    const ExponentialTimeDecayingKeyWidth key_width;
    const ExponentialTimeDecayingMode mode;
    ColumnFloat64::MutablePtr value_at_anchor = ColumnFloat64::create();
    ColumnFloat64::MutablePtr anchor_time = ColumnFloat64::create();
};

class FunctionExponentialTimeDecaying final : public IFunction
{
public:
    static constexpr auto name = "exponentialTimeDecaying";

    static FunctionPtr create(ContextPtr context)
    {
        assertExperimentalFeatureEnabled(context, name);
        return std::make_shared<FunctionExponentialTimeDecaying>(
            std::nullopt, getDefaultExponentialTimeDecayingMode(context));
    }

    FunctionExponentialTimeDecaying(
        std::optional<Float64> decay_length_,
        ExponentialTimeDecayingMode mode_)
        : decay_length(decay_length_)
        , mode(mode_)
    {
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!decay_length)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function {} requires a decay_length parameter",
                getName());

        if (!isNumber(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be a number, got {}",
                getName(),
                arguments[0].type->getName());

        assertTimeType(arguments[1].type, getName());
        return std::make_shared<DataTypeExponentialTimeDecaying>(
            *decay_length, ExponentialTimeDecayingKeyWidth::Bits64, mode);
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr &,
        size_t input_rows_count) const override
    {
        chassert(decay_length);

        auto value_column = arguments[0].column->convertToFullColumnIfConst();
        auto time_column = arguments[1].column->convertToFullColumnIfConst();
        DecayingColumnBuilder result(
            *decay_length, ExponentialTimeDecayingKeyWidth::Bits64, mode);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const Float64 value = value_column->getFloat64(row);
            const Float64 time = time_column->getFloat64(row);

            if (!std::isfinite(value))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Value of function {} must be finite", getName());
            if (!std::isfinite(time))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time of function {} must be finite", getName());

            result.append(value, time);
        }

        return result.build();
    }

private:
    std::optional<Float64> decay_length;
    ExponentialTimeDecayingMode mode = ExponentialTimeDecayingMode::Compact;
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

        assertExponentialTimeDecayingTypesCompatible(
            arguments[0].type, arguments[1].type, getName());
        return arguments[0].type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto left_column = arguments[0].column->convertToFullColumnIfConst();
        auto right_column = arguments[1].column->convertToFullColumnIfConst();
        const auto left = getDecayingColumnView(left_column, arguments[0].type);
        const auto right = getDecayingColumnView(right_column, arguments[1].type);
        DecayingColumnBuilder result(left.decay_length, left.key_width, left.mode);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            assertValidRow(left, row, getName());
            assertValidRow(right, row, getName());

            if (isEmptyRow(left, row))
            {
                const auto direct = right.column.getDirectValueAt(row);
                result.append(direct.value_at_anchor, direct.anchor_time);
                continue;
            }
            if (isEmptyRow(right, row))
            {
                const auto direct = left.column.getDirectValueAt(row);
                result.append(direct.value_at_anchor, direct.anchor_time);
                continue;
            }

            const auto left_canonical = left.column.getCanonicalValueAt(row);
            const auto right_canonical = right.column.getCanonicalValueAt(row);
            const Float64 latest_time = std::max(
                left_canonical.anchor_time,
                right_canonical.anchor_time);
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
            const Float64 target_time = target_time_column->getFloat64(row);
            if (!std::isfinite(target_time))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Target time of function {} must be finite", getName());
            result_data[row] = valueAt(input, row, target_time, target_time == unitTime(input, row));
        }
        return result;
    }
};

class FunctionExponentialTimeDecayingUnitTime final : public IFunction
{
public:
    static constexpr auto name = "exponentialTimeDecayingUnitTime";
    static FunctionPtr create(ContextPtr context)
    {
        assertExperimentalFeatureEnabled(context, name);
        return std::make_shared<FunctionExponentialTimeDecayingUnitTime>();
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
        auto input_column = arguments[0].column->convertToFullColumnIfConst();
        const auto input = getDecayingColumnView(input_column, arguments[0].type);
        auto result = ColumnFloat64::create(input_rows_count, 0.0);
        auto & result_data = result->getData();

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            assertValidRow(input, row, getName());
            result_data[row] = unitTime(input, row);
        }

        return result;
    }
};

class FunctionExponentialTimeDecayingValueAtUnitTime final : public IFunction
{
public:
    static constexpr auto name = "exponentialTimeDecayingValueAtUnitTime";
    static FunctionPtr create(ContextPtr context)
    {
        assertExperimentalFeatureEnabled(context, name);
        return std::make_shared<FunctionExponentialTimeDecayingValueAtUnitTime>();
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
        auto input_column = arguments[0].column->convertToFullColumnIfConst();
        const auto input = getDecayingColumnView(input_column, arguments[0].type);
        auto result = ColumnFloat64::create(input_rows_count, 0.0);
        auto & result_data = result->getData();

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            assertValidRow(input, row, getName());
            const Float64 target_time = unitTime(input, row);
            result_data[row] = valueAt(input, row, target_time, true);
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
        const Float64 decay_length = *tryGetExponentialTimeDecayingDecayLength(arguments[0].type);
        return ColumnFloat64::create(input_rows_count, decay_length);
    }
};

}

FunctionOverloadResolverPtr createExponentialTimeDecayingFunction(
    const Array & parameters,
    ContextPtr context)
{
    assertExperimentalFeatureEnabled(context, FunctionExponentialTimeDecaying::name);

    if (parameters.size() != 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Function {} takes exactly one parameter, the decay length",
            FunctionExponentialTimeDecaying::name);

    const Float64 decay_length
        = applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters[0]);
    if (!std::isfinite(decay_length) || decay_length <= 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Decay length of function {} must be finite and positive",
            FunctionExponentialTimeDecaying::name);

    return std::make_shared<FunctionToOverloadResolverAdaptor>(
        std::make_shared<FunctionExponentialTimeDecaying>(
            decay_length, getDefaultExponentialTimeDecayingMode(context)));
}

REGISTER_FUNCTION(ExponentialTimeDecaying)
{
    factory.registerFunction<FunctionExponentialTimeDecaying>(FunctionDocumentation{
        .description = R"(
Constructs one `ExponentialTimeDecaying64(decay_length)` value for each `(value, time)` input row.
The naturally ordered `UInt64` key is the complete identity for comparison, equality, hashing,
`GROUP BY`, `DISTINCT`, primary-key marks, and minmax indexes. The default `compact` mode
stores only that key. When `exponential_time_decay_default_mode = 'direct'`, the same key remains
authoritative while the original `(value, timestamp)` pair is retained only as printable metadata.
It can be combined by `exponentialTimeDecayedSum`, including as a
`SimpleAggregateFunction` column in an `AggregatingMergeTree`.
)",
        .syntax = "exponentialTimeDecaying(decay_length)(value, time)",
        .arguments = {
            {"value", "Value.", {"(U)Int*", "Float*", "Decimal"}},
            {"time", "Time.", {"(U)Int*", "Float*", "Decimal", "DateTime", "DateTime64"}}},
        .parameters = {
            {"decay_length", "Time difference required for a value's weight to decay to 1/e.", {"(U)Int*", "Float*", "Decimal"}}},
        .returned_value = {"Returns an `ExponentialTimeDecaying64(decay_length)` value.", {}},
        .examples = {{
            "Construct a decaying value",
            "SELECT exponentialTimeDecaying(10)(8, toFloat64(0)) "
            "SETTINGS allow_experimental_time_decay_aggregate_functions = 1",
            "(1,20.79441541679836,10)"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});

    factory.registerFunction<FunctionExponentialTimeDecayingAdd>(FunctionDocumentation{
        .description = R"(
Adds two exponentially time-decaying values using the canonical curves reconstructed from their authoritative keys.
Both inputs must have the same key width, storage mode, and identical decay lengths encoded in their types. The function rebases the older canonical curve to
`ct = greatest(A.anchor_time, B.anchor_time)` and stores `A.value_at(ct) + B.value_at(ct)` at that anchor. Direct-mode metadata does not affect the result.
Because values are stored as `Float64`, large signed values that nearly cancel can be sensitive to
addition order and grouping. Normalize magnitudes or use a numerically stable method to pre-aggregate
sensitive inputs when stronger numerical reproducibility is required.
)",
        .syntax = "exponentialTimeDecayingAdd(a, b)",
        .arguments = {
            {"a", "First value of type `ExponentialTimeDecaying64(decay_length)` or `ExponentialTimeDecaying128(decay_length)`.", {}},
            {"b", "Second value with the same parameterized type.", {}}},
        .returned_value = {"Returns the combined decaying value with the same width as the inputs.", {}},
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
Evaluates the canonical exponentially time-decaying curve reconstructed from the authoritative ordering key at any target time. Direct-mode metadata is ignored by the calculation.
Numeric, DateTime, and DateTime64 targets are converted to seconds, so `now()` and `now64()` can be used.
)",
        .syntax = "exponentialTimeDecayingValueAt(value, target_time)",
        .arguments = {
            {"value", "Value of type `ExponentialTimeDecaying64(decay_length)` or `ExponentialTimeDecaying128(decay_length)`.", {}},
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

    factory.registerFunction<FunctionExponentialTimeDecayingUnitTime>(FunctionDocumentation{
        .description = R"(
Returns the finite `Float64` representative time exposed by an `ExponentialTimeDecaying64` or `ExponentialTimeDecaying128` key.
For ordinary keys this is the unit-time bucket. If a 128-bit key represents a mathematical unit time outside the finite `Float64` range, this returns the encoded edge timestamp instead. Zero returns `0`.
)",
        .syntax = "exponentialTimeDecayingUnitTime(value)",
        .arguments = {{"value", "Value of type `ExponentialTimeDecaying64(decay_length)` or `ExponentialTimeDecaying128(decay_length)`.", {}}},
        .returned_value = {"Returns the canonical unit time of the ordering bucket.", {"Float64"}},
        .examples = {{
            "Read the unit time",
            "SELECT exponentialTimeDecayingUnitTime(exponentialTimeDecaying(10)(1, toFloat64(5))) "
            "SETTINGS allow_experimental_time_decay_aggregate_functions = 1",
            "5"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});

    factory.registerFunction<FunctionExponentialTimeDecayingValueAtUnitTime>(FunctionDocumentation{
        .description = R"(
Evaluates an `ExponentialTimeDecaying64` or `ExponentialTimeDecaying128` value at the representative time returned by `exponentialTimeDecayingUnitTime`.
For an ordinary key the result is approximately `-1` or `1`. For an extreme 128-bit tail it is the residual value stored at the finite edge timestamp. Zero returns `0`.
)",
        .syntax = "exponentialTimeDecayingValueAtUnitTime(value)",
        .arguments = {{"value", "Value of type `ExponentialTimeDecaying64(decay_length)` or `ExponentialTimeDecaying128(decay_length)`.", {}}},
        .returned_value = {"Returns the value at `exponentialTimeDecayingUnitTime(value)`.", {"Float64"}},
        .examples = {{
            "Read the residual value at unit time",
            "SELECT exponentialTimeDecayingValueAtUnitTime(exponentialTimeDecaying(10)(1, toFloat64(5))) "
            "SETTINGS allow_experimental_time_decay_aggregate_functions = 1",
            "1"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Other});

    factory.registerFunction<FunctionExponentialTimeDecayingDecayLength>(FunctionDocumentation{
        .description = "Returns the decay length encoded in an `ExponentialTimeDecaying64` or `ExponentialTimeDecaying128` type.",
        .syntax = "exponentialTimeDecayingDecayLength(value)",
        .arguments = {{"value", "Value of type `ExponentialTimeDecaying64(decay_length)` or `ExponentialTimeDecaying128(decay_length)`.", {}}},
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
