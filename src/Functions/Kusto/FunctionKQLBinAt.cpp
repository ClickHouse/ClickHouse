#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// Runs the numeric case as `plus(fixed_point, kqlBin(minus(value, fixed_point), bin_size))`.
/// Holding the already-built functions keeps the per-block work down to executing them.
class FunctionKQLBinAtNumeric final : public IFunctionBase
{
public:
    FunctionKQLBinAtNumeric(FunctionBasePtr difference_, FunctionBasePtr rounding_, FunctionBasePtr sum_, DataTypes argument_types_)
        : difference(std::move(difference_)), rounding(std::move(rounding_)), sum(std::move(sum_)), argument_types(std::move(argument_types_))
    {
    }

    String getName() const override { return "kqlBinAt"; }
    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return sum->getResultType(); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override
    {
        return difference->isSuitableForShortCircuitArgumentsExecution(arguments);
    }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<Executable>(difference, rounding, sum);
    }

private:
    class Executable final : public IExecutableFunction
    {
    public:
        Executable(FunctionBasePtr difference_, FunctionBasePtr rounding_, FunctionBasePtr sum_)
            : difference(std::move(difference_)), rounding(std::move(rounding_)), sum(std::move(sum_))
        {
        }

        String getName() const override { return "kqlBinAt"; }

    protected:
        /// The delegated functions were built over the original argument types, `Nullable` and
        /// all, and each handles its own nulls. Stripping `Nullable` here would hand them columns
        /// that no longer match what they were built for.
        bool useDefaultImplementationForNulls() const override { return false; }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            ColumnsWithTypeAndName difference_arguments{arguments[0], arguments[2]};
            ColumnWithTypeAndName shifted{
                difference->execute(difference_arguments, difference->getResultType(), input_rows_count, /*dry_run=*/false),
                difference->getResultType(),
                "kql_shifted"};

            ColumnsWithTypeAndName rounding_arguments{shifted, arguments[1]};
            ColumnWithTypeAndName rounded{
                rounding->execute(rounding_arguments, rounding->getResultType(), input_rows_count, /*dry_run=*/false),
                rounding->getResultType(),
                "kql_rounded"};

            ColumnsWithTypeAndName sum_arguments{arguments[2], rounded};
            return sum->execute(sum_arguments, result_type, input_rows_count, /*dry_run=*/false);
        }

    private:
        FunctionBasePtr difference;
        FunctionBasePtr rounding;
        FunctionBasePtr sum;
    };

    FunctionBasePtr difference;
    FunctionBasePtr rounding;
    FunctionBasePtr sum;
    DataTypes argument_types;
};

/// A function over the given arguments that never reads them and returns a constant NULL.
/// The argument types the analyzer sees stay the real ones.
class FunctionKQLBinAtNull final : public IFunctionBase
{
public:
    explicit FunctionKQLBinAtNull(DataTypes argument_types_)
        : argument_types(std::move(argument_types_)), result_type(makeNullable(std::make_shared<DataTypeNothing>()))
    {
    }

    String getName() const override { return "kqlBinAt"; }
    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return result_type; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override { return std::make_unique<Executable>(); }

private:
    class Executable final : public IExecutableFunction
    {
    public:
        String getName() const override { return "kqlBinAt"; }

    protected:
        bool useDefaultImplementationForNulls() const override { return false; }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & type, size_t input_rows_count) const override
        {
            return type->createColumnConst(input_rows_count, Field());
        }
    };

    DataTypes argument_types;
    DataTypePtr result_type;
};

/** `kqlBinAt(value, binSize, fixedPoint)` - Kusto's `bin_at()`, rounding a value down to a
  * multiple of `binSize` counted from `fixedPoint` rather than from zero.
  *
  * A datetime is exactly `toStartOfInterval(value, binSize, fixedPoint)`; numbers compute
  * `fixedPoint + bin(value - fixedPoint, binSize)`. Only the argument types say which
  * applies, so the decision is made here, during analysis, like `kqlBin` does.
  */
class FunctionKQLBinAtOverloadResolver final : public IFunctionOverloadResolver, WithContext
{
public:
    static constexpr auto name = "kqlBinAt";

    explicit FunctionKQLBinAtOverloadResolver(ContextPtr context_) : WithContext(context_) { }

    static FunctionOverloadResolverPtr create(ContextPtr context_)
    {
        return std::make_unique<FunctionKQLBinAtOverloadResolver>(std::move(context_));
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForNulls() const override { return false; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        return delegate(arguments);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return delegate(arguments)->getResultType();
    }

private:
    FunctionBasePtr delegate(const ColumnsWithTypeAndName & arguments) const
    {
        if (arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly 3 arguments", getName());

        const DataTypePtr value_type = removeNullable(arguments[0].type);
        const DataTypePtr bin_type = removeNullable(arguments[1].type);
        const DataTypePtr fixed_type = removeNullable(arguments[2].type);

        /// A NULL literal argument makes the whole result a NULL literal; the numeric chain
        /// below short-circuits it, so it must get the query regardless of the other types.
        const bool value_is_null_literal = isNothing(value_type) || isNothing(bin_type) || isNothing(fixed_type);

        /// A datetime rounded by an interval from an origin is exactly `toStartOfInterval`.
        if (!value_is_null_literal && isDateOrDate32OrDateTimeOrDateTime64(value_type))
        {
            if (!isInterval(bin_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} rounds a datetime by a timespan, but the second argument has type {}",
                    getName(),
                    arguments[1].type->getName());
            if (!isDateOrDate32OrDateTimeOrDateTime64(fixed_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} rounds a datetime from a datetime fixed point, but the third argument has type {}",
                    getName(),
                    arguments[2].type->getName());
            /// Kusto returns null for a negative bin size; `toStartOfInterval` would throw,
            /// and it only takes a constant interval, so the sign is known here. `kqlBin`
            /// makes the same check on its datetime branch.
            if (arguments[1].column && isColumnConst(*arguments[1].column))
            {
                const Field interval = assert_cast<const ColumnConst &>(*arguments[1].column).getField();
                if (!interval.isNull() && interval.safeGet<Int64>() < 0)
                {
                    DataTypes argument_types;
                    for (const auto & argument : arguments)
                        argument_types.push_back(argument.type);
                    return std::make_shared<FunctionKQLBinAtNull>(std::move(argument_types));
                }
            }
            return FunctionFactory::instance().get("toStartOfInterval", getContext())->build(arguments);
        }

        if (!value_is_null_literal && (!isNumber(value_type) || !isNumber(bin_type) || !isNumber(fixed_type)))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} expects a number rounded by a number from a number, or a datetime rounded by a timespan "
                "from a datetime, got {}, {} and {}",
                getName(),
                arguments[0].type->getName(),
                arguments[1].type->getName(),
                arguments[2].type->getName());

        /// `fixedPoint + bin(value - fixedPoint, binSize)`. Each delegated function keeps its
        /// own null handling, and a NULL literal argument short-circuits through the chain.
        /// The short circuit fires on a *column* of nulls, so a synthetic intermediate of type
        /// `Nullable(Nothing)` gets one materialized.
        const auto intermediate = [](DataTypePtr type, const char * intermediate_name)
        {
            ColumnPtr column;
            if (isNothing(removeNullable(type)))
                column = type->createColumnConstWithDefaultValue(1);
            return ColumnWithTypeAndName{column, type, intermediate_name};
        };

        ColumnsWithTypeAndName difference_arguments{arguments[0], arguments[2]};
        auto difference = FunctionFactory::instance().get("minus", getContext())->build(difference_arguments);

        ColumnsWithTypeAndName rounding_arguments{intermediate(difference->getResultType(), "kql_shifted"), arguments[1]};
        auto rounding = FunctionFactory::instance().get("kqlBin", getContext())->build(rounding_arguments);

        ColumnsWithTypeAndName sum_arguments{arguments[2], intermediate(rounding->getResultType(), "kql_rounded")};
        auto sum = FunctionFactory::instance().get("plus", getContext())->build(sum_arguments);

        DataTypes argument_types;
        for (const auto & argument : arguments)
            argument_types.push_back(argument.type);

        return std::make_shared<FunctionKQLBinAtNumeric>(
            std::move(difference), std::move(rounding), std::move(sum), std::move(argument_types));
    }
};

}

REGISTER_FUNCTION(KQLBinAt)
{
    FunctionDocumentation bin_at_documentation{
        .description = R"(
Rounds a value down to a multiple of `binSize` counted from `fixedPoint`, as the Kusto Query
Language's `bin_at()` does.

The rule depends on the argument types: a number is rounded arithmetically, and a datetime is
rounded by a timespan (which is an `Interval`) counted from a datetime fixed point. For a
datetime, the fixed point must not be later than the value.

This function backs `bin_at()` when `dialect = 'kusto'`. It is not meant to be called directly
from SQL.
)",
        .syntax = "kqlBinAt(value, binSize, fixedPoint)",
        .arguments
        = {{"value", "A number or a datetime."},
           {"binSize", "The bin size."},
           {"fixedPoint", "The point the bins are counted from."}},
        .returned_value = {"`value` rounded down to the nearest multiple of `binSize` counted from `fixedPoint`."},
        .examples
        = {{"number", "SELECT kqlBinAt(6.5, 2.5, -0.5)", "4.5"},
           {"datetime",
            "SELECT kqlBinAt(toDateTime('2026-08-01 12:34:56'), toIntervalHour(1), toDateTime('2026-08-01 00:30:00'))",
            "2026-08-01 12:30:00"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Arithmetic,
    };

    factory.registerFunction(
        FunctionKQLBinAtOverloadResolver::name,
        [](ContextPtr context) { return FunctionKQLBinAtOverloadResolver::create(std::move(context)); },
        bin_at_documentation);
}

}
