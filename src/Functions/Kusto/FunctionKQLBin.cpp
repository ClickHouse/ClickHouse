#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// Runs the numeric case as `multiply(floor(divide(x, y)), y)`, or `multiply(intDiv(x, y), y)`
/// when both operands are integers. Holding the already-built functions keeps the per-block
/// work down to executing them.
class FunctionKQLBinNumeric final : public IFunctionBase
{
public:
    FunctionKQLBinNumeric(FunctionBasePtr quotient_, FunctionBasePtr round_down_, FunctionBasePtr product_, DataTypes argument_types_)
        : quotient(std::move(quotient_))
        , round_down(std::move(round_down_))
        , product(std::move(product_))
        , argument_types(std::move(argument_types_))
    {
    }

    String getName() const override { return "kqlBin"; }
    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return product->getResultType(); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override
    {
        return quotient->isSuitableForShortCircuitArgumentsExecution(arguments);
    }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<Executable>(quotient, round_down, product);
    }

private:
    class Executable final : public IExecutableFunction
    {
    public:
        Executable(FunctionBasePtr quotient_, FunctionBasePtr round_down_, FunctionBasePtr product_)
            : quotient(std::move(quotient_)), round_down(std::move(round_down_)), product(std::move(product_))
        {
        }

        String getName() const override { return "kqlBin"; }

    protected:
        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            ColumnWithTypeAndName rounded{
                quotient->execute(arguments, quotient->getResultType(), input_rows_count, /*dry_run=*/false),
                quotient->getResultType(),
                "rounded"};

            /// Absent when the operands are integers: `intDiv` has already rounded down.
            if (round_down)
            {
                ColumnsWithTypeAndName round_arguments{rounded};
                rounded = ColumnWithTypeAndName{
                    round_down->execute(round_arguments, round_down->getResultType(), input_rows_count, /*dry_run=*/false),
                    round_down->getResultType(),
                    "rounded"};
            }

            ColumnsWithTypeAndName product_arguments{rounded, arguments[1]};
            return product->execute(product_arguments, result_type, input_rows_count, /*dry_run=*/false);
        }

    private:
        FunctionBasePtr quotient;
        FunctionBasePtr round_down;
        FunctionBasePtr product;
    };

    FunctionBasePtr quotient;
    FunctionBasePtr round_down;
    FunctionBasePtr product;
    DataTypes argument_types;
};

/** `kqlBin(value, roundTo)` - Kusto's `bin()`, rounding a value down to a multiple of `roundTo`.
  *
  * The rule reads the same in every case (`floor(value / roundTo) * roundTo`) but the way to
  * compute it is not: numbers divide, and a datetime has to be rounded by an interval. Only
  * the argument types say which applies, so the decision is made here rather than guessed
  * from how the argument was spelled - the previous KQL implementation compared the first
  * *token* against the text "datetime", so `bin(Timestamp, 1d)` over a datetime column took
  * the numeric branch and emitted `toFloat64(Timestamp)`.
  *
  * Being a resolver rather than a function means that dispatch happens once, during analysis.
  */
class FunctionKQLBinOverloadResolver final : public IFunctionOverloadResolver, WithContext
{
public:
    static constexpr auto name = "kqlBin";

    explicit FunctionKQLBinOverloadResolver(ContextPtr context_) : WithContext(context_) { }

    static FunctionOverloadResolverPtr create(ContextPtr context_)
    {
        return std::make_unique<FunctionKQLBinOverloadResolver>(std::move(context_));
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }

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
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly 2 arguments", getName());

        const DataTypePtr value_type = removeNullable(arguments[0].type);
        const DataTypePtr bin_type = removeNullable(arguments[1].type);

        /// A datetime rounded by an interval is exactly `toStartOfInterval`.
        if (isDateOrDate32OrDateTimeOrDateTime64(value_type))
        {
            if (!isInterval(bin_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} rounds a datetime by a timespan, but the second argument has type {}",
                    getName(),
                    arguments[1].type->getName());
            return FunctionFactory::instance().get("toStartOfInterval", getContext())->build(arguments);
        }

        if (!isNumber(value_type) || !isNumber(bin_type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} expects a number rounded by a number, or a datetime rounded by a timespan, got {} and {}",
                getName(),
                arguments[0].type->getName(),
                arguments[1].type->getName());

        /// Numbers: `floor(value / roundTo) * roundTo`. Integers divide exactly, so they skip
        /// the detour through floating point.
        const bool both_integral = isInteger(value_type) && isInteger(bin_type);
        auto quotient = FunctionFactory::instance().get(both_integral ? "intDiv" : "divide", getContext())->build(arguments);

        FunctionBasePtr round_down;
        DataTypePtr rounded_type = quotient->getResultType();
        if (!both_integral)
        {
            ColumnsWithTypeAndName round_arguments{ColumnWithTypeAndName{nullptr, rounded_type, "rounded"}};
            round_down = FunctionFactory::instance().get("floor", getContext())->build(round_arguments);
            rounded_type = round_down->getResultType();
        }

        ColumnsWithTypeAndName product_arguments{ColumnWithTypeAndName{nullptr, rounded_type, "rounded"}, arguments[1]};
        auto product = FunctionFactory::instance().get("multiply", getContext())->build(product_arguments);

        DataTypes argument_types;
        for (const auto & argument : arguments)
            argument_types.push_back(argument.type);

        return std::make_shared<FunctionKQLBinNumeric>(
            std::move(quotient), std::move(round_down), std::move(product), std::move(argument_types));
    }
};

}

REGISTER_FUNCTION(KQLBin)
{
    FunctionDocumentation bin_documentation{
        .description = R"(
Rounds a value down to a multiple of `roundTo`, as the Kusto Query Language's `bin()` does.

The rule depends on the argument types: a number is rounded arithmetically, and a datetime is
rounded by a timespan (which is an `Interval`).

This function backs `bin()` when `dialect = 'kusto'`. It is not meant to be called directly
from SQL.
)",
        .syntax = "kqlBin(value, roundTo)",
        .arguments = {{"value", "A number or a datetime."}, {"roundTo", "The bin size."}},
        .returned_value = {"`value` rounded down to the nearest multiple of `roundTo`."},
        .examples
        = {{"number", "SELECT kqlBin(4.5, 1)", "4"},
           {"datetime", "SELECT kqlBin(toDateTime('2026-08-01 12:34:56'), toIntervalHour(1))", "2026-08-01 12:00:00"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Arithmetic,
    };

    factory.registerFunction(
        FunctionKQLBinOverloadResolver::name,
        [](ContextPtr context) { return FunctionKQLBinOverloadResolver::create(std::move(context)); },
        bin_documentation);
}

}
