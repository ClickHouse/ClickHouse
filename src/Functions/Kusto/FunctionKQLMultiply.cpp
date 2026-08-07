#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** `kqlMultiply(x, y)` - the `*` operator of the Kusto dialect.
  *
  * Kusto scales a timespan by a number (`2 * 1h` is two hours), and a KQL timespan is an
  * `Interval` here, which the ordinary `multiply` does not take. An interval stores a plain
  * count of its unit, so the scaling reads the count as a number, multiplies, and puts the
  * result back into the same interval type. Anything without an interval multiplies as usual.
  */
class FunctionKQLMultiply final : public IFunction, WithContext
{
public:
    static constexpr auto name = "kqlMultiply";

    explicit FunctionKQLMultiply(ContextPtr context_) : WithContext(context_) { }

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionKQLMultiply>(std::move(context_)); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const DataTypePtr & left = arguments[0].type;
        const DataTypePtr & right = arguments[1].type;

        if (isInterval(left) || isInterval(right))
        {
            const DataTypePtr & interval = isInterval(left) ? left : right;
            const DataTypePtr & scale = isInterval(left) ? right : left;
            if (!isNumber(scale))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} scales a timespan by a number, but the arguments have types {} and {}",
                    getName(),
                    left->getName(),
                    right->getName());
            return interval;
        }

        return FunctionFactory::instance().get("multiply", getContext())->build(arguments)->getResultType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const bool left_is_interval = isInterval(arguments[0].type);
        if (!left_is_interval && !isInterval(arguments[1].type))
        {
            auto multiply = FunctionFactory::instance().get("multiply", getContext())->build(arguments);
            return multiply->execute(arguments, multiply->getResultType(), input_rows_count, /*dry_run=*/false);
        }

        const ColumnWithTypeAndName & interval = arguments[left_is_interval ? 0 : 1];
        const ColumnWithTypeAndName & scale = arguments[left_is_interval ? 1 : 0];

        /// The interval's column already is its count as `Int64`; only the type says otherwise.
        ColumnsWithTypeAndName multiply_arguments{{interval.column, std::make_shared<DataTypeInt64>(), ""}, scale};
        auto multiply = FunctionFactory::instance().get("multiply", getContext())->build(multiply_arguments);
        const ColumnWithTypeAndName product{
            multiply->execute(multiply_arguments, multiply->getResultType(), input_rows_count, /*dry_run=*/false),
            multiply->getResultType(),
            ""};

        const auto kind = assert_cast<const DataTypeInterval &>(*interval.type).getKind();
        ColumnsWithTypeAndName conversion_arguments{product};
        auto to_interval
            = FunctionFactory::instance().get(kind.toNameOfFunctionToIntervalDataType(), getContext())->build(conversion_arguments);
        return to_interval->execute(conversion_arguments, to_interval->getResultType(), input_rows_count, /*dry_run=*/false);
    }
};

}

REGISTER_FUNCTION(KQLMultiply)
{
    FunctionDocumentation documentation{
        .description = R"(
Multiplication as the Kusto Query Language defines it: a timespan (an `Interval`) scales by a
number on either side, so `2 * 1h` is two hours. Two arguments without an interval multiply as
[`multiply`](#multiply) does.

This function backs the `*` operator when `dialect = 'kusto'`. It is not meant to be called
directly from SQL.
)",
        .syntax = "kqlMultiply(x, y)",
        .arguments = {{"x", "A number or a timespan."}, {"y", "A number, or a timespan when `x` is a number."}},
        .returned_value = {"The product; an interval of the same kind when either argument is one."},
        .examples
        = {{"timespan", "SELECT kqlMultiply(2, toIntervalNanosecond(3600000000000))", "7200000000000"},
           {"numbers", "SELECT kqlMultiply(6, 7)", "42"}},
        .introduced_in = {26, 8},
        .category = FunctionDocumentation::Category::Arithmetic,
    };

    factory.registerFunction<FunctionKQLMultiply>(documentation);
}

}
