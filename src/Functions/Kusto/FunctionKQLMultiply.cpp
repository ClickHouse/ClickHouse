#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/Kusto/KQLExactArithmetic.h>
#include <Interpreters/Context.h>
#include <Common/assert_cast.h>

#include <cmath>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int BAD_ARGUMENTS;
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

    explicit FunctionKQLMultiply(ContextPtr context_)
        : WithContext(context_)
    {
    }

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionKQLMultiply>(std::move(context_)); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const DataTypePtr & left = arguments[0].type;
        const DataTypePtr & right = arguments[1].type;
        const DataTypePtr left_nested = removeNullable(left);
        const DataTypePtr right_nested = removeNullable(right);

        if (isInterval(left_nested) || isInterval(right_nested))
        {
            const DataTypePtr & interval = isInterval(left_nested) ? left_nested : right_nested;
            const DataTypePtr & scale = isInterval(left_nested) ? right_nested : left_nested;
            if (!isNumber(scale))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} scales a timespan by a number, but the arguments have types {} and {}",
                    getName(),
                    left->getName(),
                    right->getName());
            return left->isNullable() || right->isNullable() ? makeNullable(interval) : interval;
        }

        return FunctionFactory::instance().get("multiply", getContext())->build(arguments)->getResultType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const bool left_is_interval = isInterval(removeNullable(arguments[0].type));
        if (!left_is_interval && !isInterval(removeNullable(arguments[1].type)))
        {
            auto multiply = FunctionFactory::instance().get("multiply", getContext())->build(arguments);
            return multiply->execute(arguments, multiply->getResultType(), input_rows_count, /*dry_run=*/false);
        }

        const ColumnWithTypeAndName & interval = arguments[left_is_interval ? 0 : 1];
        const ColumnWithTypeAndName & scale = arguments[left_is_interval ? 1 : 0];

        ColumnPtr interval_full = interval.column->convertToFullColumnIfConst();
        ColumnPtr scale_full = scale.column->convertToFullColumnIfConst();

        const IColumn * interval_column = interval_full.get();
        const IColumn * scale_column = scale_full.get();
        const NullMap * interval_nulls = nullptr;
        const NullMap * scale_nulls = nullptr;
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(interval_column))
        {
            interval_nulls = &nullable->getNullMapData();
            interval_column = &nullable->getNestedColumn();
        }
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(scale_column))
        {
            scale_nulls = &nullable->getNullMapData();
            scale_column = &nullable->getNestedColumn();
        }

        const DataTypePtr scale_nested_type = removeNullable(scale.type);
        const bool scale_exact = KQLExact::isExactNumber(*scale_nested_type);

        auto result = ColumnInt64::create(input_rows_count);
        auto null_map = ColumnUInt8::create(input_rows_count);
        constexpr long double limit = static_cast<long double>(std::numeric_limits<Int64>::max()) + 1;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if ((interval_nulls && (*interval_nulls)[i]) || (scale_nulls && (*scale_nulls)[i]))
            {
                result->getData()[i] = 0;
                null_map->getData()[i] = 1;
                continue;
            }

            /// An integer or a decimal scales exactly; only a float has to go through `Float64`.
            if (scale_exact)
            {
                result->getData()[i]
                    = KQLExact::scaledTicks(interval_column->getInt(i), *scale_column, *scale_nested_type, i, getName());
            }
            else
            {
                const long double product
                    = static_cast<long double>(interval_column->getInt(i)) * static_cast<long double>(scale_column->getFloat64(i));
                if (!std::isfinite(product) || product < -limit || product >= limit)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} result does not fit a timespan", getName());

                result->getData()[i] = static_cast<Int64>(std::trunc(product));
            }
            null_map->getData()[i] = 0;
        }

        if (interval_nulls || scale_nulls)
            return ColumnNullable::create(std::move(result), std::move(null_map));
        return result;
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
