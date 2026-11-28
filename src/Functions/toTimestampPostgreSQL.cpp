#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/Exception.h>
#include <Common/FunctionDocumentation.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// `to_timestamp(seconds)` - PostgreSQL compatibility function.
/// It is `toDateTime64(seconds, 6)`: PostgreSQL returns a `timestamp with time zone`, which has microsecond
/// precision. The argument is passed to `toDateTime64` as it is, so the handling of `NULL` and the result are
/// the ones of `toDateTime64`: this function opts out of the default implementations that would otherwise be
/// applied a second time.
/// The two-argument form of PostgreSQL, `to_timestamp(text, format)`, is not implemented: the format is a
/// PostgreSQL template pattern, which neither `parseDateTime` nor `parseDateTimeInJodaSyntax` reads.
class FunctionToTimestampPostgreSQL final : public IFunction
{
public:
    static constexpr auto name = "to_timestamp";
    static constexpr UInt64 scale = 6;

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionToTimestampPostgreSQL>(FunctionFactory::instance().get("toDateTime64", context));
    }

    explicit FunctionToTimestampPostgreSQL(FunctionOverloadResolverPtr to_date_time64_) : to_date_time64(std::move(to_date_time64_)) {}

    [[nodiscard]] String getName() const override { return name; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 1; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }
    [[nodiscard]] bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        /// `toDateTime64` also converts a string, a date and a date with time. PostgreSQL takes only a number
        /// of seconds here, and a string is the first argument of the form that is not implemented.
        const DataTypePtr type = removeNullable(removeLowCardinality(arguments[0].type));
        if (!isNumber(type) && !isNothing(type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of the argument of function {}: expected a number of seconds since the Unix epoch. "
                "The form with a format string is not supported, use parseDateTime",
                arguments[0].type->getName(), getName());

        return to_date_time64->build(withScale(arguments, 1))->getResultType();
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnsWithTypeAndName arguments_with_scale = withScale(arguments, input_rows_count);
        auto to_date_time64_base = to_date_time64->build(arguments_with_scale);
        return to_date_time64_base->execute(
            arguments_with_scale, to_date_time64_base->getResultType(), input_rows_count, /* dry_run = */ false);
    }

private:
    static ColumnsWithTypeAndName withScale(const ColumnsWithTypeAndName & arguments, size_t rows)
    {
        ColumnWithTypeAndName scale_argument =
        {
            DataTypeUInt8().createColumnConst(rows, scale),
            std::make_shared<DataTypeUInt8>(),
            "scale"
        };
        return {arguments[0], scale_argument};
    }

    FunctionOverloadResolverPtr to_date_time64;
};

}

REGISTER_FUNCTION(ToTimestampPostgreSQL)
{
    FunctionDocumentation::Description description = R"(
PostgreSQL compatibility function. Converts a number of seconds since the Unix epoch to a date with time with microsecond precision.

`to_timestamp(seconds)` is [`toDateTime64`](/reference/functions/regular-functions/type-conversion-functions#toDateTime64)`(seconds, 6)`.
A fraction finer than a microsecond is truncated, while PostgreSQL rounds it, and a value outside of the range of `DateTime64` is handled as `toDateTime64` handles it.
The form of PostgreSQL with two arguments, `to_timestamp(text, format)`, is not supported: use [`parseDateTime`](/reference/functions/regular-functions/type-conversion-functions#parseDateTime) for it.
)";
    FunctionDocumentation::Syntax syntax = "to_timestamp(seconds)";
    FunctionDocumentation::Arguments arguments = {
        {"seconds", "The number of seconds since 1970-01-01 00:00:00 UTC, possibly with a fraction.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the date with time in the time zone of the session.", {"DateTime64(6)"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT to_timestamp(1700000000.5) SETTINGS session_timezone = 'UTC'", "2023-11-14 22:13:20.500000"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToTimestampPostgreSQL>(documentation, FunctionFactory::Case::Insensitive);
}

}
