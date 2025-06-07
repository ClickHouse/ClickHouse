#include <Functions/FunctionsConversion.h>

namespace DB
{

REGISTER_FUNCTION(Conversion)
{
    factory.registerFunction<detail::FunctionToUInt8>();
    factory.registerFunction<detail::FunctionToUInt16>();
    factory.registerFunction<detail::FunctionToUInt32>();
    factory.registerFunction<detail::FunctionToUInt64>();
    factory.registerFunction<detail::FunctionToUInt128>();
    factory.registerFunction<detail::FunctionToUInt256>();
    factory.registerFunction<detail::FunctionToInt8>();
    factory.registerFunction<detail::FunctionToInt16>();
    factory.registerFunction<detail::FunctionToInt32>();
    factory.registerFunction<detail::FunctionToInt64>();
    factory.registerFunction<detail::FunctionToInt128>();
    factory.registerFunction<detail::FunctionToInt256>();

    factory.registerFunction<detail::FunctionToBFloat16>(FunctionDocumentation{.description=R"(
Converts Float32 to BFloat16 with losing the precision.

Example:
[example:typical]
)",
        .examples{
            {"typical", "SELECT toBFloat16(12.3::Float32);", "12.3125"}},
        .category = FunctionDocumentation::Category::TypeConversion});

    factory.registerFunction<detail::FunctionToFloat32>();
    factory.registerFunction<detail::FunctionToFloat64>();

    factory.registerFunction<detail::FunctionToDecimal32>();
    factory.registerFunction<detail::FunctionToDecimal64>();
    factory.registerFunction<detail::FunctionToDecimal128>();
    factory.registerFunction<detail::FunctionToDecimal256>();

    factory.registerFunction<detail::FunctionToDate>();

    /// MySQL compatibility alias. Cannot be registered as alias,
    /// because we don't want it to be normalized to toDate in queries,
    /// otherwise CREATE DICTIONARY query breaks.
    factory.registerFunction("DATE", &detail::FunctionToDate::create, {}, FunctionFactory::Case::Insensitive);

    factory.registerFunction<detail::FunctionToDate32>();
    factory.registerFunction<detail::FunctionToDateTime>();
    factory.registerFunction<detail::FunctionToDateTime32>();
    factory.registerFunction<detail::FunctionToDateTime64>();
    factory.registerFunction<detail::FunctionToUUID>();
    factory.registerFunction<detail::FunctionToIPv4>();
    factory.registerFunction<detail::FunctionToIPv6>();
    factory.registerFunction<detail::FunctionToString>();

    FunctionDocumentation::Description description_to_unix_timestamp = R"(
Converts a `String`, `Date`, or `DateTime` to a Unix timestamp (seconds since `1970-01-01 00:00:00 UTC`) as `UInt32`.
    )";
    FunctionDocumentation::Syntax syntax_to_unix_timestamp = R"(
toUnixTimestamp(date, [timezone])
    )";
    FunctionDocumentation::Arguments arguments_to_unix_timestamp = {
        {"date", "Value to convert. [`Date`](/sql-reference/data-types/date)/[`Date32`](/sql-reference/data-types/date32)/[`DateTime`](/sql-reference/data-types/datetime)/[`DateTime64`](/sql-reference/data-types/datetime64)/[`String`](/sql-reference/data-types/string)."},
        {"timezone", "Optional. Timezone to use for conversion. If not specified, the server's timezone is used. [`String`](/sql-reference/data-types/string)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_unix_timestamp = "Returns the Unix timestamp as [`UInt32`](/sql-reference/data-types/int-uint).";
    FunctionDocumentation::Examples examples_to_unix_timestamp = {
        {"Usage example", R"(
SELECT
'2017-11-05 08:07:47' AS dt_str,
toUnixTimestamp(dt_str) AS from_str,
toUnixTimestamp(dt_str, 'Asia/Tokyo') AS from_str_tokyo,
toUnixTimestamp(toDateTime(dt_str)) AS from_datetime,
toUnixTimestamp(toDateTime64(dt_str, 0)) AS from_datetime64,
toUnixTimestamp(toDate(dt_str)) AS from_date,
toUnixTimestamp(toDate32(dt_str)) AS from_date32
FORMAT Vertical;
        )", R"(
Row 1:
──────
dt_str:          2017-11-05 08:07:47
from_str:        1509869267
from_str_tokyo:  1509836867
from_datetime:   1509869267
from_datetime64: 1509869267
from_date:       1509840000
from_date32:     1509840000
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_unix_timestamp = {1, 1};
    FunctionDocumentation::Category category_to_unix_timestamp = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_unix_timestamp = {
        description_to_unix_timestamp,
        syntax_to_unix_timestamp,
        arguments_to_unix_timestamp,
        returned_value_to_unix_timestamp,
        examples_to_unix_timestamp,
        introduced_in_to_unix_timestamp,
        category_to_unix_timestamp
    };
    factory.registerFunction<detail::FunctionToUnixTimestamp>(documentation_to_unix_timestamp);

    factory.registerFunction<detail::FunctionToUInt8OrZero>();
    factory.registerFunction<detail::FunctionToUInt16OrZero>();
    factory.registerFunction<detail::FunctionToUInt32OrZero>();
    factory.registerFunction<detail::FunctionToUInt64OrZero>();
    factory.registerFunction<detail::FunctionToUInt128OrZero>();
    factory.registerFunction<detail::FunctionToUInt256OrZero>();
    factory.registerFunction<detail::FunctionToInt8OrZero>();
    factory.registerFunction<detail::FunctionToInt16OrZero>();
    factory.registerFunction<detail::FunctionToInt32OrZero>();
    factory.registerFunction<detail::FunctionToInt64OrZero>();
    factory.registerFunction<detail::FunctionToInt128OrZero>();
    factory.registerFunction<detail::FunctionToInt256OrZero>();

    factory.registerFunction<detail::FunctionToBFloat16OrZero>(FunctionDocumentation{.description=R"(
Converts String to BFloat16.

If the string does not represent a floating point value, the function returns zero.

The function allows a silent loss of precision while converting from the string representation. In that case, it will return the truncated result.

Example of successful conversion:
[example:typical]

Examples of not successful conversion:
[example:invalid1]
[example:invalid2]

Example of a loss of precision:
[example:precision]
)",
        .examples{
            {"typical", "SELECT toBFloat16OrZero('12.3');", "12.3125"},
            {"invalid1", "SELECT toBFloat16OrZero('abc');", "0"},
            {"invalid2", "SELECT toBFloat16OrZero(' 1');", "0"},
            {"precision", "SELECT toBFloat16OrZero('12.3456789');", "12.375"}},
        .category = FunctionDocumentation::Category::TypeConversion});

    factory.registerFunction<detail::FunctionToFloat32OrZero>();
    factory.registerFunction<detail::FunctionToFloat64OrZero>();
    factory.registerFunction<detail::FunctionToDateOrZero>();
    factory.registerFunction<detail::FunctionToDate32OrZero>();
    factory.registerFunction<detail::FunctionToDateTimeOrZero>();
    factory.registerFunction<detail::FunctionToDateTime64OrZero>();

    factory.registerFunction<detail::FunctionToDecimal32OrZero>();
    factory.registerFunction<detail::FunctionToDecimal64OrZero>();
    factory.registerFunction<detail::FunctionToDecimal128OrZero>();
    factory.registerFunction<detail::FunctionToDecimal256OrZero>();

    factory.registerFunction<detail::FunctionToUUIDOrZero>();
    factory.registerFunction<detail::FunctionToIPv4OrZero>();
    factory.registerFunction<detail::FunctionToIPv6OrZero>();

    factory.registerFunction<detail::FunctionToUInt8OrNull>();
    factory.registerFunction<detail::FunctionToUInt16OrNull>();
    factory.registerFunction<detail::FunctionToUInt32OrNull>();
    factory.registerFunction<detail::FunctionToUInt64OrNull>();
    factory.registerFunction<detail::FunctionToUInt128OrNull>();
    factory.registerFunction<detail::FunctionToUInt256OrNull>();
    factory.registerFunction<detail::FunctionToInt8OrNull>();
    factory.registerFunction<detail::FunctionToInt16OrNull>();
    factory.registerFunction<detail::FunctionToInt32OrNull>();
    factory.registerFunction<detail::FunctionToInt64OrNull>();
    factory.registerFunction<detail::FunctionToInt128OrNull>();
    factory.registerFunction<detail::FunctionToInt256OrNull>();

    factory.registerFunction<detail::FunctionToBFloat16OrNull>(FunctionDocumentation{.description=R"(
Converts String to Nullable(BFloat16).

If the string does not represent a floating point value, the function returns NULL.

The function allows a silent loss of precision while converting from the string representation. In that case, it will return the truncated result.

Example of successful conversion:
[example:typical]

Examples of not successful conversion:
[example:invalid1]
[example:invalid2]

Example of a loss of precision:
[example:precision]
)",
    .examples{
        {"typical", "SELECT toBFloat16OrNull('12.3');", "12.3125"},
        {"invalid1", "SELECT toBFloat16OrNull('abc');", "NULL"},
        {"invalid2", "SELECT toBFloat16OrNull(' 1');", "NULL"},
        {"precision", "SELECT toBFloat16OrNull('12.3456789');", "12.375"}},
    .category = FunctionDocumentation::Category::TypeConversion});

    factory.registerFunction<detail::FunctionToFloat32OrNull>();
    factory.registerFunction<detail::FunctionToFloat64OrNull>();
    factory.registerFunction<detail::FunctionToDateOrNull>();
    factory.registerFunction<detail::FunctionToDate32OrNull>();
    factory.registerFunction<detail::FunctionToDateTimeOrNull>();
    factory.registerFunction<detail::FunctionToDateTime64OrNull>();

    factory.registerFunction<detail::FunctionToDecimal32OrNull>();
    factory.registerFunction<detail::FunctionToDecimal64OrNull>();
    factory.registerFunction<detail::FunctionToDecimal128OrNull>();
    factory.registerFunction<detail::FunctionToDecimal256OrNull>();

    factory.registerFunction<detail::FunctionToUUIDOrNull>();
    factory.registerFunction<detail::FunctionToIPv4OrNull>();
    factory.registerFunction<detail::FunctionToIPv6OrNull>();

    factory.registerFunction<detail::FunctionParseDateTimeBestEffort>();
    factory.registerFunction<detail::FunctionParseDateTimeBestEffortOrZero>();
    factory.registerFunction<detail::FunctionParseDateTimeBestEffortOrNull>();
    factory.registerFunction<detail::FunctionParseDateTimeBestEffortUS>();
    factory.registerFunction<detail::FunctionParseDateTimeBestEffortUSOrZero>();
    factory.registerFunction<detail::FunctionParseDateTimeBestEffortUSOrNull>();
    factory.registerFunction<detail::FunctionParseDateTime32BestEffort>();
    factory.registerFunction<detail::FunctionParseDateTime32BestEffortOrZero>();
    factory.registerFunction<detail::FunctionParseDateTime32BestEffortOrNull>();
    factory.registerFunction<detail::FunctionParseDateTime64BestEffort>();
    factory.registerFunction<detail::FunctionParseDateTime64BestEffortOrZero>();
    factory.registerFunction<detail::FunctionParseDateTime64BestEffortOrNull>();
    factory.registerFunction<detail::FunctionParseDateTime64BestEffortUS>();
    factory.registerFunction<detail::FunctionParseDateTime64BestEffortUSOrZero>();
    factory.registerFunction<detail::FunctionParseDateTime64BestEffortUSOrNull>();

    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalNanosecond, detail::PositiveMonotonicity>>();
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalMicrosecond, detail::PositiveMonotonicity>>();
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalMillisecond, detail::PositiveMonotonicity>>();
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalSecond, detail::PositiveMonotonicity>>();
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalMinute, detail::PositiveMonotonicity>>();
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalHour, detail::PositiveMonotonicity>>();
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalDay, detail::PositiveMonotonicity>>();
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalWeek, detail::PositiveMonotonicity>>();
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalMonth, detail::PositiveMonotonicity>>();
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalQuarter, detail::PositiveMonotonicity>>();
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalYear, detail::PositiveMonotonicity>>();
}

}
