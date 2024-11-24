#include <Functions/FunctionsConversion.h>

namespace DB
{

REGISTER_FUNCTION(Conversion)
{
    factory.registerFunction<FunctionsConversion::FunctionToUInt8>();
    factory.registerFunction<FunctionsConversion::FunctionToUInt16>();
    factory.registerFunction<FunctionsConversion::FunctionToUInt32>();
    factory.registerFunction<FunctionsConversion::FunctionToUInt64>();
    factory.registerFunction<FunctionsConversion::FunctionToUInt128>();
    factory.registerFunction<FunctionsConversion::FunctionToUInt256>();
    factory.registerFunction<FunctionsConversion::FunctionToInt8>();
    factory.registerFunction<FunctionsConversion::FunctionToInt16>();
    factory.registerFunction<FunctionsConversion::FunctionToInt32>();
    factory.registerFunction<FunctionsConversion::FunctionToInt64>();
    factory.registerFunction<FunctionsConversion::FunctionToInt128>();
    factory.registerFunction<FunctionsConversion::FunctionToInt256>();

    factory.registerFunction<FunctionsConversion::FunctionToBFloat16>(FunctionDocumentation{.description=R"(
Converts Float32 to BFloat16 with losing the precision.

Example:
[example:typical]
)",
        .examples{
            {"typical", "SELECT toBFloat16(12.3::Float32);", "12.3125"}},
        .categories{"Conversion"}});

    factory.registerFunction<FunctionsConversion::FunctionToFloat32>();
    factory.registerFunction<FunctionsConversion::FunctionToFloat64>();

    factory.registerFunction<FunctionsConversion::FunctionToDecimal32>();
    factory.registerFunction<FunctionsConversion::FunctionToDecimal64>();
    factory.registerFunction<FunctionsConversion::FunctionToDecimal128>();
    factory.registerFunction<FunctionsConversion::FunctionToDecimal256>();

    factory.registerFunction<FunctionsConversion::FunctionToDate>();

    /// MySQL compatibility alias. Cannot be registered as alias,
    /// because we don't want it to be normalized to toDate in queries,
    /// otherwise CREATE DICTIONARY query breaks.
    factory.registerFunction("DATE", &FunctionsConversion::FunctionToDate::create, {}, FunctionFactory::Case::Insensitive);

    factory.registerFunction<FunctionsConversion::FunctionToDate32>();
    factory.registerFunction<FunctionsConversion::FunctionToDateTime>();
    factory.registerFunction<FunctionsConversion::FunctionToDateTime32>();
    factory.registerFunction<FunctionsConversion::FunctionToDateTime64>();
    factory.registerFunction<FunctionsConversion::FunctionToUUID>();
    factory.registerFunction<FunctionsConversion::FunctionToIPv4>();
    factory.registerFunction<FunctionsConversion::FunctionToIPv6>();
    factory.registerFunction<FunctionsConversion::FunctionToString>();

    factory.registerFunction<FunctionsConversion::FunctionToUnixTimestamp>();

    factory.registerFunction<FunctionsConversion::FunctionToUInt8OrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToUInt16OrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToUInt32OrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToUInt64OrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToUInt128OrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToUInt256OrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToInt8OrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToInt16OrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToInt32OrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToInt64OrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToInt128OrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToInt256OrZero>();

    factory.registerFunction<FunctionsConversion::FunctionToBFloat16OrZero>(FunctionDocumentation{.description=R"(
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
        .categories{"Conversion"}});

    factory.registerFunction<FunctionsConversion::FunctionToFloat32OrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToFloat64OrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToDateOrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToDate32OrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToDateTimeOrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToDateTime64OrZero>();

    factory.registerFunction<FunctionsConversion::FunctionToDecimal32OrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToDecimal64OrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToDecimal128OrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToDecimal256OrZero>();

    factory.registerFunction<FunctionsConversion::FunctionToUUIDOrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToIPv4OrZero>();
    factory.registerFunction<FunctionsConversion::FunctionToIPv6OrZero>();

    factory.registerFunction<FunctionsConversion::FunctionToUInt8OrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToUInt16OrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToUInt32OrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToUInt64OrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToUInt128OrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToUInt256OrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToInt8OrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToInt16OrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToInt32OrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToInt64OrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToInt128OrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToInt256OrNull>();

    factory.registerFunction<FunctionsConversion::FunctionToBFloat16OrNull>(FunctionDocumentation{.description=R"(
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
    .categories{"Conversion"}});

    factory.registerFunction<FunctionsConversion::FunctionToFloat32OrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToFloat64OrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToDateOrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToDate32OrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToDateTimeOrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToDateTime64OrNull>();

    factory.registerFunction<FunctionsConversion::FunctionToDecimal32OrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToDecimal64OrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToDecimal128OrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToDecimal256OrNull>();

    factory.registerFunction<FunctionsConversion::FunctionToUUIDOrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToIPv4OrNull>();
    factory.registerFunction<FunctionsConversion::FunctionToIPv6OrNull>();

    factory.registerFunction<FunctionsConversion::FunctionParseDateTimeBestEffort>();
    factory.registerFunction<FunctionsConversion::FunctionParseDateTimeBestEffortOrZero>();
    factory.registerFunction<FunctionsConversion::FunctionParseDateTimeBestEffortOrNull>();
    factory.registerFunction<FunctionsConversion::FunctionParseDateTimeBestEffortUS>();
    factory.registerFunction<FunctionsConversion::FunctionParseDateTimeBestEffortUSOrZero>();
    factory.registerFunction<FunctionsConversion::FunctionParseDateTimeBestEffortUSOrNull>();
    factory.registerFunction<FunctionsConversion::FunctionParseDateTime32BestEffort>();
    factory.registerFunction<FunctionsConversion::FunctionParseDateTime32BestEffortOrZero>();
    factory.registerFunction<FunctionsConversion::FunctionParseDateTime32BestEffortOrNull>();
    factory.registerFunction<FunctionsConversion::FunctionParseDateTime64BestEffort>();
    factory.registerFunction<FunctionsConversion::FunctionParseDateTime64BestEffortOrZero>();
    factory.registerFunction<FunctionsConversion::FunctionParseDateTime64BestEffortOrNull>();
    factory.registerFunction<FunctionsConversion::FunctionParseDateTime64BestEffortUS>();
    factory.registerFunction<FunctionsConversion::FunctionParseDateTime64BestEffortUSOrZero>();
    factory.registerFunction<FunctionsConversion::FunctionParseDateTime64BestEffortUSOrNull>();

    factory.registerFunction<FunctionsConversion::FunctionConvert<DataTypeInterval, FunctionsConversion::NameToIntervalNanosecond, FunctionsConversion::PositiveMonotonicity>>();
    factory.registerFunction<FunctionsConversion::FunctionConvert<DataTypeInterval, FunctionsConversion::NameToIntervalMicrosecond, FunctionsConversion::PositiveMonotonicity>>();
    factory.registerFunction<FunctionsConversion::FunctionConvert<DataTypeInterval, FunctionsConversion::NameToIntervalMillisecond, FunctionsConversion::PositiveMonotonicity>>();
    factory.registerFunction<FunctionsConversion::FunctionConvert<DataTypeInterval, FunctionsConversion::NameToIntervalSecond, FunctionsConversion::PositiveMonotonicity>>();
    factory.registerFunction<FunctionsConversion::FunctionConvert<DataTypeInterval, FunctionsConversion::NameToIntervalMinute, FunctionsConversion::PositiveMonotonicity>>();
    factory.registerFunction<FunctionsConversion::FunctionConvert<DataTypeInterval, FunctionsConversion::NameToIntervalHour, FunctionsConversion::PositiveMonotonicity>>();
    factory.registerFunction<FunctionsConversion::FunctionConvert<DataTypeInterval, FunctionsConversion::NameToIntervalDay, FunctionsConversion::PositiveMonotonicity>>();
    factory.registerFunction<FunctionsConversion::FunctionConvert<DataTypeInterval, FunctionsConversion::NameToIntervalWeek, FunctionsConversion::PositiveMonotonicity>>();
    factory.registerFunction<FunctionsConversion::FunctionConvert<DataTypeInterval, FunctionsConversion::NameToIntervalMonth, FunctionsConversion::PositiveMonotonicity>>();
    factory.registerFunction<FunctionsConversion::FunctionConvert<DataTypeInterval, FunctionsConversion::NameToIntervalQuarter, FunctionsConversion::PositiveMonotonicity>>();
    factory.registerFunction<FunctionsConversion::FunctionConvert<DataTypeInterval, FunctionsConversion::NameToIntervalYear, FunctionsConversion::PositiveMonotonicity>>();
}

}
