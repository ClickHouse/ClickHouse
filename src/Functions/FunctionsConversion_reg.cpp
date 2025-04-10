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

    FunctionDocumentation::Description description_toBFloat16 = R"(
Converts Float32 to BFloat16 with losing the precision.

Supported arguments:

- Values of type (U)Int8/16/32/64/128/256.
- String representations of (U)Int8/16/32/128/256.
- Values of type Float32/64, including NaN and Inf.
- String representations of Float32/64, including NaN and Inf (case-insensitive).

)";
    FunctionDocumentation::Syntax syntax_toBFloat16 = "toBFloat16(expr)";
    FunctionDocumentation::Arguments arguments_toBFloat16 = {{"expr", "Expression returning a number or a string representation of a number. Expression."}};
    FunctionDocumentation::ReturnedValue returned_value_toBFloat16 = "16-bit brain-float value. BFloat16.";
    FunctionDocumentation::Examples examples_toBFloat16 = {{"typical", "SELECT toBFloat16(12.3::Float32);", "12.3125"}};
    FunctionDocumentation::Category categories_toBFloat16 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toBFloat16 = {
        description_toBFloat16, 
        syntax_toBFloat16, 
        arguments_toBFloat16, 
        returned_value_toBFloat16, 
        examples_toBFloat16, 
        categories_toBFloat16
    };

    factory.registerFunction<detail::FunctionToBFloat16>(documentation_toBFloat16);

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

    factory.registerFunction<detail::FunctionToUnixTimestamp>();

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

    factory.registerFunction<detail::FunctionToBFloat16OrZero>(FunctionDocumentation{
        .description = R"(
Converts a String input value to a value of type BFloat16. If the string does not represent a floating point value, the function returns zero.

Supported arguments:

- String representations of numeric values.
- Unsupported arguments (return 0):
- String representations of binary and hexadecimal values.
- Numeric values.
)",
        .syntax="toBFloat16OrZero(x)",
        .arguments = {{"x", "A String representation of a number. String."}},
        .returned_value = "16-bit brain-float value, otherwise 0. BFloat16.",
        .examples{
            {"Typical example", "SELECT toBFloat16OrZero('12.3');", "12.3125"},
            {"Invalid example 1", "SELECT toBFloat16OrZero('abc');", "0"},
            {"Invalid example 2", "SELECT toBFloat16OrZero(' 1');", "0"},
            {"Precision example", "SELECT toBFloat16OrZero('12.3456789');", "12.375"}},
        .category=FunctionDocumentation::Category::TypeConversion}
    );

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

    FunctionDocumentation::Description description_toBFloat16OrNull = R"(
Converts a String input value to a value of type `BFloat16` but if the string does not represent a floating point value, the function returns `NULL`.

Supported arguments:
- String representations of numeric values.
- Unsupported arguments (return NULL):
- String representations of binary and hexadecimal values.
- Numeric values.
)";
    FunctionDocumentation::Syntax syntax_toBFloat16OrNull = "toBFloat16OrNull(expr)";
    FunctionDocumentation::Arguments arguments_toBFloat16OrNull = {
        {"expr", "Expression returning a number or a string representation of a number. Expression."}
    };
    FunctionDocumentation::ReturnedValue returned_value_toBFloat16OrNull = "16-bit brain-float value. BFloat16.";
    FunctionDocumentation::Examples examples_toBFloat16OrNull = {
        {"typical", "SELECT toBFloat16OrNull('12.3');", "12.3125"},
        {"invalid1", "SELECT toBFloat16OrNull('abc');", "NULL"},
        {"invalid2", "SELECT toBFloat16OrNull(' 1');", "NULL"},
        {"precision", "SELECT toBFloat16OrNull('12.3456789');", "12.375"}
    };
    FunctionDocumentation::Category category_toBFloat16OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toBFloat16OrNull = {
        description_toBFloat16OrNull,
        syntax_toBFloat16OrNull,
        arguments_toBFloat16OrNull,
        returned_value_toBFloat16OrNull,
        examples_toBFloat16OrNull,
        category_toBFloat16OrNull
    };
    factory.registerFunction<detail::FunctionToBFloat16OrNull>(documentation_toBFloat16OrNull);

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
