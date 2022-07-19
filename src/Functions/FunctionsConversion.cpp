#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>


namespace DB
{

void registerFunctionFixedString(FunctionFactory & factory);

void registerCastOverloadResolvers(FunctionFactory & factory);

void registerFunctionsConversion(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToUInt8>();
    factory.registerFunction<FunctionToUInt16>();
    factory.registerFunction<FunctionToUInt32>();
    factory.registerFunction<FunctionToUInt64>();
    factory.registerFunction<FunctionToUInt128>();
    factory.registerFunction<FunctionToUInt256>();
    factory.registerFunction<FunctionToInt8>();
    factory.registerFunction<FunctionToInt16>();
    factory.registerFunction<FunctionToInt32>();
    factory.registerFunction<FunctionToInt64>();
    factory.registerFunction<FunctionToInt128>();
    factory.registerFunction<FunctionToInt256>();
    factory.registerFunction<FunctionToFloat32>();
    factory.registerFunction<FunctionToFloat64>();

    factory.registerFunction<FunctionToDecimal32>();
    factory.registerFunction<FunctionToDecimal64>();
    factory.registerFunction<FunctionToDecimal128>();
    factory.registerFunction<FunctionToDecimal256>();

    factory.registerFunction<FunctionToDate>();
    /// MySQL compatibility alias.
    factory.registerFunction<FunctionToDate>("DATE", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionToDate32>();
    factory.registerFunction<FunctionToDateTime>();
    factory.registerFunction<FunctionToDateTime32>();
    factory.registerFunction<FunctionToDateTime64>();
    factory.registerFunction<FunctionToUUID>();
    factory.registerFunction<FunctionToString>();

    registerFunctionFixedString(factory);

    factory.registerFunction<FunctionToUnixTimestamp>();

    registerCastOverloadResolvers(factory);

    factory.registerFunction<FunctionToUInt8OrZero>();
    factory.registerFunction<FunctionToUInt16OrZero>();
    factory.registerFunction<FunctionToUInt32OrZero>();
    factory.registerFunction<FunctionToUInt64OrZero>();
    factory.registerFunction<FunctionToUInt128OrZero>();
    factory.registerFunction<FunctionToUInt256OrZero>();
    factory.registerFunction<FunctionToInt8OrZero>();
    factory.registerFunction<FunctionToInt16OrZero>();
    factory.registerFunction<FunctionToInt32OrZero>();
    factory.registerFunction<FunctionToInt64OrZero>();
    factory.registerFunction<FunctionToInt128OrZero>();
    factory.registerFunction<FunctionToInt256OrZero>();
    factory.registerFunction<FunctionToFloat32OrZero>();
    factory.registerFunction<FunctionToFloat64OrZero>();
    factory.registerFunction<FunctionToDateOrZero>();
    factory.registerFunction<FunctionToDate32OrZero>();
    factory.registerFunction<FunctionToDateTimeOrZero>();
    factory.registerFunction<FunctionToDateTime64OrZero>();

    factory.registerFunction<FunctionToDecimal32OrZero>();
    factory.registerFunction<FunctionToDecimal64OrZero>();
    factory.registerFunction<FunctionToDecimal128OrZero>();
    factory.registerFunction<FunctionToDecimal256OrZero>();

    factory.registerFunction<FunctionToUUIDOrZero>();

    factory.registerFunction<FunctionToUInt8OrNull>();
    factory.registerFunction<FunctionToUInt16OrNull>();
    factory.registerFunction<FunctionToUInt32OrNull>();
    factory.registerFunction<FunctionToUInt64OrNull>();
    factory.registerFunction<FunctionToUInt128OrNull>();
    factory.registerFunction<FunctionToUInt256OrNull>();
    factory.registerFunction<FunctionToInt8OrNull>();
    factory.registerFunction<FunctionToInt16OrNull>();
    factory.registerFunction<FunctionToInt32OrNull>();
    factory.registerFunction<FunctionToInt64OrNull>();
    factory.registerFunction<FunctionToInt128OrNull>();
    factory.registerFunction<FunctionToInt256OrNull>();
    factory.registerFunction<FunctionToFloat32OrNull>();
    factory.registerFunction<FunctionToFloat64OrNull>();
    factory.registerFunction<FunctionToDateOrNull>();
    factory.registerFunction<FunctionToDate32OrNull>();
    factory.registerFunction<FunctionToDateTimeOrNull>();
    factory.registerFunction<FunctionToDateTime64OrNull>();

    factory.registerFunction<FunctionToDecimal32OrNull>();
    factory.registerFunction<FunctionToDecimal64OrNull>();
    factory.registerFunction<FunctionToDecimal128OrNull>();
    factory.registerFunction<FunctionToDecimal256OrNull>();

    factory.registerFunction<FunctionToUUIDOrNull>();

    factory.registerFunction<FunctionParseDateTimeBestEffort>();
    factory.registerFunction<FunctionParseDateTimeBestEffortOrZero>();
    factory.registerFunction<FunctionParseDateTimeBestEffortOrNull>();
    factory.registerFunction<FunctionParseDateTimeBestEffortUS>();
    factory.registerFunction<FunctionParseDateTimeBestEffortUSOrZero>();
    factory.registerFunction<FunctionParseDateTimeBestEffortUSOrNull>();
    factory.registerFunction<FunctionParseDateTime32BestEffort>();
    factory.registerFunction<FunctionParseDateTime32BestEffortOrZero>();
    factory.registerFunction<FunctionParseDateTime32BestEffortOrNull>();
    factory.registerFunction<FunctionParseDateTime64BestEffort>();
    factory.registerFunction<FunctionParseDateTime64BestEffortOrZero>();
    factory.registerFunction<FunctionParseDateTime64BestEffortOrNull>();

    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalNanosecond, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalMicrosecond, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalMillisecond, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalSecond, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalMinute, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalHour, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalDay, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalWeek, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalMonth, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalQuarter, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalYear, PositiveMonotonicity>>();
}

}
