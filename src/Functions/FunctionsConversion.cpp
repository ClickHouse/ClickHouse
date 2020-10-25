#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>
#include <Interpreters/Context.h>


namespace DB
{

FunctionOverloadResolverImplPtr CastOverloadResolver::create(const Context & context)
{
    return createImpl(context.getSettingsRef().cast_keep_nullable);
}

void registerFunctionFixedString(FunctionFactory & factory);

void registerFunctionsConversion(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToUInt8>();
    factory.registerFunction<FunctionToUInt16>();
    factory.registerFunction<FunctionToUInt32>();
    factory.registerFunction<FunctionToUInt64>();
    factory.registerFunction<FunctionToInt8>();
    factory.registerFunction<FunctionToInt16>();
    factory.registerFunction<FunctionToInt32>();
    factory.registerFunction<FunctionToInt64>();
    factory.registerFunction<FunctionToFloat32>();
    factory.registerFunction<FunctionToFloat64>();

    factory.registerFunction<FunctionToDecimal32>();
    factory.registerFunction<FunctionToDecimal64>();
    factory.registerFunction<FunctionToDecimal128>();

    factory.registerFunction<FunctionToDate>();
    factory.registerFunction<FunctionToDateTime>();
    factory.registerFunction<FunctionToDateTime64>();
    factory.registerFunction<FunctionToUUID>();
    factory.registerFunction<FunctionToString>();

    registerFunctionFixedString(factory);

    factory.registerFunction<FunctionToUnixTimestamp>();
    factory.registerFunction<CastOverloadResolver>(FunctionFactory::CaseInsensitive);

    factory.registerFunction<FunctionToUInt8OrZero>();
    factory.registerFunction<FunctionToUInt16OrZero>();
    factory.registerFunction<FunctionToUInt32OrZero>();
    factory.registerFunction<FunctionToUInt64OrZero>();
    factory.registerFunction<FunctionToInt8OrZero>();
    factory.registerFunction<FunctionToInt16OrZero>();
    factory.registerFunction<FunctionToInt32OrZero>();
    factory.registerFunction<FunctionToInt64OrZero>();
    factory.registerFunction<FunctionToFloat32OrZero>();
    factory.registerFunction<FunctionToFloat64OrZero>();
    factory.registerFunction<FunctionToDateOrZero>();
    factory.registerFunction<FunctionToDateTimeOrZero>();
    factory.registerFunction<FunctionToDateTime64OrZero>();

    factory.registerFunction<FunctionToDecimal32OrZero>();
    factory.registerFunction<FunctionToDecimal64OrZero>();
    factory.registerFunction<FunctionToDecimal128OrZero>();

    factory.registerFunction<FunctionToUInt8OrNull>();
    factory.registerFunction<FunctionToUInt16OrNull>();
    factory.registerFunction<FunctionToUInt32OrNull>();
    factory.registerFunction<FunctionToUInt64OrNull>();
    factory.registerFunction<FunctionToInt8OrNull>();
    factory.registerFunction<FunctionToInt16OrNull>();
    factory.registerFunction<FunctionToInt32OrNull>();
    factory.registerFunction<FunctionToInt64OrNull>();
    factory.registerFunction<FunctionToFloat32OrNull>();
    factory.registerFunction<FunctionToFloat64OrNull>();
    factory.registerFunction<FunctionToDateOrNull>();
    factory.registerFunction<FunctionToDateTimeOrNull>();
    factory.registerFunction<FunctionToDateTime64OrNull>();

    factory.registerFunction<FunctionToDecimal32OrNull>();
    factory.registerFunction<FunctionToDecimal64OrNull>();
    factory.registerFunction<FunctionToDecimal128OrNull>();

    factory.registerFunction<FunctionParseDateTimeBestEffort>();
    factory.registerFunction<FunctionParseDateTimeBestEffortUS>();
    factory.registerFunction<FunctionParseDateTimeBestEffortOrZero>();
    factory.registerFunction<FunctionParseDateTimeBestEffortOrNull>();
    factory.registerFunction<FunctionParseDateTime64BestEffort>();
    factory.registerFunction<FunctionParseDateTime64BestEffortOrZero>();
    factory.registerFunction<FunctionParseDateTime64BestEffortOrNull>();

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
