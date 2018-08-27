#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>

namespace DB
{

void throwExceptionForIncompletelyParsedValue(
    ReadBuffer & read_buffer, Block & block, size_t result)
{
    const IDataType & to_type = *block.getByPosition(result).type;

    WriteBufferFromOwnString message_buf;
    message_buf << "Cannot parse string " << quote << String(read_buffer.buffer().begin(), read_buffer.buffer().size())
        << " as " << to_type.getName()
        << ": syntax error";

    if (read_buffer.offset())
        message_buf << " at position " << read_buffer.offset()
            << " (parsed just " << quote << String(read_buffer.buffer().begin(), read_buffer.offset()) << ")";
    else
        message_buf << " at begin of string";

    if (to_type.isNumber())
        message_buf << ". Note: there are to" << to_type.getName() << "OrZero and to" << to_type.getName() << "OrNull functions, which returns zero/NULL instead of throwing exception.";

    throw Exception(message_buf.str(), ErrorCodes::CANNOT_PARSE_TEXT);
}


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
    factory.registerFunction<FunctionToUUID>();
    factory.registerFunction<FunctionToString>();
    factory.registerFunction<FunctionToFixedString>();

    factory.registerFunction<FunctionToUnixTimestamp>();
    factory.registerFunction<FunctionBuilderCast>(FunctionFactory::CaseInsensitive);

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

    factory.registerFunction<FunctionParseDateTimeBestEffort>();
    factory.registerFunction<FunctionParseDateTimeBestEffortOrZero>();
    factory.registerFunction<FunctionParseDateTimeBestEffortOrNull>();

    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalSecond, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalMinute, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalHour, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalDay, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalWeek, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalMonth, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalYear, PositiveMonotonicity>>();
}

}
