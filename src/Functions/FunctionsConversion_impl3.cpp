#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffort, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::BestEffort>;
template class FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffortOrZero, ConvertFromStringExceptionMode::Zero, ConvertFromStringParsingMode::BestEffort>;
template class FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffortOrNull, ConvertFromStringExceptionMode::Null, ConvertFromStringParsingMode::BestEffort>;

template class FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffortUS, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::BestEffortUS>;
template class FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffortUSOrZero, ConvertFromStringExceptionMode::Zero, ConvertFromStringParsingMode::BestEffortUS>;
template class FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffortUSOrNull, ConvertFromStringExceptionMode::Null, ConvertFromStringParsingMode::BestEffortUS>;

template class FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTime32BestEffort, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::BestEffort>;
template class FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTime32BestEffortOrZero, ConvertFromStringExceptionMode::Zero, ConvertFromStringParsingMode::BestEffort>;
template class FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTime32BestEffortOrNull, ConvertFromStringExceptionMode::Null, ConvertFromStringParsingMode::BestEffort>;

template class FunctionConvertFromString<
    DataTypeDateTime64, NameParseDateTime64BestEffort, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::BestEffort>;
template class FunctionConvertFromString<
    DataTypeDateTime64, NameParseDateTime64BestEffortOrZero, ConvertFromStringExceptionMode::Zero, ConvertFromStringParsingMode::BestEffort>;
template class FunctionConvertFromString<
    DataTypeDateTime64, NameParseDateTime64BestEffortOrNull, ConvertFromStringExceptionMode::Null, ConvertFromStringParsingMode::BestEffort>;

template class FunctionConvertFromString<
    DataTypeDateTime64, NameParseDateTime64BestEffortUS, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::BestEffortUS>;
template class FunctionConvertFromString<
    DataTypeDateTime64, NameParseDateTime64BestEffortUSOrZero, ConvertFromStringExceptionMode::Zero, ConvertFromStringParsingMode::BestEffortUS>;
template class FunctionConvertFromString<
    DataTypeDateTime64, NameParseDateTime64BestEffortUSOrNull, ConvertFromStringExceptionMode::Null, ConvertFromStringParsingMode::BestEffortUS>;

template class FunctionConvert<DataTypeTime, NameToTime, ToDateTimeMonotonicity<DataTypeTime>>;

template class FunctionConvert<DataTypeTime64, NameToTime64, ToDateTimeMonotonicity<DataTypeTime64>>;

}

}
