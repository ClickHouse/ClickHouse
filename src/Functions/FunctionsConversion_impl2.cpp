#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvertFromString<DataTypeUInt8, NameToUInt8OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeUInt16, NameToUInt16OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeUInt32, NameToUInt32OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeUInt64, NameToUInt64OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeUInt128, NameToUInt128OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeUInt256, NameToUInt256OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeInt8, NameToInt8OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeInt16, NameToInt16OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeInt32, NameToInt32OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeInt64, NameToInt64OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeInt128, NameToInt128OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeInt256, NameToInt256OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeBFloat16, NameToBFloat16OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeFloat32, NameToFloat32OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeFloat64, NameToFloat64OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeDate, NameToDateOrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeDate32, NameToDate32OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeTime, NameToTimeOrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeTime64, NameToTime64OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeDateTime, NameToDateTimeOrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeDateTime64, NameToDateTime64OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeDecimal<Decimal32>, NameToDecimal32OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeDecimal<Decimal64>, NameToDecimal64OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeDecimal<Decimal128>, NameToDecimal128OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeDecimal<Decimal256>, NameToDecimal256OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeUUID, NameToUUIDOrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeIPv4, NameToIPv4OrNull, ConvertFromStringExceptionMode::Null>;
template class FunctionConvertFromString<DataTypeIPv6, NameToIPv6OrNull, ConvertFromStringExceptionMode::Null>;

}

}
