#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvertFromString<DataTypeUInt8, NameToUInt8OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeUInt16, NameToUInt16OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeUInt32, NameToUInt32OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeUInt64, NameToUInt64OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeUInt128, NameToUInt128OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeUInt256, NameToUInt256OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeInt8, NameToInt8OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeInt16, NameToInt16OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeInt32, NameToInt32OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeInt64, NameToInt64OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeInt128, NameToInt128OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeInt256, NameToInt256OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeBFloat16, NameToBFloat16OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeFloat32, NameToFloat32OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeFloat64, NameToFloat64OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeDate, NameToDateOrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeDate32, NameToDate32OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeTime, NameToTimeOrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeTime64, NameToTime64OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeDateTime, NameToDateTimeOrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeDateTime64, NameToDateTime64OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeDecimal<Decimal32>, NameToDecimal32OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeDecimal<Decimal64>, NameToDecimal64OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeDecimal<Decimal128>, NameToDecimal128OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeDecimal<Decimal256>, NameToDecimal256OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeUUID, NameToUUIDOrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeIPv4, NameToIPv4OrZero, ConvertFromStringExceptionMode::Zero>;
template class FunctionConvertFromString<DataTypeIPv6, NameToIPv6OrZero, ConvertFromStringExceptionMode::Zero>;

}

}
