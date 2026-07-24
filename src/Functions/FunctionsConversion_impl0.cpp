#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeUInt8, NameToUInt8, ToNumberMonotonicity<UInt8>>;
template class FunctionConvert<DataTypeUInt16, NameToUInt16, ToNumberMonotonicity<UInt16>>;
template class FunctionConvert<DataTypeUInt32, NameToUInt32, ToNumberMonotonicity<UInt32>>;
template class FunctionConvert<DataTypeUInt64, NameToUInt64, ToNumberMonotonicity<UInt64>>;
template class FunctionConvert<DataTypeUInt128, NameToUInt128, ToNumberMonotonicity<UInt128>>;
template class FunctionConvert<DataTypeUInt256, NameToUInt256, ToNumberMonotonicity<UInt256>>;
template class FunctionConvert<DataTypeInt8, NameToInt8, ToNumberMonotonicity<Int8>>;
template class FunctionConvert<DataTypeInt16, NameToInt16, ToNumberMonotonicity<Int16>>;
template class FunctionConvert<DataTypeInt32, NameToInt32, ToNumberMonotonicity<Int32>>;
template class FunctionConvert<DataTypeInt64, NameToInt64, ToNumberMonotonicity<Int64>>;
template class FunctionConvert<DataTypeInt128, NameToInt128, ToNumberMonotonicity<Int128>>;
template class FunctionConvert<DataTypeInt256, NameToInt256, ToNumberMonotonicity<Int256>>;
template class FunctionConvert<DataTypeBFloat16, NameToBFloat16, ToNumberMonotonicity<BFloat16>>;
template class FunctionConvert<DataTypeFloat32, NameToFloat32, ToNumberMonotonicity<Float32>>;
template class FunctionConvert<DataTypeFloat64, NameToFloat64, ToNumberMonotonicity<Float64>>;

template class FunctionConvert<DataTypeDate, NameToDate, ToDateMonotonicity<DataTypeDate>>;

template class FunctionConvert<DataTypeDate32, NameToDate32, ToDateMonotonicity<DataTypeDate32>>;

template class FunctionConvert<DataTypeDateTime, NameToDateTime, ToDateTimeMonotonicity<DataTypeDateTime>>;

template class FunctionConvert<DataTypeDateTime, NameToDateTime32, ToDateTimeMonotonicity<DataTypeDateTime>>;

template class FunctionConvert<DataTypeDateTime64, NameToDateTime64, ToDateTimeMonotonicity<DataTypeDateTime64>>;

template class FunctionConvert<DataTypeUUID, NameToUUID, ToNumberMonotonicity<UInt128>>;
template class FunctionConvert<DataTypeIPv4, NameToIPv4, ToNumberMonotonicity<UInt32>>;
template class FunctionConvert<DataTypeIPv6, NameToIPv6, ToNumberMonotonicity<UInt128>>;
template class FunctionConvert<DataTypeString, NameToString, ToStringMonotonicity>;
template class FunctionConvert<DataTypeUInt32, NameToUnixTimestamp, ToNumberMonotonicity<UInt32>>;
template class FunctionConvert<DataTypeDecimal<Decimal32>, NameToDecimal32, UnknownMonotonicity>;
template class FunctionConvert<DataTypeDecimal<Decimal64>, NameToDecimal64, UnknownMonotonicity>;
template class FunctionConvert<DataTypeDecimal<Decimal128>, NameToDecimal128, UnknownMonotonicity>;
template class FunctionConvert<DataTypeDecimal<Decimal256>, NameToDecimal256, UnknownMonotonicity>;

}

}
