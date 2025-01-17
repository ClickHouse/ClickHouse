#include "parseFieldFromString.h"
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Common/assert_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

Field parseFieldFromString(const String & value, DB::DataTypePtr data_type)
{
    DataTypePtr check_type;
    if (data_type->isNullable())
        check_type = static_cast<const DataTypeNullable *>(data_type.get())->getNestedType();
    else
        check_type = data_type;

    WhichDataType which(check_type->getTypeId());
    if (which.isStringOrFixedString())
        return value;
    if (isBool(check_type))
        return parse<bool>(value);
    if (which.isInt8())
        return parse<Int8>(value);
    if (which.isUInt8())
        return parse<UInt8>(value);
    if (which.isInt16())
        return parse<Int16>(value);
    if (which.isUInt16())
        return parse<UInt16>(value);
    if (which.isInt32())
        return parse<Int32>(value);
    if (which.isUInt32())
        return parse<UInt32>(value);
    if (which.isInt64())
        return parse<Int64>(value);
    if (which.isUInt64())
        return parse<UInt64>(value);
    if (which.isFloat32())
        return parse<Float32>(value);
    if (which.isFloat64())
        return parse<Float64>(value);
    if (which.isDate())
        return UInt16{LocalDate{std::string(value)}.getDayNum()};
    if (which.isDate32())
        return Int32{LocalDate{std::string(value)}.getExtenedDayNum()};
    if (which.isDateTime64())
    {
        ReadBufferFromString in(value);
        DateTime64 time = 0;
        readDateTime64Text(
            time, 6, in,
            assert_cast<const DataTypeDateTime64 *>(data_type.get())->getTimeZone());
        return time;
    }

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS, "Unsupported DeltaLake type for {}",
        check_type->getColumnType());
}

}
