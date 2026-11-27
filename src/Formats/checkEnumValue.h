#pragma once

#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Core/AccurateComparison.h>
#include <DataTypes/DataTypeEnum.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONVERT_TYPE;
}

/// Like CAST to an Enum: throws if the number `value` is not an element of `type`.
template <typename EnumType, typename T>
void checkEnumValue(const IDataType & type, T value)
{
    typename EnumType::FieldType enum_value = 0;
    if (!accurate::convertNumeric<T, typename EnumType::FieldType, false>(value, enum_value))
        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Value {} cannot be converted to enum type", value);
    (void)assert_cast<const EnumType &>(type).getNameForValue(enum_value);
}

}
