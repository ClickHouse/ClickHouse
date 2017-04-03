#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/DataTypeTraits.h>

#include <Core/FieldVisitors.h>

#include <Interpreters/convertFieldToType.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
}


/** Checking for a `Field from` of `From` type falls to a range of values of type `To`.
  * `From` and `To` - numeric types. They can be floating-point types.
  * `From` is one of UInt64, Int64, Float64,
  *  whereas `To` can also be 8, 16, 32 bit.
  *
  * If falls into a range, then `from` is converted to the `Field` closest to the `To` type.
  * If not, return Field(Null).
  */

namespace
{

template <typename From, typename To>
static Field convertNumericTypeImpl(const Field & from)
{
    From value = from.get<From>();

    if (static_cast<long double>(value) != static_cast<long double>(To(value)))
        return {};

    return Field(typename NearestFieldType<To>::Type(value));
}

template <typename To>
static Field convertNumericType(const Field & from, const IDataType & type)
{
    if (from.getType() == Field::Types::UInt64)
        return convertNumericTypeImpl<UInt64, To>(from);
    if (from.getType() == Field::Types::Int64)
        return convertNumericTypeImpl<Int64, To>(from);
    if (from.getType() == Field::Types::Float64)
        return convertNumericTypeImpl<Float64, To>(from);

    throw Exception("Type mismatch in IN or VALUES section. Expected: " + type.getName() + ". Got: "
        + Field::Types::toString(from.getType()), ErrorCodes::TYPE_MISMATCH);
}


Field convertFieldToTypeImpl(const Field & src, const IDataType & type)
{
    if (type.isNumeric())
    {
        if (typeid_cast<const DataTypeUInt8 *>(&type))        return convertNumericType<UInt8>(src, type);
        if (typeid_cast<const DataTypeUInt16 *>(&type))        return convertNumericType<UInt16>(src, type);
        if (typeid_cast<const DataTypeUInt32 *>(&type))        return convertNumericType<UInt32>(src, type);
        if (typeid_cast<const DataTypeUInt64 *>(&type))        return convertNumericType<UInt64>(src, type);
        if (typeid_cast<const DataTypeInt8 *>(&type))        return convertNumericType<Int8>(src, type);
        if (typeid_cast<const DataTypeInt16 *>(&type))        return convertNumericType<Int16>(src, type);
        if (typeid_cast<const DataTypeInt32 *>(&type))        return convertNumericType<Int32>(src, type);
        if (typeid_cast<const DataTypeInt64 *>(&type))        return convertNumericType<Int64>(src, type);
        if (typeid_cast<const DataTypeFloat32 *>(&type))    return convertNumericType<Float32>(src, type);
        if (typeid_cast<const DataTypeFloat64 *>(&type))    return convertNumericType<Float64>(src, type);

        const bool is_date = typeid_cast<const DataTypeDate *>(&type);
        bool is_datetime = false;
        bool is_enum = false;

        if (!is_date)
            if (!(is_datetime = typeid_cast<const DataTypeDateTime *>(&type)))
                if (!(is_enum = dynamic_cast<const IDataTypeEnum *>(&type)))
                    throw Exception{"Logical error: unknown numeric type " + type.getName(), ErrorCodes::LOGICAL_ERROR};

        /// Numeric values for Enums should not be used directly in IN section
        if (src.getType() == Field::Types::UInt64 && !is_enum)
            return src;

        if (src.getType() == Field::Types::String)
        {
            if (is_date)
            {
                /// Convert 'YYYY-MM-DD' Strings to Date
                return UInt64(stringToDate(src.get<const String &>()));
            }
            else if (is_datetime)
            {
                /// Convert 'YYYY-MM-DD hh:mm:ss' Strings to DateTime
                return stringToDateTime(src.get<const String &>());
            }
            else if (is_enum)
            {
                /// Convert String to Enum's value
                return dynamic_cast<const IDataTypeEnum &>(type).castToValue(src);
            }
        }

        throw Exception("Type mismatch in IN or VALUES section. Expected: " + type.getName() + ". Got: "
            + Field::Types::toString(src.getType()), ErrorCodes::TYPE_MISMATCH);
    }
    else if (const DataTypeArray * type_array = typeid_cast<const DataTypeArray *>(&type))
    {
        if (src.getType() != Field::Types::Array)
            throw Exception("Type mismatch in IN or VALUES section. Expected: " + type.getName() + ". Got: "
                + Field::Types::toString(src.getType()), ErrorCodes::TYPE_MISMATCH);

        const IDataType & nested_type = *DataTypeTraits::removeNullable(type_array->getNestedType());

        const Array & src_arr = src.get<Array>();
        size_t src_arr_size = src_arr.size();

        Array res(src_arr_size);
        for (size_t i = 0; i < src_arr_size; ++i)
            res[i] = convertFieldToType(src_arr[i], nested_type);

        return res;
    }
    else
    {
        if (src.getType() == Field::Types::UInt64
            || src.getType() == Field::Types::Int64
            || src.getType() == Field::Types::Float64
            || src.getType() == Field::Types::Array
            || (src.getType() == Field::Types::String
                && !typeid_cast<const DataTypeString *>(&type)
                && !typeid_cast<const DataTypeFixedString *>(&type)))
            throw Exception("Type mismatch in IN or VALUES section. Expected: " + type.getName() + ". Got: "
                + Field::Types::toString(src.getType()), ErrorCodes::TYPE_MISMATCH);
    }

    return src;
}

}

Field convertFieldToType(const Field & from_value, const IDataType & to_type, const IDataType * from_type_hint)
{
    if (from_value.isNull())
        return from_value;

    if (from_type_hint && from_type_hint->equals(to_type))
        return from_value;

    if (to_type.isNullable())
    {
        const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(to_type);
        const DataTypePtr & nested_type = nullable_type.getNestedType();
        return convertFieldToTypeImpl(from_value, *nested_type);
    }
    else
        return convertFieldToTypeImpl(from_value, to_type);
}


}
