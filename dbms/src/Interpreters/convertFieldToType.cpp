#include <Interpreters/convertFieldToType.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>

#include <Core/AccurateComparison.h>
#include <Common/FieldVisitors.h>
#include <Common/typeid_cast.h>
#include <Common/NaNUtils.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <common/DateLUT.h>
#include <DataTypes/DataTypeAggregateFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
    extern const int TOO_LARGE_STRING_SIZE;
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
    To result;
    if (!accurate::convertNumeric(from.get<From>(), result))
        return {};
    return result;
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


template <typename From, typename To>
static Field convertIntToDecimalType(const Field & from, const To & type)
{
    using FieldType = typename To::FieldType;

    From value = from.get<From>();
    if (!type.canStoreWhole(value))
        throw Exception("Number is too much to place in " + type.getName(), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    FieldType scaled_value = type.getScaleMultiplier() * value;
    return DecimalField<FieldType>(scaled_value, type.getScale());
}


template <typename T>
static Field convertStringToDecimalType(const Field & from, const DataTypeDecimal<T> & type)
{
    using FieldType = typename DataTypeDecimal<T>::FieldType;

    const String & str_value = from.get<String>();
    T value = type.parseFromString(str_value);
    return DecimalField<FieldType>(value, type.getScale());
}


template <typename To>
static Field convertDecimalType(const Field & from, const To & type)
{
    if (from.getType() == Field::Types::UInt64)
        return convertIntToDecimalType<UInt64>(from, type);
    if (from.getType() == Field::Types::Int64)
        return convertIntToDecimalType<Int64>(from, type);
    if (from.getType() == Field::Types::String)
        return convertStringToDecimalType(from, type);

    throw Exception("Type mismatch in IN or VALUES section. Expected: " + type.getName() + ". Got: "
        + Field::Types::toString(from.getType()), ErrorCodes::TYPE_MISMATCH);
}


DayNum stringToDate(const String & s)
{
    ReadBufferFromString in(s);
    DayNum date{};

    readDateText(date, in);
    if (!in.eof())
        throw Exception("String is too long for Date: " + s, ErrorCodes::TOO_LARGE_STRING_SIZE);

    return date;
}

UInt64 stringToDateTime(const String & s)
{
    ReadBufferFromString in(s);
    time_t date_time{};

    readDateTimeText(date_time, in);
    if (!in.eof())
        throw Exception("String is too long for DateTime: " + s, ErrorCodes::TOO_LARGE_STRING_SIZE);

    return UInt64(date_time);
}

Field convertFieldToTypeImpl(const Field & src, const IDataType & type, const IDataType * from_type_hint)
{
    WhichDataType which_type(type);
    WhichDataType which_from_type;
    if (from_type_hint)
        which_from_type = WhichDataType(*from_type_hint);

    /// Conversion between Date and DateTime and vice versa.
    if (which_type.isDate() && which_from_type.isDateTime())
    {
        return static_cast<const DataTypeDateTime &>(*from_type_hint).getTimeZone().toDayNum(src.get<UInt64>());
    }
    else if (which_type.isDateTime() && which_from_type.isDate())
    {
        return static_cast<const DataTypeDateTime &>(type).getTimeZone().fromDayNum(DayNum(src.get<UInt64>()));
    }
    else if (type.isValueRepresentedByNumber())
    {
        if (which_type.isUInt8()) return convertNumericType<UInt8>(src, type);
        if (which_type.isUInt16()) return convertNumericType<UInt16>(src, type);
        if (which_type.isUInt32()) return convertNumericType<UInt32>(src, type);
        if (which_type.isUInt64()) return convertNumericType<UInt64>(src, type);
        if (which_type.isInt8()) return convertNumericType<Int8>(src, type);
        if (which_type.isInt16()) return convertNumericType<Int16>(src, type);
        if (which_type.isInt32()) return convertNumericType<Int32>(src, type);
        if (which_type.isInt64()) return convertNumericType<Int64>(src, type);
        if (which_type.isFloat32()) return convertNumericType<Float32>(src, type);
        if (which_type.isFloat64()) return convertNumericType<Float64>(src, type);
        if (auto * ptype = typeid_cast<const DataTypeDecimal<Decimal32> *>(&type)) return convertDecimalType(src, *ptype);
        if (auto * ptype = typeid_cast<const DataTypeDecimal<Decimal64> *>(&type)) return convertDecimalType(src, *ptype);
        if (auto * ptype = typeid_cast<const DataTypeDecimal<Decimal128> *>(&type)) return convertDecimalType(src, *ptype);

        if (!which_type.isDateOrDateTime() && !which_type.isUUID() && !which_type.isEnum())
            throw Exception{"Logical error: unknown numeric type " + type.getName(), ErrorCodes::LOGICAL_ERROR};

        /// Numeric values for Enums should not be used directly in IN section
        if (src.getType() == Field::Types::UInt64 && !which_type.isEnum())
            return src;

        if (src.getType() == Field::Types::String)
        {
            if (which_type.isDate())
            {
                /// Convert 'YYYY-MM-DD' Strings to Date
                return stringToDate(src.get<const String &>());
            }
            else if (which_type.isDateTime())
            {
                /// Convert 'YYYY-MM-DD hh:mm:ss' Strings to DateTime
                return stringToDateTime(src.get<const String &>());
            }
            else if (which_type.isUUID())
            {
                return stringToUUID(src.get<const String &>());
            }
            else if (which_type.isEnum())
            {
                /// Convert String to Enum's value
                return dynamic_cast<const IDataTypeEnum &>(type).castToValue(src);
            }
        }
    }
    else if (which_type.isStringOrFixedString())
    {
        if (src.getType() == Field::Types::String)
            return src;
    }
    else if (const DataTypeArray * type_array = typeid_cast<const DataTypeArray *>(&type))
    {
        if (src.getType() == Field::Types::Array)
        {
            const DataTypePtr nested_type = removeNullable(type_array->getNestedType());

            const Array & src_arr = src.get<Array>();
            size_t src_arr_size = src_arr.size();

            Array res(src_arr_size);
            for (size_t i = 0; i < src_arr_size; ++i)
            {
                res[i] = convertFieldToType(src_arr[i], *nested_type);
                if (res[i].isNull() && !type_array->getNestedType()->isNullable())
                    throw Exception("Type mismatch of array elements in IN or VALUES section. Expected: " + type_array->getNestedType()->getName()
                        + ". Got NULL in position " + toString(i + 1), ErrorCodes::TYPE_MISMATCH);
            }

            return res;
        }
    }
    else if (const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(&type))
    {
        if (src.getType() == Field::Types::Tuple)
        {
            const TupleBackend & src_tuple = src.get<Tuple>();
            size_t src_tuple_size = src_tuple.size();
            size_t dst_tuple_size = type_tuple->getElements().size();

            if (dst_tuple_size != src_tuple_size)
                throw Exception("Bad size of tuple in IN or VALUES section. Expected size: "
                    + toString(dst_tuple_size) + ", actual size: " + toString(src_tuple_size), ErrorCodes::TYPE_MISMATCH);

            TupleBackend res(dst_tuple_size);
            for (size_t i = 0; i < dst_tuple_size; ++i)
                res[i] = convertFieldToType(src_tuple[i], *type_tuple->getElements()[i]);

            return res;
        }
    }
    else if (const DataTypeAggregateFunction * agg_func_type = typeid_cast<const DataTypeAggregateFunction *>(&type))
    {
        if (src.getType() != Field::Types::AggregateFunctionState)
            throw Exception(String("Cannot convert ") + src.getTypeName() + " to " + agg_func_type->getName(),
                    ErrorCodes::TYPE_MISMATCH);

        auto & name = src.get<AggregateFunctionStateData>().name;
        if (agg_func_type->getName() != name)
            throw Exception("Cannot convert " + name + " to " + agg_func_type->getName(), ErrorCodes::TYPE_MISMATCH);

        return src;
    }

    if (src.getType() == Field::Types::String)
    {
        const auto col = type.createColumn();
        ReadBufferFromString buffer(src.get<String>());
        type.deserializeAsTextEscaped(*col, buffer, FormatSettings{});

        return (*col)[0];
    }


    // TODO (nemkov): should we attempt to parse value using or `type.deserializeAsTextEscaped()` type.deserializeAsTextEscaped() ?
    throw Exception("Type mismatch in IN or VALUES section. Expected: " + type.getName() + ". Got: "
        + Field::Types::toString(src.getType()), ErrorCodes::TYPE_MISMATCH);
}

}

Field convertFieldToType(const Field & from_value, const IDataType & to_type, const IDataType * from_type_hint)
{
    if (from_value.isNull())
        return from_value;

    if (from_type_hint && from_type_hint->equals(to_type))
        return from_value;

    if (auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(&to_type))
        return convertFieldToType(from_value, *low_cardinality_type->getDictionaryType(), from_type_hint);
    else if (auto * nullable_type = typeid_cast<const DataTypeNullable *>(&to_type))
        return convertFieldToTypeImpl(from_value, *nullable_type->getNestedType(), from_type_hint);
    else
        return convertFieldToTypeImpl(from_value, to_type, from_type_hint);
}


}
