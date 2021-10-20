#include <Interpreters/convertFieldToType.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

#include <Core/AccurateComparison.h>
#include <Common/typeid_cast.h>
#include <Common/NaNUtils.h>

#include <common/DateLUT.h>
#include <DataTypes/DataTypeAggregateFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
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
    if (from.getType() == Field::Types::UInt128)
        return convertNumericTypeImpl<UInt128, To>(from);
    if (from.getType() == Field::Types::Int128)
        return convertNumericTypeImpl<Int128, To>(from);
    if (from.getType() == Field::Types::UInt256)
        return convertNumericTypeImpl<UInt256, To>(from);
    if (from.getType() == Field::Types::Int256)
        return convertNumericTypeImpl<Int256, To>(from);

    throw Exception("Type mismatch in IN or VALUES section. Expected: " + type.getName() + ". Got: "
        + Field::Types::toString(from.getType()), ErrorCodes::TYPE_MISMATCH);
}


template <typename From, typename T>
static Field convertIntToDecimalType(const Field & from, const DataTypeDecimal<T> & type)
{
    From value = from.get<From>();
    if (!type.canStoreWhole(value))
        throw Exception("Number is too big to place in " + type.getName(), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    T scaled_value = type.getScaleMultiplier() * static_cast<T>(value);
    return DecimalField<T>(scaled_value, type.getScale());
}


template <typename T>
static Field convertStringToDecimalType(const Field & from, const DataTypeDecimal<T> & type)
{
    const String & str_value = from.get<String>();
    T value = type.parseFromString(str_value);
    return DecimalField<T>(value, type.getScale());
}

template <typename From, typename T>
static Field convertDecimalToDecimalType(const Field & from, const DataTypeDecimal<T> & type)
{
    auto field = from.get<DecimalField<From>>();
    T value = convertDecimals<DataTypeDecimal<From>, DataTypeDecimal<T>>(field.getValue(), field.getScale(), type.getScale());
    return DecimalField<T>(value, type.getScale());
}

template <typename To>
static Field convertDecimalType(const Field & from, const To & type)
{
    if (from.getType() == Field::Types::UInt64)
        return convertIntToDecimalType<UInt64>(from, type);
    if (from.getType() == Field::Types::Int64)
        return convertIntToDecimalType<Int64>(from, type);
    if (from.getType() == Field::Types::UInt128)
        return convertIntToDecimalType<UInt128>(from, type);
    if (from.getType() == Field::Types::Int128)
        return convertIntToDecimalType<Int128>(from, type);
    if (from.getType() == Field::Types::UInt256)
        return convertIntToDecimalType<UInt256>(from, type);
    if (from.getType() == Field::Types::Int256)
        return convertIntToDecimalType<Int256>(from, type);

    if (from.getType() == Field::Types::String)
        return convertStringToDecimalType(from, type);

    if (from.getType() == Field::Types::Decimal32)
        return convertDecimalToDecimalType<Decimal32>(from, type);
    if (from.getType() == Field::Types::Decimal64)
        return convertDecimalToDecimalType<Decimal64>(from, type);
    if (from.getType() == Field::Types::Decimal128)
        return convertDecimalToDecimalType<Decimal128>(from, type);

    throw Exception("Type mismatch in IN or VALUES section. Expected: " + type.getName() + ". Got: "
        + Field::Types::toString(from.getType()), ErrorCodes::TYPE_MISMATCH);
}


Field convertFieldToTypeImpl(const Field & src, const IDataType & type, const IDataType * from_type_hint)
{
    if (from_type_hint && from_type_hint->equals(type))
    {
        return src;
    }

    WhichDataType which_type(type);
    WhichDataType which_from_type;

    if (from_type_hint)
    {
        which_from_type = WhichDataType(*from_type_hint);
    }

    /// Conversion between Date and DateTime and vice versa.
    if (which_type.isDate() && which_from_type.isDateTime())
    {
        return static_cast<UInt16>(static_cast<const DataTypeDateTime &>(*from_type_hint).getTimeZone().toDayNum(src.get<UInt64>()).toUnderType());
    }
    else if (which_type.isDateTime() && which_from_type.isDate())
    {
        return static_cast<const DataTypeDateTime &>(type).getTimeZone().fromDayNum(DayNum(src.get<UInt64>()));
    }
    else if (type.isValueRepresentedByNumber() && src.getType() != Field::Types::String)
    {
        if (which_type.isUInt8()) return convertNumericType<UInt8>(src, type);
        if (which_type.isUInt16()) return convertNumericType<UInt16>(src, type);
        if (which_type.isUInt32()) return convertNumericType<UInt32>(src, type);
        if (which_type.isUInt64()) return convertNumericType<UInt64>(src, type);
        if (which_type.isUInt128()) return convertNumericType<UInt128>(src, type);
        if (which_type.isUInt256()) return convertNumericType<UInt256>(src, type);
        if (which_type.isInt8()) return convertNumericType<Int8>(src, type);
        if (which_type.isInt16()) return convertNumericType<Int16>(src, type);
        if (which_type.isInt32()) return convertNumericType<Int32>(src, type);
        if (which_type.isInt64()) return convertNumericType<Int64>(src, type);
        if (which_type.isInt128()) return convertNumericType<Int128>(src, type);
        if (which_type.isInt256()) return convertNumericType<Int256>(src, type);
        if (which_type.isFloat32()) return convertNumericType<Float32>(src, type);
        if (which_type.isFloat64()) return convertNumericType<Float64>(src, type);
        if (const auto * ptype = typeid_cast<const DataTypeDecimal<Decimal32> *>(&type)) return convertDecimalType(src, *ptype);
        if (const auto * ptype = typeid_cast<const DataTypeDecimal<Decimal64> *>(&type)) return convertDecimalType(src, *ptype);
        if (const auto * ptype = typeid_cast<const DataTypeDecimal<Decimal128> *>(&type)) return convertDecimalType(src, *ptype);

        if (which_type.isEnum() && (src.getType() == Field::Types::UInt64 || src.getType() == Field::Types::Int64))
        {
            /// Convert UInt64 or Int64 to Enum's value
            return dynamic_cast<const IDataTypeEnum &>(type).castToValue(src);
        }

        if ((which_type.isDate() || which_type.isDateTime()) && src.getType() == Field::Types::UInt64)
        {
            /// We don't need any conversion UInt64 is under type of Date and DateTime
            return src;
        }

        if (which_type.isDateTime64() && src.getType() == Field::Types::Decimal64)
        {
            /// Already in needed type.
            return src;
        }

        if (which_type.isDateTime64()
            && (which_from_type.isNativeInt() || which_from_type.isNativeUInt() || which_from_type.isDate() || which_from_type.isDateTime() || which_from_type.isDateTime64()))
        {
            const auto scale = static_cast<const DataTypeDateTime64 &>(type).getScale();
            const auto decimal_value = DecimalUtils::decimalFromComponents<DateTime64>(src.reinterpret<Int64>(), 0, scale);
            return Field(DecimalField<DateTime64>(decimal_value, scale));
        }
    }
    else if (which_type.isUUID() && src.getType() == Field::Types::UUID)
    {
        /// Already in needed type.
        return src;
    }
    else if (which_type.isStringOrFixedString())
    {
        if (src.getType() == Field::Types::String)
        {
            if (which_type.isFixedString())
            {
                size_t n = assert_cast<const DataTypeFixedString &>(type).getN();
                const auto & src_str = src.get<String>();
                if (src_str.size() < n)
                {
                    String src_str_extended = src_str;
                    src_str_extended.resize(n);
                    return src_str_extended;
                }
            }
            return src;
        }
    }
    else if (const DataTypeArray * type_array = typeid_cast<const DataTypeArray *>(&type))
    {
        if (src.getType() == Field::Types::Array)
        {
            const Array & src_arr = src.get<Array>();
            size_t src_arr_size = src_arr.size();

            const auto & element_type = *(type_array->getNestedType());
            bool have_unconvertible_element = false;
            Array res(src_arr_size);
            for (size_t i = 0; i < src_arr_size; ++i)
            {
                res[i] = convertFieldToType(src_arr[i], element_type);
                if (res[i].isNull() && !element_type.isNullable())
                {
                    // See the comment for Tuples below.
                    have_unconvertible_element = true;
                }
            }

            return have_unconvertible_element ? Field(Null()) : Field(res);
        }
    }
    else if (const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(&type))
    {
        if (src.getType() == Field::Types::Tuple)
        {
            const auto & src_tuple = src.get<Tuple>();
            size_t src_tuple_size = src_tuple.size();
            size_t dst_tuple_size = type_tuple->getElements().size();

            if (dst_tuple_size != src_tuple_size)
                throw Exception("Bad size of tuple in IN or VALUES section. Expected size: "
                    + toString(dst_tuple_size) + ", actual size: " + toString(src_tuple_size), ErrorCodes::TYPE_MISMATCH);

            Tuple res(dst_tuple_size);
            bool have_unconvertible_element = false;
            for (size_t i = 0; i < dst_tuple_size; ++i)
            {
                const auto & element_type = *(type_tuple->getElements()[i]);
                res[i] = convertFieldToType(src_tuple[i], element_type);
                if (!res[i].isNull() || element_type.isNullable())
                    continue;

                /*
                 * Either the source element was Null, or the conversion did not
                 * succeed, because the source and the requested types of the
                 * element are compatible, but the value is not convertible
                 * (e.g. trying to convert -1 from Int8 to UInt8). In these
                 * cases, consider the whole tuple also compatible but not
                 * convertible. According to the specification of this function,
                 * we must return Null in this case.
                 *
                 * The following elements might be not even compatible, so it
                 * makes sense to check them to detect user errors. Remember
                 * that there is an unconvertible element, and try to process
                 * the remaining ones. The convertFieldToType for each element
                 * will throw if it detects incompatibility.
                 */
                have_unconvertible_element = true;
            }

            return have_unconvertible_element ? Field(Null()) : Field(res);
        }
    }
    else if (const DataTypeMap * type_map = typeid_cast<const DataTypeMap *>(&type))
    {
        if (src.getType() == Field::Types::Map)
        {
            const auto & key_type = *type_map->getKeyType();
            const auto & value_type = *type_map->getValueType();

            const auto & map = src.get<Map>();
            size_t map_size = map.size();

            Map res(map_size);

            bool have_unconvertible_element = false;

            for (size_t i = 0; i < map_size; ++i)
            {
                const auto & map_entry = map[i].get<Tuple>();

                const auto & key = map_entry[0];
                const auto & value = map_entry[1];

                Tuple updated_entry(2);

                updated_entry[0] = convertFieldToType(key, key_type);

                if (updated_entry[0].isNull() && !key_type.isNullable())
                    have_unconvertible_element = true;

                updated_entry[1] = convertFieldToType(value, value_type);
                if (updated_entry[1].isNull() && !value_type.isNullable())
                    have_unconvertible_element = true;

                res[i] = updated_entry;
            }

            return have_unconvertible_element ? Field(Null()) : Field(res);
        }
    }
    else if (const DataTypeAggregateFunction * agg_func_type = typeid_cast<const DataTypeAggregateFunction *>(&type))
    {
        if (src.getType() != Field::Types::AggregateFunctionState)
            throw Exception(String("Cannot convert ") + src.getTypeName() + " to " + agg_func_type->getName(),
                    ErrorCodes::TYPE_MISMATCH);

        const auto & name = src.get<AggregateFunctionStateData>().name;
        if (agg_func_type->getName() != name)
            throw Exception("Cannot convert " + name + " to " + agg_func_type->getName(), ErrorCodes::TYPE_MISMATCH);

        return src;
    }

    /// Conversion from string by parsing.
    if (src.getType() == Field::Types::String)
    {
        /// Promote data type to avoid overflows. Note that overflows in the largest data type are still possible.
        const IDataType * type_to_parse = &type;
        DataTypePtr holder;

        if (type.canBePromoted())
        {
            holder = type.promoteNumericType();
            type_to_parse = holder.get();
        }

        const auto col = type_to_parse->createColumn();
        ReadBufferFromString in_buffer(src.get<String>());
        try
        {
            type_to_parse->getDefaultSerialization()->deserializeWholeText(*col, in_buffer, FormatSettings{});
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format("while converting '{}' to {}", src.get<String>(), type.getName()));
            throw;
        }
        if (!in_buffer.eof())
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot convert string {} to type {}", src.get<String>(), type.getName());

        Field parsed = (*col)[0];
        return convertFieldToType(parsed, type, from_type_hint);
    }

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

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(&to_type))
        return convertFieldToType(from_value, *low_cardinality_type->getDictionaryType(), from_type_hint);
    else if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(&to_type))
    {
        const IDataType & nested_type = *nullable_type->getNestedType();

        /// NULL remains NULL after any conversion.
        if (WhichDataType(nested_type).isNothing())
            return {};

        if (from_type_hint && from_type_hint->equals(nested_type))
            return from_value;
        return convertFieldToTypeImpl(from_value, nested_type, from_type_hint);
    }
    else
        return convertFieldToTypeImpl(from_value, to_type, from_type_hint);
}


Field convertFieldToTypeOrThrow(const Field & from_value, const IDataType & to_type, const IDataType * from_type_hint)
{
    bool is_null = from_value.isNull();
    if (is_null && !to_type.isNullable())
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot convert NULL to {}", to_type.getName());

    Field converted = convertFieldToType(from_value, to_type, from_type_hint);

    if (!is_null && converted.isNull())
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "Cannot convert value '{}'{}: it cannot be represented as {}",
            toString(from_value),
            from_type_hint ? " from " + from_type_hint->getName() : "",
            to_type.getName());

    return converted;
}

}
