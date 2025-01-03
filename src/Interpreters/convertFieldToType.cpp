#include <Interpreters/convertFieldToType.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeDynamic.h>

#include <Core/AccurateComparison.h>

#include <Common/typeid_cast.h>
#include <Common/NaNUtils.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Common/FieldVisitorToString.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/DateLUT.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
    extern const int UNEXPECTED_DATA_AFTER_PARSED_VALUE;
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
Field convertNumericTypeImpl(const Field & from)
{
    To result;
    if (!accurate::convertNumeric(from.safeGet<From>(), result))
        return {};
    return result;
}

template <typename To>
Field convertNumericType(const Field & from, const IDataType & type)
{
    if (from.getType() == Field::Types::UInt64 || from.getType() == Field::Types::Bool)
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

    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch in IN or VALUES section. Expected: {}. Got: {}",
        type.getName(), from.getType());
}


template <typename From, typename T>
Field convertIntToDecimalType(const Field & from, const DataTypeDecimal<T> & type)
{
    From value = from.safeGet<From>();
    if (!type.canStoreWhole(value))
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Number is too big to place in {}", type.getName());

    T scaled_value = type.getScaleMultiplier() * T(static_cast<typename T::NativeType>(value));
    return DecimalField<T>(scaled_value, type.getScale());
}


template <typename T>
Field convertStringToDecimalType(const Field & from, const DataTypeDecimal<T> & type)
{
    const String & str_value = from.safeGet<String>();
    T value = type.parseFromString(str_value);
    return DecimalField<T>(value, type.getScale());
}

template <typename From, typename T>
Field convertDecimalToDecimalType(const Field & from, const DataTypeDecimal<T> & type)
{
    auto field = from.safeGet<DecimalField<From>>();
    T value = convertDecimals<DataTypeDecimal<From>, DataTypeDecimal<T>>(field.getValue(), field.getScale(), type.getScale());
    return DecimalField<T>(value, type.getScale());
}

template <typename From, typename T>
Field convertFloatToDecimalType(const Field & from, const DataTypeDecimal<T> & type)
{
    From value = from.safeGet<From>();
    if (!type.canStoreWhole(value))
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Number is too big to place in {}", type.getName());

    //String sValue = convertFieldToString(from);
    //int fromScale = sValue.length()- sValue.find('.') - 1;
    UInt32 scale = type.getScale();

    auto scaled_value = convertToDecimal<DataTypeNumber<From>, DataTypeDecimal<T>>(value, scale);
    return DecimalField<T>(scaled_value, scale);
}

template <typename To>
Field convertDecimalType(const Field & from, const To & type)
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
    if (from.getType() == Field::Types::Decimal256)
        return convertDecimalToDecimalType<Decimal256>(from, type);

    if (from.getType() == Field::Types::Float64)
        return convertFloatToDecimalType<Float64>(from, type);

    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch in IN or VALUES section. Expected: {}. Got: {}",
        type.getName(), from.getType());
}


Field convertFieldToTypeImpl(const Field & src, const IDataType & type, const IDataType * from_type_hint, const FormatSettings & format_settings)
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
        return static_cast<UInt16>(static_cast<const DataTypeDateTime &>(*from_type_hint).getTimeZone().toDayNum(src.safeGet<UInt64>()).toUnderType());
    }
    if (which_type.isDate32() && which_from_type.isDateTime())
    {
        return static_cast<Int32>(
            static_cast<const DataTypeDateTime &>(*from_type_hint).getTimeZone().toDayNum(src.safeGet<UInt64>()).toUnderType());
    }
    if (which_type.isDateTime() && which_from_type.isDate())
    {
        return static_cast<const DataTypeDateTime &>(type).getTimeZone().fromDayNum(DayNum(src.safeGet<UInt64>()));
    }
    if (which_type.isDateTime() && which_from_type.isDate32())
    {
        return static_cast<const DataTypeDateTime &>(type).getTimeZone().fromDayNum(DayNum(src.safeGet<Int32>()));
    }
    if (which_type.isDateTime64() && which_from_type.isDate())
    {
        const auto & date_time64_type = static_cast<const DataTypeDateTime64 &>(type);
        const auto value = date_time64_type.getTimeZone().fromDayNum(DayNum(src.safeGet<UInt16>()));
        return DecimalField(
            DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(value, 0, date_time64_type.getScaleMultiplier()),
            date_time64_type.getScale());
    }
    if (which_type.isDateTime64() && which_from_type.isDate32())
    {
        const auto & date_time64_type = static_cast<const DataTypeDateTime64 &>(type);
        const auto value = date_time64_type.getTimeZone().fromDayNum(ExtendedDayNum(static_cast<Int32>(src.safeGet<Int32>())));
        return DecimalField(
            DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(value, 0, date_time64_type.getScaleMultiplier()),
            date_time64_type.getScale());
    }
    if (type.isValueRepresentedByNumber() && src.getType() != Field::Types::String)
    {
        /// Bool is not represented in which_type, so we need to type it separately
        if (isInt64OrUInt64orBoolFieldType(src.getType()) && type.getName() == "Bool")
            return bool(src.safeGet<bool>());

        if (which_type.isUInt8())
            return convertNumericType<UInt8>(src, type);
        if (which_type.isUInt16())
            return convertNumericType<UInt16>(src, type);
        if (which_type.isUInt32())
            return convertNumericType<UInt32>(src, type);
        if (which_type.isUInt64())
            return convertNumericType<UInt64>(src, type);
        if (which_type.isUInt128())
            return convertNumericType<UInt128>(src, type);
        if (which_type.isUInt256())
            return convertNumericType<UInt256>(src, type);
        if (which_type.isInt8())
            return convertNumericType<Int8>(src, type);
        if (which_type.isInt16())
            return convertNumericType<Int16>(src, type);
        if (which_type.isInt32())
            return convertNumericType<Int32>(src, type);
        if (which_type.isInt64())
            return convertNumericType<Int64>(src, type);
        if (which_type.isInt128())
            return convertNumericType<Int128>(src, type);
        if (which_type.isInt256())
            return convertNumericType<Int256>(src, type);
        if (which_type.isFloat32())
            return convertNumericType<Float32>(src, type);
        if (which_type.isFloat64())
            return convertNumericType<Float64>(src, type);
        if (const auto * ptype = typeid_cast<const DataTypeDecimal<Decimal32> *>(&type))
            return convertDecimalType(src, *ptype);
        if (const auto * ptype = typeid_cast<const DataTypeDecimal<Decimal64> *>(&type))
            return convertDecimalType(src, *ptype);
        if (const auto * ptype = typeid_cast<const DataTypeDecimal<Decimal128> *>(&type))
            return convertDecimalType(src, *ptype);
        if (const auto * ptype = typeid_cast<const DataTypeDecimal<Decimal256> *>(&type))
            return convertDecimalType(src, *ptype);

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

        if (which_type.isDate32() && src.getType() == Field::Types::Int64)
        {
            /// We don't need any conversion Int64 is under type of Date32
            return src;
        }

        if (which_type.isDateTime64() && src.getType() == Field::Types::Decimal64)
        {
            const auto & from_type = src.safeGet<Decimal64>();
            const auto & to_type = static_cast<const DataTypeDateTime64 &>(type);

            const auto scale_from = from_type.getScale();
            const auto scale_to = to_type.getScale();
            const auto scale_multiplier_diff = scale_from > scale_to ? from_type.getScaleMultiplier() / to_type.getScaleMultiplier()
                                                                     : to_type.getScaleMultiplier() / from_type.getScaleMultiplier();

            if (scale_multiplier_diff == 1) /// Already in needed type.
                return src;

            /// in case if we need to make DateTime64(a) from DateTime64(b), a != b, we need to convert datetime value to the right scale
            const UInt64 value = scale_from > scale_to ? from_type.getValue().value / scale_multiplier_diff
                                                       : from_type.getValue().value * scale_multiplier_diff;
            return DecimalField(DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(value, 0, 1), scale_to);
        }

        /// For toDate('xxx') in 1::Int64, we CAST `src` to UInt64, which may
        /// produce wrong result in some special cases.
        if (which_type.isDate() && src.getType() == Field::Types::Int64)
        {
            return convertNumericType<UInt64>(src, type);
        }

        /// For toDate32('xxx') in 1, we CAST `src` to Int64. Also, it may
        /// produce wrong result in some special cases.
        if (which_type.isDate32() && src.getType() == Field::Types::UInt64)
        {
            return convertNumericType<Int64>(src, type);
        }

        if (which_type.isDateTime64()
            && (src.getType() == Field::Types::UInt64 || src.getType() == Field::Types::Int64 || src.getType() == Field::Types::Decimal64))
        {
            const auto scale = static_cast<const DataTypeDateTime64 &>(type).getScale();
            const auto decimal_value
                = DecimalUtils::decimalFromComponents<DateTime64>(applyVisitor(FieldVisitorConvertToNumber<Int64>(), src), 0, scale);
            return Field(DecimalField<DateTime64>(decimal_value, scale));
        }

        if (which_type.isIPv4() && src.getType() == Field::Types::IPv4)
        {
            /// Already in needed type.
            return src;
        }
        if (which_type.isIPv4() && src.getType() == Field::Types::UInt64)
        {
            /// convert through UInt32 which is the underlying type for native IPv4
            return static_cast<IPv4>(convertNumericType<UInt32>(src, type).safeGet<UInt32>());
        }
    }
    else if (which_type.isUUID() && src.getType() == Field::Types::UUID)
    {
        /// Already in needed type.
        return src;
    }
    else if (which_type.isIPv6())
    {
        /// Already in needed type.
        if (src.getType() == Field::Types::IPv6)
            return src;
        /// Treat FixedString(16) as a binary representation of IPv6
        if (which_from_type.isFixedString() && assert_cast<const DataTypeFixedString *>(from_type_hint)->getN() == IPV6_BINARY_LENGTH)
        {
            const auto col = type.createColumn();
            ReadBufferFromString in_buffer(src.safeGet<String>());
            type.getDefaultSerialization()->deserializeBinary(*col, in_buffer, {});
            return (*col)[0];
        }
    }
    else if (which_type.isStringOrFixedString())
    {
        if (src.getType() == Field::Types::String)
        {
            if (which_type.isFixedString())
            {
                size_t n = assert_cast<const DataTypeFixedString &>(type).getN();
                const auto & src_str = src.safeGet<String>();
                if (src_str.size() < n)
                {
                    String src_str_extended = src_str;
                    src_str_extended.resize(n);
                    return src_str_extended;
                }
            }
            return src;
        }

        return applyVisitor(FieldVisitorToString(), src);
    }
    else if (const DataTypeArray * type_array = typeid_cast<const DataTypeArray *>(&type))
    {
        if (src.getType() == Field::Types::Array)
        {
            const Array & src_arr = src.safeGet<Array>();
            size_t src_arr_size = src_arr.size();

            const auto & element_type = *(type_array->getNestedType());
            bool have_unconvertible_element = false;
            Array res(src_arr_size);
            for (size_t i = 0; i < src_arr_size; ++i)
            {
                res[i] = convertFieldToType(src_arr[i], element_type, nullptr, format_settings);
                if (res[i].isNull() && !canContainNull(element_type))
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
            const auto & src_tuple = src.safeGet<Tuple>();
            size_t src_tuple_size = src_tuple.size();
            size_t dst_tuple_size = type_tuple->getElements().size();

            if (dst_tuple_size != src_tuple_size)
                throw Exception(
                    ErrorCodes::TYPE_MISMATCH,
                    "Bad size of tuple in IN or VALUES section. "
                    "Expected size: {}, actual size: {}",
                    dst_tuple_size,
                    src_tuple_size);

            Tuple res(dst_tuple_size);
            bool have_unconvertible_element = false;
            for (size_t i = 0; i < dst_tuple_size; ++i)
            {
                const auto & element_type = *(type_tuple->getElements()[i]);
                res[i] = convertFieldToType(src_tuple[i], element_type, nullptr, format_settings);
                if (res[i].isNull() && !canContainNull(element_type))
                {
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

            const auto & map = src.safeGet<Map>();
            size_t map_size = map.size();

            Map res(map_size);

            bool have_unconvertible_element = false;

            for (size_t i = 0; i < map_size; ++i)
            {
                const auto & map_entry = map[i].safeGet<Tuple>();

                const auto & key = map_entry[0];
                const auto & value = map_entry[1];

                Tuple updated_entry(2);

                updated_entry[0] = convertFieldToType(key, key_type, nullptr, format_settings);

                if (updated_entry[0].isNull() && !canContainNull(key_type))
                    have_unconvertible_element = true;

                updated_entry[1] = convertFieldToType(value, value_type, nullptr, format_settings);
                if (updated_entry[1].isNull() && !canContainNull(value_type))
                    have_unconvertible_element = true;

                res[i] = updated_entry;
            }

            return have_unconvertible_element ? Field(Null()) : Field(res);
        }
    }
    else if (const DataTypeAggregateFunction * agg_func_type = typeid_cast<const DataTypeAggregateFunction *>(&type))
    {
        if (src.getType() != Field::Types::AggregateFunctionState)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot convert {} to {}", src.getTypeName(), agg_func_type->getName());

        const auto & name = src.safeGet<AggregateFunctionStateData>().name;
        if (agg_func_type->getName() != name)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot convert {} to {}", name, agg_func_type->getName());

        return src;
    }
    else if (isObjectDeprecated(type))
    {
        if (src.getType() == Field::Types::Object)
            return src; /// Already in needed type.

        const auto * from_type_tuple = typeid_cast<const DataTypeTuple *>(from_type_hint);
        if (src.getType() == Field::Types::Tuple && from_type_tuple && from_type_tuple->haveExplicitNames())
        {
            const auto & names = from_type_tuple->getElementNames();
            const auto & tuple = src.safeGet<const Tuple &>();

            if (names.size() != tuple.size())
                throw Exception(
                    ErrorCodes::TYPE_MISMATCH,
                    "Bad size of tuple in IN or VALUES section (while converting to Object). Expected size: {}, actual size: {}",
                    names.size(),
                    tuple.size());

            Object object;
            for (size_t i = 0; i < names.size(); ++i)
                object[names[i]] = tuple[i];

            return object;
        }

        if (src.getType() == Field::Types::Map)
        {
            Object object;
            const auto & map = src.safeGet<const Map &>();
            for (const auto & element : map)
            {
                const auto & map_entry = element.safeGet<Tuple>();
                const auto & key = map_entry[0];
                const auto & value = map_entry[1];

                if (key.getType() != Field::Types::String)
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot convert from Map with key of type {} to Object", key.getTypeName());

                object[key.safeGet<const String &>()] = value;
            }

            return object;
        }
    }
    else if (const DataTypeVariant * type_variant = typeid_cast<const DataTypeVariant *>(&type))
    {
        /// If we have type hint and Variant contains such type, no need to convert field.
        if (from_type_hint && type_variant->tryGetVariantDiscriminator(from_type_hint->getName()))
            return src;

        /// Create temporary column and check if we can insert this field to the variant.
        /// If we can insert, no need to convert anything.
        auto col = type_variant->createColumn();
        if (col->tryInsert(src))
            return src;
    }
    else if (isDynamic(type))
    {
        /// We can insert any field to Dynamic column.
        return src;
    }
    else if (isObject(type))
    {
        if (src.getType() == Field::Types::Object)
            return src; /// Already in needed type.

        /// TODO: add conversion from Map/Tuple to Object.
    }

    /// Conversion from string by parsing.
    if (src.getType() == Field::Types::String)
    {
        /// Promote data type to avoid overflows. Note that overflows in the largest data type are still possible.
        /// But don't promote Float32, since we want to keep the exact same value
        /// Also don't promote domain types (like bool) because we would otherwise use the serializer of the promoted type (e.g. UInt64 for
        /// bool, which does not allow 'true' and 'false' as input values)
        const IDataType * type_to_parse = &type;
        DataTypePtr holder;

        if (type.canBePromoted() && !which_type.isFloat32() && !type.getCustomSerialization())
        {
            holder = type.promoteNumericType();
            type_to_parse = holder.get();
        }

        const auto col = type_to_parse->createColumn();
        ReadBufferFromString in_buffer(src.safeGet<String>());
        try
        {
            type_to_parse->getDefaultSerialization()->deserializeWholeText(*col, in_buffer, format_settings);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNEXPECTED_DATA_AFTER_PARSED_VALUE)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot convert string '{}' to type {}", src.safeGet<String>(), type.getName());

            e.addMessage(fmt::format("while converting '{}' to {}", src.safeGet<String>(), type.getName()));
            throw;
        }

        Field parsed = (*col)[0];
        return convertFieldToType(parsed, type, from_type_hint, format_settings);
    }

    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch in IN or VALUES section. Expected: {}. Got: {}",
        type.getName(), src.getType());
}

}

Field tryConvertFieldToType(const Field & from_value, const IDataType & to_type, const IDataType * from_type_hint, const FormatSettings & format_settings)
{
    /// TODO: implement proper tryConvertFieldToType without try/catch by adding template flag to convertFieldToTypeImpl to not throw an exception.
    try
    {
        return convertFieldToType(from_value, to_type, from_type_hint, format_settings);
    } catch (...)
    {
        return {};
    }
}

Field convertFieldToType(const Field & from_value, const IDataType & to_type, const IDataType * from_type_hint, const FormatSettings & format_settings)
{
    if (from_value.isNull())
        return from_value;

    if (from_type_hint && from_type_hint->equals(to_type))
        return from_value;

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(&to_type))
        return convertFieldToType(from_value, *low_cardinality_type->getDictionaryType(), from_type_hint, format_settings);
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(&to_type))
    {
        const IDataType & nested_type = *nullable_type->getNestedType();

        /// NULL remains NULL after any conversion.
        if (WhichDataType(nested_type).isNothing())
            return {};

        if (from_type_hint && from_type_hint->equals(nested_type))
            return from_value;
        return convertFieldToTypeImpl(from_value, nested_type, from_type_hint, format_settings);
    }
    return convertFieldToTypeImpl(from_value, to_type, from_type_hint, format_settings);
}


Field convertFieldToTypeOrThrow(const Field & from_value, const IDataType & to_type, const IDataType * from_type_hint, const FormatSettings & format_settings)
{
    bool is_null = from_value.isNull();
    if (is_null && !canContainNull(to_type))
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot convert NULL to {}", to_type.getName());

    Field converted = convertFieldToType(from_value, to_type, from_type_hint, format_settings);

    if (!is_null && converted.isNull())
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "Cannot convert value '{}'{}: it cannot be represented as {}",
            toString(from_value),
            from_type_hint ? " from " + from_type_hint->getName() : "",
            to_type.getName());

    return converted;
}

template <typename T>
static bool decimalEqualsFloat(Field field, Float64 float_value)
{
    auto decimal_field = field.safeGet<DecimalField<T>>();
    auto decimal_to_float = DecimalUtils::convertTo<Float64>(decimal_field.getValue(), decimal_field.getScale());
    return decimal_to_float == float_value;
}

std::optional<Field> convertFieldToTypeStrict(const Field & from_value, const IDataType & from_type, const IDataType & to_type, const FormatSettings & format_settings)
{
    Field result_value = convertFieldToType(from_value, to_type, &from_type, format_settings);

    if (Field::isDecimal(from_value.getType()) && Field::isDecimal(result_value.getType()))
    {
        bool is_equal = applyVisitor(FieldVisitorAccurateEquals{}, from_value, result_value);
        return is_equal ? result_value : std::optional<Field>{};
    }

    if (from_value.getType() == Field::Types::Float64 && Field::isDecimal(result_value.getType()))
    {
        /// Convert back to Float64 and compare
        if (result_value.getType() == Field::Types::Decimal32)
            return decimalEqualsFloat<Decimal32>(result_value, from_value.safeGet<Float64>()) ? result_value : std::optional<Field>{};
        if (result_value.getType() == Field::Types::Decimal64)
            return decimalEqualsFloat<Decimal64>(result_value, from_value.safeGet<Float64>()) ? result_value : std::optional<Field>{};
        if (result_value.getType() == Field::Types::Decimal128)
            return decimalEqualsFloat<Decimal128>(result_value, from_value.safeGet<Float64>()) ? result_value : std::optional<Field>{};
        if (result_value.getType() == Field::Types::Decimal256)
            return decimalEqualsFloat<Decimal256>(result_value, from_value.safeGet<Float64>()) ? result_value : std::optional<Field>{};
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown decimal type {}", result_value.getTypeName());
    }

    return result_value;
}

}
