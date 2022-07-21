#include <unordered_set>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/typeid_cast.h>

#include <DataTypes/getLeastSupertype.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeFactory.h>
#include <base/EnumReflection.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_COMMON_TYPE;
}

namespace
{

String typeToString(const DataTypePtr & type) { return type->getName(); }
String typeToString(const TypeIndex & type) { return String(magic_enum::enum_name(type)); }

template <typename DataTypes>
String getExceptionMessagePrefix(const DataTypes & types)
{
    WriteBufferFromOwnString res;
    res << "There is no supertype for types ";

    bool first = true;
    for (const auto & type : types)
    {
        if (!first)
            res << ", ";
        first = false;

        res << typeToString(type);
    }

    return res.str();
}

template <LeastSupertypeOnError on_error, typename DataTypes>
DataTypePtr throwOrReturn(const DataTypes & types, std::string_view message_suffix, int error_code)
{
    if constexpr (on_error == LeastSupertypeOnError::String)
        return std::make_shared<DataTypeString>();

    if constexpr (on_error == LeastSupertypeOnError::Null)
        return nullptr;

    if (message_suffix.empty())
        throw Exception(error_code, getExceptionMessagePrefix(types));

    throw Exception(error_code, "{} {}", getExceptionMessagePrefix(types), message_suffix);
}

template <LeastSupertypeOnError on_error>
DataTypePtr getNumericType(const TypeIndexSet & types)
{
    bool all_numbers = true;

    size_t max_bits_of_signed_integer = 0;
    size_t max_bits_of_unsigned_integer = 0;
    size_t max_mantissa_bits_of_floating = 0;

    auto maximize = [](size_t & what, size_t value)
    {
        if (value > what)
            what = value;
    };

    for (const auto & type : types)
    {
        if (type == TypeIndex::UInt8)
            maximize(max_bits_of_unsigned_integer, 8);
        else if (type == TypeIndex::UInt16)
            maximize(max_bits_of_unsigned_integer, 16);
        else if (type == TypeIndex::UInt32)
            maximize(max_bits_of_unsigned_integer, 32);
        else if (type == TypeIndex::UInt64)
            maximize(max_bits_of_unsigned_integer, 64);
        else if (type == TypeIndex::UInt128)
            maximize(max_bits_of_unsigned_integer, 128);
        else if (type == TypeIndex::UInt256)
            maximize(max_bits_of_unsigned_integer, 256);
        else if (type == TypeIndex::Int8 || type == TypeIndex::Enum8)
            maximize(max_bits_of_signed_integer, 8);
        else if (type == TypeIndex::Int16 || type == TypeIndex::Enum16)
            maximize(max_bits_of_signed_integer, 16);
        else if (type == TypeIndex::Int32)
            maximize(max_bits_of_signed_integer, 32);
        else if (type == TypeIndex::Int64)
            maximize(max_bits_of_signed_integer, 64);
        else if (type == TypeIndex::Int128)
            maximize(max_bits_of_signed_integer, 128);
        else if (type == TypeIndex::Int256)
            maximize(max_bits_of_signed_integer, 256);
        else if (type == TypeIndex::Float32)
            maximize(max_mantissa_bits_of_floating, 24);
        else if (type == TypeIndex::Float64)
            maximize(max_mantissa_bits_of_floating, 53);
        else if (type != TypeIndex::Nothing)
            all_numbers = false;
    }

    if (max_bits_of_signed_integer || max_bits_of_unsigned_integer || max_mantissa_bits_of_floating)
    {
        if (!all_numbers)
            return throwOrReturn<on_error>(types, "because some of them are numbers and some of them are not", ErrorCodes::NO_COMMON_TYPE);

        /// If there are signed and unsigned types of same bit-width, the result must be signed number with at least one more bit.
        /// Example, common of Int32, UInt32 = Int64.

        size_t min_bit_width_of_integer = std::max(max_bits_of_signed_integer, max_bits_of_unsigned_integer);

        /// If unsigned is not covered by signed.
        if (max_bits_of_signed_integer && max_bits_of_unsigned_integer >= max_bits_of_signed_integer) //-V1051
        {
            // Because 128 and 256 bit integers are significantly slower, we should not promote to them.
            // But if we already have wide numbers, promotion is necessary.
            if (min_bit_width_of_integer != 64)
                ++min_bit_width_of_integer;
            else
                return throwOrReturn<on_error>(types,
                    "because some of them are signed integers and some are unsigned integers,"
                    " but there is no signed integer type, that can exactly represent all required unsigned integer values",
                    ErrorCodes::NO_COMMON_TYPE);
        }

        /// If the result must be floating.
        if (max_mantissa_bits_of_floating)
        {
            size_t min_mantissa_bits = std::max(min_bit_width_of_integer, max_mantissa_bits_of_floating);
            if (min_mantissa_bits <= 24)
                return std::make_shared<DataTypeFloat32>();
            else if (min_mantissa_bits <= 53)
                return std::make_shared<DataTypeFloat64>();
            else
                return throwOrReturn<on_error>(types,
                    " because some of them are integers and some are floating point,"
                    " but there is no floating point type, that can exactly represent all required integers", ErrorCodes::NO_COMMON_TYPE);
        }

        /// If the result must be signed integer.
        if (max_bits_of_signed_integer)
        {
            if (min_bit_width_of_integer <= 8)
                return std::make_shared<DataTypeInt8>();
            else if (min_bit_width_of_integer <= 16)
                return std::make_shared<DataTypeInt16>();
            else if (min_bit_width_of_integer <= 32)
                return std::make_shared<DataTypeInt32>();
            else if (min_bit_width_of_integer <= 64)
                return std::make_shared<DataTypeInt64>();
            else if (min_bit_width_of_integer <= 128)
                return std::make_shared<DataTypeInt128>();
            else if (min_bit_width_of_integer <= 256)
                return std::make_shared<DataTypeInt256>();
            else
                return throwOrReturn<on_error>(types,
                    " because some of them are signed integers and some are unsigned integers,"
                    " but there is no signed integer type, that can exactly represent all required unsigned integer values", ErrorCodes::NO_COMMON_TYPE);
        }

        /// All unsigned.
        {
            if (min_bit_width_of_integer <= 8)
                return std::make_shared<DataTypeUInt8>();
            else if (min_bit_width_of_integer <= 16)
                return std::make_shared<DataTypeUInt16>();
            else if (min_bit_width_of_integer <= 32)
                return std::make_shared<DataTypeUInt32>();
            else if (min_bit_width_of_integer <= 64)
                return std::make_shared<DataTypeUInt64>();
            else if (min_bit_width_of_integer <= 128)
                return std::make_shared<DataTypeUInt128>();
            else if (min_bit_width_of_integer <= 256)
                return std::make_shared<DataTypeUInt256>();
            else
                return throwOrReturn<on_error>(types,
                    " but as all data types are unsigned integers, we must have found maximum unsigned integer type", ErrorCodes::NO_COMMON_TYPE);
        }
    }

    return {};
}

}

template <LeastSupertypeOnError on_error>
DataTypePtr getLeastSupertype(const DataTypes & types)
{
    /// Trivial cases

    if (types.empty())
        return std::make_shared<DataTypeNothing>();

    if (types.size() == 1)
        return types[0];

    /// All types are equal
    {
        bool all_equal = true;
        for (size_t i = 1, size = types.size(); i < size; ++i)
        {
            if (!types[i]->equals(*types[0]))
            {
                all_equal = false;
                break;
            }
        }

        if (all_equal)
            return types[0];
    }

    /// Recursive rules

    /// If there are Nothing types, skip them
    {
        DataTypes non_nothing_types;
        non_nothing_types.reserve(types.size());

        for (const auto & type : types)
            if (!typeid_cast<const DataTypeNothing *>(type.get()))
                non_nothing_types.emplace_back(type);

        if (non_nothing_types.size() < types.size())
            return getLeastSupertype<on_error>(non_nothing_types);
    }

    /// For Arrays
    {
        bool have_array = false;
        bool all_arrays = true;

        DataTypes nested_types;
        nested_types.reserve(types.size());

        for (const auto & type : types)
        {
            if (const DataTypeArray * type_array = typeid_cast<const DataTypeArray *>(type.get()))
            {
                have_array = true;
                nested_types.emplace_back(type_array->getNestedType());
            }
            else
                all_arrays = false;
        }

        if (have_array)
        {
            if (!all_arrays)
                return throwOrReturn<on_error>(types, "because some of them are Array and some of them are not", ErrorCodes::NO_COMMON_TYPE);

            return std::make_shared<DataTypeArray>(getLeastSupertype<on_error>(nested_types));
        }
    }

    /// For tuples
    {
        bool have_tuple = false;
        bool all_tuples = true;
        size_t tuple_size = 0;

        std::vector<DataTypes> nested_types;

        for (const auto & type : types)
        {
            if (const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(type.get()))
            {
                if (!have_tuple)
                {
                    tuple_size = type_tuple->getElements().size();
                    nested_types.resize(tuple_size);
                    for (size_t elem_idx = 0; elem_idx < tuple_size; ++elem_idx)
                        nested_types[elem_idx].reserve(types.size());
                }
                else if (tuple_size != type_tuple->getElements().size())
                    return throwOrReturn<on_error>(types, "because Tuples have different sizes", ErrorCodes::NO_COMMON_TYPE);

                have_tuple = true;

                for (size_t elem_idx = 0; elem_idx < tuple_size; ++elem_idx)
                    nested_types[elem_idx].emplace_back(type_tuple->getElements()[elem_idx]);
            }
            else
                all_tuples = false;
        }

        if (have_tuple)
        {
            if (!all_tuples)
                return throwOrReturn<on_error>(types, "because some of them are Tuple and some of them are not", ErrorCodes::NO_COMMON_TYPE);

            DataTypes common_tuple_types(tuple_size);
            for (size_t elem_idx = 0; elem_idx < tuple_size; ++elem_idx)
                common_tuple_types[elem_idx] = getLeastSupertype<on_error>(nested_types[elem_idx]);

            return std::make_shared<DataTypeTuple>(common_tuple_types);
        }
    }

    /// For maps
    {
        bool have_maps = false;
        bool all_maps = true;
        DataTypes key_types;
        DataTypes value_types;
        key_types.reserve(types.size());
        value_types.reserve(types.size());

        for (const auto & type : types)
        {
            if (const DataTypeMap * type_map = typeid_cast<const DataTypeMap *>(type.get()))
            {
                have_maps = true;
                key_types.emplace_back(type_map->getKeyType());
                value_types.emplace_back(type_map->getValueType());
            }
            else
                all_maps = false;
        }

        if (have_maps)
        {
            if (!all_maps)
                return throwOrReturn<on_error>(types, "because some of them are Maps and some of them are not", ErrorCodes::NO_COMMON_TYPE);

            return std::make_shared<DataTypeMap>(
                getLeastSupertype<on_error>(key_types),
                getLeastSupertype<on_error>(value_types));
        }
    }

    /// For LowCardinality. This is above Nullable, because LowCardinality can contain Nullable but cannot be inside Nullable.
    {
        bool have_low_cardinality = false;
        bool have_not_low_cardinality = false;

        DataTypes nested_types;
        nested_types.reserve(types.size());

        for (const auto & type : types)
        {
            if (const DataTypeLowCardinality * type_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get()))
            {
                have_low_cardinality = true;
                nested_types.emplace_back(type_low_cardinality->getDictionaryType());
            }
            else
            {
                have_not_low_cardinality = true;
                nested_types.emplace_back(type);
            }
        }

        /// All LowCardinality gives LowCardinality.
        /// LowCardinality with high cardinality gives high cardinality.
        if (have_low_cardinality)
        {
            if (have_not_low_cardinality)
                return getLeastSupertype<on_error>(nested_types);
            else
                return std::make_shared<DataTypeLowCardinality>(getLeastSupertype<on_error>(nested_types));
        }
    }

    /// For Nullable
    {
        bool have_nullable = false;

        DataTypes nested_types;
        nested_types.reserve(types.size());

        for (const auto & type : types)
        {
            if (const DataTypeNullable * type_nullable = typeid_cast<const DataTypeNullable *>(type.get()))
            {
                have_nullable = true;

                if (!type_nullable->onlyNull())
                    nested_types.emplace_back(type_nullable->getNestedType());
            }
            else
                nested_types.emplace_back(type);
        }

        if (have_nullable)
        {
            return std::make_shared<DataTypeNullable>(getLeastSupertype<on_error>(nested_types));
        }
    }

    /// Non-recursive rules

    TypeIndexSet type_ids;
    for (const auto & type : types)
        type_ids.insert(type->getTypeId());

    /// For String and FixedString, or for different FixedStrings, the common type is String.
    /// No other types are compatible with Strings. TODO Enums?
    {
        UInt32 have_string = type_ids.count(TypeIndex::String);
        UInt32 have_fixed_string = type_ids.count(TypeIndex::FixedString);

        if (have_string || have_fixed_string)
        {
            bool all_strings = type_ids.size() == (have_string + have_fixed_string);
            if (!all_strings)
                return throwOrReturn<on_error>(types, "because some of them are String/FixedString and some of them are not", ErrorCodes::NO_COMMON_TYPE);

            return std::make_shared<DataTypeString>();
        }
    }

    /// For Date and DateTime/DateTime64, the common type is DateTime/DateTime64. No other types are compatible.
    {
        UInt32 have_date = type_ids.count(TypeIndex::Date);
        UInt32 have_date32 = type_ids.count(TypeIndex::Date32);
        UInt32 have_datetime = type_ids.count(TypeIndex::DateTime);
        UInt32 have_datetime64 = type_ids.count(TypeIndex::DateTime64);

        if (have_date || have_date32 || have_datetime || have_datetime64)
        {
            bool all_date_or_datetime = type_ids.size() == (have_date + have_date32 + have_datetime + have_datetime64);
            if (!all_date_or_datetime)
                return throwOrReturn<on_error>(types,
                    "because some of them are Date/Date32/DateTime/DateTime64 and some of them are not",
                    ErrorCodes::NO_COMMON_TYPE);

            if (have_datetime64 == 0 && have_date32 == 0)
            {
                for (const auto & type : types)
                {
                    if (isDateTime(type))
                        return type;
                }

                return std::make_shared<DataTypeDateTime>();
            }

            /// For Date and Date32, the common type is Date32
            if (have_datetime == 0 && have_datetime64 == 0)
            {
                for (const auto & type : types)
                {
                    if (isDate32(type))
                        return type;
                }
            }

            /// For Datetime and Date32, the common type is Datetime64
            if (have_datetime == 1 && have_date32 == 1 && have_datetime64 == 0)
            {
                return std::make_shared<DataTypeDateTime64>(0);
            }

            UInt8 max_scale = 0;
            size_t max_scale_date_time_index = 0;

            for (size_t i = 0; i < types.size(); ++i)
            {
                const auto & type = types[i];

                if (const auto * date_time64_type = typeid_cast<const DataTypeDateTime64 *>(type.get()))
                {
                    const auto scale = date_time64_type->getScale();
                    if (scale >= max_scale)
                    {
                        max_scale_date_time_index = i;
                        max_scale = scale;
                    }
                }
            }

            return types[max_scale_date_time_index];
        }
    }

    /// Decimals
    {
        UInt32 have_decimal32 = type_ids.count(TypeIndex::Decimal32);
        UInt32 have_decimal64 = type_ids.count(TypeIndex::Decimal64);
        UInt32 have_decimal128 = type_ids.count(TypeIndex::Decimal128);

        if (have_decimal32 || have_decimal64 || have_decimal128)
        {
            UInt32 num_supported = have_decimal32 + have_decimal64 + have_decimal128;

            std::vector<TypeIndex> int_ids = {TypeIndex::Int8, TypeIndex::UInt8, TypeIndex::Int16, TypeIndex::UInt16,
                                            TypeIndex::Int32, TypeIndex::UInt32, TypeIndex::Int64, TypeIndex::UInt64};
            std::vector<UInt32> num_ints(int_ids.size(), 0);

            TypeIndex max_int = TypeIndex::Nothing;
            for (size_t i = 0; i < int_ids.size(); ++i)
            {
                UInt32 num = type_ids.count(int_ids[i]);
                num_ints[i] = num;
                num_supported += num;
                if (num)
                    max_int = int_ids[i];
            }

            if (num_supported != type_ids.size())
                return throwOrReturn<on_error>(types, "because some of them have no lossless conversion to Decimal", ErrorCodes::NO_COMMON_TYPE);

            UInt32 max_scale = 0;
            for (const auto & type : types)
            {
                UInt32 scale = getDecimalScale(*type, 0);
                if (scale > max_scale)
                    max_scale = scale;
            }

            UInt32 min_precision = max_scale + leastDecimalPrecisionFor(max_int);

            /// special cases Int32 -> Dec32, Int64 -> Dec64
            if (max_scale == 0)
            {
                if (max_int == TypeIndex::Int32)
                    min_precision = DataTypeDecimal<Decimal32>::maxPrecision();
                else if (max_int == TypeIndex::Int64)
                    min_precision = DataTypeDecimal<Decimal64>::maxPrecision();
            }

            if (min_precision > DataTypeDecimal<Decimal128>::maxPrecision())
                return throwOrReturn<on_error>(types, "because the least supertype is Decimal("
                                + toString(min_precision) + ',' + toString(max_scale) + ')',
                                ErrorCodes::NO_COMMON_TYPE);

            if (have_decimal128 || min_precision > DataTypeDecimal<Decimal64>::maxPrecision())
                return std::make_shared<DataTypeDecimal<Decimal128>>(DataTypeDecimal<Decimal128>::maxPrecision(), max_scale);
            if (have_decimal64 || min_precision > DataTypeDecimal<Decimal32>::maxPrecision())
                return std::make_shared<DataTypeDecimal<Decimal64>>(DataTypeDecimal<Decimal64>::maxPrecision(), max_scale);
            return std::make_shared<DataTypeDecimal<Decimal32>>(DataTypeDecimal<Decimal32>::maxPrecision(), max_scale);
        }
    }

    /// For numeric types, the most complicated part.
    {
        auto numeric_type = getNumericType<on_error>(type_ids);
        if (numeric_type)
            return numeric_type;
    }

    /// All other data types (UUID, AggregateFunction, Enum...) are compatible only if they are the same (checked in trivial cases).
    return throwOrReturn<on_error>(types, "", ErrorCodes::NO_COMMON_TYPE);
}

DataTypePtr getLeastSupertypeOrString(const DataTypes & types)
{
    return getLeastSupertype<LeastSupertypeOnError::String>(types);
}

DataTypePtr tryGetLeastSupertype(const DataTypes & types)
{
    return getLeastSupertype<LeastSupertypeOnError::Null>(types);
}

template <LeastSupertypeOnError on_error>
DataTypePtr getLeastSupertype(const TypeIndexSet & types)
{
    if (types.empty())
        return std::make_shared<DataTypeNothing>();

    if (types.size() == 1)
    {
        WhichDataType which(*types.begin());
        if (which.isNothing())
            return std::make_shared<DataTypeNothing>();

    #define DISPATCH(TYPE) \
        if (which.idx == TypeIndex::TYPE) \
            return std::make_shared<DataTypeNumber<TYPE>>(); /// NOLINT

        FOR_NUMERIC_TYPES(DISPATCH)
    #undef DISPATCH

        if (which.isString())
            return std::make_shared<DataTypeString>();

        return throwOrReturn<on_error>(types, "because cannot get common type by type indexes with non-simple types", ErrorCodes::NO_COMMON_TYPE);
    }

    if (types.contains(TypeIndex::String))
    {
        bool only_string = types.size() == 2 && types.contains(TypeIndex::Nothing);
        if (!only_string)
            return throwOrReturn<on_error>(types, "because some of them are String and some of them are not", ErrorCodes::NO_COMMON_TYPE);

        return std::make_shared<DataTypeString>();
    }

    auto numeric_type = getNumericType<on_error>(types);
    if (numeric_type)
        return numeric_type;

    return throwOrReturn<on_error>(types, "", ErrorCodes::NO_COMMON_TYPE);
}

DataTypePtr getLeastSupertypeOrString(const TypeIndexSet & types)
{
    return getLeastSupertype<LeastSupertypeOnError::String>(types);
}

DataTypePtr tryGetLeastSupertype(const TypeIndexSet & types)
{
    return getLeastSupertype<LeastSupertypeOnError::Null>(types);
}

template DataTypePtr getLeastSupertype<LeastSupertypeOnError::Throw>(const DataTypes & types);
template DataTypePtr getLeastSupertype<LeastSupertypeOnError::Throw>(const TypeIndexSet & types);

}
