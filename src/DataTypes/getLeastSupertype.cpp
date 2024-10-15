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
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeDynamic.h>


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
        throw Exception(error_code, "There is no supertype for types {}", getExceptionMessagePrefix(types));

    throw Exception(error_code, "There is no supertype for types {} {}", getExceptionMessagePrefix(types), message_suffix);
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
        what = std::max(value, what);
    };

    for (const auto & type : types)
    {
        if (type == TypeIndex::UInt8)
            maximize(max_bits_of_unsigned_integer, 8);
        else if (type == TypeIndex::UInt16)
            maximize(max_bits_of_unsigned_integer, 16);
        else if (type == TypeIndex::UInt32 || type == TypeIndex::IPv4)
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
        if (max_bits_of_signed_integer && max_bits_of_unsigned_integer >= max_bits_of_signed_integer)
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
            if (min_mantissa_bits <= 53)
                return std::make_shared<DataTypeFloat64>();
            return throwOrReturn<on_error>(
                types,
                " because some of them are integers and some are floating point,"
                " but there is no floating point type, that can exactly represent all required integers",
                ErrorCodes::NO_COMMON_TYPE);
        }

        /// If the result must be signed integer.
        if (max_bits_of_signed_integer)
        {
            if (min_bit_width_of_integer <= 8)
                return std::make_shared<DataTypeInt8>();
            if (min_bit_width_of_integer <= 16)
                return std::make_shared<DataTypeInt16>();
            if (min_bit_width_of_integer <= 32)
                return std::make_shared<DataTypeInt32>();
            if (min_bit_width_of_integer <= 64)
                return std::make_shared<DataTypeInt64>();
            if (min_bit_width_of_integer <= 128)
                return std::make_shared<DataTypeInt128>();
            if (min_bit_width_of_integer <= 256)
                return std::make_shared<DataTypeInt256>();
            return throwOrReturn<on_error>(
                types,
                " because some of them are signed integers and some are unsigned integers,"
                " but there is no signed integer type, that can exactly represent all required unsigned integer values",
                ErrorCodes::NO_COMMON_TYPE);
        }

        /// All unsigned.
        {
            if (min_bit_width_of_integer <= 8)
                return std::make_shared<DataTypeUInt8>();
            if (min_bit_width_of_integer <= 16)
                return std::make_shared<DataTypeUInt16>();
            if (min_bit_width_of_integer <= 32)
                return std::make_shared<DataTypeUInt32>();
            if (min_bit_width_of_integer <= 64)
                return std::make_shared<DataTypeUInt64>();
            if (min_bit_width_of_integer <= 128)
                return std::make_shared<DataTypeUInt128>();
            if (min_bit_width_of_integer <= 256)
                return std::make_shared<DataTypeUInt256>();
            return throwOrReturn<on_error>(
                types,
                " but as all data types are unsigned integers, we must have found maximum unsigned integer type",
                ErrorCodes::NO_COMMON_TYPE);
        }
    }

    return {};
}

/// Check if we can convert UInt64 to Int64 to avoid error "There is no supertype for types UInt64, Int64"
/// during inferring field types.
/// Example:
/// [-3236599669630092879, 5607475129431807682]
/// First field is inferred as Int64, but second one as UInt64, although it also can be Int64.
/// We don't support Int128 as supertype for Int64 and UInt64, because Int128 is inefficient.
/// But in this case the result type can be inferred as Array(Int64).
void convertUInt64toInt64IfPossible(const DataTypes & types, TypeIndexSet & types_set)
{
    /// Check if we have UInt64 and at least one Integer type.
    if (!types_set.contains(TypeIndex::UInt64)
        || (!types_set.contains(TypeIndex::Int8) && !types_set.contains(TypeIndex::Int16) && !types_set.contains(TypeIndex::Int32)
            && !types_set.contains(TypeIndex::Int64)))
        return;

    bool all_uint64_can_be_int64 = true;
    for (const auto & type : types)
    {
        if (const auto * uint64_type = typeid_cast<const DataTypeUInt64 *>(type.get()))
            all_uint64_can_be_int64 &= uint64_type->canUnsignedBeSigned();
    }

    if (all_uint64_can_be_int64)
    {
        types_set.erase(TypeIndex::UInt64);
        types_set.insert(TypeIndex::Int64);
    }
}

DataTypePtr findSmallestIntervalSuperType(const DataTypes &types, TypeIndexSet &types_set)
{
    auto min_interval = IntervalKind::Kind::Year;
    DataTypePtr smallest_type;

    bool is_higher_interval = false; // For Years, Quarters and Months

    for (const auto &type : types)
    {
        if (const auto * interval_type = typeid_cast<const DataTypeInterval *>(type.get()))
        {
            auto current_interval = interval_type->getKind().kind;
            if (current_interval > IntervalKind::Kind::Week)
                is_higher_interval = true;
            if (current_interval < min_interval)
            {
                min_interval = current_interval;
                smallest_type = type;
            }
        }
    }

    if (is_higher_interval && min_interval <= IntervalKind::Kind::Week)
        throw Exception(ErrorCodes::NO_COMMON_TYPE, "Cannot compare intervals {} and {} because the number of days in a month is not fixed", types[0]->getName(), types[1]->getName());

    if (smallest_type)
    {
        types_set.clear();
        types_set.insert(smallest_type->getTypeId());
    }

    return smallest_type;
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

    /// If one of the types is Dynamic, the supertype is Dynamic
    {
        bool have_dynamic = false;
        size_t max_dynamic_types = 0;

        for (const auto & type : types)
        {
            if (const auto & dynamic_type = typeid_cast<const DataTypeDynamic *>(type.get()))
            {
                have_dynamic = true;
                max_dynamic_types = std::max(max_dynamic_types, dynamic_type->getMaxDynamicTypes());
            }
        }

        if (have_dynamic)
            return std::make_shared<DataTypeDynamic>(max_dynamic_types);
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

            auto nested_type = getLeastSupertype<on_error>(nested_types);
            /// When on_error == LeastSupertypeOnError::Null and we cannot get least supertype,
            /// nested_type will be nullptr, we should return nullptr in this case.
            if (!nested_type)
                return nullptr;

            return std::make_shared<DataTypeArray>(nested_type);
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
            {
                auto common_type = getLeastSupertype<on_error>(nested_types[elem_idx]);
                /// When on_error == LeastSupertypeOnError::Null and we cannot get least supertype,
                /// common_type will be nullptr, we should return nullptr in this case.
                if (!common_type)
                    return nullptr;
                common_tuple_types[elem_idx] = common_type;
            }

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

            auto keys_common_type = getLeastSupertype<on_error>(key_types);

            auto values_common_type = getLeastSupertype<on_error>(value_types);
            /// When on_error == LeastSupertypeOnError::Null and we cannot get least supertype for keys or values,
            /// keys_common_type or values_common_type will be nullptr, we should return nullptr in this case.
            if (!keys_common_type || !values_common_type)
                return nullptr;

            return std::make_shared<DataTypeMap>(keys_common_type, values_common_type);
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

            auto nested_type = getLeastSupertype<on_error>(nested_types);

            /// When on_error == LeastSupertypeOnError::Null and we cannot get least supertype,
            /// nested_type will be nullptr, we should return nullptr in this case.
            if (!nested_type)
                return nullptr;
            return std::make_shared<DataTypeLowCardinality>(nested_type);
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
            auto nested_type = getLeastSupertype<on_error>(nested_types);
            /// When on_error == LeastSupertypeOnError::Null and we cannot get least supertype,
            /// nested_type will be nullptr, we should return nullptr in this case.
            if (!nested_type)
                return nullptr;
            /// Common type for Nullable(Nothing) and Variant(...) is Variant(...)
            if (isVariant(nested_type))
                return nested_type;
            return std::make_shared<DataTypeNullable>(nested_type);
        }
    }

    /// Non-recursive rules

    TypeIndexSet type_ids;
    for (const auto & type : types)
        type_ids.insert(type->getTypeId());

    /// For String and FixedString, or for different FixedStrings, the common type is String.
    /// If there are Enums and any type of Strings, the common type is String.
    /// No other types are compatible with Strings.
    {
        size_t have_string = type_ids.count(TypeIndex::String);
        size_t have_fixed_string = type_ids.count(TypeIndex::FixedString);
        size_t have_enums = type_ids.count(TypeIndex::Enum8) + type_ids.count(TypeIndex::Enum16);

        if (have_string || have_fixed_string)
        {
            bool all_compatible_with_string = type_ids.size() == (have_string + have_fixed_string + have_enums);
            if (!all_compatible_with_string)
                return throwOrReturn<on_error>(types, "because some of them are String/FixedString/Enum and some of them are not", ErrorCodes::NO_COMMON_TYPE);

            return std::make_shared<DataTypeString>();
        }
    }

    /// For Date and DateTime/DateTime64, the common type is DateTime/DateTime64. No other types are compatible.
    {
        size_t have_date = type_ids.count(TypeIndex::Date);
        size_t have_date32 = type_ids.count(TypeIndex::Date32);
        size_t have_datetime = type_ids.count(TypeIndex::DateTime);
        size_t have_datetime64 = type_ids.count(TypeIndex::DateTime64);

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
        size_t have_decimal32 = type_ids.count(TypeIndex::Decimal32);
        size_t have_decimal64 = type_ids.count(TypeIndex::Decimal64);
        size_t have_decimal128 = type_ids.count(TypeIndex::Decimal128);
        size_t have_decimal256 = type_ids.count(TypeIndex::Decimal256);

        if (have_decimal32 || have_decimal64 || have_decimal128 || have_decimal256)
        {
            size_t num_supported = have_decimal32 + have_decimal64 + have_decimal128 + have_decimal256;

            std::array<TypeIndex, 8> int_ids = {TypeIndex::Int8, TypeIndex::UInt8, TypeIndex::Int16, TypeIndex::UInt16,
                                                TypeIndex::Int32, TypeIndex::UInt32, TypeIndex::Int64, TypeIndex::UInt64};

            TypeIndex max_int = TypeIndex::Nothing;
            for (auto int_id : int_ids)
            {
                size_t num = type_ids.count(int_id);
                num_supported += num;
                if (num)
                    max_int = int_id;
            }

            if (num_supported != type_ids.size())
                return throwOrReturn<on_error>(types, "because some of them have no lossless conversion to Decimal", ErrorCodes::NO_COMMON_TYPE);

            UInt32 max_scale = 0;
            for (const auto & type : types)
            {
                auto type_id = type->getTypeId();
                if (type_id != TypeIndex::Decimal32
                    && type_id != TypeIndex::Decimal64
                    && type_id != TypeIndex::Decimal128
                    && type_id != TypeIndex::Decimal256)
                {
                    continue;
                }

                max_scale = std::max(max_scale, getDecimalScale(*type));
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

            if (min_precision > DataTypeDecimal<Decimal256>::maxPrecision())
                return throwOrReturn<on_error>(types, "because the least supertype is Decimal("
                                + toString(min_precision) + ',' + toString(max_scale) + ')',
                                ErrorCodes::NO_COMMON_TYPE);

            if (have_decimal256 || min_precision > DataTypeDecimal<Decimal128>::maxPrecision())
                return std::make_shared<DataTypeDecimal<Decimal256>>(DataTypeDecimal<Decimal256>::maxPrecision(), max_scale);
            if (have_decimal128 || min_precision > DataTypeDecimal<Decimal64>::maxPrecision())
                return std::make_shared<DataTypeDecimal<Decimal128>>(DataTypeDecimal<Decimal128>::maxPrecision(), max_scale);
            if (have_decimal64 || min_precision > DataTypeDecimal<Decimal32>::maxPrecision())
                return std::make_shared<DataTypeDecimal<Decimal64>>(DataTypeDecimal<Decimal64>::maxPrecision(), max_scale);
            return std::make_shared<DataTypeDecimal<Decimal32>>(DataTypeDecimal<Decimal32>::maxPrecision(), max_scale);
        }
    }

    /// For numeric types, the most complicated part.
    {
        /// First, if we have signed integers, try to convert all UInt64 to Int64 if possible.
        convertUInt64toInt64IfPossible(types, type_ids);
        auto numeric_type = getNumericType<on_error>(type_ids);
        if (numeric_type)
            return numeric_type;
    }

    /// For interval data types.
    {
        auto res = findSmallestIntervalSuperType(types, type_ids);
        if (res)
            return res;
    }

    /// All other data types (UUID, AggregateFunction, Enum...) are compatible only if they are the same (checked in trivial cases).
    return throwOrReturn<on_error>(types, "", ErrorCodes::NO_COMMON_TYPE);
}

DataTypePtr getLeastSupertypeOrString(const DataTypes & types)
{
    return getLeastSupertype<LeastSupertypeOnError::String>(types);
}

template<>
DataTypePtr getLeastSupertype<LeastSupertypeOnError::Variant>(const DataTypes & types)
{
    auto common_type = getLeastSupertype<LeastSupertypeOnError::Null>(types);
    if (common_type)
        return common_type;

    /// Create Variant with provided arguments as variants.
    DataTypes variants;
    for (const auto & type : types)
    {
        /// Nested Variant types are not supported. If we have Variant type
        /// we use all its variants in the result Variant.
        if (isVariant(type))
        {
            const DataTypes & nested_variants = assert_cast<const DataTypeVariant &>(*type).getVariants();
            variants.insert(variants.end(), nested_variants.begin(), nested_variants.end());
        }
        else
        {
            variants.push_back(removeNullableOrLowCardinalityNullable(type));
        }
    }

    return std::make_shared<DataTypeVariant>(variants);
}

DataTypePtr getLeastSupertypeOrVariant(const DataTypes & types)
{
    return getLeastSupertype<LeastSupertypeOnError::Variant>(types);
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
