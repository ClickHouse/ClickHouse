#include <DataTypes/Utils.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>

namespace DB
{

bool canBeSafelyCast(const DataTypePtr & from_type, const DataTypePtr & to_type)
{
    auto from_which_type = WhichDataType(from_type->getTypeId());
    bool to_type_was_nullable = isNullableOrLowCardinalityNullable(to_type);
    auto to_type_unwrapped = removeNullable(removeLowCardinality(to_type));

    if (from_type->equals(*to_type_unwrapped))
        return true;

    auto to_which_type = WhichDataType(to_type_unwrapped->getTypeId());

    switch (from_which_type.idx)
    {
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
        case TypeIndex::UInt128:
        case TypeIndex::UInt256:
        {
            if (to_which_type.isUInt() &&
                to_type_unwrapped->getSizeOfValueInMemory() >= from_type->getSizeOfValueInMemory())
                return true;

            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64:
        case TypeIndex::Int128:
        case TypeIndex::Int256:
        {
            if (to_which_type.isInt() &&
                to_type_unwrapped->getSizeOfValueInMemory() >= from_type->getSizeOfValueInMemory())
                return true;

            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::BFloat16:
        {
            if (to_which_type.isFloat32() || to_which_type.isFloat64() || to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::Float32:
        {
            if (to_which_type.isFloat64() || to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::Float64:
        case TypeIndex::Date:
        case TypeIndex::Date32:
        case TypeIndex::DateTime:
        case TypeIndex::DateTime64:
        case TypeIndex::Time:
        case TypeIndex::Time64:
        case TypeIndex::FixedString:
        case TypeIndex::Enum8:
        case TypeIndex::Enum16:
        case TypeIndex::IPv6:
        {
            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        case TypeIndex::Decimal128:
        case TypeIndex::Decimal256:
        {
            if (to_which_type.isDecimal())
            {
                auto from_type_decimal_precision = getDecimalPrecision(*from_type);
                auto to_type_decimal_precision = getDecimalPrecision(*to_type_unwrapped);
                if (from_type_decimal_precision > to_type_decimal_precision)
                    return false;

                auto from_type_decimal_scale = getDecimalScale(*from_type);
                auto to_type_decimal_scale = getDecimalScale(*to_type_unwrapped);
                if (from_type_decimal_scale > to_type_decimal_scale)
                    return false;

                return true;
            }

            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::UUID:
        {
            if (to_which_type.isUInt128() || to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::IPv4:
        {
            if (to_which_type.isUInt32() || to_which_type.isUInt64() || to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::Nullable:
        {
            if (to_type_was_nullable)
            {
                const auto & from_type_nullable = assert_cast<const DataTypeNullable &>(*from_type);
                return canBeSafelyCast(from_type_nullable.getNestedType(), to_type_unwrapped);
            }

            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::LowCardinality:
        {
            const auto & from_type_low_cardinality = assert_cast<const DataTypeLowCardinality &>(*from_type);
            return canBeSafelyCast(from_type_low_cardinality.getDictionaryType(), to_type_unwrapped);
        }
        case TypeIndex::Array:
        {
            if (to_which_type.isArray())
            {
                const auto & from_type_array = assert_cast<const DataTypeArray &>(*from_type);
                const auto & to_type_array = assert_cast<const DataTypeArray &>(*to_type_unwrapped);
                return canBeSafelyCast(from_type_array.getNestedType(), to_type_array.getNestedType());
            }

            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::Map:
        {
            if (to_which_type.isMap())
            {
                const auto & from_type_map = assert_cast<const DataTypeMap &>(*from_type);
                const auto & to_type_map = assert_cast<const DataTypeMap &>(*to_type_unwrapped);
                if (!canBeSafelyCast(from_type_map.getKeyType(), to_type_map.getKeyType()))
                    return false;

                if (!canBeSafelyCast(from_type_map.getValueType(), to_type_map.getValueType()))
                    return false;

                return true;
            }

            if (to_which_type.isArray())
            {
                // Map nested type is Array(Tuple(key_type, value_type))
                const auto & from_type_map = assert_cast<const DataTypeMap &>(*from_type);
                const auto & to_type_array = assert_cast<const DataTypeArray &>(*to_type_unwrapped);
                const auto * to_type_nested_tuple_type = typeid_cast<const DataTypeTuple *>(to_type_array.getNestedType().get());
                if (!to_type_nested_tuple_type)
                    return false;

                const auto & to_type_tuple_elements = to_type_nested_tuple_type->getElements();
                if (to_type_tuple_elements.size() != 2)
                    return false;

                if (!canBeSafelyCast(from_type_map.getKeyType(), to_type_tuple_elements[0]))
                    return false;

                if (!canBeSafelyCast(from_type_map.getValueType(), to_type_tuple_elements[1]))
                    return false;

                return true;
            }

            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::Tuple:
        {
            if (to_which_type.isTuple())
            {
                const auto & from_type_tuple = assert_cast<const DataTypeTuple &>(*from_type);
                const auto & to_type_tuple = assert_cast<const DataTypeTuple &>(*to_type_unwrapped);

                const auto & from_tuple_type_elements = from_type_tuple.getElements();
                const auto & to_tuple_type_elements = to_type_tuple.getElements();

                size_t lhs_type_elements_size = from_tuple_type_elements.size();
                if (lhs_type_elements_size != to_tuple_type_elements.size())
                    return false;

                for (size_t i = 0; i < lhs_type_elements_size; ++i)
                    if (!canBeSafelyCast(from_tuple_type_elements[i], to_tuple_type_elements[i]))
                        return false;

                return true;
            }

            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::QBit:
            return to_which_type.isQBit();
        case TypeIndex::Object:
        case TypeIndex::Variant:
        case TypeIndex::Dynamic:
        {
            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::String:
        case TypeIndex::Set:
        case TypeIndex::Interval:
        case TypeIndex::Function:
        case TypeIndex::AggregateFunction:
        case TypeIndex::Nothing:
        case TypeIndex::JSONPaths:
            return false;
    }

    return true;
}

namespace
{

/// Recursive helper: walks `full_type` and produces a narrowed type by dropping any
/// leaf subfield whose dotted name (path so far) is in `expired_substreams`.
/// Returns nullptr if the type is fully expired (every leaf is in `expired_substreams`).
///
/// `path` accumulates the dotted prefix (e.g. "data", "data.c2s", "data.c2s.gold").
DataTypePtr narrowDataTypeImpl(const DataTypePtr & full_type, const String & path, const NameSet & expired_substreams)
{
    /// Leaf hit: the whole path is expired.
    if (expired_substreams.contains(path))
        return nullptr;

    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(full_type.get()))
    {
        if (!tuple->hasExplicitNames())
            return full_type;

        const auto & elements = tuple->getElements();
        const auto & names = tuple->getElementNames();
        DataTypes new_elements;
        Strings new_names;
        new_elements.reserve(elements.size());
        new_names.reserve(names.size());

        for (size_t i = 0; i < elements.size(); ++i)
        {
            auto element_path = path + "." + names[i];
            auto narrowed = narrowDataTypeImpl(elements[i], element_path, expired_substreams);
            if (narrowed)
            {
                new_elements.push_back(std::move(narrowed));
                new_names.push_back(names[i]);
            }
        }

        if (new_elements.empty())
            return nullptr;
        if (new_elements.size() == elements.size())
        {
            /// Quick path: nothing was narrowed. Reuse the original Tuple to keep type identity stable.
            bool all_same = true;
            for (size_t i = 0; i < new_elements.size(); ++i)
            {
                if (new_elements[i].get() != elements[i].get())
                {
                    all_same = false;
                    break;
                }
            }
            if (all_same)
                return full_type;
        }
        return std::make_shared<DataTypeTuple>(std::move(new_elements), std::move(new_names));
    }

    if (const auto * array = typeid_cast<const DataTypeArray *>(full_type.get()))
    {
        auto narrowed_nested = narrowDataTypeImpl(array->getNestedType(), path, expired_substreams);
        if (!narrowed_nested)
            return nullptr;
        if (narrowed_nested.get() == array->getNestedType().get())
            return full_type;
        return std::make_shared<DataTypeArray>(std::move(narrowed_nested));
    }

    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(full_type.get()))
    {
        auto narrowed_nested = narrowDataTypeImpl(nullable->getNestedType(), path, expired_substreams);
        if (!narrowed_nested)
            return nullptr;
        if (narrowed_nested.get() == nullable->getNestedType().get())
            return full_type;
        return std::make_shared<DataTypeNullable>(std::move(narrowed_nested));
    }

    if (const auto * map = typeid_cast<const DataTypeMap *>(full_type.get()))
    {
        auto narrowed_value = narrowDataTypeImpl(map->getValueType(), path, expired_substreams);
        if (!narrowed_value)
            return nullptr;
        if (narrowed_value.get() == map->getValueType().get())
            return full_type;
        return std::make_shared<DataTypeMap>(map->getKeyType(), std::move(narrowed_value));
    }

    /// Scalar leaf type that is not in the expired set: keep as-is.
    return full_type;
}

}

DataTypePtr narrowDataTypeByExpiredSubstreams(
    const DataTypePtr & full_type,
    const String & column_name,
    const NameSet & expired_substreams)
{
    return narrowDataTypeImpl(full_type, column_name, expired_substreams);
}

}
