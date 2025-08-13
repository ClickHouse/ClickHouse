#include <DataTypes/Utils.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>

namespace DB
{

bool canBeSafelyCasted(const DataTypePtr & from_type, const DataTypePtr & to_type)
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
                return canBeSafelyCasted(from_type_nullable.getNestedType(), to_type_unwrapped);
            }

            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::LowCardinality:
        {
            const auto & from_type_low_cardinality = assert_cast<const DataTypeLowCardinality &>(*from_type);
            return canBeSafelyCasted(from_type_low_cardinality.getDictionaryType(), to_type_unwrapped);
        }
        case TypeIndex::Array:
        {
            if (to_which_type.isArray())
            {
                const auto & from_type_array = assert_cast<const DataTypeArray &>(*from_type);
                const auto & to_type_array = assert_cast<const DataTypeArray &>(*to_type_unwrapped);
                return canBeSafelyCasted(from_type_array.getNestedType(), to_type_array.getNestedType());
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
                if (!canBeSafelyCasted(from_type_map.getKeyType(), to_type_map.getKeyType()))
                    return false;

                if (!canBeSafelyCasted(from_type_map.getValueType(), to_type_map.getValueType()))
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

                if (!canBeSafelyCasted(from_type_map.getKeyType(), to_type_tuple_elements[0]))
                    return false;

                if (!canBeSafelyCasted(from_type_map.getValueType(), to_type_tuple_elements[1]))
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
                    if (!canBeSafelyCasted(from_tuple_type_elements[i], to_tuple_type_elements[i]))
                        return false;

                return true;
            }

            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::String:
        case TypeIndex::ObjectDeprecated:
        case TypeIndex::Object:
        case TypeIndex::Set:
        case TypeIndex::Interval:
        case TypeIndex::Function:
        case TypeIndex::AggregateFunction:
        case TypeIndex::Nothing:
        case TypeIndex::JSONPaths:
        case TypeIndex::Variant:
        case TypeIndex::Dynamic:
            return false;
    }

    return true;
}

}
