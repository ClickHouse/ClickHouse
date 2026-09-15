#include <DataTypes/DataTypeCustom.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/IDataType.h>

#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

DataTypePtr removeCustomTypeTransparentWrappers(DataTypePtr type)
{
    while (type)
    {
        if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        {
            type = low_cardinality_type->getDictionaryType();
            continue;
        }

        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
        {
            type = nullable_type->getNestedType();
            continue;
        }

        break;
    }

    return type;
}

bool hasDirectSemanticIdentity(const IDataType & type)
{
    const auto * custom_name = type.getCustomName();
    return custom_name && custom_name->getSemanticIdentity().has_value();
}

bool hasDirectValueValidation(const IDataType & type)
{
    const auto * custom_name = type.getCustomName();
    return custom_name && custom_name->requiresValueValidation();
}

void assertCustomDataTypesCompatibleImpl(
    DataTypePtr left_type, DataTypePtr right_type, const String & operation)
{
    left_type = removeCustomTypeTransparentWrappers(std::move(left_type));
    right_type = removeCustomTypeTransparentWrappers(std::move(right_type));

    const bool left_contains = containsCustomTypeSemanticIdentity(left_type);
    const bool right_contains = containsCustomTypeSemanticIdentity(right_type);
    if (!left_contains && !right_contains)
        return;

    if (!left_contains || !right_contains)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "{} cannot combine incompatible types {} and {} because one side has custom semantic type identity",
            operation,
            left_type->getName(),
            right_type->getName());

    const auto left_identity = getCustomTypeSemanticIdentity(left_type);
    const auto right_identity = getCustomTypeSemanticIdentity(right_type);
    if (left_identity || right_identity)
    {
        if (!left_identity || !right_identity)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "{} cannot combine semantic custom type {} with layout-compatible type {}",
                operation,
                left_identity ? left_type->getName() : right_type->getName(),
                left_identity ? right_type->getName() : left_type->getName());

        if (*left_identity != *right_identity)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "{} cannot combine custom types with different semantic identities: {} and {}",
                operation,
                left_type->getName(),
                right_type->getName());
        return;
    }

    if (const auto * left_array = typeid_cast<const DataTypeArray *>(left_type.get()))
    {
        const auto * right_array = typeid_cast<const DataTypeArray *>(right_type.get());
        if (right_array)
        {
            assertCustomDataTypesCompatibleImpl(
                left_array->getNestedType(), right_array->getNestedType(), operation);
            return;
        }
    }
    else if (const auto * left_tuple = typeid_cast<const DataTypeTuple *>(left_type.get()))
    {
        const auto * right_tuple = typeid_cast<const DataTypeTuple *>(right_type.get());
        if (right_tuple && left_tuple->getElements().size() == right_tuple->getElements().size())
        {
            for (size_t i = 0; i < left_tuple->getElements().size(); ++i)
                assertCustomDataTypesCompatibleImpl(
                    left_tuple->getElements()[i], right_tuple->getElements()[i], operation);
            return;
        }
    }
    else if (const auto * left_map = typeid_cast<const DataTypeMap *>(left_type.get()))
    {
        const auto * right_map = typeid_cast<const DataTypeMap *>(right_type.get());
        if (right_map)
        {
            assertCustomDataTypesCompatibleImpl(
                left_map->getNestedType(), right_map->getNestedType(), operation);
            return;
        }
    }
    else if (const auto * left_variant = typeid_cast<const DataTypeVariant *>(left_type.get()))
    {
        const auto * right_variant = typeid_cast<const DataTypeVariant *>(right_type.get());
        if (right_variant && left_variant->getVariants().size() == right_variant->getVariants().size())
        {
            for (size_t i = 0; i < left_variant->getVariants().size(); ++i)
                assertCustomDataTypesCompatibleImpl(
                    left_variant->getVariant(i), right_variant->getVariant(i), operation);
            return;
        }
    }

    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "{} cannot combine incompatible types {} and {} containing custom semantic types",
        operation,
        left_type->getName(),
        right_type->getName());
}

void validateCustomDataTypeColumnImpl(
    const IColumn & column, const DataTypePtr & type, const String & operation)
{
    if (!type || !containsCustomTypeValueValidation(type))
        return;

    ColumnPtr full_column = column.convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        validateCustomDataTypeColumnImpl(
            *full_column, low_cardinality_type->getDictionaryType(), operation);
        return;
    }

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        const auto & nullable_column = typeid_cast<const ColumnNullable &>(*full_column);
        validateCustomDataTypeColumnImpl(
            nullable_column.getNestedColumn(), nullable_type->getNestedType(), operation);
        return;
    }

    if (const auto * custom_name = type->getCustomName(); custom_name && custom_name->requiresValueValidation())
    {
        custom_name->validateColumn(*full_column, operation);
        return;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        const auto & array_column = typeid_cast<const ColumnArray &>(*full_column);
        validateCustomDataTypeColumnImpl(
            array_column.getData(), array_type->getNestedType(), operation);
        return;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto & tuple_column = typeid_cast<const ColumnTuple &>(*full_column);
        const auto & element_types = tuple_type->getElements();
        for (size_t i = 0; i < element_types.size(); ++i)
            validateCustomDataTypeColumnImpl(
                tuple_column.getColumn(i), element_types[i], operation);
        return;
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        const auto & map_column = typeid_cast<const ColumnMap &>(*full_column);
        validateCustomDataTypeColumnImpl(
            map_column.getNestedColumn(), map_type->getNestedType(), operation);
        return;
    }

    if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(type.get()))
    {
        const auto & variant_column = typeid_cast<const ColumnVariant &>(*full_column);
        for (size_t i = 0; i < variant_type->getVariants().size(); ++i)
            validateCustomDataTypeColumnImpl(
                variant_column.getVariantByGlobalDiscriminator(i), variant_type->getVariant(i), operation);
    }
}

}

std::optional<String> getCustomTypeSemanticIdentity(const IDataType & type)
{
    const auto * custom_name = type.getCustomName();
    return custom_name ? custom_name->getSemanticIdentity() : std::nullopt;
}

std::optional<String> getCustomTypeSemanticIdentity(const DataTypePtr & type)
{
    return type ? getCustomTypeSemanticIdentity(*type) : std::nullopt;
}

bool containsCustomTypeSemanticIdentity(const IDataType & type)
{
    bool contains = hasDirectSemanticIdentity(type);
    if (!contains)
    {
        type.forEachChild([&](const IDataType & child)
        {
            contains |= hasDirectSemanticIdentity(child);
        });
    }
    return contains;
}

bool containsCustomTypeSemanticIdentity(const DataTypePtr & type)
{
    return type && containsCustomTypeSemanticIdentity(*type);
}

bool containsCustomTypeValueValidation(const IDataType & type)
{
    bool contains = hasDirectValueValidation(type);
    if (!contains)
    {
        type.forEachChild([&](const IDataType & child)
        {
            contains |= hasDirectValueValidation(child);
        });
    }
    return contains;
}

bool containsCustomTypeValueValidation(const DataTypePtr & type)
{
    return type && containsCustomTypeValueValidation(*type);
}

void assertCustomDataTypesCompatible(
    const DataTypePtr & left_type, const DataTypePtr & right_type, const String & operation)
{
    assertCustomDataTypesCompatibleImpl(left_type, right_type, operation);
}

void assertCustomDataTypeSetKeyTypesCompatible(
    const DataTypePtr & probe_type, const DataTypePtr & set_type)
{
    if (!containsCustomTypeSemanticIdentity(probe_type) && !containsCustomTypeSemanticIdentity(set_type))
        return;

    const auto nested_probe_type = removeCustomTypeTransparentWrappers(probe_type);
    const auto nested_set_type = removeCustomTypeTransparentWrappers(set_type);

    /// The default Variant adaptor probes each alternative separately, while the
    /// set retains its Variant type. Permit wrapping an exact alternative.
    if (const auto * variant = typeid_cast<const DataTypeVariant *>(nested_set_type.get()))
    {
        for (const auto & alternative : variant->getVariants())
        {
            if (nested_probe_type->getName() == alternative->getName())
                return;
        }
    }

    assertCustomDataTypesCompatible(probe_type, set_type, "IN");
}

void validateCustomDataTypeColumn(
    const IColumn & column, const DataTypePtr & type, const String & operation)
{
    validateCustomDataTypeColumnImpl(column, type, operation);
}

}
