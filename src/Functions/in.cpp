#include <algorithm>
#include <cmath>

#include <Common/FieldAccurateComparison.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnsCommon.h>
#include <Interpreters/Context.h>
#include <Interpreters/Set.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/convertColumnToType.h>
#include <Interpreters/convertFieldToType.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace Setting
{
    extern const SettingsBool in_null_as_unknown;
}

namespace
{

[[nodiscard]] bool mayContainFloatForIn(const DataTypePtr & type)
{
    const auto nested = removeNullable(removeLowCardinality(type));
    if (isFloat(nested) || isDynamic(nested) || isVariant(nested))
        return true;
    if (const auto * array = typeid_cast<const DataTypeArray *>(nested.get()))
        return mayContainFloatForIn(array->getNestedType());
    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(nested.get()))
        return std::ranges::any_of(tuple->getElements(), mayContainFloatForIn);
    if (const auto * map = typeid_cast<const DataTypeMap *>(nested.get()))
        return mayContainFloatForIn(map->getKeyType()) || mayContainFloatForIn(map->getValueType());
    return false;
}

[[nodiscard]] bool fieldContainsNaNForIn(const Field & field)
{
    if (field.getType() == Field::Types::Float64)
        return std::isnan(field.safeGet<Float64>());
    if (field.getType() == Field::Types::Array)
        return std::ranges::any_of(field.safeGet<Array>(), fieldContainsNaNForIn);
    if (field.getType() == Field::Types::Tuple)
        return std::ranges::any_of(field.safeGet<Tuple>(), fieldContainsNaNForIn);
    if (field.getType() == Field::Types::Map)
        return std::ranges::any_of(field.safeGet<Map>(), fieldContainsNaNForIn);
    return false;
}

[[nodiscard]] bool hasCompoundDecimalFloatPair(const DataTypePtr & left_type, const DataTypePtr & right_type)
{
    const auto left = removeNullable(removeLowCardinality(left_type));
    const auto right = removeNullable(removeLowCardinality(right_type));
    if (const auto * left_array = typeid_cast<const DataTypeArray *>(left.get()))
    {
        if (const auto * right_array = typeid_cast<const DataTypeArray *>(right.get()))
            return hasCompoundDecimalFloatPair(left_array->getNestedType(), right_array->getNestedType());
    }
    if (const auto * left_tuple = typeid_cast<const DataTypeTuple *>(left.get()))
    {
        if (const auto * right_tuple = typeid_cast<const DataTypeTuple *>(right.get());
            right_tuple && left_tuple->getElements().size() == right_tuple->getElements().size())
        {
            for (size_t i = 0; i < left_tuple->getElements().size(); ++i)
                if (hasCompoundDecimalFloatPair(left_tuple->getElements()[i], right_tuple->getElements()[i]))
                    return true;
        }
    }
    if (const auto * left_map = typeid_cast<const DataTypeMap *>(left.get()))
    {
        if (const auto * right_map = typeid_cast<const DataTypeMap *>(right.get()))
            return hasCompoundDecimalFloatPair(left_map->getKeyType(), right_map->getKeyType())
                || hasCompoundDecimalFloatPair(left_map->getValueType(), right_map->getValueType());
    }
    return (isFloat(left) && isDecimal(right)) || (isDecimal(left) && isFloat(right));
}

[[nodiscard]] bool hasCompoundNumericTypeMismatch(const DataTypePtr & left_type, const DataTypePtr & right_type)
{
    const auto left = removeNullable(removeLowCardinality(left_type));
    const auto right = removeNullable(removeLowCardinality(right_type));
    if (const auto * left_array = typeid_cast<const DataTypeArray *>(left.get()))
    {
        if (const auto * right_array = typeid_cast<const DataTypeArray *>(right.get()))
            return hasCompoundNumericTypeMismatch(left_array->getNestedType(), right_array->getNestedType());
    }
    if (const auto * left_tuple = typeid_cast<const DataTypeTuple *>(left.get()))
    {
        if (const auto * right_tuple = typeid_cast<const DataTypeTuple *>(right.get());
            right_tuple && left_tuple->getElements().size() == right_tuple->getElements().size())
        {
            for (size_t i = 0; i < left_tuple->getElements().size(); ++i)
                if (hasCompoundNumericTypeMismatch(left_tuple->getElements()[i], right_tuple->getElements()[i]))
                    return true;
        }
    }
    if (const auto * left_map = typeid_cast<const DataTypeMap *>(left.get()))
    {
        if (const auto * right_map = typeid_cast<const DataTypeMap *>(right.get()))
            return hasCompoundNumericTypeMismatch(left_map->getKeyType(), right_map->getKeyType())
                || hasCompoundNumericTypeMismatch(left_map->getValueType(), right_map->getValueType());
    }
    return isNumber(left) && isNumber(right) && !left->equals(*right);
}

[[nodiscard]] bool hasProbeCarrierAgainstNumericKey(const DataTypePtr & probe_type, const DataTypePtr & key_type)
{
    const auto probe = removeNullable(removeLowCardinality(probe_type));
    const auto key = removeNullable(removeLowCardinality(key_type));
    if (isDynamic(probe) || isVariant(probe))
        return isNumber(key);
    if (const auto * probe_array = typeid_cast<const DataTypeArray *>(probe.get()))
    {
        if (const auto * key_array = typeid_cast<const DataTypeArray *>(key.get()))
            return hasProbeCarrierAgainstNumericKey(probe_array->getNestedType(), key_array->getNestedType());
    }
    if (const auto * probe_tuple = typeid_cast<const DataTypeTuple *>(probe.get()))
    {
        if (const auto * key_tuple = typeid_cast<const DataTypeTuple *>(key.get());
            key_tuple && probe_tuple->getElements().size() == key_tuple->getElements().size())
        {
            for (size_t i = 0; i < probe_tuple->getElements().size(); ++i)
                if (hasProbeCarrierAgainstNumericKey(probe_tuple->getElements()[i], key_tuple->getElements()[i]))
                    return true;
        }
    }
    if (const auto * probe_map = typeid_cast<const DataTypeMap *>(probe.get()))
    {
        if (const auto * key_map = typeid_cast<const DataTypeMap *>(key.get()))
            return hasProbeCarrierAgainstNumericKey(probe_map->getKeyType(), key_map->getKeyType())
                || hasProbeCarrierAgainstNumericKey(probe_map->getValueType(), key_map->getValueType());
    }
    return false;
}

[[nodiscard]] bool hasCompoundStringConversionLeaf(const DataTypePtr & left_type, const DataTypePtr & right_type)
{
    const auto left = removeNullable(removeLowCardinality(left_type));
    const auto right = removeNullable(removeLowCardinality(right_type));
    if (const auto * left_array = typeid_cast<const DataTypeArray *>(left.get()))
    {
        if (const auto * right_array = typeid_cast<const DataTypeArray *>(right.get()))
            return hasCompoundStringConversionLeaf(left_array->getNestedType(), right_array->getNestedType());
    }
    if (const auto * left_tuple = typeid_cast<const DataTypeTuple *>(left.get()))
    {
        if (const auto * right_tuple = typeid_cast<const DataTypeTuple *>(right.get());
            right_tuple && left_tuple->getElements().size() == right_tuple->getElements().size())
        {
            for (size_t i = 0; i < left_tuple->getElements().size(); ++i)
                if (hasCompoundStringConversionLeaf(left_tuple->getElements()[i], right_tuple->getElements()[i]))
                    return true;
        }
    }
    if (const auto * left_map = typeid_cast<const DataTypeMap *>(left.get()))
    {
        if (const auto * right_map = typeid_cast<const DataTypeMap *>(right.get()))
            return hasCompoundStringConversionLeaf(left_map->getKeyType(), right_map->getKeyType())
                || hasCompoundStringConversionLeaf(left_map->getValueType(), right_map->getValueType());
    }
    const auto text_convertible = [](const DataTypePtr & type)
    {
        return isNumber(type) || isBool(type) || isEnum(type)
            || isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(type)
            || isUUID(type) || isIPv4(type) || isIPv6(type);
    };
    return (isStringOrFixedString(left) && text_convertible(right))
        || (text_convertible(left) && isStringOrFixedString(right))
        || (isStringOrFixedString(left) && isStringOrFixedString(right) && !left->equals(*right));
}

[[nodiscard]] bool matchesSetColumnConversion(
    const IColumn & left, const IColumn & source, size_t row,
    const DataTypePtr & left_type, const DataTypePtr & source_type)
{
    /// The source column type restores nested `Bool` tags while retaining
    /// `FixedString` padding when the compound value is converted.
    auto source_row = source.cut(row, 1);
    auto converted = tryConvertColumnToTypeOrNull(
        *source_row, source_type, left_type, {}, /*strict=*/true);
    return converted && left.compareAt(row, 0, *converted, 1) == 0;
}

[[nodiscard]] bool hasCompoundCarrierForIn(const DataTypePtr & left_type, const DataTypePtr & right_type, bool nested = false)
{
    const auto left = removeNullable(removeLowCardinality(left_type));
    const auto right = removeNullable(removeLowCardinality(right_type));
    if (isDynamic(right) || isVariant(right))
        return nested || isArray(left) || isMap(left);
    if (const auto * left_array = typeid_cast<const DataTypeArray *>(left.get()))
    {
        if (const auto * right_array = typeid_cast<const DataTypeArray *>(right.get()))
            return hasCompoundCarrierForIn(left_array->getNestedType(), right_array->getNestedType(), true);
    }
    if (const auto * left_map = typeid_cast<const DataTypeMap *>(left.get()))
    {
        if (const auto * right_map = typeid_cast<const DataTypeMap *>(right.get()))
            return hasCompoundCarrierForIn(left_map->getKeyType(), right_map->getKeyType(), true)
                || hasCompoundCarrierForIn(left_map->getValueType(), right_map->getValueType(), true);
    }
    if (const auto * left_tuple = typeid_cast<const DataTypeTuple *>(left.get()))
    {
        if (const auto * right_tuple = typeid_cast<const DataTypeTuple *>(right.get());
            right_tuple && left_tuple->getElements().size() == right_tuple->getElements().size())
        {
            for (size_t i = 0; i < left_tuple->getElements().size(); ++i)
                if (hasCompoundCarrierForIn(left_tuple->getElements()[i], right_tuple->getElements()[i], true))
                    return true;
        }
    }
    return false;
}

[[nodiscard]] bool hasMixedStringDomain(const DataTypePtr & left_type, const DataTypePtr & right_type)
{
    const auto left = removeNullable(removeLowCardinality(left_type));
    const auto right = removeNullable(removeLowCardinality(right_type));
    if (const auto * left_array = typeid_cast<const DataTypeArray *>(left.get()))
    {
        if (const auto * right_array = typeid_cast<const DataTypeArray *>(right.get()))
            return hasMixedStringDomain(left_array->getNestedType(), right_array->getNestedType());
    }
    if (const auto * left_tuple = typeid_cast<const DataTypeTuple *>(left.get()))
    {
        if (const auto * right_tuple = typeid_cast<const DataTypeTuple *>(right.get());
            right_tuple && left_tuple->getElements().size() == right_tuple->getElements().size())
        {
            for (size_t i = 0; i < left_tuple->getElements().size(); ++i)
                if (hasMixedStringDomain(left_tuple->getElements()[i], right_tuple->getElements()[i]))
                    return true;
        }
    }
    if (const auto * left_map = typeid_cast<const DataTypeMap *>(left.get()))
    {
        if (const auto * right_map = typeid_cast<const DataTypeMap *>(right.get()))
            return hasMixedStringDomain(left_map->getKeyType(), right_map->getKeyType())
                || hasMixedStringDomain(left_map->getValueType(), right_map->getValueType());
    }
    const bool left_identifier = isUUID(left) || isIPv4(left) || isIPv6(left);
    const bool right_identifier = isUUID(right) || isIPv4(right) || isIPv6(right);
    return (isStringOrFixedString(left) && (isNumber(right) || isBool(right) || isEnum(right) || right_identifier))
        || (isStringOrFixedString(right) && (isNumber(left) || isBool(left) || isEnum(left) || left_identifier))
        || (isStringOrFixedString(left) && isStringOrFixedString(right) && !left->equals(*right));
}

[[nodiscard]] bool hasNamedTupleLayoutMismatch(const DataTypePtr & left_type, const DataTypePtr & right_type)
{
    const auto left = removeNullable(removeLowCardinality(left_type));
    const auto right = removeNullable(removeLowCardinality(right_type));
    if (const auto * left_array = typeid_cast<const DataTypeArray *>(left.get()))
    {
        if (const auto * right_array = typeid_cast<const DataTypeArray *>(right.get()))
            return hasNamedTupleLayoutMismatch(left_array->getNestedType(), right_array->getNestedType());
    }
    if (const auto * left_tuple = typeid_cast<const DataTypeTuple *>(left.get()))
    {
        if (const auto * right_tuple = typeid_cast<const DataTypeTuple *>(right.get()))
        {
            if (left_tuple->hasExplicitNames() && right_tuple->hasExplicitNames()
                && left_tuple->getElementNames() != right_tuple->getElementNames())
                return true;
            if (left_tuple->getElements().size() == right_tuple->getElements().size())
                for (size_t i = 0; i < left_tuple->getElements().size(); ++i)
                    if (hasNamedTupleLayoutMismatch(left_tuple->getElements()[i], right_tuple->getElements()[i]))
                        return true;
        }
    }
    if (const auto * left_map = typeid_cast<const DataTypeMap *>(left.get()))
    {
        if (const auto * right_map = typeid_cast<const DataTypeMap *>(right.get()))
            return hasNamedTupleLayoutMismatch(left_map->getKeyType(), right_map->getKeyType())
                || hasNamedTupleLayoutMismatch(left_map->getValueType(), right_map->getValueType());
    }
    return false;
}

[[nodiscard]] bool hasNumericTemporalOrEnumPair(const DataTypePtr & left_type, const DataTypePtr & right_type)
{
    const auto left = removeNullable(removeLowCardinality(left_type));
    const auto right = removeNullable(removeLowCardinality(right_type));
    if (const auto * left_array = typeid_cast<const DataTypeArray *>(left.get()))
    {
        if (const auto * right_array = typeid_cast<const DataTypeArray *>(right.get()))
            return hasNumericTemporalOrEnumPair(left_array->getNestedType(), right_array->getNestedType());
    }
    if (const auto * left_tuple = typeid_cast<const DataTypeTuple *>(left.get()))
    {
        if (const auto * right_tuple = typeid_cast<const DataTypeTuple *>(right.get());
            right_tuple && left_tuple->getElements().size() == right_tuple->getElements().size())
        {
            for (size_t i = 0; i < left_tuple->getElements().size(); ++i)
                if (hasNumericTemporalOrEnumPair(left_tuple->getElements()[i], right_tuple->getElements()[i]))
                    return true;
        }
    }
    if (const auto * left_map = typeid_cast<const DataTypeMap *>(left.get()))
    {
        if (const auto * right_map = typeid_cast<const DataTypeMap *>(right.get()))
            return hasNumericTemporalOrEnumPair(left_map->getKeyType(), right_map->getKeyType())
                || hasNumericTemporalOrEnumPair(left_map->getValueType(), right_map->getValueType());
    }
    const bool left_temporal = isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(left);
    const bool right_temporal = isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(right);
    return (isNumber(left) && (right_temporal || isEnum(right)))
        || ((left_temporal || isEnum(left)) && isNumber(right));
}

[[nodiscard]] bool hasNestedCalendarDateInComparison(const DataTypePtr & left_type, const DataTypePtr & right_type)
{
    const auto left = removeNullable(removeLowCardinality(left_type));
    const auto right = removeNullable(removeLowCardinality(right_type));
    if (const auto * left_array = typeid_cast<const DataTypeArray *>(left.get()))
    {
        if (const auto * right_array = typeid_cast<const DataTypeArray *>(right.get()))
            return hasNestedCalendarDateInComparison(left_array->getNestedType(), right_array->getNestedType());
    }
    if (const auto * left_tuple = typeid_cast<const DataTypeTuple *>(left.get()))
    {
        if (const auto * right_tuple = typeid_cast<const DataTypeTuple *>(right.get());
            right_tuple && left_tuple->getElements().size() == right_tuple->getElements().size())
        {
            for (size_t i = 0; i < left_tuple->getElements().size(); ++i)
                if (hasNestedCalendarDateInComparison(left_tuple->getElements()[i], right_tuple->getElements()[i]))
                    return true;
        }
    }
    if (const auto * left_map = typeid_cast<const DataTypeMap *>(left.get()))
    {
        if (const auto * right_map = typeid_cast<const DataTypeMap *>(right.get()))
            return hasNestedCalendarDateInComparison(left_map->getKeyType(), right_map->getKeyType())
                || hasNestedCalendarDateInComparison(left_map->getValueType(), right_map->getValueType());
    }
    return ((isDate(left) || isDate32(left)) && (isDateTime(right) || isDateTime64(right)))
        || ((isDate(right) || isDate32(right)) && (isDateTime(left) || isDateTime64(left)));
}

[[nodiscard]] bool hasCalendarDateType(const DataTypePtr & type)
{
    const auto bare_type = removeNullable(removeLowCardinality(type));
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(bare_type.get()))
        return hasCalendarDateType(array_type->getNestedType());
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(bare_type.get()))
        return std::any_of(tuple_type->getElements().begin(), tuple_type->getElements().end(), hasCalendarDateType);
    if (const auto * map_type = typeid_cast<const DataTypeMap *>(bare_type.get()))
        return hasCalendarDateType(map_type->getKeyType()) || hasCalendarDateType(map_type->getValueType());
    return isDate(bare_type) || isDate32(bare_type) || isDateTime(bare_type) || isDateTime64(bare_type);
}

[[nodiscard]] bool accurateEqualsCompoundFields(
    const Field & left, const Field & right, const DataTypePtr & left_type, const DataTypePtr & right_type)
{
    const auto left_bare = removeNullable(removeLowCardinality(left_type));
    const auto right_bare = removeNullable(removeLowCardinality(right_type));
    if (left.isNull() || right.isNull())
        return left.isNull() && right.isNull();
    if (const auto * left_array = typeid_cast<const DataTypeArray *>(left_bare.get()))
    {
        if (const auto * right_array = typeid_cast<const DataTypeArray *>(right_bare.get()))
        {
            const auto & left_elements = left.safeGet<Array>();
            const auto & right_elements = right.safeGet<Array>();
            return left_elements.size() == right_elements.size()
                && std::equal(left_elements.begin(), left_elements.end(), right_elements.begin(), [&](const auto & l, const auto & r)
                {
                    return accurateEqualsCompoundFields(l, r, left_array->getNestedType(), right_array->getNestedType());
                });
        }
    }
    if (const auto * left_tuple = typeid_cast<const DataTypeTuple *>(left_bare.get()))
    {
        if (const auto * right_tuple = typeid_cast<const DataTypeTuple *>(right_bare.get());
            right_tuple && left_tuple->getElements().size() == right_tuple->getElements().size())
        {
            const auto & left_elements = left.safeGet<Tuple>();
            const auto & right_elements = right.safeGet<Tuple>();
            if (left_elements.size() != right_elements.size())
                return false;
            for (size_t i = 0; i < left_elements.size(); ++i)
                if (!accurateEqualsCompoundFields(
                        left_elements[i], right_elements[i], left_tuple->getElements()[i], right_tuple->getElements()[i]))
                    return false;
            return true;
        }
    }
    if (const auto * left_map = typeid_cast<const DataTypeMap *>(left_bare.get()))
    {
        if (const auto * right_map = typeid_cast<const DataTypeMap *>(right_bare.get()))
        {
            const auto & left_entries = left.safeGet<Map>();
            const auto & right_entries = right.safeGet<Map>();
            if (left_entries.size() != right_entries.size())
                return false;
            for (size_t i = 0; i < left_entries.size(); ++i)
            {
                const auto & left_pair = left_entries[i].safeGet<Tuple>();
                const auto & right_pair = right_entries[i].safeGet<Tuple>();
                if (!accurateEqualsCompoundFields(left_pair[0], right_pair[0], left_map->getKeyType(), right_map->getKeyType())
                    || !accurateEqualsCompoundFields(left_pair[1], right_pair[1], left_map->getValueType(), right_map->getValueType()))
                    return false;
            }
            return true;
        }
    }
    if (isFloat(left_bare) && isDecimal(right_bare))
    {
        const auto converted = tryConvertFieldToType(left, *right_bare, left_bare.get(), {}, /*strict=*/true);
        return !converted.isNull() && accurateEquals(converted, right);
    }
    if (isDecimal(left_bare) && isFloat(right_bare))
    {
        const auto converted = tryConvertFieldToType(right, *left_bare, right_bare.get(), {}, /*strict=*/true);
        return !converted.isNull() && accurateEquals(left, converted);
    }
    if ((isStringOrFixedString(left_bare) || isStringOrFixedString(right_bare))
        && !left_bare->equals(*right_bare))
    {
        /// Compare in the probe domain, retaining source tags such as nested `Bool`.
        auto source_column = right_bare->createColumn();
        source_column->insert(right);
        auto converted = tryConvertColumnToTypeOrNull(
            *source_column, right_bare, left_bare, {}, /*strict=*/true);
        return converted && accurateEquals(left, (*converted)[0]);
    }
    return accurateEquals(left, right);
}

[[nodiscard]] bool accurateEqualsCompoundWithActiveTypes(
    const Field & left, const Field & right, const DataTypePtr & left_type, const DataTypePtr & right_type,
    const IColumn & right_column, size_t right_row)
{
    if (left.isNull() || right.isNull())
        return left.isNull() && right.isNull();

    const auto left_bare = removeNullable(removeLowCardinality(left_type));
    const auto right_bare = removeNullable(removeLowCardinality(right_type));
    const IColumn * source = &right_column;
    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(source))
        source = &nullable->getNestedColumn();

    DataTypePtr active_type;
    if (const auto * dynamic = checkAndGetColumn<ColumnDynamic>(source))
        active_type = dynamic->getTypeAt(right_row);
    else if (const auto * variant = checkAndGetColumn<ColumnVariant>(source))
    {
        const auto discriminator = variant->globalDiscriminatorAt(right_row);
        const auto & variant_type = assert_cast<const DataTypeVariant &>(*right_bare);
        if (discriminator < variant_type.getVariants().size())
            active_type = variant_type.getVariant(discriminator);
    }
    if (active_type)
    {
        if (hasNumericTemporalOrEnumPair(left_bare, active_type))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Setting 'in_null_as_unknown' does not yet support comparing {} with an active carrier of {}",
                left_bare->getName(), active_type->getName());
        return accurateEqualsCompoundFields(left, right, left_bare, active_type);
    }

    if (const auto * left_array = typeid_cast<const DataTypeArray *>(left_bare.get()))
    {
        if (const auto * right_array = typeid_cast<const DataTypeArray *>(right_bare.get()))
        {
            const auto & array_column = assert_cast<const ColumnArray &>(*source);
            const auto & left_elements = left.safeGet<Array>();
            const auto & right_elements = right.safeGet<Array>();
            if (left_elements.size() != right_elements.size())
                return false;
            const size_t offset = right_row ? array_column.getOffsets()[right_row - 1] : 0;
            for (size_t i = 0; i < left_elements.size(); ++i)
                if (!accurateEqualsCompoundWithActiveTypes(left_elements[i], right_elements[i],
                    left_array->getNestedType(), right_array->getNestedType(), array_column.getData(), offset + i))
                    return false;
            return true;
        }
    }
    if (const auto * left_tuple = typeid_cast<const DataTypeTuple *>(left_bare.get()))
    {
        if (const auto * right_tuple = typeid_cast<const DataTypeTuple *>(right_bare.get());
            right_tuple && left_tuple->getElements().size() == right_tuple->getElements().size())
        {
            const auto & tuple_column = assert_cast<const ColumnTuple &>(*source);
            const auto & left_elements = left.safeGet<Tuple>();
            const auto & right_elements = right.safeGet<Tuple>();
            if (left_elements.size() != right_elements.size())
                return false;
            for (size_t i = 0; i < left_elements.size(); ++i)
                if (!accurateEqualsCompoundWithActiveTypes(left_elements[i], right_elements[i],
                    left_tuple->getElements()[i], right_tuple->getElements()[i], tuple_column.getColumn(i), right_row))
                    return false;
            return true;
        }
    }
    if (const auto * left_map = typeid_cast<const DataTypeMap *>(left_bare.get()))
    {
        if (const auto * right_map = typeid_cast<const DataTypeMap *>(right_bare.get()))
        {
            const auto & map_column = assert_cast<const ColumnMap &>(*source);
            const auto & left_entries = left.safeGet<Map>();
            const auto & right_entries = right.safeGet<Map>();
            if (left_entries.size() != right_entries.size())
                return false;
            const auto & nested = map_column.getNestedColumn();
            const auto & entries = map_column.getNestedData();
            const size_t offset = right_row ? nested.getOffsets()[right_row - 1] : 0;
            for (size_t i = 0; i < left_entries.size(); ++i)
            {
                const auto & left_pair = left_entries[i].safeGet<Tuple>();
                const auto & right_pair = right_entries[i].safeGet<Tuple>();
                if (!accurateEqualsCompoundWithActiveTypes(left_pair[0], right_pair[0],
                    left_map->getKeyType(), right_map->getKeyType(), entries.getColumn(0), offset + i)
                    || !accurateEqualsCompoundWithActiveTypes(left_pair[1], right_pair[1],
                        left_map->getValueType(), right_map->getValueType(), entries.getColumn(1), offset + i))
                    return false;
            }
            return true;
        }
    }
    return accurateEqualsCompoundFields(left, right, left_bare, right_bare);
}

class FunctionScalarCarrierValueEqualsForIn final : public IFunction
{
public:
    static constexpr auto name = "__scalarCarrierValueEqualsForIn";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionScalarCarrierValueEqualsForIn>(); }

    [[nodiscard]] String getName() const override { return name; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 2; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto left_type = removeNullable(removeLowCardinality(arguments[0]));
        if (!isNumber(left_type) && !isBool(left_type) && !isStringOrFixedString(left_type) && !isEnum(left_type)
            && !isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(left_type)
            && !isUUID(left_type) && !isIPv4(left_type) && !isIPv6(left_type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be numeric, Bool, String, Enum, temporal, UUID or IP", getName());
        const auto source_type = removeNullable(removeLowCardinality(arguments[1]));
        if (!isDynamic(source_type) && !isVariant(source_type)
            && !isNumber(source_type) && !isBool(source_type) && !isStringOrFixedString(source_type)
            && !isEnum(source_type) && !isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(source_type)
            && !isUUID(source_type) && !isIPv4(source_type) && !isIPv6(source_type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function {} must be a scalar, Dynamic or Variant", getName());
        auto result_type = std::make_shared<DataTypeUInt8>();
        if (canContainNull(*arguments[1]))
            return std::make_shared<DataTypeNullable>(result_type);
        return result_type;
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto left_type = removeNullable(removeLowCardinality(arguments[0].type));
        const auto source_type = removeNullable(removeLowCardinality(arguments[1].type));
        auto left_values = recursiveRemoveLowCardinality(arguments[0].column->convertToFullColumnIfConst());
        auto sources = recursiveRemoveLowCardinality(arguments[1].column->convertToFullColumnIfConst());
        const IColumn * left = left_values.get();
        const IColumn * carrier = sources.get();
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(left))
            left = &nullable->getNestedColumn();
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(carrier))
            carrier = &nullable->getNestedColumn();

        auto result = ColumnUInt8::create(input_rows_count, UInt8{0});
        auto null_map = ColumnUInt8::create(input_rows_count, UInt8{0});
        auto & values = result->getData();
        auto & null_values = null_map->getData();
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            if (sources->isNullAt(row))
            {
                null_values[row] = 1;
                continue;
            }
            if (left_values->isNullAt(row))
                continue;
            if (isFloat(left_type) && std::isnan(left->getFloat64(row)))
                continue;

            auto source_row = carrier->cut(row, 1);
            DataTypePtr active_type = (isDynamic(source_type) || isVariant(source_type)) ? nullptr : source_type;
            if (const auto * dynamic = checkAndGetColumn<ColumnDynamic>(carrier))
                active_type = dynamic->getTypeAt(row);
            else if (const auto * variant = checkAndGetColumn<ColumnVariant>(carrier))
            {
                const auto discriminator = variant->globalDiscriminatorAt(row);
                const auto & variant_type = assert_cast<const DataTypeVariant &>(*source_type);
                if (discriminator < variant_type.getVariants().size())
                    active_type = variant_type.getVariant(discriminator);
            }
            if (active_type && isDecimal(active_type) && isNumber(left_type))
            {
                auto extracted = tryConvertColumnToTypeOrNull(
                    *source_row, source_type, active_type, {}, /*strict=*/true);
                values[row] = extracted && accurateEqualsCompoundFields((*left)[row], (*extracted)[0], left_type, active_type);
                continue;
            }
            if (active_type && (isDate(left_type) || isDate32(left_type))
                && (isDateTime(active_type) || isDateTime64(active_type)))
            {
                auto converted_date = tryConvertColumnToTypeOrNull(
                    *source_row, source_type, left_type, {}, /*strict=*/false);
                values[row] = converted_date && left->compareAt(row, 0, *converted_date, 1) == 0;
                continue;
            }
            auto converted = tryConvertColumnToTypeOrNull(
                *source_row, source_type, left_type, {}, /*strict=*/true);
            values[row] = converted && left->compareAt(row, 0, *converted, 1) == 0;
        }
        if (canContainNull(*arguments[1].type))
            return ColumnNullable::create(std::move(result), std::move(null_map));
        return result;
    }
};

class FunctionMapValueEqualsForIn final : public IFunction
{
public:
    static constexpr auto name = "__mapValueEqualsForIn";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapValueEqualsForIn>(); }

    [[nodiscard]] String getName() const override { return name; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 2; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isMap(removeNullable(removeLowCardinality(arguments[0]))))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a Map", getName());
        const auto source_type = removeNullable(removeLowCardinality(arguments[1]));
        if (!isStringOrFixedString(source_type) && !isMap(source_type) && !isDynamic(source_type) && !isVariant(source_type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function {} must be a String, Map, Dynamic or Variant", getName());
        auto result_type = std::make_shared<DataTypeUInt8>();
        if (canContainNull(*arguments[1]))
            return std::make_shared<DataTypeNullable>(result_type);
        return result_type;
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto map_type = removeNullable(removeLowCardinality(arguments[0].type));
        auto maps = arguments[0].column->convertToFullColumnIfConst();
        auto sources = arguments[1].column->convertToFullColumnIfConst();
        const auto source_type = removeNullable(removeLowCardinality(arguments[1].type));
        const IColumn * carrier = sources.get();
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(carrier))
            carrier = &nullable->getNestedColumn();
        auto result = ColumnUInt8::create(input_rows_count, UInt8{0});
        auto null_map = ColumnUInt8::create(input_rows_count, UInt8{0});
        auto & values = result->getData();
        auto & null_values = null_map->getData();
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            if (sources->isNullAt(row))
            {
                null_values[row] = 1;
                continue;
            }
            /// A textual compound carrier has no element type, so its calendar
            /// precision cannot be compared safely with a typed map.
            if (isStringOrFixedString(source_type) && hasCalendarDateType(map_type))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Setting 'in_null_as_unknown' does not yet support comparing {} with a string map carrier",
                    map_type->getName());
            /// The source column retains the active `Dynamic`/`Variant` alternative and
            /// nested `Bool` tags that extracting a `Field` would discard.
            auto source_row = sources->cut(row, 1);
            DataTypePtr active_type;
            if (const auto * dynamic = checkAndGetColumn<ColumnDynamic>(carrier))
                active_type = dynamic->getTypeAt(row);
            else if (const auto * variant = checkAndGetColumn<ColumnVariant>(carrier))
            {
                const auto discriminator = variant->globalDiscriminatorAt(row);
                const auto & variant_type = assert_cast<const DataTypeVariant &>(*source_type);
                if (discriminator < variant_type.getVariants().size())
                    active_type = variant_type.getVariant(discriminator);
            }
            if (active_type && isMap(active_type) && hasNestedCalendarDateInComparison(map_type, active_type))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Setting 'in_null_as_unknown' does not yet support comparing {} with a map carrier of {}",
                    map_type->getName(), active_type->getName());
            if (active_type && isMap(active_type) && hasCompoundDecimalFloatPair(map_type, active_type))
            {
                auto active_row = carrier->cut(row, 1);
                auto extracted = tryConvertColumnToTypeOrNull(
                    *active_row, source_type, active_type, {}, /*strict=*/true);
                values[row] = extracted && accurateEqualsCompoundFields((*maps)[row], (*extracted)[0], map_type, active_type);
                continue;
            }
            if (isMap(source_type) && hasCompoundCarrierForIn(map_type, source_type))
            {
                values[row] = accurateEqualsCompoundWithActiveTypes(
                    (*maps)[row], (*sources)[row], map_type, source_type, *sources, row);
                continue;
            }
            if (isMap(source_type) && hasCompoundStringConversionLeaf(map_type, source_type))
            {
                values[row] = matchesSetColumnConversion(*maps, *sources, row, map_type, arguments[1].type);
                continue;
            }
            auto converted = tryConvertColumnToTypeOrNull(
                *source_row, arguments[1].type, map_type, {}, /*strict=*/true);
            if (!converted)
                continue;
            values[row] = maps->compareAt(row, 0, *converted, 1) == 0;
        }
        for (size_t row = 0; row < input_rows_count; ++row)
            if (values[row] && fieldContainsNaNForIn((*maps)[row]))
                values[row] = 0;
        if (canContainNull(*arguments[1].type))
            return ColumnNullable::create(std::move(result), std::move(null_map));
        return result;
    }
};

class FunctionArrayValueEqualsForIn final : public IFunction
{
public:
    static constexpr auto name = "__arrayValueEqualsForIn";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayValueEqualsForIn>(); }

    [[nodiscard]] String getName() const override { return name; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 2; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isArray(removeNullable(removeLowCardinality(arguments[0]))))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be an Array", getName());
        const auto source_type = removeNullable(removeLowCardinality(arguments[1]));
        if (!isDynamic(source_type) && !isVariant(source_type) && !isStringOrFixedString(source_type) && !isArray(source_type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function {} must be a Dynamic, Variant, String or Array", getName());
        auto result_type = std::make_shared<DataTypeUInt8>();
        if (canContainNull(*arguments[1]))
            return std::make_shared<DataTypeNullable>(result_type);
        return result_type;
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto left_type = removeNullable(removeLowCardinality(arguments[0].type));
        const auto & left_array_type = assert_cast<const DataTypeArray &>(*left_type);
        auto left_values = recursiveRemoveLowCardinality(arguments[0].column->convertToFullColumnIfConst());
        auto sources = recursiveRemoveLowCardinality(arguments[1].column->convertToFullColumnIfConst());
        const IColumn * carrier = sources.get();
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(carrier))
            carrier = &nullable->getNestedColumn();
        const auto source_type = removeNullable(removeLowCardinality(arguments[1].type));

        auto result = ColumnUInt8::create(input_rows_count, UInt8{0});
        auto null_map = ColumnUInt8::create(input_rows_count, UInt8{0});
        auto & values = result->getData();
        auto & null_values = null_map->getData();
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            if (sources->isNullAt(row))
            {
                null_values[row] = 1;
                continue;
            }
            if (left_values->isNullAt(row))
                continue;

            /// A textual compound carrier has no element type, so its calendar
            /// precision cannot be compared safely with a typed array.
            if (isStringOrFixedString(source_type) && hasCalendarDateType(left_type))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Setting 'in_null_as_unknown' does not yet support comparing {} with a string array carrier",
                    left_type->getName());

            if (isStringOrFixedString(source_type) || isArray(source_type))
            {
                if (isArray(source_type) && hasCompoundStringConversionLeaf(left_type, source_type))
                {
                    values[row] = matchesSetColumnConversion(*left_values, *sources, row, left_type, arguments[1].type);
                    continue;
                }
                auto source_row = sources->cut(row, 1);
                auto converted = tryConvertColumnToTypeOrNull(
                    *source_row, arguments[1].type, left_type, {}, /*strict=*/true);
                values[row] = converted && left_values->compareAt(row, 0, *converted, 1) == 0;
                continue;
            }

            DataTypePtr active_type;
            if (const auto * dynamic = checkAndGetColumn<ColumnDynamic>(carrier))
                active_type = dynamic->getTypeAt(row);
            else if (const auto * variant = checkAndGetColumn<ColumnVariant>(carrier))
            {
                const auto discriminator = variant->globalDiscriminatorAt(row);
                const auto & variant_type = assert_cast<const DataTypeVariant &>(*source_type);
                if (discriminator < variant_type.getVariants().size())
                    active_type = variant_type.getVariant(discriminator);
            }
            const auto * active_array_type = active_type ? typeid_cast<const DataTypeArray *>(active_type.get()) : nullptr;
            if (!active_array_type)
                continue;

            auto left_row = left_values->cut(row, 1);
            auto source_row = carrier->cut(row, 1);
            const auto left_element_type = removeNullable(removeLowCardinality(left_array_type.getNestedType()));
            const auto source_element_type = removeNullable(removeLowCardinality(active_array_type->getNestedType()));
            if (hasNestedCalendarDateInComparison(left_type, active_type)
                || (!left_element_type->equals(*source_element_type)
                    && (isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(left_element_type)
                        || isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(source_element_type))))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Setting 'in_null_as_unknown' does not yet support comparing {} with an array carrier of {}",
                    left_type->getName(), active_type->getName());
            if (hasCompoundDecimalFloatPair(left_element_type, source_element_type))
            {
                /// Compare the extracted value in its source type so nested decimals
                /// retain their scale instead of being cast to floating point.
                auto extracted_source = tryConvertColumnToTypeOrNull(
                    *source_row, source_type, active_type, {}, /*strict=*/true);
                values[row] = extracted_source
                    && accurateEqualsCompoundFields((*left_row)[0], (*extracted_source)[0], left_type, active_type);
            }
            else
            {
                auto converted_source = tryConvertColumnToTypeOrNull(
                    *source_row, source_type, left_type, {}, /*strict=*/true);
                values[row] = converted_source && left_row->compareAt(0, 0, *converted_source, 1) == 0;
            }
        }
        for (size_t row = 0; row < input_rows_count; ++row)
            if (values[row] && fieldContainsNaNForIn((*left_values)[row]))
                values[row] = 0;
        if (canContainNull(*arguments[1].type))
            return ColumnNullable::create(std::move(result), std::move(null_map));
        return result;
    }
};

template <typename ColumnType>
ColumnPtr flipFloatZeroSign(const ColumnPtr & column)
{
    const auto * float_column = checkAndGetColumn<ColumnType>(column.get());
    if (!float_column)
        return nullptr;

    const auto & values = float_column->getData();
    if (std::none_of(values.begin(), values.end(), [](auto value) { return value == 0; }))
        return nullptr;

    auto alternate = column->cloneResized(column->size());
    for (auto & value : assert_cast<ColumnType &>(*alternate).getData())
    {
        if (value == 0)
            value = -value;
    }
    return alternate;
}

ColumnPtr flipFloatZeroSign(const ColumnPtr & column)
{
    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(column.get()))
    {
        auto nested = flipFloatZeroSign(nullable->getNestedColumnPtr());
        if (!nested)
            return nullptr;
        return ColumnNullable::create(nested, nullable->getNullMapColumnPtr());
    }

    if (auto alternate = flipFloatZeroSign<ColumnFloat32>(column))
        return alternate;
    if (auto alternate = flipFloatZeroSign<ColumnBFloat16>(column))
        return alternate;
    return flipFloatZeroSign<ColumnFloat64>(column);
}

/** in(x, set) - function for evaluating the IN
  * notIn(x, set) - and NOT IN.
  */

class FunctionIn final : public IFunction
{
public:
    FunctionIn(String name_, bool negative_, bool null_is_skipped_, bool ignore_set_, bool in_null_as_unknown_)
        : function_name(std::move(name_))
        , negative(negative_)
        , null_is_skipped(null_is_skipped_)
        , ignore_set(ignore_set_)
        , in_null_as_unknown(in_null_as_unknown_)
    {
    }

    /// ignore_set flag means that we don't use set from the second argument, just return zero column.
    /// It is needed to perform type analysis without creation of set.

    static FunctionPtr create(String name, bool negative, bool null_is_skipped, bool ignore_set, bool in_null_as_unknown)
    {
        return std::make_shared<FunctionIn>(std::move(name), negative, null_is_skipped, ignore_set, in_null_as_unknown);
    }

    [[nodiscard]] String getName() const override
    {
        return function_name;
    }

    void updateHash(SipHash & hash) const override { hash.update(in_null_as_unknown); }
    [[nodiscard]] bool useNullAsUnknownSemanticsForIn() const override { return in_null_as_unknown; }
    [[nodiscard]] bool isInMembershipFunction() const override { return true; }

    /// The `IgnoreSet` variants are called with the left operand alone during type analysis, but
    /// `inIgnoreSet(x, set)` written explicitly stays valid, hence the variadic arity.
    [[nodiscard]] bool isVariadic() const override { return ignore_set; }

    [[nodiscard]] size_t getNumberOfArguments() const override
    {
        return ignore_set ? 0 : 2;
    }

    [[nodiscard]] String getSignatureString() const override
    {
        /// The `IgnoreSet` variants are called with the left operand alone during type analysis,
        /// hence the optional second argument there.
        const auto * const result_type = in_null_as_unknown ? "Nullable(UInt8)" : "UInt8";
        return ignore_set ? fmt::format("(Any, [Any]) -> {}", result_type) : fmt::format("(Any, Any) -> {}", result_type);
    }

    /// The `Any` matchers document the arity and return type, but they cannot express the legacy
    /// rejection of an argument with a dynamic structure (`Dynamic`, `JSON`/`Object`, and nested
    /// forms), nor the exact arity diagnostic of the `IgnoreSet` variants. Restore both here, then
    /// delegate to the signature.
    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (ignore_set && (arguments.empty() || arguments.size() > 2))
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 1 or 2",
                getName(),
                arguments.size());

        if (arguments[0].type->hasDynamicStructure())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}", arguments[0].type->getName(), getName());

        if (in_null_as_unknown
            && typeid_cast<const DataTypeTuple *>(removeNullable(removeLowCardinality(arguments[0].type)).get()))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Setting 'in_null_as_unknown' does not support a Tuple left operand in function {}",
                getName());

        auto result_type = IFunction::getReturnTypeImpl(arguments);
        return in_null_as_unknown ? makeNullable(result_type) : result_type;
    }

    /// We can't use the default constant-folding wrapper because it would wrap a
    /// dry-run result as a ColumnConst, and `FilterTransform`'s header evaluation
    /// would then see a subquery-IN with an unbuilt set as "always false" (the
    /// dry-run path returns 0) and skip the filter entirely. Handle the size-0
    /// ColumnConst arguments produced by `ActionsDAG::addColumn` directly below.
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return false; }

    [[nodiscard]] bool useDefaultImplementationForDynamic() const override
    {
        return false;
    }

    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return null_is_skipped && !in_null_as_unknown; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    /// An empty set makes the result independent of the left operand, so the result is constant
    /// even when that operand is not. Reporting it here keeps DAG nodes in agreement with
    /// `executeImpl`, which returns a ColumnConst for the same case.
    [[nodiscard]] ColumnPtr getConstantResultForNonConstArguments(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        /// The -IgnoreSet variants carry no set to inspect: they exist to resolve types without one.
        if (ignore_set || arguments.size() != 2)
            return nullptr;

        /// Under the standard semantics, an immutable empty set determines the result even for a
        /// `NULL` left operand. Under the legacy semantics, keep the default nullable wrapper so a
        /// nullable left operand still produces `NULL`. `LowCardinality` does not affect the value.
        const auto result_type_without_low_cardinality = removeLowCardinality(result_type);
        if ((!in_null_as_unknown && !isUInt8(result_type_without_low_cardinality))
            || (in_null_as_unknown && !isUInt8(removeNullable(result_type_without_low_cardinality))))
            return nullptr;

        const ColumnSet * column_set = tryGetColumnSet(arguments[1].column);
        if (!column_set)
            return nullptr;

        auto future_set = column_set->getData();
        if (!future_set)
            return nullptr;

        if (!isImmutableEmptySet(*future_set))
            return nullptr;

        return result_type->createColumnConst(1, static_cast<UInt8>(negative));
    }

    [[nodiscard]] ColumnPtr executeImplDryRun(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return executeImpl(arguments, true, input_rows_count);
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return executeImpl(arguments, false, input_rows_count);
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, bool dry_run, size_t input_rows_count) const
    {
        if (ignore_set)
            return createResultColumn(input_rows_count, false);

        ColumnPtr column_set_ptr = arguments[1].column;
        const ColumnSet * column_set = tryGetColumnSet(column_set_ptr);
        if (!column_set)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function '{}' must be Set; found {}",
                getName(), column_set_ptr->getName());

        auto future_set = column_set->getData();
        if (!future_set)
        {
            if (dry_run)
                return createResultColumn(input_rows_count, false);

            throw Exception(ErrorCodes::LOGICAL_ERROR, "No Set is passed as the second argument for function '{}'", getName());
        }

        auto set = future_set->get();
        if (!set)
        {
            if (dry_run)
                return createResultColumn(input_rows_count, false);

            throw Exception(ErrorCodes::LOGICAL_ERROR, "Not-ready Set is passed as the second argument for function '{}'", getName());
        }

        if (in_null_as_unknown && arguments[0].type->onlyNull())
        {
            if (set->sourceIsEmpty())
                return createResultColumn(input_rows_count, negative);

            auto unknown = ColumnNullable::create(ColumnUInt8::create(1, UInt8{0}), ColumnUInt8::create(1, UInt8{1}));
            return ColumnConst::create(std::move(unknown), input_rows_count);
        }

        /// Empty set: return a constant result, checked before input_rows_count == 0 so that header
        /// evaluation produces a `ColumnConst` detectable by `ConstantFilterDescription`.
        if (set->getTotalRowCount() == 0)
        {
            /// Conversion to the left operand type can discard every literal even though the
            /// source list is not empty. Such a set still depends on a `NULL` left operand under
            /// SQL three-valued semantics, so do not apply the logically-empty-set shortcut.
            const auto * tuple_set = typeid_cast<const FutureSetFromTuple *>(future_set.get());
            const bool normalized_from_nonempty_tuple = in_null_as_unknown && tuple_set && !tuple_set->isSourceEmpty();
            if (!normalized_from_nonempty_tuple)
            {
                if (canReportEmptySetAsConstant(*future_set))
                    return ColumnConst::create(createResultColumn(1, negative), input_rows_count);

                if (in_null_as_unknown)
                    return createResultColumn(input_rows_count, negative);
            }
        }

        /// Unwrap ColumnConst for the first argument if needed.
        ColumnWithTypeAndName left_arg = arguments[0];
        bool left_is_const = isColumnConst(*left_arg.column);

        if (left_is_const)
            left_arg.column = assert_cast<const ColumnConst &>(*left_arg.column).getDataColumnPtr();
        else if (input_rows_count == 0)
            return createResultColumn(0, false);

        const ColumnTuple * tuple = typeid_cast<const ColumnTuple *>(left_arg.column.get());
        const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(left_arg.type.get());

        ColumnsWithTypeAndName columns_of_key_columns;
        auto set_types = set->getDataTypes();
        bool check_numeric_probe_exactness = false;

        if (in_null_as_unknown && set_types.size() == 1 && set_types[0]->onlyNull() && !set->sourceIsEmpty())
        {
            auto unknown = ColumnNullable::create(ColumnUInt8::create(1, UInt8{0}), ColumnUInt8::create(1, UInt8{1}));
            return ColumnConst::create(std::move(unknown), input_rows_count);
        }

        if (in_null_as_unknown && set_types.size() == 1)
        {
            const auto left_type = removeNullable(removeLowCardinality(left_arg.type));
            const auto key_type = removeNullable(removeLowCardinality(set_types[0]));
            const bool mixed_temporal_types = !left_type->equals(*key_type)
                && (isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(left_type)
                    || isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(key_type));
            const bool mixed_nested_calendar_types = (isArray(left_type) || isMap(left_type) || isTuple(left_type))
                && hasNestedCalendarDateInComparison(left_type, key_type);
            const bool mixed_numeric_temporal_or_enum_types = hasNumericTemporalOrEnumPair(left_type, key_type);
            check_numeric_probe_exactness = isNumber(left_type) && isNumber(key_type)
                && !left_type->equals(*key_type);
            /// A set containing only `NULL` has no key whose type-sensitive hash
            /// could miss a match. Leave it to the ordinary unknown-result handling.
            const bool only_null_key = set_types[0]->onlyNull() || (set->hasNull() && set->getTotalRowCount() == 1);
            const bool mixed_string_domain = hasMixedStringDomain(left_type, key_type);
            if (hasProbeCarrierAgainstNumericKey(left_type, key_type) && !only_null_key)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Setting 'in_null_as_unknown' does not yet support comparing a {} probe with a numeric prepared set of {} in function {}",
                    left_arg.type->getName(), set_types[0]->getName(), getName());
            if (!isDynamic(left_type) && !isVariant(left_type)
                && (isDynamic(key_type) || isVariant(key_type) || hasCompoundCarrierForIn(left_type, key_type))
                && !only_null_key)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Setting 'in_null_as_unknown' does not yet support comparing {} with a prepared set of {} in function {}",
                    left_arg.type->getName(), set_types[0]->getName(), getName());
            if ((mixed_temporal_types || mixed_nested_calendar_types || mixed_numeric_temporal_or_enum_types)
                && !only_null_key)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Setting 'in_null_as_unknown' does not yet support comparing {} with a prepared set of {} in function {}",
                    left_arg.type->getName(), set_types[0]->getName(), getName());
            if (mixed_string_domain && !only_null_key)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Setting 'in_null_as_unknown' does not yet support comparing {} with a prepared set of {} in function {}",
                    left_arg.type->getName(), set_types[0]->getName(), getName());
            if (hasNamedTupleLayoutMismatch(left_type, key_type) && !only_null_key)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Setting 'in_null_as_unknown' does not yet support comparing {} with a prepared set of {} in function {}",
                    left_arg.type->getName(), set_types[0]->getName(), getName());
            if ((isArray(left_type) || isMap(left_type) || isTuple(left_type))
                && hasCompoundNumericTypeMismatch(left_type, key_type) && !only_null_key)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Setting 'in_null_as_unknown' does not yet support comparing {} with a prepared set of {} in function {}",
                    left_arg.type->getName(), set_types[0]->getName(), getName());
        }

        if (tuple && set_types.size() != 1 && set_types.size() == tuple->tupleSize())
        {
            const auto & tuple_columns = tuple->getColumns();
            const DataTypes & tuple_types = type_tuple->getElements();
            size_t tuple_size = tuple_columns.size();
            for (size_t i = 0; i < tuple_size; ++i)
                columns_of_key_columns.emplace_back(tuple_columns[i], tuple_types[i], "_" + toString(i));
        }
        else
            columns_of_key_columns.emplace_back(left_arg);

        auto res = set->execute(columns_of_key_columns, in_null_as_unknown ? false : negative);

        const auto left_type_without_wrappers = removeNullable(removeLowCardinality(left_arg.type));
        const auto left_type_id = left_type_without_wrappers->getTypeId();
        const bool left_is_float = left_type_id == TypeIndex::BFloat16
            || left_type_id == TypeIndex::Float32 || left_type_id == TypeIndex::Float64;
        const auto set_key_type_id = set_types.empty()
            ? TypeIndex::Nothing : removeNullable(removeLowCardinality(set_types[0]))->getTypeId();
        const bool set_key_is_float = set_key_type_id == TypeIndex::BFloat16
            || set_key_type_id == TypeIndex::Float32 || set_key_type_id == TypeIndex::Float64;
        if (in_null_as_unknown && columns_of_key_columns.size() == 1 && (left_is_float || set_key_is_float))
        {
            /// Prepared sets compare floating keys by their bits. Probe the other signed-zero
            /// representation too, so a match wins over a source `NULL` in this mode.
            auto full_column = recursiveRemoveLowCardinality(left_arg.column->convertToFullIfWrapped());
            DataTypePtr probe_type = removeLowCardinality(left_arg.type);
            if (!left_is_float)
            {
                probe_type = makeNullable(removeNullable(removeLowCardinality(set_types[0])));
                full_column = castColumnAccurateOrNull(
                    ColumnWithTypeAndName(full_column, removeLowCardinality(left_arg.type), left_arg.name), probe_type);
            }
            if (auto alternate = flipFloatZeroSign(full_column))
            {
                ColumnsWithTypeAndName alternate_key;
                alternate_key.emplace_back(alternate, probe_type, left_arg.name);
                auto alternate_res = set->execute(alternate_key, false);
                auto combined = ColumnUInt8::create(res->size());
                for (size_t row = 0; row < res->size(); ++row)
                {
                    /// A failed conversion has a synthetic `NULL` with a zero in its
                    /// nested storage. It must not match a genuine `NULL` set key.
                    const bool valid_zero = !full_column->isNullAt(row) && full_column->getFloat64(row) == 0;
                    combined->getData()[row] = res->getBool(row) || (valid_zero && alternate_res->getBool(row));
                }
                res = std::move(combined);
            }

            /// Row-wise floating equality rejects `NaN`, while prepared sets compare
            /// raw bits. Keep the two representations consistent in this mode.
            bool has_nan_match = false;
            for (size_t row = 0; left_is_float && row < res->size(); ++row)
            {
                if (res->getBool(row) && !left_arg.column->isNullAt(row) && std::isnan(left_arg.column->getFloat64(row)))
                {
                    has_nan_match = true;
                    break;
                }
            }
            if (has_nan_match)
            {
                auto without_nan_matches = ColumnUInt8::create(res->size());
                for (size_t row = 0; row < res->size(); ++row)
                    without_nan_matches->getData()[row] = res->getBool(row)
                        && (left_arg.column->isNullAt(row) || !std::isnan(left_arg.column->getFloat64(row)));
                res = std::move(without_nan_matches);
            }
        }

        if (in_null_as_unknown && columns_of_key_columns.size() == 1
            && !left_is_float && mayContainFloatForIn(left_arg.type))
        {
            /// The set hashes compound keys by their bits, but row-wise equality
            /// rejects a compound value containing `NaN` at any depth.
            auto without_nan_matches = ColumnUInt8::create(res->size());
            for (size_t row = 0; row < res->size(); ++row)
                without_nan_matches->getData()[row] = res->getBool(row)
                    && !fieldContainsNaNForIn((*left_arg.column)[row]);
            res = std::move(without_nan_matches);
        }

        if (check_numeric_probe_exactness && columns_of_key_columns.size() == 1)
        {
            /// A rounded probe can find a different stored key. A successful cast
            /// alone is insufficient: compare its round trip in the original type.
            auto exact_matches = ColumnUInt8::create(res->size(), UInt8{0});
            auto & exact_data = exact_matches->getData();
            const auto key_type = removeNullable(removeLowCardinality(set_types[0]));
            const auto left_type = removeNullable(removeLowCardinality(left_arg.type));
            for (size_t row = 0; row < res->size(); ++row)
            {
                if (!res->getBool(row) || left_arg.column->isNullAt(row))
                    continue;
                auto probe = left_arg.column->cut(row, 1);
                if (isDecimal(left_type) && isNumber(key_type) && !isDecimal(key_type))
                {
                    /// Ordinary set lookup casts the decimal probe to the key type.
                    /// Verify that this conversion still denotes the same number.
                    auto converted_key = castColumn(
                        ColumnWithTypeAndName{probe, left_arg.type, left_arg.name}, makeNullable(key_type));
                    exact_data[row] = !converted_key->isNullAt(0)
                        && accurateEqualsCompoundFields((*probe)[0], (*converted_key)[0], left_type, key_type);
                    continue;
                }
                auto converted = tryConvertColumnToTypeOrNull(
                    *probe, left_arg.type, key_type, {}, /*strict=*/true);
                if (!converted)
                    continue;
                if (isDecimal(key_type) && isNumber(left_type))
                    exact_data[row] = accurateEquals((*probe)[0], (*converted)[0]);
                else
                {
                    auto restored = tryConvertColumnToTypeOrNull(
                        *converted, key_type, left_type, {}, /*strict=*/true);
                    exact_data[row] = restored && accurateEquals((*probe)[0], (*restored)[0]);
                }
            }
            res = std::move(exact_matches);
        }

        if (in_null_as_unknown)
            res = applyStandardNullSemantics(*res, *left_arg.column, set->hasNull());

        if (left_is_const)
            return ColumnConst::create(std::move(res), input_rows_count);

        if (res->size() != input_rows_count)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Output size is different from input size, expect {}, get {}", input_rows_count, res->size());

        return res;
    }

private:
    [[nodiscard]] ColumnPtr createResultColumn(size_t size, bool value) const
    {
        auto result = ColumnUInt8::create(size, static_cast<UInt8>(value));
        if (!in_null_as_unknown)
            return result;

        return ColumnNullable::create(std::move(result), ColumnUInt8::create(size, UInt8{0}));
    }

    [[nodiscard]] ColumnPtr applyStandardNullSemantics(const IColumn & membership, const IColumn & left, bool set_has_null) const
    {
        const size_t size = membership.size();
        auto result = ColumnUInt8::create(size);
        auto null_map = ColumnUInt8::create(size);
        auto & result_data = result->getData();
        auto & null_map_data = null_map->getData();

        for (size_t row = 0; row < size; ++row)
        {
            const bool left_is_null = left.isNullAt(row);
            const bool is_member = membership.getBool(row);
            const bool is_unknown = left_is_null || (!is_member && set_has_null);

            null_map_data[row] = is_unknown;
            result_data[row] = negative ? !is_member : is_member;
        }

        return ColumnNullable::create(std::move(result), std::move(null_map));
    }

    /// The set argument arrives either bare or wrapped in a ColumnConst.
    static const ColumnSet * tryGetColumnSet(const ColumnPtr & column)
    {
        if (const auto * column_set = checkAndGetColumnConstData<const ColumnSet>(column.get()))
            return column_set;
        return checkAndGetColumn<const ColumnSet>(column.get());
    }

    /// Emptiness is a property of the plan only for a tuple set, which is finished in its constructor
    /// and never mutated afterwards. Under standard semantics, use the original input size because
    /// type conversion can discard values. A storage-backed set is the live set of an `ENGINE = Set`
    /// table and grows on INSERT, and a subquery set is still consulted for index analysis.
    [[nodiscard]] bool isImmutableEmptySet(const FutureSet & future_set) const
    {
        const auto * tuple_set = typeid_cast<const FutureSetFromTuple *>(&future_set);
        if (!tuple_set)
            return false;

        return in_null_as_unknown
            ? tuple_set->isSourceEmpty()
            : tuple_set->get()->getTotalRowCount() == 0;
    }

    /// A tuple set is reported as constant by the DAG node too, and a subquery set holds no value while
    /// step headers are being declared. A storage-backed set is readable straight away and is not folded,
    /// so a constant here would land in a declared header that the stream cannot fill.
    static bool canReportEmptySetAsConstant(const FutureSet & future_set)
    {
        return typeid_cast<const FutureSetFromTuple *>(&future_set)
            || typeid_cast<const FutureSetFromSubquery *>(&future_set);
    }

    String function_name;
    bool negative;
    bool null_is_skipped;
    bool ignore_set;
    bool in_null_as_unknown;
};

void registerFunctionsInImpl(FunctionFactory & factory, bool ignore_set)
{
    const String suffix = ignore_set ? "IgnoreSet" : "";
    static constexpr auto ignore_set_description_suffix = " This is the IgnoreSet variant used for type analysis without creating the set.";

    /// in
    FunctionDocumentation::Description description_in = R"(
Checks if the left operand is a member of the right operand set. Returns 1 if it is, 0 otherwise. NULL values in the left operand are skipped (treated as not in the set).
    )";
    FunctionDocumentation::Syntax syntax_in = "in(x, set)";
    FunctionDocumentation::Arguments arguments_in = {{"x", "The value to check.", {}}, {"set", "The set of values.", {}}};
    FunctionDocumentation::ReturnedValue returned_value_in = {"Returns 1 if x is in the set, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_in = {{"Basic usage", "SELECT 1 IN (1, 2, 3)", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in_in = {1, 1};
    FunctionDocumentation::Category category_in = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_in = {description_in, syntax_in, arguments_in, {}, returned_value_in, examples_in, introduced_in_in, category_in};
    if (ignore_set)
        documentation_in.description = String(documentation_in.description) + ignore_set_description_suffix;
    String full_name_in = "in";
    full_name_in += suffix;
    factory.registerFunction(full_name_in, [ignore_set, n = full_name_in](ContextPtr context)
    {
        return FunctionIn::create(n, false, true, ignore_set, context && context->getSettingsRef()[Setting::in_null_as_unknown]);
    }, std::move(documentation_in));

    /// globalIn
    FunctionDocumentation::Description description_globalIn = R"(
Same as `in`, but uses global set distribution in distributed queries. The set is sent to all remote servers.
    )";
    FunctionDocumentation::Syntax syntax_globalIn = "globalIn(x, set)";
    FunctionDocumentation::Arguments arguments_globalIn = {{"x", "The value to check.", {}}, {"set", "The set of values.", {}}};
    FunctionDocumentation::ReturnedValue returned_value_globalIn = {"Returns 1 if x is in the set, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_globalIn = {{"Basic usage", "SELECT 1 IN (1, 2, 3)", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in_globalIn = {1, 1};
    FunctionDocumentation::Category category_globalIn = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_globalIn = {description_globalIn, syntax_globalIn, arguments_globalIn, {}, returned_value_globalIn, examples_globalIn, introduced_in_globalIn, category_globalIn};
    if (ignore_set)
        documentation_globalIn.description = String(documentation_globalIn.description) + ignore_set_description_suffix;
    String full_name_globalIn = "globalIn";
    full_name_globalIn += suffix;
    factory.registerFunction(full_name_globalIn, [ignore_set, n = full_name_globalIn](ContextPtr context)
    {
        return FunctionIn::create(n, false, true, ignore_set, context && context->getSettingsRef()[Setting::in_null_as_unknown]);
    }, std::move(documentation_globalIn));

    /// notIn
    FunctionDocumentation::Description description_notIn = R"(
Checks if the left operand is NOT a member of the right operand set. Returns 1 if it is not in the set, 0 otherwise. NULL values in the left operand are skipped.
    )";
    FunctionDocumentation::Syntax syntax_notIn = "notIn(x, set)";
    FunctionDocumentation::Arguments arguments_notIn = {{"x", "The value to check.", {}}, {"set", "The set of values.", {}}};
    FunctionDocumentation::ReturnedValue returned_value_notIn = {"Returns 1 if x is not in the set, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_notIn = {{"Basic usage", "SELECT 4 NOT IN (1, 2, 3)", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in_notIn = {1, 1};
    FunctionDocumentation::Category category_notIn = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_notIn = {description_notIn, syntax_notIn, arguments_notIn, {}, returned_value_notIn, examples_notIn, introduced_in_notIn, category_notIn};
    if (ignore_set)
        documentation_notIn.description = String(documentation_notIn.description) + ignore_set_description_suffix;
    String full_name_notIn = "notIn";
    full_name_notIn += suffix;
    factory.registerFunction(full_name_notIn, [ignore_set, n = full_name_notIn](ContextPtr context)
    {
        return FunctionIn::create(n, true, true, ignore_set, context && context->getSettingsRef()[Setting::in_null_as_unknown]);
    }, std::move(documentation_notIn));

    /// globalNotIn
    FunctionDocumentation::Description description_globalNotIn = R"(
Same as `notIn`, but uses global set distribution in distributed queries. The set is sent to all remote servers.
    )";
    FunctionDocumentation::Syntax syntax_globalNotIn = "globalNotIn(x, set)";
    FunctionDocumentation::Arguments arguments_globalNotIn = {{"x", "The value to check.", {}}, {"set", "The set of values.", {}}};
    FunctionDocumentation::ReturnedValue returned_value_globalNotIn = {"Returns 1 if x is not in the set, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_globalNotIn = {{"Basic usage", "SELECT 4 NOT IN (1, 2, 3)", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in_globalNotIn = {1, 1};
    FunctionDocumentation::Category category_globalNotIn = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_globalNotIn = {description_globalNotIn, syntax_globalNotIn, arguments_globalNotIn, {}, returned_value_globalNotIn, examples_globalNotIn, introduced_in_globalNotIn, category_globalNotIn};
    if (ignore_set)
        documentation_globalNotIn.description = String(documentation_globalNotIn.description) + ignore_set_description_suffix;
    String full_name_globalNotIn = "globalNotIn";
    full_name_globalNotIn += suffix;
    factory.registerFunction(full_name_globalNotIn, [ignore_set, n = full_name_globalNotIn](ContextPtr context)
    {
        return FunctionIn::create(n, true, true, ignore_set, context && context->getSettingsRef()[Setting::in_null_as_unknown]);
    }, std::move(documentation_globalNotIn));

    /// nullIn
    FunctionDocumentation::Description description_nullIn = R"(
Checks if the left operand is a member of the right operand set. Unlike `in`, NULL values are not skipped: NULL is compared with set elements, and NULL = NULL evaluates to true.
    )";
    FunctionDocumentation::Syntax syntax_nullIn = "nullIn(x, set)";
    FunctionDocumentation::Arguments arguments_nullIn = {{"x", "The value to check.", {}}, {"set", "The set of values.", {}}};
    FunctionDocumentation::ReturnedValue returned_value_nullIn = {"Returns 1 if x is in the set, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_nullIn = {{"Basic usage", "SELECT nullIn(NULL, tuple(1, NULL))", "0"}};
    FunctionDocumentation::IntroducedIn introduced_in_nullIn = {1, 1};
    FunctionDocumentation::Category category_nullIn = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_nullIn = {description_nullIn, syntax_nullIn, arguments_nullIn, {}, returned_value_nullIn, examples_nullIn, introduced_in_nullIn, category_nullIn};
    if (ignore_set)
        documentation_nullIn.description = String(documentation_nullIn.description) + ignore_set_description_suffix;
    String full_name_nullIn = "nullIn";
    full_name_nullIn += suffix;
    factory.registerFunction(full_name_nullIn, [ignore_set, n = full_name_nullIn](ContextPtr)
    {
        return FunctionIn::create(n, false, false, ignore_set, false);
    }, std::move(documentation_nullIn));

    /// globalNullIn
    FunctionDocumentation::Description description_globalNullIn = R"(
Same as `nullIn`, but uses global set distribution in distributed queries. The set is sent to all remote servers.
    )";
    FunctionDocumentation::Syntax syntax_globalNullIn = "globalNullIn(x, set)";
    FunctionDocumentation::Arguments arguments_globalNullIn = {{"x", "The value to check.", {}}, {"set", "The set of values.", {}}};
    FunctionDocumentation::ReturnedValue returned_value_globalNullIn = {"Returns 1 if x is in the set, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_globalNullIn = {{"Basic usage", "SELECT nullIn(NULL, tuple(1, NULL))", "0"}};
    FunctionDocumentation::IntroducedIn introduced_in_globalNulllIn = {1, 1};
    FunctionDocumentation::Category category_globalNullIn = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_globalNullIn = {description_globalNullIn, syntax_globalNullIn, arguments_globalNullIn, {}, returned_value_globalNullIn, examples_globalNullIn, introduced_in_globalNulllIn, category_globalNullIn};
    if (ignore_set)
        documentation_globalNullIn.description = String(documentation_globalNullIn.description) + ignore_set_description_suffix;
    String full_name_globalNullIn = "globalNullIn";
    full_name_globalNullIn += suffix;
    factory.registerFunction(full_name_globalNullIn, [ignore_set, n = full_name_globalNullIn](ContextPtr)
    {
        return FunctionIn::create(n, false, false, ignore_set, false);
    }, std::move(documentation_globalNullIn));

    /// notNullIn
    FunctionDocumentation::Description description_notNullIn = R"(
Checks if the left operand is NOT a member of the right operand set. Unlike `notIn`, NULL values are not skipped: NULL is compared with set elements, and NULL = NULL evaluates to true.
    )";
    FunctionDocumentation::Syntax syntax_notNullIn = "notNullIn(x, set)";
    FunctionDocumentation::Arguments arguments_notNullIn = {{"x", "The value to check.", {}}, {"set", "The set of values.", {}}};
    FunctionDocumentation::ReturnedValue returned_value_notNullIn = {"Returns 1 if x is not in the set, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_notNullIn = {{"Basic usage", "SELECT notNullIn(NULL, tuple(1, NULL))", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in_notNulllIn = {1, 1};
    FunctionDocumentation::Category category_notNullIn = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_notNullIn = {description_notNullIn, syntax_notNullIn, arguments_notNullIn, {}, returned_value_notNullIn, examples_notNullIn, introduced_in_notNulllIn, category_notNullIn};
    if (ignore_set)
        documentation_notNullIn.description = String(documentation_notNullIn.description) + ignore_set_description_suffix;
    String full_name_notNullIn = "notNullIn";
    full_name_notNullIn += suffix;
    factory.registerFunction(full_name_notNullIn, [ignore_set, n = full_name_notNullIn](ContextPtr)
    {
        return FunctionIn::create(n, true, false, ignore_set, false);
    }, std::move(documentation_notNullIn));

    /// globalNotNullIn
    FunctionDocumentation::Description description_globalNotNullIn = R"(
Same as `notNullIn`, but uses global set distribution in distributed queries. The set is sent to all remote servers.
    )";
    FunctionDocumentation::Syntax syntax_globalNotNullIn = "globalNotNullIn(x, set)";
    FunctionDocumentation::Arguments arguments_globalNotNullIn = {{"x", "The value to check.", {}}, {"set", "The set of values.", {}}};
    FunctionDocumentation::ReturnedValue returned_value_globalNotNullIn = {"Returns 1 if x is not in the set, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_globalNotNullIn = {{"Basic usage", "SELECT notNullIn(NULL, tuple(1, NULL))", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in_globalNotNulllIn = {1, 1};
    FunctionDocumentation::Category category_globalNotNullIn = FunctionDocumentation::Category::Comparison;
    FunctionDocumentation documentation_globalNotNullIn = {description_globalNotNullIn, syntax_globalNotNullIn, arguments_globalNotNullIn, {}, returned_value_globalNotNullIn, examples_globalNotNullIn, introduced_in_globalNotNulllIn, category_globalNotNullIn};
    if (ignore_set)
        documentation_globalNotNullIn.description = String(documentation_globalNotNullIn.description) + ignore_set_description_suffix;
    String full_name_globalNotNullIn = "globalNotNullIn";
    full_name_globalNotNullIn += suffix;
    factory.registerFunction(full_name_globalNotNullIn, [ignore_set, n = full_name_globalNotNullIn](ContextPtr)
    {
        return FunctionIn::create(n, true, false, ignore_set, false);
    }, std::move(documentation_globalNotNullIn));
}

}

REGISTER_FUNCTION(In)
{
    registerFunctionsInImpl(factory, false);
    registerFunctionsInImpl(factory, true);
    factory.registerFunction<FunctionScalarCarrierValueEqualsForIn>(FunctionDocumentation::INTERNAL_FUNCTION_DOCS);
    factory.registerFunction<FunctionMapValueEqualsForIn>(FunctionDocumentation::INTERNAL_FUNCTION_DOCS);
    factory.registerFunction<FunctionArrayValueEqualsForIn>(FunctionDocumentation::INTERNAL_FUNCTION_DOCS);
}

}
