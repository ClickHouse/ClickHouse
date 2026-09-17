#include <Columns/validateColumnType.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesDecimal.h>

#include <optional>

namespace DB
{

namespace
{

/// A Decimal/DateTime64/Time64 column carries its scale in the column object, but
/// getDataType() reports only the physical TypeIndex (Decimal32/64/128/256), which is the
/// same for every scale. So the top-level TypeIndex check passes a scale-7 column against a
/// DateTime64(3) type. Compare the column's own scale against the type's declared scale.
bool decimalScaleMatches(const IColumn & col, const IDataType & type)
{
    auto type_scale = tryGetDecimalScale(type);
    if (!type_scale)
        return true; /// Not a decimal-backed type.

    std::optional<UInt32> col_scale;
    if (const auto * c32 = typeid_cast<const ColumnDecimal<Decimal32> *>(&col))
        col_scale = c32->getScale();
    else if (const auto * c64 = typeid_cast<const ColumnDecimal<Decimal64> *>(&col))
        col_scale = c64->getScale();
    else if (const auto * c128 = typeid_cast<const ColumnDecimal<Decimal128> *>(&col))
        col_scale = c128->getScale();
    else if (const auto * c256 = typeid_cast<const ColumnDecimal<Decimal256> *>(&col))
        col_scale = c256->getScale();
    else if (const auto * cdt = typeid_cast<const ColumnDecimal<DateTime64> *>(&col))
        col_scale = cdt->getScale();
    else if (const auto * ctime = typeid_cast<const ColumnDecimal<Time64> *>(&col))
        col_scale = ctime->getScale();

    /// TypeIndex already matched, so a non-ColumnDecimal here should not happen; do not over-reject.
    if (!col_scale)
        return true;

    return *col_scale == *type_scale;
}

}

bool columnMatchesType(const IColumn & column, const IDataType & type, bool strict_decimal_scale)
{
    const IColumn * col = &column;

    /// Strip wrappers that don't change the logical type.
    if (const auto * col_const = typeid_cast<const ColumnConst *>(col))
        col = &col_const->getDataColumn();
    if (const auto * col_sparse = typeid_cast<const ColumnSparse *>(col))
        col = &col_sparse->getValuesColumn();

    if (col->getDataType() != type.getColumnType())
        return false;

    if (strict_decimal_scale && !decimalScaleMatches(*col, type))
        return false;

    if (const auto * col_array = typeid_cast<const ColumnArray *>(col))
    {
        if (const auto * type_array = typeid_cast<const DataTypeArray *>(&type))
            return columnMatchesType(col_array->getData(), *type_array->getNestedType(), strict_decimal_scale);
        return false;
    }

    if (const auto * col_nullable = typeid_cast<const ColumnNullable *>(col))
    {
        if (const auto * type_nullable = typeid_cast<const DataTypeNullable *>(&type))
            return columnMatchesType(col_nullable->getNestedColumn(), *type_nullable->getNestedType(), strict_decimal_scale);
        return false;
    }

    if (const auto * col_tuple = typeid_cast<const ColumnTuple *>(col))
    {
        if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(&type))
        {
            const auto & type_elements = type_tuple->getElements();
            if (col_tuple->tupleSize() != type_elements.size())
                return false;
            for (size_t i = 0; i < col_tuple->tupleSize(); ++i)
                if (!columnMatchesType(col_tuple->getColumn(i), *type_elements[i], strict_decimal_scale))
                    return false;
            return true;
        }
        return false;
    }

    if (const auto * col_map = typeid_cast<const ColumnMap *>(col))
    {
        if (const auto * type_map = typeid_cast<const DataTypeMap *>(&type))
            return columnMatchesType(col_map->getNestedColumn(), *type_map->getNestedType(), strict_decimal_scale);
        return false;
    }

    if (const auto * col_lc = typeid_cast<const ColumnLowCardinality *>(col))
    {
        if (const auto * type_lc = typeid_cast<const DataTypeLowCardinality *>(&type))
            return columnMatchesType(*col_lc->getDictionary().getNestedColumn(), *type_lc->getDictionaryType(), strict_decimal_scale);
        return false;
    }

    if (const auto * col_variant = typeid_cast<const ColumnVariant *>(col))
    {
        if (const auto * type_variant = typeid_cast<const DataTypeVariant *>(&type))
        {
            const auto & type_variants = type_variant->getVariants();
            if (col_variant->getVariants().size() != type_variants.size())
                return false;
            for (size_t i = 0; i < type_variants.size(); ++i)
                if (!columnMatchesType(col_variant->getVariantByGlobalDiscriminator(i), *type_variants[i], strict_decimal_scale))
                    return false;
            return true;
        }
        return false;
    }

    return true;
}

}
