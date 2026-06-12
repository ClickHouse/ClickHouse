#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NullableUtils.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/Serializations/SerializationNullableWithParentNullMap.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Setting
{
extern const SettingsBool allow_nullable_tuple_in_extracted_subcolumns;
}

static bool isNullableTupleInExtractedSubcolumnsEnabledByGlobalSetting()
{
    auto context = Context::getGlobalContextInstance();
    return context && context->getSettingsRef()[Setting::allow_nullable_tuple_in_extracted_subcolumns];
}

static bool canExtractedSubcolumnsBeInsideNullable(const ColumnPtr & column)
{
    if (checkAndGetColumn<ColumnTuple>(column.get()))
        return isNullableTupleInExtractedSubcolumnsEnabledByGlobalSetting();

    return column->canBeInsideNullable();
}

bool canExtractedSubcolumnsBeInsideNullable(const DataTypePtr & type)
{
    if (isTuple(type))
        return isNullableTupleInExtractedSubcolumnsEnabledByGlobalSetting();

    return type->canBeInsideNullable();
}

bool canExtractedSubcolumnsBeInsideNullableOrLowCardinalityNullable(const DataTypePtr & type)
{
    return canExtractedSubcolumnsBeInsideNullable(removeLowCardinality(type));
}

DataTypePtr makeExtractedSubcolumnsNullableOrLowCardinalityNullableSafe(const DataTypePtr & type)
{
    if (!canExtractedSubcolumnsBeInsideNullableOrLowCardinalityNullable(type))
        return type;

    return makeNullableOrLowCardinalityNullableSafe(type);
}

ColumnPtr extractNestedColumnsAndNullMap(ColumnRawPtrs & key_columns, ConstNullMapPtr & null_map)
{
    ColumnPtr null_map_holder;

    auto addNullMap = [&](const ColumnNullable * column_nullable)
    {
        if (!null_map_holder)
        {
            /// First nullable column: just take its null map as the base
            null_map_holder = column_nullable->getNullMapColumnPtr();
        }
        else
        {
            /// Subsequent nullable columns: OR their null maps into the accumulated one
            MutableColumnPtr mutable_null_map_holder = IColumn::mutate(std::move(null_map_holder));

            PaddedPODArray<UInt8> & mutable_null_map = assert_cast<ColumnUInt8 &>(*mutable_null_map_holder).getData();
            const PaddedPODArray<UInt8> & other_null_map = column_nullable->getNullMapData();

            for (size_t i = 0, size = mutable_null_map.size(); i < size; ++i)
                mutable_null_map[i] |= other_null_map[i];

            null_map_holder = std::move(mutable_null_map_holder);
        }
    };

    for (auto & column : key_columns)
    {
        if (const auto * column_nullable = checkAndGetColumn<ColumnNullable>(&*column))
        {
            /// Top-level Nullable(...) always contributes to the combined null map
            addNullMap(column_nullable);

            const IColumn * nested_column = &column_nullable->getNestedColumn();
            column = nested_column;

            /// Special case: Nullable(Tuple(...))
            /// If the nested column is a tuple, also fold in null maps of nullable tuple elements
            if (const auto * tuple = checkAndGetColumn<ColumnTuple>(nested_column))
            {
                const auto & tuple_columns = tuple->getColumns();
                for (const auto & element : tuple_columns)
                {
                    if (const auto * elem_nullable = checkAndGetColumn<ColumnNullable>(element.get()))
                    {
                        addNullMap(elem_nullable);
                    }
                }
            }
        }
    }

    null_map = null_map_holder ? &assert_cast<const ColumnUInt8 &>(*null_map_holder).getData() : nullptr;

    return null_map_holder;
}


ColumnPtr applyParentNullMapToExtractedSubcolumn(
    ColumnPtr column, const NullMap & parent_null_map, size_t column_offset, size_t parent_null_map_offset, size_t length)
{
    chassert(column_offset + length <= column->size());

    /// The helpers used below require a mask of the full column size. The mask is zero for the rows of
    /// the applied range that are NULL in the parent and one elsewhere, so rows outside the range are
    /// left intact.
    IColumn::Filter keep_mask(column->size(), 1);
    for (size_t i = 0; i < length; ++i)
        keep_mask[column_offset + i] = !parent_null_map[parent_null_map_offset + i];

    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(column.get()))
    {
        auto res = ColumnNullable::create(nullable->getNestedColumnPtr(), IColumn::mutate(nullable->getNullMapColumnPtr()));
        assert_cast<ColumnNullable &>(res->assumeMutableRef()).applyNegatedNullMap(keep_mask);
        return res;
    }

    if (checkAndGetColumn<ColumnVariant>(column.get()) || checkAndGetColumn<ColumnDynamic>(column.get()))
    {
        auto mutable_column = IColumn::mutate(std::move(column));

        if (auto * variant = typeid_cast<ColumnVariant *>(mutable_column.get()))
            variant->applyNegatedNullMap(keep_mask);
        else
            assert_cast<ColumnDynamic &>(*mutable_column).applyNegatedNullMap(keep_mask);

        return mutable_column;
    }

    if (const auto * low_cardinality = checkAndGetColumn<ColumnLowCardinality>(column.get()))
    {
        if (!low_cardinality->nestedIsNullable())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot apply the parent null map to LowCardinality subcolumn {} with a non-nullable dictionary",
                column->getName());

        /// NULL is represented by the index of the null value in the dictionary, which is always 0 for a
        /// nullable dictionary. Filter out the indexes at the rows that are NULL in the parent and expand
        /// the indexes column back: expanding fills the removed positions with zeros, i.e. with NULL.
        /// Only the indexes are rewritten; the dictionary is shared unchanged.
        chassert(low_cardinality->getDictionary().getNullValueIndex() == 0);

        auto indexes = IColumn::mutate(low_cardinality->getIndexesPtr());
        indexes->filter(keep_mask);
        indexes->expand(keep_mask, false);

        return ColumnLowCardinality::create(low_cardinality->getDictionaryPtr(), std::move(indexes), low_cardinality->isSharedDictionary());
    }

    throw Exception(
        ErrorCodes::LOGICAL_ERROR, "Cannot apply the parent null map to subcolumn {} that cannot represent NULL values", column->getName());
}


DataTypePtr NullableSubcolumnCreator::create(const DataTypePtr & prev) const
{
    if (!canExtractedSubcolumnsBeInsideNullable(prev))
        return prev;
    return makeNullableSafe(prev);
}

SerializationPtr NullableSubcolumnCreator::create(const SerializationPtr & prev_serialization, const DataTypePtr & prev_type) const
{
    if (prev_type && !canExtractedSubcolumnsBeInsideNullable(prev_type))
    {
        /// The extracted subcolumn cannot be wrapped into Nullable, but some types can represent NULL
        /// themselves: Nullable (possibly inside LowCardinality), Dynamic and Variant. For them return a
        /// serialization that also reads the outer null map and marks the corresponding rows as NULL in
        /// the subcolumn's own null representation.
        if (canContainNull(*prev_type))
            return SerializationNullableWithParentNullMap::create(prev_serialization);
        return prev_serialization;
    }

    return SerializationNullable::create(prev_serialization);
}

ColumnPtr NullableSubcolumnCreator::create(const ColumnPtr & prev) const
{
    if (canExtractedSubcolumnsBeInsideNullable(prev))
        return ColumnNullable::create(prev, null_map);

    /// The extracted subcolumn cannot be wrapped into Nullable, but if it can represent NULL itself,
    /// mark rows that are NULL in the outer column as NULL in it.
    if (null_map && canContainNull(*prev))
    {
        const auto & outer_null_map_data = assert_cast<const ColumnUInt8 &>(*null_map).getData();
        return applyParentNullMapToExtractedSubcolumn(prev, outer_null_map_data, 0, 0, prev->size());
    }

    return prev;
}

}
