#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NullableUtils.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/assert_cast.h>


namespace DB
{

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


DataTypePtr NullableSubcolumnCreator::create(const DataTypePtr & prev) const
{
    /// A non-nullable `LowCardinality(T)` element cannot be wrapped in `Nullable`, but it must still
    /// be able to represent the outer tuple's NULLs, so it is promoted to `LowCardinality(Nullable(T))`.
    /// For every other type this matches the previous `canBeInsideNullable ? makeNullableSafe : prev`.
    return makeExtractedSubcolumnsNullableOrLowCardinalityNullableSafe(prev);
}

SerializationPtr NullableSubcolumnCreator::create(const SerializationPtr & prev_serialization, const DataTypePtr & prev_type) const
{
    if (prev_type && !canExtractedSubcolumnsBeInsideNullable(prev_type))
        return prev_serialization;
    return SerializationNullable::create(prev_serialization);
}

ColumnPtr NullableSubcolumnCreator::create(const ColumnPtr & prev) const
{
    if (canExtractedSubcolumnsBeInsideNullable(prev))
        return ColumnNullable::create(prev, null_map);

    /// `prev` already carries its own nulls and cannot be wrapped again, but the outer null map
    /// must still be folded into the element's own nulls (see commit message for the symptom).
    if (null_map)
    {
        const auto & outer_null_data = assert_cast<const ColumnUInt8 &>(*null_map).getData();

        if (const auto * prev_nullable = checkAndGetColumn<ColumnNullable>(prev.get()))
        {
            const auto & inner_null_data = prev_nullable->getNullMapData();
            chassert(inner_null_data.size() == outer_null_data.size());

            auto combined_null_map = ColumnUInt8::create(inner_null_data.size());
            auto & combined_data = combined_null_map->getData();
            for (size_t i = 0; i < inner_null_data.size(); ++i)
                combined_data[i] = inner_null_data[i] | outer_null_data[i];

            return ColumnNullable::create(prev_nullable->getNestedColumnPtr(), std::move(combined_null_map));
        }

        /// A `LowCardinality(...)` element carries nullability in its dictionary, not a
        /// `ColumnNullable` wrapper, so it reaches here unchanged. Mirror what `create(DataTypePtr)`
        /// returns: fold the outer mask in by repointing outer-`NULL` rows at the dictionary's null
        /// index, promoting a non-nullable dictionary to `Nullable` first so it has a null index.
        if (const auto * prev_lc = checkAndGetColumn<ColumnLowCardinality>(prev.get()))
        {
            if (prev_lc->nestedIsNullable())
                return prev_lc->applyExternalNullMap(outer_null_data);

            auto lc_nullable = prev_lc->cloneNullable();
            return assert_cast<const ColumnLowCardinality &>(*lc_nullable).applyExternalNullMap(outer_null_data);
        }
    }

    return prev;
}

}
