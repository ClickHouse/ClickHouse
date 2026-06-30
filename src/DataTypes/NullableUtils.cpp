#include <Columns/ColumnArray.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/DataTypeArray.h>
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
extern const SettingsBool allow_experimental_nullable_array_type;
}

static bool isNullableTupleInExtractedSubcolumnsEnabledByGlobalSetting()
{
    auto context = Context::getGlobalContextInstance();
    return context && context->getSettingsRef()[Setting::allow_nullable_tuple_in_extracted_subcolumns];
}

bool allowNullableArrayType(const Settings & settings)
{
    return settings[Setting::allow_experimental_nullable_array_type];
}

bool hasNullableArray(const DataTypePtr & type)
{
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        if (isArray(nullable_type->getNestedType()))
            return true;
    }

    bool res = false;
    type->forEachChild([&res](const IDataType & child)
    {
        if (res)
            return;

        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(&child))
        {
            if (isArray(nullable_type->getNestedType()))
            {
                res = true;
                return;
            }
        }
    });

    return res;
}

bool canBeInsideNullableWithSettings(const IDataType & type, const Settings & settings)
{
    if (isArray(type))
        return allowNullableArrayType(settings);
    return type.canBeInsideNullable();
}

bool canBeInsideNullableWithSettings(const DataTypePtr & type, const Settings & settings)
{
    return canBeInsideNullableWithSettings(*type, settings);
}

static bool canExtractedSubcolumnsBeInsideNullable(const ColumnPtr & column)
{
    if (checkAndGetColumn<ColumnTuple>(column.get()))
        return isNullableTupleInExtractedSubcolumnsEnabledByGlobalSetting();

    /// `Nullable(Array)` is allowed only with `allow_experimental_nullable_array_type`, but Dynamic subcolumns
    /// like `Array(T).null` must stay illegal — see `canExtractedSubcolumnsBeInsideNullable` below.
    if (checkAndGetColumn<ColumnArray>(column.get()))
        return false;

    return column->canBeInsideNullable();
}

bool canExtractedSubcolumnsBeInsideNullable(const DataTypePtr & type)
{
    if (isTuple(type))
        return isNullableTupleInExtractedSubcolumnsEnabledByGlobalSetting();

    /// Not the same as `IDataType::canBeInsideNullable()`.
    /// `Array` / `Map` may appear inside `Nullable(Array)` / `Nullable(Map)` when enabled by settings,
    /// yet bare `Array(T).null` / `Map(...).null` are not valid Dynamic subcolumn paths: the null map
    /// belongs to the `Nullable(...)` wrapper, not to the compound type name parsed from `getSubcolumn`.
    /// Without this check, `getSubcolumn(42::Dynamic, 'Array(UInt64).null')` would succeed with an all-ones null map.
    if (isArray(type) || isMap(type))
        return false;

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


void applyParentNullMapToExtractedSubcolumn(
    const MutableColumnPtr & column, const NullMap & parent_null_map, size_t column_offset, size_t parent_null_map_offset)
{
    chassert(column_offset <= column->size());
    const size_t length = column->size() - column_offset;
    chassert(parent_null_map_offset + length <= parent_null_map.size());

    /// When no row of the range is NULL in the parent, the subcolumn already holds the correct values and
    /// nothing needs to be marked NULL.
    if (memoryIsZero(parent_null_map.data(), parent_null_map_offset, parent_null_map_offset + length))
        return;

    /// Build a keep-mask that covers only the applied range: zero for the rows that are NULL in the parent
    /// and one elsewhere.
    IColumn::Filter keep_mask(length);
    for (size_t i = 0; i < length; ++i)
        keep_mask[i] = !parent_null_map[parent_null_map_offset + i];

    if (auto * nullable = typeid_cast<ColumnNullable *>(column.get()))
    {
        nullable->applyNegatedNullMap(keep_mask, column_offset);
        return;
    }

    if (auto * variant = typeid_cast<ColumnVariant *>(column.get()))
    {
        variant->applyNegatedNullMap(keep_mask, column_offset);
        return;
    }

    if (auto * dynamic = typeid_cast<ColumnDynamic *>(column.get()))
    {
        dynamic->applyNegatedNullMap(keep_mask, column_offset);
        return;
    }

    if (auto * low_cardinality = typeid_cast<ColumnLowCardinality *>(column.get()))
    {
        low_cardinality->applyNegatedNullMap(keep_mask, column_offset);
        return;
    }

    throw Exception(
        ErrorCodes::LOGICAL_ERROR, "Cannot apply the parent null map to subcolumn {} that cannot represent NULL values", column->getName());
}


DataTypePtr NullableSubcolumnCreator::create(const DataTypePtr & prev) const
{
    if (isArray(prev))
        return makeNullableAllowingArray(prev);
    if (!canExtractedSubcolumnsBeInsideNullable(prev))
        return prev;
    return makeNullableSafe(prev);
}

SerializationPtr NullableSubcolumnCreator::create(const SerializationPtr & prev_serialization, const DataTypePtr & prev_type) const
{
    if (prev_type && isArray(prev_type))
        return SerializationNullable::create(prev_serialization);
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
    if (checkAndGetColumn<ColumnArray>(prev.get()))
        return ColumnNullable::create(prev, null_map);
    if (canExtractedSubcolumnsBeInsideNullable(prev))
        return ColumnNullable::create(prev, null_map);

    /// The extracted subcolumn cannot be wrapped into Nullable, but if it can represent NULL itself,
    /// mark rows that are NULL in the outer column as NULL in it.
    if (null_map && canContainNull(*prev))
    {
        const auto & outer_null_map_data = assert_cast<const ColumnUInt8 &>(*null_map).getData();
        auto mutable_column = IColumn::mutate(prev);
        applyParentNullMapToExtractedSubcolumn(mutable_column, outer_null_map_data, 0, 0);
        return mutable_column;
    }

    return prev;
}

}
