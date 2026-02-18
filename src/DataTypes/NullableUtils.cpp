#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NullableUtils.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <Core/ServerSettings.h>
#include <Interpreters/Context.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ServerSetting
{
extern const ServerSettingsBool allow_nullable_tuple_in_extracted_subcolumns;
}

static bool isNullableTupleEnabledByServerSetting()
{
    auto context = Context::getGlobalContextInstance();
    return context && context->getServerSettings()[ServerSetting::allow_nullable_tuple_in_extracted_subcolumns];
}

static bool canBeInsideNullableBySettings(const ColumnPtr & column)
{
    if (checkAndGetColumn<ColumnTuple>(column.get()))
        return isNullableTupleEnabledByServerSetting();

    return column->canBeInsideNullable();
}

bool canBeInsideNullableBySettings(const DataTypePtr & type)
{
    if (isTuple(type))
        return isNullableTupleEnabledByServerSetting();

    return type->canBeInsideNullable();
}

bool canBeInsideNullableOrLowCardinalityNullableBySettings(const DataTypePtr & type)
{
    return canBeInsideNullableBySettings(removeLowCardinality(type));
}

DataTypePtr makeNullableOrLowCardinalityNullableSafeBySettings(const DataTypePtr & type)
{
    if (!canBeInsideNullableOrLowCardinalityNullableBySettings(type))
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
    if (!canBeInsideNullableBySettings(prev))
        return prev;
    return makeNullableSafe(prev);
}

SerializationPtr NullableSubcolumnCreator::create(const SerializationPtr & prev_serialization, const DataTypePtr & prev_type) const
{
    if (prev_type && !canBeInsideNullableBySettings(prev_type))
        return prev_serialization;
    return std::make_shared<SerializationNullable>(prev_serialization);
}

ColumnPtr NullableSubcolumnCreator::create(const ColumnPtr & prev) const
{
    if (canBeInsideNullableBySettings(prev))
        return ColumnNullable::create(prev, null_map);
    return prev;
}

}
