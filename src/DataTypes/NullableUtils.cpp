#include <DataTypes/NullableUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <Common/assert_cast.h>

namespace DB
{

ColumnPtr extractNestedColumnsAndNullMap(ColumnRawPtrs & key_columns, ConstNullMapPtr & null_map)
{
    ColumnPtr null_map_holder;

    if (key_columns.size() == 1)
    {
        auto & column = key_columns[0];
        if (const auto * column_nullable = checkAndGetColumn<ColumnNullable>(&*column))
        {
            null_map_holder = column_nullable->getNullMapColumnPtr();
            null_map = &column_nullable->getNullMapData();
            column = &column_nullable->getNestedColumn();
        }
    }
    else
    {
        for (auto & column : key_columns)
        {
            if (const auto * column_nullable = checkAndGetColumn<ColumnNullable>(&*column))
            {
                column = &column_nullable->getNestedColumn();

                if (!null_map_holder)
                {
                    null_map_holder = column_nullable->getNullMapColumnPtr();
                }
                else
                {
                    MutableColumnPtr mutable_null_map_holder = IColumn::mutate(std::move(null_map_holder));

                    PaddedPODArray<UInt8> & mutable_null_map = assert_cast<ColumnUInt8 &>(*mutable_null_map_holder).getData();
                    const PaddedPODArray<UInt8> & other_null_map = column_nullable->getNullMapData();
                    for (size_t i = 0, size = mutable_null_map.size(); i < size; ++i)
                        mutable_null_map[i] |= other_null_map[i];

                    null_map_holder = std::move(mutable_null_map_holder);
                }
            }
        }

        null_map = null_map_holder ? &assert_cast<const ColumnUInt8 &>(*null_map_holder).getData() : nullptr;
    }

    return null_map_holder;
}


DataTypePtr NullableSubcolumnCreator::create(const DataTypePtr & prev) const
{
    return makeNullableSafe(prev);
}

SerializationPtr NullableSubcolumnCreator::create(const SerializationPtr & prev_serialization, const DataTypePtr & prev_type) const
{
    if (prev_type && !prev_type->canBeInsideNullable())
        return prev_serialization;
    return std::make_shared<SerializationNullable>(prev_serialization);
}

ColumnPtr NullableSubcolumnCreator::create(const ColumnPtr & prev) const
{
    if (prev->canBeInsideNullable())
        return ColumnNullable::create(prev, null_map);
    return prev;
}

}
