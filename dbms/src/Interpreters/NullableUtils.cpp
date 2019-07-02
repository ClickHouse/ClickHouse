#include <Interpreters/NullableUtils.h>


namespace DB
{

void extractNestedColumnsAndNullMap(ColumnRawPtrs & key_columns, ColumnPtr & null_map_holder, ConstNullMapPtr & null_map)
{
    if (key_columns.size() == 1)
    {
        auto & column = key_columns[0];
        if (auto * column_nullable = checkAndGetColumn<ColumnNullable>(*column))
        {
            null_map = &column_nullable->getNullMapData();
            column = &column_nullable->getNestedColumn();
        }
    }
    else
    {
        for (auto & column : key_columns)
        {
            if (auto * column_nullable = checkAndGetColumn<ColumnNullable>(*column))
            {
                column = &column_nullable->getNestedColumn();

                if (!null_map_holder)
                {
                    null_map_holder = column_nullable->getNullMapColumnPtr();
                }
                else
                {
                    MutableColumnPtr mutable_null_map_holder = (*std::move(null_map_holder)).mutate();

                    PaddedPODArray<UInt8> & mutable_null_map = static_cast<ColumnUInt8 &>(*mutable_null_map_holder).getData();
                    const PaddedPODArray<UInt8> & other_null_map = column_nullable->getNullMapData();
                    for (size_t i = 0, size = mutable_null_map.size(); i < size; ++i)
                        mutable_null_map[i] |= other_null_map[i];

                    null_map_holder = std::move(mutable_null_map_holder);
                }
            }
        }

        null_map = null_map_holder ? &static_cast<const ColumnUInt8 &>(*null_map_holder).getData() : nullptr;
    }
}

}
