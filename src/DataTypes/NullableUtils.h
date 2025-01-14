#pragma once

#include <Columns/ColumnNullable.h>
#include <DataTypes/Serializations/ISerialization.h>


namespace DB
{

/** Replace Nullable key_columns to corresponding nested columns.
  * In 'null_map' return a map of positions where at least one column was NULL.
  * @returns ownership column of null_map.
  */
ColumnPtr extractNestedColumnsAndNullMap(ColumnRawPtrs & key_columns, ConstNullMapPtr & null_map);

struct NullableSubcolumnCreator : public ISerialization::ISubcolumnCreator
{
    const ColumnPtr null_map;

    explicit NullableSubcolumnCreator(const ColumnPtr & null_map_) : null_map(null_map_) {}

    DataTypePtr create(const DataTypePtr & prev) const override;
    SerializationPtr create(const SerializationPtr & prev_serialization, const DataTypePtr & prev_type) const override;
    ColumnPtr create(const ColumnPtr & prev) const override;
};

}
