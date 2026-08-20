#pragma once

#include <Columns/ColumnNullable.h>
#include <Core/Types.h>
#include <DataTypes/Serializations/ISerialization.h>


namespace DB
{

/** Replace Nullable key_columns to corresponding nested columns.
  * In 'null_map' return a map of positions where at least one column was NULL.
  * @returns ownership column of null_map.
  */
ColumnPtr extractNestedColumnsAndNullMap(ColumnRawPtrs & key_columns, ConstNullMapPtr & null_map);

/** Returns whether `type` can be wrapped into `Nullable(...)` with current
  * `allow_nullable_tuple_in_extracted_subcolumns` setting value from global context.
  * Config changes for global context settings are applied after server restart.
  * For non-tuple types this matches `IDataType::canBeInsideNullable()`.
  */
bool canExtractedSubcolumnsBeInsideNullable(const DataTypePtr & type);

/** Same check as `canExtractedSubcolumnsBeInsideNullable()`, but for
  * `LowCardinality(T)` checks whether nested `T` can be nullable by
  * settings, i.e. whether wrapping into `LowCardinality(Nullable(T))` is
  * possible.
  */
bool canExtractedSubcolumnsBeInsideNullableOrLowCardinalityNullable(const DataTypePtr & type);

/** Wraps `type` into `Nullable(...)` or `LowCardinality(Nullable(...))` when
  * allowed by type capabilities and current
  * `allow_nullable_tuple_in_extracted_subcolumns` setting value from global context.
  * Config changes for global context settings are applied after server restart.
  * Returns `type` unchanged when wrapping is not allowed.
  */
DataTypePtr makeExtractedSubcolumnsNullableOrLowCardinalityNullableSafe(const DataTypePtr & type);

/** This function marks rows of an extracted subcolumn as `NULL` in place, according to the null map of the
  * outer `Nullable` column. It processes the suffix of `column` that starts at `column_offset`, that is the
  * rows `[column_offset, column->size())`. For the `i`-th row of that suffix it marks the row as `NULL` when
  * `parent_null_map[parent_null_map_offset + i]` is set. The parent null map's offset is separate because
  * the suffix can sit at a different position in the parent null map than it does in the subcolumn. The
  * column must be able to represent `NULL` itself, so it must be a `ColumnNullable`, a `ColumnVariant`, a
  * `ColumnDynamic`, or a `ColumnLowCardinality` with a nullable dictionary, and it must be exclusively owned
  * by the caller.
  */
void applyParentNullMapToExtractedSubcolumn(
    const MutableColumnPtr & column,
    const NullMap & parent_null_map,
    size_t column_offset,
    size_t parent_null_map_offset);

struct NullableSubcolumnCreator : public ISerialization::ISubcolumnCreator
{
    const ColumnPtr null_map;

    explicit NullableSubcolumnCreator(const ColumnPtr & null_map_) : null_map(null_map_) {}

    DataTypePtr create(const DataTypePtr & prev) const override;
    SerializationPtr create(const SerializationPtr & prev_serialization, const DataTypePtr & prev_type) const override;
    ColumnPtr create(const ColumnPtr & prev) const override;
};

}
