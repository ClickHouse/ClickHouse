#pragma once

#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <Columns/IColumn_fwd.h>
#include <DataTypes/IDataType_fwd.h>

#include <memory>
#include <optional>

namespace DB
{

class Set;
using SetPtr = std::shared_ptr<Set>;
class DataTypeTuple;

struct GetSetElementParams
{
    bool transform_null_in = true;
    bool forbid_unknown_enum_values = false;
};

/// The key columns that `IN` compares separately for a left operand of this type. A plain `Tuple` with more
/// than one element is unpacked into one key column per element, so a NULL element of a member never matches
/// with `transform_null_in = 0`. Anything else is one key whose NULL fields are ordinary values: an empty or
/// single-element tuple, and a `Nullable(Tuple(...))`, because the set is built as a single
/// `Nullable(Tuple(...))` column anyway.
DataTypes getInKeyColumnTypes(const DataTypePtr & lhs_type);

/// Whether a `Nullable(Tuple(...))` key with the tuple type `key_tuple_type` can hold the tuple value of an explicit
/// all-NULL right-hand tuple of `rhs_tuple_size` elements: the sizes match and every element can hold NULL (see
/// `canContainNull`). For example, `Nullable(Tuple(Nullable(UInt8), Nullable(UInt8)))` and
/// `Nullable(Tuple(LowCardinality(Nullable(String)), Variant(UInt8, String)))` can hold `(NULL, NULL)`, and
/// `Nullable(Tuple(UInt8, Nullable(UInt8)))` cannot. Every form of `IN` then reads such a right-hand tuple both as
/// top-level NULL members and as that tuple value, in either `transform_null_in` mode, because the key is a single key
/// column whose NULL fields are values (see `getInKeyColumnTypes`).
bool nullableTupleKeyCanHoldAllNullTuple(const DataTypeTuple & key_tuple_type, size_t rhs_tuple_size);

/** Get set elements for constant part of IN subquery.
  * Throws exception if parameters are not valid for IN function.
  *
  * Example: SELECT id FROM test_table WHERE id IN (1, 2, 3, 4);
  * Example: SELECT id FROM test_table WHERE id IN ((1, 2), (3, 4));
  */
/// `rhs_column` is a size-1 (const) column holding the constant right-hand side of `IN` (a scalar,
/// Array or Tuple). Values are read column-natively - no `Field` is materialized.
ColumnsWithTypeAndName getSetElementsForConstantValue(const DataTypePtr & expression_type, const ColumnPtr & rhs_column, const DataTypePtr & rhs_type, GetSetElementParams params);

/** The set elements of `x IN (v1, v2, ...)` for a scalar key, read straight from a tuple of plain values (UInt64,
  * Int64 or String `Field`s, as a `ConstantNode` with a deferred column keeps them), without building one size-1
  * column per value. Gives the same set as `getSetElementsForConstantValue`. Returns nullopt for any other key or
  * value types; the caller then uses `getSetElementsForConstantValue`.
  */
std::optional<ColumnsWithTypeAndName> tryGetSetElementsForTupleOfPlainValues(
    const DataTypePtr & expression_type, const Tuple & rhs_elements, GetSetElementParams params);

}
