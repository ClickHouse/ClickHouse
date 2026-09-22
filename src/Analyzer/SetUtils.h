#pragma once

#include <Core/ColumnsWithTypeAndName.h>
#include <Columns/IColumn_fwd.h>
#include <DataTypes/IDataType_fwd.h>

#include <memory>

namespace DB
{

class Set;
using SetPtr = std::shared_ptr<Set>;

struct GetSetElementParams
{
    bool transform_null_in = true;
    bool forbid_unknown_enum_values = false;
};

/// The key columns that `IN` compares separately for a left operand of this type. A plain `Tuple` with more than
/// one element is unpacked into one key column per element, so a NULL element of a member never matches with
/// `transform_null_in = 0`. Anything else is one key whose NULL fields are ordinary values: an empty or
/// single-element tuple, and a `Nullable(Tuple(...))`, because the set is built as a single `Nullable(Tuple(...))`
/// column anyway.
DataTypes getInKeyColumnTypes(const DataTypePtr & lhs_type);

/** Get set elements for constant part of IN subquery.
  * Throws exception if parameters are not valid for IN function.
  *
  * Example: SELECT id FROM test_table WHERE id IN (1, 2, 3, 4);
  * Example: SELECT id FROM test_table WHERE id IN ((1, 2), (3, 4));
  */
/// `rhs_column` is a size-1 (const) column holding the constant right-hand side of `IN` (a scalar,
/// Array or Tuple). Values are read column-natively - no `Field` is materialized.
ColumnsWithTypeAndName getSetElementsForConstantValue(const DataTypePtr & expression_type, const ColumnPtr & rhs_column, const DataTypePtr & rhs_type, GetSetElementParams params);

}
