#pragma once

#include <Core/ColumnsWithTypeAndName.h>
#include <Columns/IColumn_fwd.h>
#include <DataTypes/IDataType_fwd.h>
#include <Formats/FormatSettings.h>

#include <memory>

namespace DB
{

class Set;
using SetPtr = std::shared_ptr<Set>;
class Context;
using ContextPtr = std::shared_ptr<const Context>;
class DataTypeTuple;

struct GetSetElementParams
{
    bool transform_null_in = true;
    bool forbid_unknown_enum_values = false;
    /// The query's format settings, so that a member rendered to `String` (`'yes' IN (true)`) holds
    /// the same text that `CAST(x AS String)` produces under the same session settings, e.g. with
    /// `bool_true_representation`. `convertFieldToType` applies them exactly where `CAST` does.
    FormatSettings format_settings = {};
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
ColumnsWithTypeAndName getSetElementsForConstantValue(const DataTypePtr & expression_type, const ColumnPtr & rhs_column, const DataTypePtr & rhs_type, const GetSetElementParams & params);

/// The same, with `GetSetElementParams` taken from the query settings of `context`.
/// The callers sit in deeply recursive visitors (`CollectSetsVisitor`, `QueryAnalyzer::resolveFunction`,
/// `ActionsMatcher`), so the large `GetSetElementParams` (it holds `FormatSettings`) is built here,
/// out of their stack frames, rather than by each caller.
ColumnsWithTypeAndName getSetElementsForConstantValue(const DataTypePtr & expression_type, const ColumnPtr & rhs_column, const DataTypePtr & rhs_type, const ContextPtr & context);

}
