#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Columns/IColumn_fwd.h>
#include <DataTypes/IDataType_fwd.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

ASTPtr getASTForExternalDatabaseFromQueryTree(ContextPtr context, const QueryTreeNodePtr & query_tree, const TableExpressionNodePtr & table_expression);

/// Whether the size-1 `column` holds an `Enum` value, also inside a `Nullable`, `Variant`, `Dynamic` or `Tuple`.
/// `Array` and `Map` are not searched.
bool holdsEnumValue(const ColumnPtr & column, const DataTypePtr & type);

}
