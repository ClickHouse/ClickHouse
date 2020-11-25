#pragma once

#include <memory>
#include <vector>


namespace DB::AST
{

class INode;

template <class T, char Separator = ','>
class List;

template <class T>
class SimpleClause;

template <class T = INode>
using PtrTo = std::shared_ptr<T>;

using Ptr = PtrTo<>;
using PtrList = std::vector<Ptr>;

class CodecArgExpr;
class CodecExpr;
class ColumnExpr;
class ColumnFunctionExpr;
class ColumnIdentifier;
class ColumnLambdaExpr;
class ColumnTypeExpr;
class DatabaseIdentifier;
class EngineClause;
class EngineExpr;
class EnumValue;
class Identifier;
class JoinExpr;
class JsonExpr;
class JsonValue;
class LimitExpr;
class Literal;
class NumberLiteral;
class OrderExpr;
class PartitionClause;
class Query;
class RatioExpr;
class SchemaClause;
class SelectStmt;
class SelectUnionQuery;
class SettingExpr;
class SettingsClause;
class StringLiteral;
class TableArgExpr;
class TableColumnPropertyExpr;
class TableElementExpr;
class TableExpr;
class TableFunctionExpr;
class TableIdentifier;
class TTLExpr;

using CodecArgList = List<CodecArgExpr>;
using ColumnExprList = List<ColumnExpr>;
using ColumnNameList = List<Identifier>;
using ColumnParamList = ColumnExprList;
using ColumnTypeExprList = List<ColumnTypeExpr>;
using EnumValueList = List<EnumValue>;
using JsonExprList = List<JsonExpr>;
using JsonValueList = List<JsonValue>;
using OrderExprList = List<OrderExpr>;
using QueryList = List<Query, ';'>;
using SettingExprList = List<SettingExpr>;
using TableArgList = List<TableArgExpr>;
using TableElementList = List<TableElementExpr>;
using TTLExprList = List<TTLExpr>;

using ClusterClause = SimpleClause<StringLiteral>;
using DestinationClause = SimpleClause<TableIdentifier>;
using OrderByClause = SimpleClause<OrderExprList>;
using TTLClause = SimpleClause<TTLExprList>;
using UUIDClause = SimpleClause<StringLiteral>;

}
