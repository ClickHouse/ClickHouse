#pragma once

#include <memory>
#include <vector>


namespace DB::AST
{
class INode;
template <class T, char Separator>
class List;

template <class T = INode>
using PtrTo = std::shared_ptr<T>;

using Ptr = PtrTo<>;
using PtrList = std::vector<Ptr>;

class ColumnExpr;
class ColumnFunctionExpr;
class ColumnIdentifier;
class ColumnLambdaExpr;
class ColumnPropertyExpr;
class DatabaseIdentifier;
class EngineClause;
class EngineExpr;
class Identifier;
class JoinExpr;
class LimitExpr;
class Literal;
class NumberLiteral;
class OrderByClause;
class OrderExpr;
class Query;
class RatioExpr;
class SelectStmt;
class SelectUnionQuery;
class SettingExpr;
class SettingsClause;
class StringLiteral;
class TableArgExpr;
class TableElementExpr;
class TableExpr;
class TableIdentifier;
class TTLExpr;

using ColumnExprList = List<ColumnExpr, ','>;
using ColumnParamList = List<Literal, ','>;
using OrderExprList = List<OrderExpr, ','>;
using QueryList = List<Query, ';'>;
using SettingExprList = List<SettingExpr, ','>;
using TableArgList = List<TableArgExpr, ','>;
using TableElementList = List<TableElementExpr, ','>;
using TTLExprList = List<TTLExpr, ','>;

}
