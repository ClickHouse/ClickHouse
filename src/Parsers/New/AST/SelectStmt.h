#pragma once

#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>

#include <Core/Types.h>


namespace DB::AST
{

// Exprs

class JoinExpr : public INode
{
    public:
        explicit JoinExpr(PtrTo<TableIdentifier> id);
};

class RatioExpr : public INode
{
    public:
        RatioExpr(PtrTo<NumberLiteral> num1, PtrTo<NumberLiteral> num2);
};

class OrderExpr : public INode
{
    public:
        enum NullsOrder {
            NATURAL,
            NULLS_FIRST,
            NULLS_LAST,
        };

        OrderExpr(PtrTo<ColumnExpr> expr, NullsOrder nulls, PtrTo<StringLiteral> collate);
};

using OrderExprList = List<OrderExpr, ','>;

class LimitExpr : public INode
{
    public:
        explicit LimitExpr(PtrTo<NumberLiteral> limit);
        LimitExpr(PtrTo<NumberLiteral> limit, PtrTo<NumberLiteral> offset);
};

class SettingExpr : public INode
{
    public:
        SettingExpr(PtrTo<Identifier> name, PtrTo<Literal> value);
};

using SettingExprList = List<SettingExpr, ','>;

// Clauses

class WithClause : public INode
{
    public:
        explicit WithClause(PtrTo<ColumnExprList> expr_list);
};

class FromClause : public INode
{
    public:
        FromClause(PtrTo<JoinExpr> join_expr, bool final);
};

class SampleClause : public INode
{
    public:
        SampleClause(PtrTo<RatioExpr> ratio, PtrTo<RatioExpr> offset);
};

class ArrayJoinClause : public INode
{
    public:
        ArrayJoinClause(PtrTo<ColumnExprList> expr_list, bool left);
};

class PrewhereClause : public INode
{
    public:
        explicit PrewhereClause(PtrTo<ColumnExpr> expr);
};

class WhereClause : public INode
{
    public:
        explicit WhereClause(PtrTo<ColumnExpr> expr);
};

class GroupByClause : public INode
{
    public:
        GroupByClause(PtrTo<ColumnExprList> expr_list, bool with_totals);
};

class HavingClause : public INode
{
    public:
        explicit HavingClause(PtrTo<ColumnExpr> expr);
};

class OrderByClause : public INode
{
    public:
        explicit OrderByClause(PtrTo<OrderExprList> expr_list);
};

class LimitByClause : public INode
{
    public:
        LimitByClause(PtrTo<LimitExpr> expr, PtrTo<ColumnExprList> expr_list);
};

class LimitClause : public INode
{
    public:
        explicit LimitClause(PtrTo<LimitExpr> expr);
};

class SettingsClause : public INode
{
    public:
        explicit SettingsClause(PtrTo<SettingExprList> expr_list);
};

class SelectStmt : public INode
{
    public:
        void setWithClause(PtrTo<WithClause> clause);
        void setFromClause(PtrTo<FromClause> clause);
        void setSampleClause(PtrTo<SampleClause> clause);
        void setArrayJoinClause(PtrTo<ArrayJoinClause> clause);
        void setPrewhereClause(PtrTo<PrewhereClause> clause);
        void setWhereClause(PtrTo<WhereClause> clause);
        void setGroupByClause(PtrTo<GroupByClause> clause);
        void setHavingClause(PtrTo<HavingClause> clause);
        void setOrderByClause(PtrTo<OrderByClause> clause);
        void setLimitByClause(PtrTo<LimitByClause> clause);
        void setLimitClause(PtrTo<LimitClause> clause);
        void setSettingsClause(PtrTo<SettingsClause> clause);
};

}
