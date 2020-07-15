#pragma once

#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/JoinExpr.h>
#include <Parsers/New/AST/LimitExpr.h>
#include <Parsers/New/AST/OrderExpr.h>
#include <Parsers/New/AST/RatioExpr.h>
#include <Parsers/New/AST/SettingExpr.h>

#include <Core/Types.h>


namespace DB::AST
{

// Clauses

class WithClause : public INode
{
    public:
        explicit WithClause(PtrTo<ColumnExprList> expr_list);

    private:
        PtrTo<ColumnExprList> exprs;
};

class FromClause : public INode
{
    public:
        FromClause(PtrTo<JoinExpr> join_expr, bool final_);

        ASTPtr convertToOld() const override;

    private:
        PtrTo<JoinExpr> expr;
        bool final;
};

class SampleClause : public INode
{
    public:
        explicit SampleClause(PtrTo<RatioExpr> ratio_);
        SampleClause(PtrTo<RatioExpr> ratio_, PtrTo<RatioExpr> offset_);

    private:
        PtrTo<RatioExpr> ratio, offset;
};

class ArrayJoinClause : public INode
{
    public:
        ArrayJoinClause(PtrTo<ColumnExprList> expr_list, bool left_);

    private:
        PtrTo<ColumnExprList> exprs;
        bool left;
};

class PrewhereClause : public INode
{
    public:
        explicit PrewhereClause(PtrTo<ColumnExpr> expr_);

    private:
        PtrTo<ColumnExpr> expr;
};

class WhereClause : public INode
{
    public:
        explicit WhereClause(PtrTo<ColumnExpr> expr_);

    private:
        PtrTo<ColumnExpr> expr;
};

class GroupByClause : public INode
{
    public:
        GroupByClause(PtrTo<ColumnExprList> expr_list, bool with_totals_);

    private:
        PtrTo<ColumnExprList> exprs;
        bool with_totals;
};

class HavingClause : public INode
{
    public:
        explicit HavingClause(PtrTo<ColumnExpr> expr_);

    private:
        PtrTo<ColumnExpr> expr;
};

class OrderByClause : public INode
{
    public:
        explicit OrderByClause(PtrTo<OrderExprList> expr_list);

    private:
        PtrTo<OrderExprList> exprs;
};

class LimitByClause : public INode
{
    public:
        LimitByClause(PtrTo<LimitExpr> expr, PtrTo<ColumnExprList> expr_list);

    private:
        PtrTo<LimitExpr> limit;
        PtrTo<ColumnExprList> by;
};

class LimitClause : public INode
{
    public:
        explicit LimitClause(PtrTo<LimitExpr> expr_);

    private:
        PtrTo<LimitExpr> expr;
};

class SettingsClause : public INode
{
    public:
        explicit SettingsClause(PtrTo<SettingExprList> expr_list);

    private:
        PtrTo<SettingExprList> exprs;
};

// Statement

class SelectStmt : public INode
{
    public:
        explicit SelectStmt(PtrTo<ColumnExprList> expr_list);

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

        ASTPtr convertToOld() const override;

    private:
        PtrTo<ColumnExprList> columns;

        PtrTo<WithClause> with;
        PtrTo<FromClause> from;
        PtrTo<SampleClause> sample;
        PtrTo<ArrayJoinClause> array_join;
        PtrTo<PrewhereClause> prewhere;
        PtrTo<WhereClause> where;
        PtrTo<GroupByClause> group_by;
        PtrTo<HavingClause> having;
        PtrTo<OrderByClause> order_by;
        PtrTo<LimitByClause> limit_by;
        PtrTo<LimitClause> limit;
        PtrTo<SettingsClause> settings;
};

}
