#pragma once

#include <Parsers/New/AST/Query.h>

#include <Core/Types.h>

#include <list>


namespace DB::AST
{

// Clauses

using WithClause = SimpleClause<ColumnExprList>;

class FromClause : public INode
{
    public:
        FromClause(PtrTo<JoinExpr> join_expr, bool final);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            EXPR = 0,  // JoinExpr
        };

        const bool final;
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

using PrewhereClause = SimpleClause<ColumnExpr>;

using WhereClause = SimpleClause<ColumnExpr>;

class GroupByClause : public INode
{
    public:
        GroupByClause(PtrTo<ColumnExprList> expr_list, bool with_totals_);

        bool withTotals() const { return with_totals; }

    private:
        const bool with_totals;
};

using HavingClause = SimpleClause<ColumnExpr>;

class LimitByClause : public INode
{
    public:
        LimitByClause(PtrTo<LimitExpr> expr, PtrTo<ColumnExprList> expr_list);

    private:
        PtrTo<LimitExpr> limit;
        PtrTo<ColumnExprList> by;
};

using LimitClause = SimpleClause<LimitExpr>;

class SettingsClause : public INode
{
    public:
        explicit SettingsClause(PtrTo<SettingExprList> expr_list);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            EXPRS = 0,  // SettingExprList
        };
};

// Statement

class SelectStmt : public INode
{
    public:
        SelectStmt(bool distinct_, PtrTo<ColumnExprList> expr_list);

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
        enum ChildIndex : UInt8
        {
            COLUMNS = 0,  // ColumnExprList
            WITH,         // WithClause (optional)
            FROM,         // FromClause (optional)
            SAMPLE,       // SampleClause (optional)
            ARRAY_JOIN,   // ArrayJoinClause (optional)
            PREWHERE,     // PrewhereClause (optional)
            WHERE,        // WhereClause (optional)
            GROUP_BY,     // GroupByClause (optional)
            HAVING,       // HavingClause (optional)
            ORDER_BY,     // OrderByClause (optional)
            LIMIT_BY,     // LimitByClause (optional)
            LIMIT,        // LimitClause (optional)
            SETTINGS,     // SettingsClause (optional)

            MAX_INDEX,
        };

        const bool distinct;
};

class SelectUnionQuery : public Query
{
    public:
        SelectUnionQuery() = default;
        explicit SelectUnionQuery(PtrTo<List<SelectStmt>> stmts);

        void appendSelect(PtrTo<SelectStmt> stmt);
        void appendSelect(PtrTo<SelectUnionQuery> query);
        void shouldBeScalar() { is_scalar = true; }

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            STMTS = 0,  // List<SelectStmt>
        };

        bool is_scalar = false;
};

}
