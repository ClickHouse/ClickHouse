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
        explicit FromClause(PtrTo<JoinExpr> join_expr);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            EXPR = 0,  // JoinExpr
        };
};

class ArrayJoinClause : public INode
{
    public:
        ArrayJoinClause(PtrTo<ColumnExprList> expr_list, bool left);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            EXPRS = 0,  // ColumnExprList
        };

        const bool left;
};

using PrewhereClause = SimpleClause<ColumnExpr>;

using GroupByClause = SimpleClause<ColumnExprList>;

using HavingClause = SimpleClause<ColumnExpr>;

class LimitByClause : public INode
{
    public:
        LimitByClause(PtrTo<LimitExpr> expr, PtrTo<ColumnExprList> expr_list);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            LIMIT = 0,  // LimitExpr
            EXPRS = 1,   // ColumnExprList
        };
};

class LimitClause : public INode
{
    public:
        LimitClause(bool with_ties, PtrTo<LimitExpr> expr);

        ASTPtr convertToOld() const override;

        const bool with_ties;  // FIXME: bad interface, because old AST stores this inside ASTSelectQuery.

    private:
        enum ChildIndex : UInt8
        {
            EXPR = 0,  // LimitExpr
        };
};

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
        enum class ModifierType
        {
            NONE,
            CUBE,
            ROLLUP,
        };

        SelectStmt(bool distinct_, ModifierType type, bool totals, PtrTo<ColumnExprList> expr_list);

        void setWithClause(PtrTo<WithClause> clause);
        void setFromClause(PtrTo<FromClause> clause);
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

        const ModifierType modifier_type;
        const bool distinct, with_totals;
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
