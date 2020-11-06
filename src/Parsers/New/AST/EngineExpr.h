#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

// Clauses

using PartitionByClause = SimpleClause<ColumnExpr>;

using PrimaryKeyClause = SimpleClause<ColumnExpr>;

using SampleByClause = SimpleClause<ColumnExpr>;

using TTLClause = SimpleClause<TTLExprList>;

class EngineClause : public INode
{
    public:
        explicit EngineClause(PtrTo<EngineExpr> expr);

        void setOrderByClause(PtrTo<OrderByClause> clause);
        void setPartitionByClause(PtrTo<PartitionByClause> clause);
        void setPrimaryKeyClause(PtrTo<PrimaryKeyClause> clause);
        void setSampleByClause(PtrTo<SampleByClause> clause);
        void setTTLClause(PtrTo<TTLClause> clause);
        void setSettingsClause(PtrTo<SettingsClause> clause);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            ENGINE = 0,    // EngineExpr
            ORDER_BY,      // OrderByClause (optional)
            PARTITION_BY,  // PartitionByClause (optional)
            PRIMARY_KEY,   // PrimaryKeyClause (optional)
            SAMPLE_BY,     // SampleByClause (optional)
            TTL,           // TTLClause (optional)
            SETTINGS,      // SettingsClause (optional)

            MAX_INDEX,
        };
};

// Expressions

class EngineExpr : public INode
{
    public:
        EngineExpr(PtrTo<Identifier> identifier, PtrTo<ColumnExprList> args);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,  // Identifier
            ARGS,      // ColumnExprList (optional)
        };
};

class TTLExpr : public INode
{
    public:
        enum class TTLType
        {
            DELETE,
            TO_DISK,
            TO_VOLUME,
        };

        TTLExpr(PtrTo<ColumnExpr> expr, TTLType type, PtrTo<StringLiteral> literal);

    private:
        enum ChildIndex : UInt8
        {
            EXPR = 0,
            TYPE = 1,
        };

        TTLType ttl_type;
};

}
