#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

// Clauses

class PartitionByClause : public INode
{
    public:
        explicit PartitionByClause(PtrTo<ColumnExpr> expr);
};

class PrimaryKeyClause : public INode
{
    public:
        explicit PrimaryKeyClause(PtrTo<ColumnExpr> expr);
};

class SampleByClause : public INode
{
    public:
        explicit SampleByClause(PtrTo<ColumnExpr> expr);
};

class TTLClause : public INode
{
    public:
        explicit TTLClause(PtrTo<TTLExprList> list);
};

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

    private:
        enum ChildIndex : UInt8
        {
            ENGINE = 0,
            ORDER_BY,
            PARTITION_BY,
            PRIMARY_KEY,
            SAMPLE_BY,
            TTL,
            SETTINGS,

            MAX_INDEX,
        };
};

// Expressions

class EngineExpr : public INode
{
    public:
        EngineExpr(PtrTo<Identifier> identifier, PtrTo<ColumnExprList> args);
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
