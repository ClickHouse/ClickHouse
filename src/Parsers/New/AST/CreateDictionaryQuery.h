#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class DictionaryAttributeExpr : public INode
{
    public:
        DictionaryAttributeExpr(PtrTo<Identifier> identifier, PtrTo<ColumnTypeExpr> type);

        void setDefaultClause(PtrTo<Literal> literal);
        void setExpressionClause(PtrTo<ColumnExpr> expr);

        void setHierarchicalFlag() { hierarchical = true; }
        void setInjectiveFlag() { injective = true; }
        void setIsObjectIdFlag() { is_object_id = true; }

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,    // Identifier
            TYPE,        // ColumnTypeExpr
            DEFAULT,     // Literal (optional)
            EXPRESSION,  // ColumnExpr (optional)

            MAX_INDEX,
        };

        bool hierarchical = false, injective = false, is_object_id = false;
};

using DictionaryPrimaryKeyClause = SimpleClause<ColumnExprList>;

using DictionarySchemaClause = SimpleClause<DictionaryAttributeList>;

class DictionaryArgExpr : public INode
{
    public:
        explicit DictionaryArgExpr(PtrTo<Identifier> identifier, PtrTo<ColumnExpr> expr);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            KEY = 0,  // Identifier
            VALUE,    // ColumnExpr: literal, identifier or function
        };
};

class SourceClause : public INode
{
    public:
        SourceClause(PtrTo<Identifier> identifier, PtrTo<DictionaryArgList> list);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,  // Identifier
            ARGS = 1,  // DictionaryArgList (optional)
        };
};

class LifetimeClause : public INode
{
    public:
        explicit LifetimeClause(PtrTo<NumberLiteral> max, PtrTo<NumberLiteral> min = nullptr);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            MAX = 0,  // NumberLiteral
            MIN,      // NumberLiteral (optional)
        };
};

class LayoutClause : public INode
{
    public:
        LayoutClause(PtrTo<Identifier> identifier, PtrTo<DictionaryArgList> list);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,  // Identifier
            ARGS = 1,  // DictionaryArgList (optional)
        };
};

class RangeClause : public INode
{
    public:
        RangeClause(PtrTo<Identifier> max, PtrTo<Identifier> min);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            MAX = 0,  // Identifier
            MIN,      // Identifier
        };
};

class DictionarySettingsClause : public INode
{
    public:
        explicit DictionarySettingsClause(PtrTo<SettingExprList> list);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            LIST = 0,  // SettingExprList
        };
};

class DictionaryEngineClause : public INode
{
    public:
        explicit DictionaryEngineClause(PtrTo<DictionaryPrimaryKeyClause> clause);

        void setSourceClause(PtrTo<SourceClause> clause);
        void setLifetimeClause(PtrTo<LifetimeClause> clause);
        void setLayoutClause(PtrTo<LayoutClause> clause);
        void setRangeClause(PtrTo<RangeClause> clause);
        void setSettingsClause(PtrTo<DictionarySettingsClause> clause);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            PRIMARY_KEY = 0,  // DictionaryPrimaryKeyClause
            SOURCE,           // SourceClause (optional)
            LIFETIME,         // LifetimeClause (optional)
            LAYOUT,           // LayoutClause (optional)
            RANGE,            // RangeClause (optional)
            SETTINGS,         // DictionarySettingsClause (optional)

            MAX_INDEX,
        };
};

class CreateDictionaryQuery : public DDLQuery
{
    public:
        CreateDictionaryQuery(
            PtrTo<ClusterClause> cluster,
            bool attach,
            bool if_not_exists,
            PtrTo<TableIdentifier> identifier,
            PtrTo<UUIDClause> uuid,
            PtrTo<DictionarySchemaClause> schema,
            PtrTo<DictionaryEngineClause> engine);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,  // TableIdentifier
            UUID,      // UUIDClause (optional)
            SCHEMA,    // DictionarySchemaClause
            ENGINE,    // DictionaryEngineClause
        };

        const bool attach, if_not_exists;
};

}
