#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class AlterTableClause : public INode
{
    public:
        static PtrTo<AlterTableClause> createAdd(bool if_not_exists, PtrTo<TableElementExpr> element, PtrTo<Identifier> after);
        static PtrTo<AlterTableClause> createComment(bool if_exists, PtrTo<Identifier> identifier, PtrTo<StringLiteral> literal);
        static PtrTo<AlterTableClause> createDrop(bool if_exists, PtrTo<Identifier> identifier);
        static PtrTo<AlterTableClause> createModify(bool if_exists, PtrTo<TableElementExpr> element);

    private:
        enum class ClauseType
        {
            ADD,
            COMMENT,
            DROP,
            MODIFY,
        };

        const ClauseType clause_type;
        union
        {
            bool if_exists;
            bool if_not_exists;
        };

        AlterTableClause(ClauseType type, PtrList exprs);
};

class AlterTableQuery : public DDLQuery
{
    public:
        AlterTableQuery(PtrTo<TableIdentifier> identifier, PtrTo<List<AlterTableClause>> clauses);
};

}
