#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTRenameQuery.h>

namespace DB
{

/// Visits AST nodes, add default database to DDLs if not set.
class AddDefaultDatabaseVisitor
{
public:
    AddDefaultDatabaseVisitor(const String & default_database_)
    :   default_database(default_database_)
    {}

    void visit(ASTPtr & ast) const
    {
        visitChildren(ast);

        if (!tryVisit<ASTQueryWithTableAndOutput>(ast) &&
            !tryVisit<ASTRenameQuery>(ast))
        {}
    }

private:
    const String default_database;

    void visit(ASTQueryWithTableAndOutput * node, ASTPtr &) const
    {
        if (node->database.empty())
            node->database = default_database;
    }

    void visit(ASTRenameQuery * node, ASTPtr &) const
    {
        for (ASTRenameQuery::Element & elem : node->elements)
        {
            if (elem.from.database.empty())
                elem.from.database = default_database;
            if (elem.to.database.empty())
                elem.to.database = default_database;
        }
    }

    void visitChildren(ASTPtr & ast) const
    {
        for (auto & child : ast->children)
            visit(child);
    }

    template <typename T>
    bool tryVisit(ASTPtr & ast) const
    {
        if (T * t = dynamic_cast<T *>(ast.get()))
        {
            visit(t, ast);
            return true;
        }
        return false;
    }
};

}
