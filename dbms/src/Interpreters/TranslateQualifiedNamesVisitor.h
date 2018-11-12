#pragma once

#include <memory>
#include <vector>

#include <Common/typeid_cast.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{

/// Visitors consist of functions with unified interface 'void visit(Casted & x, ASTPtr & y)', there x is y, successfully casted to Casted.
/// Both types and fuction could have const specifiers. The second argument is used by visitor to replaces AST node (y) if needed.

/// It visits nodes, find columns (general identifiers and asterisks) and translate their names according to tables' names.
class TranslateQualifiedNamesVisitor
{
public:
    TranslateQualifiedNamesVisitor(const NameSet & source_columns_, const std::vector<DatabaseAndTableWithAlias> & tables_,
                                   std::ostream * ostr_ = nullptr)
    :   source_columns(source_columns_),
        tables(tables_),
        visit_depth(0),
        ostr(ostr_)
    {}

    void visit(ASTPtr & ast) const
    {
        if (!tryVisit<ASTIdentifier>(ast) &&
            !tryVisit<ASTQualifiedAsterisk>(ast) &&
            !tryVisit<ASTTableJoin>(ast) &&
            !tryVisit<ASTSelectQuery>(ast))
            visitChildren(ast); /// default: do nothing, visit children
    }

private:
    const NameSet & source_columns;
    const std::vector<DatabaseAndTableWithAlias> & tables;
    mutable size_t visit_depth;
    std::ostream * ostr;

    void visit(ASTIdentifier & node, ASTPtr & ast, const DumpASTNode & dump) const;
    void visit(ASTQualifiedAsterisk & node, ASTPtr & ast, const DumpASTNode & dump) const;
    void visit(ASTTableJoin & node, ASTPtr & ast, const DumpASTNode & dump) const;
    void visit(ASTSelectQuery & ast, ASTPtr &, const DumpASTNode & dump) const;

    void visitChildren(ASTPtr &) const;

    template <typename T>
    bool tryVisit(ASTPtr & ast) const
    {
        if (T * t = typeid_cast<T *>(ast.get()))
        {
            DumpASTNode dump(*ast, ostr, visit_depth, "translateQualifiedNames");
            visit(*t, ast, dump);
            return true;
        }
        return false;
    }
};

}
