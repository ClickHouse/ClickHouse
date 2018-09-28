#pragma once

#include <memory>
#include <vector>

#include <Common/typeid_cast.h>
#include <Parsers/IAST.h>
#include <Interpreters/evaluateQualified.h>

namespace DB
{

class ASTIdentifier;
class ASTQualifiedAsterisk;
class ASTSelectQuery;
struct ASTTableJoin;

class NamesAndTypesList;


class TranslateQualifiedNamesVisitor
{
public:
    TranslateQualifiedNamesVisitor(const NamesAndTypesList & source_columns_, const std::vector<DatabaseAndTableWithAlias> & tables_,
                                   std::ostream * ostr_ = nullptr)
    :   source_columns(source_columns_),
        tables(tables_),
        visit_depth(0),
        ostr(ostr_)
    {}

    void visit(ASTPtr & ast) const
    {
        DumpASTNode dump(*ast, ostr, visit_depth, "translateQualifiedNames");

        if (!tryVisit<ASTIdentifier>(ast, dump) &&
            !tryVisit<ASTQualifiedAsterisk>(ast, dump) &&
            !tryVisit<ASTTableJoin>(ast, dump) &&
            !tryVisit<ASTSelectQuery>(ast, dump))
            visitChildren(ast); /// default: do nothing, visit children
    }

private:
    const NamesAndTypesList & source_columns;
    const std::vector<DatabaseAndTableWithAlias> & tables;
    mutable size_t visit_depth;
    std::ostream * ostr;

    void visit(ASTIdentifier * node, ASTPtr & ast, const DumpASTNode & dump) const;
    void visit(ASTQualifiedAsterisk * node, ASTPtr & ast, const DumpASTNode & dump) const;
    void visit(ASTTableJoin * node, ASTPtr & ast, const DumpASTNode & dump) const;
    void visit(ASTSelectQuery * ast, ASTPtr &, const DumpASTNode & dump) const;

    void visitChildren(ASTPtr &) const;

    template <typename T>
    bool tryVisit(ASTPtr & ast, const DumpASTNode & dump) const
    {
        if (T * t = typeid_cast<T *>(ast.get()))
        {
            visit(t, ast, dump);
            return true;
        }
        return false;
    }
};

}
