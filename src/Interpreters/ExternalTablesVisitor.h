#pragma once

#include <Interpreters/Context.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/typeid_cast.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/IdentifierSemantic.h>

namespace DB
{

/// If node is ASTIdentifier try to extract external_storage.
class ExternalTablesMatcher
{
public:
    struct Data
    {
        const Context & context;
        Tables & external_tables;
    };

    static void visit(ASTPtr & ast, Data & data)
    {
        if (const auto * t = ast->as<ASTIdentifier>())
            visit(*t, ast, data);
    }

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

private:
    static void visit(const ASTIdentifier & node, ASTPtr &, Data & data)
    {
        if (auto opt_name = IdentifierSemantic::getTableName(node))
            if (StoragePtr external_storage = data.context.tryGetExternalTable(*opt_name))
                data.external_tables[*opt_name] = external_storage;
    }
};

/// Finds in the query the usage of external tables. Fills in external_tables.
using ExternalTablesVisitor = InDepthNodeVisitor<ExternalTablesMatcher, false>;

}
