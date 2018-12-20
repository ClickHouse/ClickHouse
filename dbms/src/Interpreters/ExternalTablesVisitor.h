#pragma once

#include <Interpreters/Context.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/typeid_cast.h>
#include <Interpreters/InDepthNodeVisitor.h>

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

    static constexpr const char * label = "ExternalTables";

    static std::vector<ASTPtr *> visit(ASTPtr & ast, Data & data)
    {
        if (auto * t = typeid_cast<ASTIdentifier *>(ast.get()))
            return visit(*t, ast, data);
        return {};
    }

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

private:
    static std::vector<ASTPtr *> visit(const ASTIdentifier & node, ASTPtr &, Data & data)
    {
        if (node.special())
            if (StoragePtr external_storage = data.context.tryGetExternalTable(node.name))
                data.external_tables[node.name] = external_storage;
        return {};
    }
};

/// Finds in the query the usage of external tables. Fills in external_tables.
using ExternalTablesVisitor = InDepthNodeVisitor<ExternalTablesMatcher, false>;

}
