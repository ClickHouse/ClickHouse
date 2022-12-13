#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Core/QualifiedTableName.h>


namespace DB
{
using TableNamesSet = std::unordered_set<QualifiedTableName>;

/// Returns a list of all tables explicitly referenced in the create query of a specified table.
/// For example, a column default expression can use dictGet() and thus reference a dictionary.
/// Does not validate AST, works a best-effort way.
TableNamesSet getDependenciesFromCreateQuery(const ContextPtr & global_context, const QualifiedTableName & table_name, const ASTPtr & ast);

/// Visits ASTCreateQuery and extracts the names of all tables explicitly referenced in the create query.
class DDLDependencyVisitor
{
public:
    struct Data
    {
        ASTPtr create_query;
        QualifiedTableName table_name;
        String default_database;
        ContextPtr global_context;
        TableNamesSet dependencies;
    };

    using Visitor = ConstInDepthNodeVisitor<DDLDependencyVisitor, /* top_to_bottom= */ true>;

    static void visit(const ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
};

}
