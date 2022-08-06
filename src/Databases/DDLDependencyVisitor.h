#pragma once
#include <Core/QualifiedTableName.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

using TableNamesSet = std::unordered_set<QualifiedTableName>;

/// Returns a list of all tables which should be loaded before a specified table by its create query.
/// For example, a column default expression can use dictGet() to refer to a dictionary;
/// or dictionary source can refer to a local ClickHouse table.
/// Does not validate AST, works a best-effort way.
TableNamesSet getDependenciesSetFromCreateQuery(const ASTPtr & ast, const ContextPtr & global_context);

/// Visits ASTCreateQuery and extracts the names of all tables which should be loaded before a specified table by its create query.
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

    using Visitor = ConstInDepthNodeVisitor<DDLDependencyVisitor, true>;

    static void visit(const ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
};

}
