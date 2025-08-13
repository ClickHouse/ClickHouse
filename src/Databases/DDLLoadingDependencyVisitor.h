#pragma once
#include <Core/QualifiedTableName.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>


namespace DB
{

class ASTFunction;
class ASTFunctionWithKeyValueArguments;
class ASTStorage;

using TableNamesSet = std::unordered_set<QualifiedTableName>;

/// Returns a list of all tables which should be loaded before a specified table.
/// For example, a local ClickHouse table should be loaded before a dictionary which uses that table as its source.
/// Does not validate AST, works a best-effort way.
TableNamesSet getLoadingDependenciesFromCreateQuery(ContextPtr global_context, const QualifiedTableName & table, const ASTPtr & ast);


class DDLMatcherBase
{
public:
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
    static ssize_t getPositionOfTableNameArgumentToVisit(const ASTFunction & function);
    static ssize_t getPositionOfTableNameArgumentToEvaluate(const ASTFunction & function);
};

/// Visits ASTCreateQuery and extracts the names of all tables which should be loaded before a specified table.
/// TODO: Combine this class with DDLDependencyVisitor (because loading dependencies are a subset of referential dependencies).
class DDLLoadingDependencyVisitor : public DDLMatcherBase
{
public:
    struct Data
    {
        String default_database;
        TableNamesSet dependencies;
        ContextPtr global_context;
        ASTPtr create_query;
        QualifiedTableName table_name;
    };

    using Visitor = ConstInDepthNodeVisitor<DDLLoadingDependencyVisitor, true>;

    static void visit(const ASTPtr & ast, Data & data);

private:
    static void visit(const ASTFunction & function, Data & data);
    static void visit(const ASTFunctionWithKeyValueArguments & dict_source, Data & data);
    static void visit(const ASTStorage & storage, Data & data);

    static void extractTableNameFromArgument(const ASTFunction & function, Data & data, size_t arg_idx);
};

}
