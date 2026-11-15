#pragma once
#include <Core/QualifiedTableName.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>


namespace DB
{

class ASTConstraintDeclaration;
class ASTFunction;
class ASTFunctionWithKeyValueArguments;
class ASTStorage;

using TableNamesSet = std::unordered_set<QualifiedTableName>;

/// Returns a list of all tables which should be loaded before a specified table.
/// For example, a local ClickHouse table should be loaded before a dictionary which uses that table as its source.
/// Does not validate AST, works a best-effort way.
/// `current_database` is the database against which the unqualified table names of the CREATE query
/// have to be resolved: the current database of the query for a freshly executed CREATE, or the
/// database owning the table for the metadata loaded at startup.
TableNamesSet getLoadingDependenciesFromCreateQuery(ContextPtr global_context, const QualifiedTableName & table, const ASTPtr & ast, const String & current_database, bool can_throw = false);


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
        /// The default database of the server, used where a nested query is executed with the global
        /// context rather than with the context of the CREATE query.
        String default_database;
        /// The database against which the unqualified table names of the CREATE query are resolved.
        String current_database;
        TableNamesSet dependencies;
        ContextPtr global_context;
        ASTPtr create_query;
        QualifiedTableName table_name;
        bool can_throw{};
    };

    using Visitor = ConstInDepthNodeVisitor<DDLLoadingDependencyVisitor, true>;

    static void visit(const ASTPtr & ast, Data & data);

private:
    static void visit(const ASTFunction & function, Data & data);
    static void visit(const ASTFunctionWithKeyValueArguments & dict_source, Data & data);
    static void visit(const ASTStorage & storage, Data & data);
    static void visit(const ASTConstraintDeclaration & constraint, Data & data);

    static void addDependenciesOfExecutedSubqueries(const ASTPtr & ast, Data & data);
    static void extractTableNameFromArgument(const ASTFunction & function, Data & data, size_t arg_idx);
};

}
