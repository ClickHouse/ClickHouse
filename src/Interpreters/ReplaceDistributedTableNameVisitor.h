#pragma once

#include <vector>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/IAST.h>
#include <Storages/StorageDistributed.h>


namespace DB
{

class IStorage;

using ConstStoragePtr = std::shared_ptr<const IStorage>;
using StoragePtr = std::shared_ptr<IStorage>;

class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;

/// Replace distributed table tame to local name. And collect table storage, cluster and sharding key infos.
class ReplaceDistributedTableNameVisitor
{
public:
    struct Scope;
    struct TableHash;
    using ScopePtr = std::shared_ptr<Scope>;

    /// Represent query scope for an ASTSelectQuery which does not include subqueries.
    /// Contains only distributed tables with db.table format.
    using Tables = std::vector<StorageID>;
    /// Distributed table to local table mapping.
    using TablesMap = std::unordered_map<StorageID, StorageID, TableHash>;

    struct TableHash
    {
        std::size_t operator()(const StorageID & table) const
        {
            return std::hash<String>()(table.database_name) ^ std::hash<String>()(table.table_name);
        }
    };

    struct Scope
    {
        /// Used to find db for 'tbl.col' style identifiers
        Tables tables;
        TablesMap tables_map;
        /// Parent select query scope: used to correlated subqueries
        ScopePtr parent_scope;

        /// Get db name for a distributed table name.
        /// Invoker may pass a local table, and will get null.
        std::optional<String> getDBName(const String & table_name);
        std::optional<StorageID> getLocalTable(const StorageID & table_name);

        void clear();
    };

    /// Storage list for query
    std::vector<StorageID> storages;
    /// Cluster list for storages
    std::vector<ClusterPtr> clusters;
    /// Sharding key columns for storages
    std::vector<String> sharding_keys;

    bool has_distributed_table = false;
    bool has_local_table = false;
    bool has_table_function = false;
    bool has_non_merge_tree_table = false; /// TODO remove

    explicit ReplaceDistributedTableNameVisitor(ContextPtr context_) : context(context_) { }

    void visit(ASTPtr & ast);

private:
    void visitImpl(ASTPtr & ast, ScopePtr & scope);

    void enter(ASTPtr & ast, ScopePtr & scope);
    void leave(ASTPtr & ast, ScopePtr & scope);

    bool needChildVisit(ASTPtr &, ASTPtr &) { return true; }

    void visitChildren(ASTPtr & ast, ScopePtr & scope)
    {
        for (auto & child : ast->children)
            if (needChildVisit(ast, child))
                visitImpl(child, scope);
    }

    void enter(ASTSelectQuery & select_query, ScopePtr & scope);
    void leave(ASTSelectQuery & select_query, ScopePtr & scope);

    void enter(ASTTableIdentifier & table_ident, ScopePtr & scope);
    std::shared_ptr<ASTTableIdentifier> enter(ASTFunction & table_function, ASTPtr & table_function_ast, ScopePtr & scope);
    void enter(ASTIdentifier & ident, ScopePtr & scope);

    ContextPtr context;
};


}
