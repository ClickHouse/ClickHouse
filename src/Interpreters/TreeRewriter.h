#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Aliases.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class ASTFunction;
struct ASTTablesInSelectQueryElement;
class TableJoin;
struct Settings;
struct SelectQueryOptions;
using Scalars = std::map<String, Block>;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;
struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<const StorageSnapshot>;

struct TreeRewriterResult
{
    ConstStoragePtr storage;
    StorageSnapshotPtr storage_snapshot;
    std::shared_ptr<TableJoin> analyzed_join;
    const ASTTablesInSelectQueryElement * ast_join = nullptr;

    NamesAndTypesList source_columns;
    NameSet source_columns_set; /// Set of names of source_columns.
    /// Set of columns that are enough to read from the table to evaluate the expression. It does not include joined columns.
    NamesAndTypesList required_source_columns;
    /// Same as above but also record alias columns which are expanded. This is for RBAC access check.
    Names required_source_columns_before_expanding_alias_columns;

    /// Set of alias columns that are expanded to their alias expressions. We still need the original columns to check access permission.
    NameSet expanded_aliases;

    Aliases aliases;
    std::vector<const ASTFunction *> aggregates;

    std::vector<const ASTFunction *> window_function_asts;

    /// Which column is needed to be ARRAY-JOIN'ed to get the specified.
    /// For example, for `SELECT s.v ... ARRAY JOIN a AS s` will get "s.v" -> "a.v".
    NameToNameMap array_join_result_to_source;

    /// For the ARRAY JOIN section, mapping from the alias to the full column name.
    /// For example, for `ARRAY JOIN [1,2] AS b` "b" -> "array(1,2)" will enter here.
    /// Note: not used further.
    NameToNameMap array_join_alias_to_name;

    /// The backward mapping for array_join_alias_to_name.
    /// Note: not used further.
    NameToNameMap array_join_name_to_alias;

    /// Predicate optimizer overrides the sub queries
    bool rewrite_subqueries = false;

    /// Whether the query contains explicit columns like "SELECT column1 + column2 FROM table1".
    /// Queries like "SELECT count() FROM table1", "SELECT 1" don't contain explicit columns.
    bool has_explicit_columns = false;

    /// Whether it's possible to use the trivial count optimization,
    /// i.e. use a fast call of IStorage::totalRows() (or IStorage::totalRowsByPartitionPredicate())
    /// instead of actual retrieving columns and counting rows.
    bool optimize_trivial_count = false;

    /// Cache isRemote() call for storage, because it may be too heavy.
    bool is_remote_storage = false;

    /// Rewrite _shard_num to shardNum()
    bool has_virtual_shard_num = false;

    /// Results of scalar sub queries
    Scalars scalars;
    Scalars local_scalars;

    explicit TreeRewriterResult(
        const NamesAndTypesList & source_columns_,
        ConstStoragePtr storage_ = {},
        const StorageSnapshotPtr & storage_snapshot_ = {},
        bool add_special = true);

    void collectSourceColumns(bool add_special);
    void collectUsedColumns(const ASTPtr & query, bool is_select);
    Names requiredSourceColumns() const { return required_source_columns.getNames(); }
    const Names & requiredSourceColumnsForAccessCheck() const { return required_source_columns_before_expanding_alias_columns; }
    NameSet getArrayJoinSourceNameSet() const;
    const Scalars & getScalars() const { return scalars; }
};

using TreeRewriterResultPtr = std::shared_ptr<const TreeRewriterResult>;

/// Tree Rewriter in terms of CMU slides @sa https://15721.courses.cs.cmu.edu/spring2020/slides/19-optimizer1.pdf
///
/// Optimises AST tree and collect information for further expression analysis in ExpressionAnalyzer.
/// Result AST has the following invariants:
///  * all aliases are substituted
///  * qualified names are translated
///  * scalar subqueries are executed replaced with constants
///  * unneeded columns are removed from SELECT clause
///  * duplicated columns are removed from ORDER BY, LIMIT BY, USING(...).
class TreeRewriter : WithContext
{
public:
    explicit TreeRewriter(ContextPtr context_) : WithContext(context_) {}

    /// Analyze and rewrite not select query
    TreeRewriterResultPtr analyze(
        ASTPtr & query,
        const NamesAndTypesList & source_columns_,
        ConstStoragePtr storage = {},
        const StorageSnapshotPtr & storage_snapshot = {},
        bool allow_aggregations = false,
        bool allow_self_aliases = true,
        bool execute_scalar_subqueries = true) const;

    /// Analyze and rewrite select query
    TreeRewriterResultPtr analyzeSelect(
        ASTPtr & query,
        TreeRewriterResult && result,
        const SelectQueryOptions & select_options = {},
        const std::vector<TableWithColumnNamesAndTypes> & tables_with_columns = {},
        const Names & required_result_columns = {},
        std::shared_ptr<TableJoin> table_join = {}) const;

private:
    static void normalize(ASTPtr & query, Aliases & aliases, const NameSet & source_columns_set, bool ignore_alias, const Settings & settings, bool allow_self_aliases, ContextPtr context_);
};

}
