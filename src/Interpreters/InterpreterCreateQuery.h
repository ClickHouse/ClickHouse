#pragma once
#include "config.h"

#include <Core/NamesAndAliases.h>
#include <Core/QualifiedTableName.h>
#include <Access/Common/AccessRightsElement.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/IInterpreter.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>


namespace DB
{

class ASTCreateQuery;
class ASTColumnDeclaration;
class ASTExpressionList;
class ASTStorage;
class IDatabase;
class DDLGuard;
using DatabasePtr = std::shared_ptr<IDatabase>;
using DDLGuardPtr = std::unique_ptr<DDLGuard>;


/** Allows to create new table or database,
  *  or create an object for existing table or database.
  */
class InterpreterCreateQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    BlockIO execute() override;

    /// List of columns and their types in AST.
    static ASTPtr formatColumns(const NamesAndTypesList & columns);
    static ASTPtr formatColumns(const NamesAndTypesList & columns, const NamesAndAliases & alias_columns);
    static ASTPtr formatColumns(const ColumnsDescription & columns);
    static ASTPtr formatIndices(const IndicesDescription & indices);
    static ASTPtr formatConstraints(const ConstraintsDescription & constraints);
    static ASTPtr formatProjections(const ProjectionsDescription & projections);

    void setForceRestoreData(bool has_force_restore_data_flag_)
    {
        has_force_restore_data_flag = has_force_restore_data_flag_;
    }

    void setInternal(bool internal_)
    {
        internal = internal_;
    }

    void setForceAttach(bool force_attach_)
    {
        force_attach = force_attach_;
    }

    void setLoadDatabaseWithoutTables(bool load_database_without_tables_)
    {
        load_database_without_tables = load_database_without_tables_;
    }

    void setDontNeedDDLGuard()
    {
        need_ddl_guard = false;
    }

    void setIsRestoreFromBackup(bool is_restore_from_backup_)
    {
        is_restore_from_backup = is_restore_from_backup_;
    }

    static DataTypePtr getColumnType(const ASTColumnDeclaration & col_decl, LoadingStrictnessLevel mode, bool make_columns_nullable);

    /// Obtain information about columns, their types, default values and column comments,
    ///  for case when columns in CREATE query is specified explicitly.
    /// check_defaults_over_virtual_columns rejects DEFAULT/MATERIALIZED expressions over virtual columns;
    /// pass false for objects that never evaluate their own column defaults over an insert block
    /// (ordinary views and external-target materialized views).
    static ColumnsDescription getColumnsDescription(const ASTExpressionList & columns, ContextPtr context, LoadingStrictnessLevel mode, bool is_restore_from_backup = false, bool check_defaults_over_virtual_columns = true);
    static ConstraintsDescription
    getConstraintsDescription(const ASTExpressionList * constraints, const ColumnsDescription & columns, ContextPtr local_context);

    static void prepareOnClusterQuery(ASTCreateQuery & create, ContextPtr context, const String & cluster_name);

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr) const override;

    /// Check access right, validate definer statement and replace `CURRENT USER` with actual name.
    static void processSQLSecurityOption(ContextMutablePtr context_, ASTSQLSecurity & sql_security, bool is_materialized_view = false, LoadingStrictnessLevel mode = LoadingStrictnessLevel::CREATE);

private:
    struct TableProperties
    {
        ColumnsDescription columns;
        IndicesDescription indices;
        ConstraintsDescription constraints;
        ProjectionsDescription projections;
        bool columns_inferred_from_select_query = false;
    };

    BlockIO createDatabase(ASTCreateQuery & create);
    BlockIO createTable(ASTCreateQuery & create);

    /// Calculate list of columns, constraints, indices, etc... of table. Rewrite query in canonical way.
    TableProperties getTablePropertiesAndNormalizeCreateQuery(ASTCreateQuery & create, LoadingStrictnessLevel mode);
    void validateTableStructure(const ASTCreateQuery & create, const TableProperties & properties) const;
    void validateMaterializedViewColumnsAndEngine(const ASTCreateQuery & create, const TableProperties & properties, const DatabasePtr & database);
    void setEngine(ASTCreateQuery & create) const;
    AccessRightsElements getRequiredAccess() const;

    /// Create IStorage and add it to database. If table already exists and IF NOT EXISTS specified, do nothing and return false.
    bool doCreateTable(ASTCreateQuery & create, const TableProperties & properties, DDLGuardPtr & ddl_guard, LoadingStrictnessLevel mode);
    BlockIO doCreateOrReplaceTable(ASTCreateQuery & create, const InterpreterCreateQuery::TableProperties & properties, LoadingStrictnessLevel mode);
    BlockIO doCreateOrReplaceTemporaryTable(ASTCreateQuery & create, const InterpreterCreateQuery::TableProperties & properties, LoadingStrictnessLevel mode);
#if CLICKHOUSE_CLOUD
    /// Converts the "*MergeTree" table engine to "Replicated*MergeTree" or "Shared*MergeTree" if the corresponding settings are enabled.
    void convertTableEngineForCloud(ASTStorage & table_engine, TableProperties & properties) const;
#endif
    /// Inserts data in created table if it's CREATE ... SELECT (or attaches the source partitions if it's
    /// CREATE ... CLONE AS). `published_table_name`, when not empty, is the user-visible name the table
    /// being filled will be published under: the table itself carries the internal `_tmp_replace_*` name of
    /// `doCreateOrReplaceTable`, so the fill is authorized against `published_table_name` instead.
    BlockIO fillTableIfNeeded(const ASTCreateQuery & create, const String & published_table_name = {});

    /// Whether this CREATE MATERIALIZED VIEW ... POPULATE should be populated atomically: the feature
    /// setting is enabled and the query is an immediate INSERT SELECT into a non-window, non-clone view.
    bool shouldPopulateMaterializedViewAtomically(const ASTCreateQuery & create) const;

    /// The name of the single source table an atomically populated materialized view would be subscribed
    /// to (the table of its FROM clause), or std::nullopt when the view has no such single source. Pure
    /// AST analysis - it does not touch the catalog, so it can be called before the view is created.
    std::optional<QualifiedTableName> tryGetAtomicPopulateSourceName(const ASTCreateQuery & create) const;

    /// Resolves the single source table that an atomically populated materialized view is subscribed to,
    /// if it can provide a pinned point-in-time snapshot. Returns nullptr when atomic population does not
    /// apply: the view has no single source table, or the source cannot be pinned (a view, `Distributed`,
    /// `Merge`, `Log` family, or a table not in an `Atomic` database). In the last case it logs a warning;
    /// the caller then falls back to the legacy non-atomic population. Throws UNKNOWN_TABLE when the
    /// source, which existed when the view's SELECT was validated, is gone - dropped, renamed or exchanged
    /// away before the caller acquired the source-name DDL guard; the caller runs inside
    /// fillMaterializedViewAtomically's rollback scope, so the just-published view is dropped rather than
    /// left behind by a legacy population that would fail on the vanished name outside that scope.
    StoragePtr getValidatedAtomicPopulateSource(const ASTCreateQuery & create);

    /// Atomically populate a freshly created materialized view: subscribe the view to new inserts of its
    /// source table and capture a pinned snapshot of the existing source data together, under a brief
    /// exclusive lock on the source, then populate the view from that snapshot without holding the lock.
    /// This guarantees every row inserted concurrently with the population is delivered to the view exactly
    /// once. Returns std::nullopt when atomic population does not apply - there is no single source table to
    /// subscribe to, or it cannot provide a pinned snapshot (see getValidatedAtomicPopulateSource); the
    /// caller then falls back to the regular, non-atomic path.
    ///
    /// The view itself was already created and started by doCreateTable before this runs, so on any failure
    /// here - an exclusive-lock timeout on a busy source, or a runtime failure of the population itself,
    /// which executes eagerly inside this scope - the just-created view is dropped before the exception is
    /// rethrown. Otherwise the failed CREATE would leave behind a view that is not registered as a dependent
    /// of its source, which future inserts would silently never populate - or, for an execution-time
    /// failure, a subscribed view with partial data that a retry would refuse to re-create.
    ///
    /// `ddl_guard` is the guard of the view being created, still held by the caller. It is kept until the
    /// view is subscribed to its source and released before the (potentially long) population runs, so that
    /// concurrent DDL on the view name cannot slip in between publishing the view and subscribing it.
    ///
    /// `source_ddl_guard` is the guard of the source table's name, held by the caller across the cut for
    /// the same reason on the source side: without it, a concurrent RENAME or EXCHANGE of the source could
    /// change the owner of the name between resolving the source and registering the subscription (which
    /// is keyed by name), wiring the view to one table while backfilling it from another, or leaving the
    /// subscription on a name nobody owns. It is likewise released before the population runs.
    std::optional<BlockIO> fillMaterializedViewAtomically(const ASTCreateQuery & create, DDLGuardPtr & ddl_guard, DDLGuardPtr & source_ddl_guard);

    /// The body of fillMaterializedViewAtomically; the wrapper adds the drop-on-failure rollback.
    std::optional<BlockIO> fillMaterializedViewAtomicallyImpl(const ASTCreateQuery & create, DDLGuardPtr & ddl_guard, DDLGuardPtr & source_ddl_guard);

    void assertOrSetUUID(ASTCreateQuery & create, const DatabasePtr & database) const;

    /// Update create query with columns description from storage if query doesn't have it.
    /// It's used to prevent automatic schema inference while table creation on each server startup.
    void addColumnsDescriptionToCreateQueryIfNecessary(ASTCreateQuery & create, const StoragePtr & storage);

    BlockIO executeQueryOnCluster(ASTCreateQuery & create);

    void convertMergeTreeTableIfPossible(ASTCreateQuery & create, DatabasePtr database, bool to_replicated);

    /// Remove transaction metadata files (txn_version.txt and txn_version.txt.tmp) from all parts for a table.
    static void clearTransactionMetadata(const String & table_data_path, ContextPtr local_context);

    void throwIfTooManyEntities(ASTCreateQuery & create) const;
#if CLICKHOUSE_CLOUD
    static bool allowPreserveEngine(ASTStorage & storage, ContextPtr context_);
#endif

    ASTPtr query_ptr;

    /// Skip safety threshold when loading tables.
    bool has_force_restore_data_flag = false;
    /// Is this an internal query - not from the user.
    bool internal = false;
    bool force_attach = false;
    bool load_database_without_tables = false;
    bool need_ddl_guard = true;
    bool is_restore_from_backup = false;

    String as_database_saved;
    String as_table_saved;
};
}
