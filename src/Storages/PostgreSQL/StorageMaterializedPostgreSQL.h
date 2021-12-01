#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include "PostgreSQLReplicationHandler.h"
#include "MaterializedPostgreSQLSettings.h"

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <common/shared_ptr_helper.h>
#include <memory>


namespace DB
{

/** Case of single MaterializedPostgreSQL table engine.
 *
 * A user creates a table with engine MaterializedPostgreSQL. Order by expression must be specified (needed for
 * nested ReplacingMergeTree table). This storage owns its own replication handler, which loads table data
 * from PostgreSQL into nested ReplacingMergeTree table. If table is not created, but attached, replication handler
 * will not start loading-from-snapshot procedure, instead it will continue from last committed lsn.
 *
 * Main point: Both tables exist on disk; database engine interacts only with the main table and main table takes
 * total ownershot over nested table. Nested table has name `main_table_uuid` + NESTED_SUFFIX.
 *
**/


/** Case of MaterializedPostgreSQL database engine.
 *
 * MaterializedPostgreSQL table exists only in memory and acts as a wrapper for nested table, i.e. only provides an
 * interface to work with nested table. Both tables share the same StorageID.
 *
 * Main table is never created or dropped via database method. The only way database engine interacts with
 * MaterializedPostgreSQL table - in tryGetTable() method, a MaterializedPostgreSQL table is returned in order to wrap
 * and redirect read requests. Set of such wrapper-tables is cached inside database engine. All other methods in
 * regard to materializePostgreSQL table are handled by replication handler.
 *
 * All database methods, apart from tryGetTable(), are devoted only to nested table.
 * NOTE: It makes sense to allow rename method for MaterializedPostgreSQL table via database method.
 * TODO: Make sure replication-to-table data channel is done only by relation_id.
 *
 * Also main table has the same InMemoryMetadata as its nested table, so if metadata of nested table changes - main table also has
 * to update its metadata, because all read requests are passed to MaterializedPostgreSQL table and then it redirects read
 * into nested table.
 *
 * When there is a need to update table structure, there will be created a new MaterializedPostgreSQL table with its own nested table,
 * it will have updated table schema and all data will be loaded from scratch in the background, while previous table with outadted table
 * structure will still serve read requests. When data is loaded, nested tables will be swapped, metadata of metarialzied table will be
 * updated according to nested table.
 *
**/

class StorageMaterializedPostgreSQL final : public shared_ptr_helper<StorageMaterializedPostgreSQL>, public IStorage, WithContext
{
    friend struct shared_ptr_helper<StorageMaterializedPostgreSQL>;

public:
    StorageMaterializedPostgreSQL(const StorageID & table_id_, ContextPtr context_);

    StorageMaterializedPostgreSQL(StoragePtr nested_storage_, ContextPtr context_);

    String getName() const override { return "MaterializedPostgreSQL"; }

    void startup() override;

    void shutdown() override;

    /// Used only for single MaterializedPostgreSQL storage.
    void dropInnerTableIfAny(bool no_delay, ContextPtr local_context) override;

    NamesAndTypesList getVirtuals() const override;

    bool needRewriteQueryWithFinal(const Names & column_names) const override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    /// This method is called only from MateriaizePostgreSQL database engine, because it needs to maintain
    /// an invariant: a table exists only if its nested table exists. This atomic variable is set to _true_
    /// only once - when nested table is successfully created and is never changed afterwards.
    bool hasNested() { return has_nested.load(); }

    void createNestedIfNeeded(PostgreSQLTableStructurePtr table_structure);

    StoragePtr getNested() const;

    StoragePtr tryGetNested() const;

    /// Create a temporary MaterializedPostgreSQL table with current_table_name + TMP_SUFFIX.
    /// An empty wrapper is returned - it does not have inMemory metadata, just acts as an empty wrapper over
    /// temporary nested, which will be created shortly after.
    StoragePtr createTemporary() const;

    ContextPtr getNestedTableContext() const { return nested_context; }

    StorageID getNestedStorageID() const;

    void setNestedStorageID(const StorageID & id) { nested_table_id.emplace(id); }

    static std::shared_ptr<Context> makeNestedTableContext(ContextPtr from_context);

    /// Get nested table (or throw if it does not exist), set in-memory metadata (taken from nested table)
    /// for current table, set has_nested = true.
    StoragePtr prepare();

    bool supportsFinal() const override { return true; }

protected:
    StorageMaterializedPostgreSQL(
        const StorageID & table_id_,
        bool is_attach_,
        const String & remote_database_name,
        const String & remote_table_name,
        const postgres::ConnectionInfo & connection_info,
        const StorageInMemoryMetadata & storage_metadata,
        ContextPtr context_,
        std::unique_ptr<MaterializedPostgreSQLSettings> replication_settings);

private:
    static std::shared_ptr<ASTColumnDeclaration> getMaterializedColumnsDeclaration(
            const String name, const String type, UInt64 default_value);

    ASTPtr getColumnDeclaration(const DataTypePtr & data_type) const;

    ASTPtr getCreateNestedTableQuery(PostgreSQLTableStructurePtr table_structure);

    String getNestedTableName() const;

    /// Not nullptr only for single MaterializedPostgreSQL storage, because for MaterializedPostgreSQL
    /// database engine there is one replication handler for all tables.
    std::unique_ptr<PostgreSQLReplicationHandler> replication_handler;

    /// Distinguish between single MaterilizePostgreSQL table engine and MaterializedPostgreSQL database engine,
    /// because table with engine MaterilizePostgreSQL acts differently in each case.
    bool is_materialized_postgresql_database = false;

    /// Will be set to `true` only once - when nested table was loaded by replication thread.
    /// After that, it will never be changed. Needed for MaterializedPostgreSQL database engine
    /// because there is an invariant - table exists only if its nested table exists, but nested
    /// table is not loaded immediately. It is made atomic, because it is accessed only by database engine,
    /// and updated by replication handler (only once).
    std::atomic<bool> has_nested = false;

    /// Nested table context is a copy of global context, but modified to answer isInternalQuery() == true.
    /// This is needed to let database engine know whether to access nested table or a wrapper over nested (materialized table).
    ContextMutablePtr nested_context;

    /// Save nested storageID to be able to fetch it. It is set once nested is created and will be
    /// updated only when nested is reloaded or renamed.
    std::optional<StorageID> nested_table_id;

    /// Needed only for the case of single MaterializedPostgreSQL storage - in order to make
    /// delayed storage forwarding into replication handler.
    String remote_table_name;

    /// Needed only for the case of single MaterializedPostgreSQL storage, because in case of create
    /// query (not attach) initial setup will be done immediately and error message is thrown at once.
    /// It results in the fact: single MaterializedPostgreSQL storage is created only if its nested table is created.
    /// In case of attach - this setup will be done in a separate thread in the background. It will also
    /// be checked for nested table and attempted to load it if it does not exist for some reason.
    bool is_attach = true;
};

}

#endif
