#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include "PostgreSQLReplicationHandler.h"
#include "MaterializePostgreSQLSettings.h"

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <ext/shared_ptr_helper.h>
#include <memory>


namespace DB
{

/** Case of single MaterializePostgreSQL table engine.
 *
 * A user creates a table with engine MaterializePostgreSQL. Order by expression must be specified (needed for
 * nested ReplacingMergeTree table). This storage owns its own replication handler, which loads table data
 * from PostgreSQL into nested ReplacingMergeTree table. If table is not created, but attached, replication handler
 * will not start loading-from-snapshot procedure, instead it will continue from last commited lsn.
 *
 * Main point: Both tables exist on disk; database engine interacts only with the main table and main table takes
 * total ownershot over nested table. Nested table has name `main_table_uuid` + NESTED_SUFFIX.
 *
 * TODO: a check is needed for existance of nested, now this case is checked via replication slot existance.
**/


/** Case of MaterializePostgreSQL database engine.
 *
 * MaterializePostgreSQL table exists only in memory and acts as a wrapper for nested table, i.e. only provides an
 * interface to work with nested table. Both tables share the same StorageID.
 *
 * Main table is never created or droppped via database method. The only way database engine interacts with
 * MaterializePostgreSQL table - in tryGetTable() method, a MaterializePostgreSQL table is returned in order to wrap
 * and redirect read requests. Set of such wrapper-tables is cached inside database engine. All other methods in
 * regard to materializePostgreSQL table are handled by replication handler.
 *
 * All database methods, apart from tryGetTable(), are devoted only to nested table.
 * TODO: It makes sence to allow rename method for MaterializePostgreSQL table via database method.
 * TODO: Make sure replication-to-table data channel is done only by relation_id.
 *
 * Also main table has the same InMemoryMetadata as its nested table, so if metadata of nested table changes - main table also has
 * to update its metadata, because all read requests are passed to MaterializePostgreSQL table and then it redirects read
 * into nested table.
 *
 * When there is a need to update table structure, there will be created a new MaterializePostgreSQL table with its own nested table,
 * it will have updated table schema and all data will be loaded from scratch in the background, while previous table with outadted table
 * structure will still serve read requests. When data is loaded, nested tables will be swapped, metadata of metarialzied table will be
 * updated according to nested table.
 *
**/

class StorageMaterializePostgreSQL final : public ext::shared_ptr_helper<StorageMaterializePostgreSQL>, public IStorage, WithContext
{
    friend struct ext::shared_ptr_helper<StorageMaterializePostgreSQL>;

public:
    StorageMaterializePostgreSQL(const StorageID & table_id_, ContextPtr context_);

    StorageMaterializePostgreSQL(StoragePtr nested_table, ContextPtr context);

    String getName() const override { return "MaterializePostgreSQL"; }

    void startup() override;

    void shutdown() override;

    /// Used only for single MaterializePostgreSQL storage.
    void dropInnerTableIfAny(bool no_delay, ContextPtr local_context) override;

    NamesAndTypesList getVirtuals() const override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    bool hasNested() { return has_nested.load(); }

    void createNestedIfNeeded(PostgreSQLTableStructurePtr table_structure);

    StoragePtr getNested() const;

    StoragePtr tryGetNested() const;

    StoragePtr createTemporary() const;

    ContextPtr getNestedTableContext() const { return nested_context; }

    void renameNested();

    StorageID getNestedStorageID() const;

    void setNestedStorageID(const StorageID & id) { nested_table_id.emplace(id); }

    static std::shared_ptr<Context> makeNestedTableContext(ContextPtr from_context);

    StoragePtr prepare();

protected:
    StorageMaterializePostgreSQL(
        const StorageID & table_id_,
        bool is_attach_,
        const String & remote_database_name,
        const String & remote_table_name,
        const postgres::ConnectionInfo & connection_info,
        const StorageInMemoryMetadata & storage_metadata,
        ContextPtr context_,
        std::unique_ptr<MaterializePostgreSQLSettings> replication_settings);

private:
    static std::shared_ptr<ASTColumnDeclaration> getMaterializedColumnsDeclaration(
            const String name, const String type, UInt64 default_value);

    ASTPtr getColumnDeclaration(const DataTypePtr & data_type) const;

    ASTPtr getCreateNestedTableQuery(PostgreSQLTableStructurePtr table_structure);

    String getNestedTableName() const;

    /// Not nullptr only for single MaterializePostgreSQL storage, because for MaterializePostgreSQL
    /// database engine there is one replication handler for all tables.
    std::unique_ptr<PostgreSQLReplicationHandler> replication_handler;

    /// Distinguish between single MaterilizePostgreSQL table engine and MaterializePostgreSQL database engine,
    /// because table with engine MaterilizePostgreSQL acts differently in each case.
    bool is_materialize_postgresql_database = false;

    /// Will be set to `true` only once - when nested table was loaded by replication thread.
    /// After that, it will never be changed. Needed for MaterializePostgreSQL database engine
    /// because there is an invariant - table exists only if its nested table exists, but nested
    /// table is not loaded immediately. It is made atomic, because it is accessed only by database engine,
    /// and updated by replication handler (only once).
    std::atomic<bool> has_nested = false;

    /// Nested table context is a copy of global context, but contains query context with defined
    /// ReplacingMergeTree storage in factoriesLog. This is needed to let database engine know
    /// whether to access nested table or a wrapper over nested (materialized table).
    ContextPtr nested_context;

    std::optional<StorageID> nested_table_id;

    /// Needed only for the case of single MaterializePostgreSQL storage - in order to make
    /// delayed storage forwarding into replication handler.
    String remote_table_name;

    bool is_attach;
};

}

#endif
