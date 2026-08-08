#include <Storages/PostgreSQL/StorageMaterializedPostgreSQL.h>
#include <Storages/PostgreSQL/MaterializedPostgreSQLSettings.h>
#include <Core/UUID.h>

#if USE_LIBPQXX
#include <Common/logger_useful.h>
#include <Common/FailPoint.h>

#include <Common/Macros.h>
#include <Common/parseAddress.h>
#include <Common/assert_cast.h>

#include <Core/Settings.h>
#include <Core/PostgreSQL/Connection.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>

#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <DataTypes/dataTypeToAST.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>

#include <Databases/LoadingStrictnessLevel.h>

#include <Interpreters/applyTableOverride.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/Context.h>

#include <Storages/StorageFactory.h>
#include <Storages/ReadFinalForExternalReplicaStorage.h>
#include <Storages/StoragePostgreSQL.h>

#include <QueryPipeline/Pipe.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_materialized_postgresql_table;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUInt64 postgresql_connection_attempt_timeout;
}

namespace MaterializedPostgreSQLSetting
{
    extern const MaterializedPostgreSQLSettingsString materialized_postgresql_tables_list;
    extern const MaterializedPostgreSQLSettingsBool materialized_postgresql_use_extended_date_and_time_types;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_BACKUP_TABLE;
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char materialized_postgresql_fail_nested_table_drop[];
}


/// For the case of single storage.
StorageMaterializedPostgreSQL::StorageMaterializedPostgreSQL(
    const StorageID & table_id_,
    LoadingStrictnessLevel mode,
    const String & remote_database_name,
    const String & remote_table_name_,
    const postgres::ConnectionInfo & connection_info,
    const StorageInMemoryMetadata & storage_metadata,
    ContextPtr context_,
    std::unique_ptr<MaterializedPostgreSQLSettings> replication_settings)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , log(getLogger("StorageMaterializedPostgreSQL(" + postgres::formatNameForLogs(remote_database_name, remote_table_name_) + ")"))
    , is_materialized_postgresql_database(false)
    , has_nested(false)
    , nested_context(makeNestedTableContext(context_->getGlobalContext()))
    , nested_table_id(StorageID(table_id_.database_name, getNestedTableName()))
    , remote_table_name(remote_table_name_)
    , is_attach(mode >= LoadingStrictnessLevel::ATTACH)
{
    if (table_id_.uuid == UUIDHelpers::Nil)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage MaterializedPostgreSQL is allowed only for Atomic database");

    setInMemoryMetadata(storage_metadata.withVirtuals(createVirtuals()));

    (*replication_settings)[MaterializedPostgreSQLSetting::materialized_postgresql_tables_list] = remote_table_name_;

    replication_handler = std::make_unique<PostgreSQLReplicationHandler>(
            remote_database_name,
            remote_table_name_,
            table_id_.database_name,
            toString(table_id_.uuid),
            connection_info,
            getContext(),
            is_attach,
            *replication_settings,
            /* is_materialized_postgresql_database */false);

    replication_handler->addStorage(remote_table_name, this);
    replication_handler->startup(/* delayed */is_attach);
}


/// For the case of MaterializePosgreSQL database engine.
/// It is used when nested ReplacingMergeeTree table has not yet be created by replication thread.
/// In this case this storage can't be used for read queries.
StorageMaterializedPostgreSQL::StorageMaterializedPostgreSQL(
        const StorageID & table_id_,
        ContextPtr context_,
        const String & postgres_database_name,
        const String & postgres_table_name)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , log(getLogger("StorageMaterializedPostgreSQL(" + postgres::formatNameForLogs(postgres_database_name, postgres_table_name) + ")"))
    , is_materialized_postgresql_database(true)
    , has_nested(false)
    , nested_context(makeNestedTableContext(context_->getGlobalContext()))
    , nested_table_id(table_id_)
{
}


/// Constructor for MaterializedPostgreSQL table engine - for the case of MaterializePosgreSQL database engine.
/// It is used when nested ReplacingMergeTree table has already been created by replication thread.
/// This storage is ready to handle read queries.
StorageMaterializedPostgreSQL::StorageMaterializedPostgreSQL(
        StoragePtr nested_storage_,
        ContextPtr context_,
        const String & postgres_database_name,
        const String & postgres_table_name)
    : IStorage(StorageID(nested_storage_->getStorageID().database_name, nested_storage_->getStorageID().table_name))
    , WithContext(context_->getGlobalContext())
    , log(getLogger("StorageMaterializedPostgreSQL(" + postgres::formatNameForLogs(postgres_database_name, postgres_table_name) + ")"))
    , is_materialized_postgresql_database(true)
    , has_nested(true)
    , nested_context(makeNestedTableContext(context_->getGlobalContext()))
    , nested_table_id(nested_storage_->getStorageID())
{
    auto nested_metadata = nested_storage_->getInMemoryMetadataPtr(context_, false);
    setInMemoryMetadata(*nested_metadata);
}

VirtualColumnsDescription StorageMaterializedPostgreSQL::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_sign", std::make_shared<DataTypeInt8>(), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_version", std::make_shared<DataTypeUInt64>(), "", VirtualsMaterializationPlace::Reader);
    return desc;
}

/// A temporary clone table might be created for current table in order to update its schema and reload
/// all data in the background while current table will still handle read requests.
StoragePtr StorageMaterializedPostgreSQL::createTemporary() const
{
    auto table_id = getStorageID();
    auto tmp_table_id = StorageID(table_id.database_name, table_id.table_name + TMP_SUFFIX);

    /// If for some reason it already exists - drop it.
    auto tmp_storage = DatabaseCatalog::instance().tryGetTable(tmp_table_id, nested_context);
    if (tmp_storage)
    {
        LOG_TRACE(getLogger("MaterializedPostgreSQLStorage"), "Temporary table {} already exists, dropping", tmp_table_id.getNameForLogs());
        InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, getContext(), getContext(), tmp_table_id, /* sync */true);
    }

    auto new_context = Context::createCopy(context);
    return std::make_shared<StorageMaterializedPostgreSQL>(tmp_table_id, new_context, "temporary", table_id.table_name);
}


StoragePtr StorageMaterializedPostgreSQL::getNested() const
{
    return DatabaseCatalog::instance().getTable(getNestedStorageID(), nested_context);
}


StoragePtr StorageMaterializedPostgreSQL::tryGetNested() const
{
    return DatabaseCatalog::instance().tryGetTable(getNestedStorageID(), nested_context);
}


String StorageMaterializedPostgreSQL::getNestedTableName() const
{
    auto table_id = getStorageID();

    if (is_materialized_postgresql_database)
        return table_id.table_name;

    return toString(table_id.uuid) + NESTED_TABLE_SUFFIX;
}


StorageID StorageMaterializedPostgreSQL::getNestedStorageID() const
{
    if (nested_table_id.has_value())
        return nested_table_id.value();

    auto table_id = getStorageID();
    throw Exception(ErrorCodes::LOGICAL_ERROR,
            "No storageID found for inner table. ({})", table_id.getNameForLogs());
}


void StorageMaterializedPostgreSQL::createNestedIfNeeded(const NestedTableEngineSpec & engine_spec, PostgreSQLTableStructurePtr table_structure, const ASTTableOverride * table_override)
{
    if (tryGetNested())
        return;

    try
    {
        const auto ast_create = getCreateNestedTableQuery(engine_spec, std::move(table_structure), table_override);
        auto table_id = getStorageID();
        auto tmp_nested_table_id = StorageID(table_id.database_name, getNestedTableName());
        LOG_DEBUG(log, "Creating clickhouse table for postgresql table {} (ast: {})",
                  table_id.getNameForLogs(), ast_create->formatForLogging());

        InterpreterCreateQuery interpreter(ast_create, nested_context);
        interpreter.execute();

        auto nested_storage = DatabaseCatalog::instance().getTable(tmp_nested_table_id, nested_context);
        /// Save storage_id with correct uuid.
        nested_table_id = nested_storage->getStorageID();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }
}


std::shared_ptr<Context> StorageMaterializedPostgreSQL::makeNestedTableContext(ContextPtr from_context)
{
    auto new_context = Context::createCopy(from_context);
    new_context->setInternalQuery(true);
    return new_context;
}


void StorageMaterializedPostgreSQL::set(StoragePtr nested_storage)
{
    nested_table_id = nested_storage->getStorageID();
    auto nested_metadata = nested_storage->getInMemoryMetadataPtr(getContext(), false);
    setInMemoryMetadata(*nested_metadata);
    has_nested.store(true);
}


void StorageMaterializedPostgreSQL::shutdown(bool is_drop)
{
    /// On the DROP path (`InterpreterDropQuery` calls `flushAndShutdown(true)` before `dropInnerTableIfAny`),
    /// run the coordinated fail-close teardown BEFORE anything is stopped: every refusable (throwing) Keeper
    /// step of the teardown then executes while the replication handler and the nested table are still fully
    /// alive, so a refused (thrown) drop leaves the table consuming as if the DROP had never been issued.
    /// Waiting until `dropInnerTableIfAny` instead would refuse the drop only after this shutdown had already
    /// stopped the handler and the nested table, leaving the table mounted but dead until a server restart.
    if (is_drop && replication_handler && replication_handler->isCoordinated() && !coordinated_teardown_done)
    {
        try
        {
            replication_handler->coordinatedTeardownBeforeDataDrop();
            coordinated_teardown_done = true;
        }
        catch (...)
        {
            /// The drop is refused. The teardown stops the handler only after all its refusable Keeper work
            /// has succeeded - except the last replica's removal of the shared coordination nodes; if it
            /// failed there, re-arm the handler's retrying startup path so the table resumes replicating
            /// once Keeper is reachable again instead of staying mounted but dead.
            if (replication_handler->isStopped())
                replication_handler->restartCoordinatedReplicationAfterFailedTeardown();
            throw;
        }
    }

    if (replication_handler)
        replication_handler->shutdown();
    auto nested = tryGetNested();
    if (nested)
        nested->shutdown();
}


void StorageMaterializedPostgreSQL::checkTableCanBeDetached() const
{
    /// In a coordinated MaterializedPostgreSQL database, dynamically adding/removing a table mutates the
    /// shared publication and only takes effect on one replica, so it is refused on every replica (see
    /// `DatabaseMaterializedPostgreSQL::attachTable` / `detachTablePermanently`). Refuse here, before
    /// `InterpreterDropQuery` calls `flushAndShutdown` on the table: otherwise a rejected DETACH would
    /// still have shut the nested ReplacingMergeTree down and silently stopped replication of it.
    if (is_coordinated)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "DETACH TABLE is not supported for a coordinated MaterializedPostgreSQL setup "
            "(materialized_postgresql_keeper_path is set). "
            "Recreate the database with an updated materialized_postgresql_tables_list instead");
}


void StorageMaterializedPostgreSQL::checkTableCanBeDropped(ContextPtr /* query_context */) const
{
    /// In a coordinated MaterializedPostgreSQL database, dropping (or truncating) an individual nested
    /// `ReplicatedReplacingMergeTree` on a single replica does not update the shared publication or the
    /// configured `materialized_postgresql_tables_list`: the remaining replicas would keep consuming a
    /// publication that still contains the table, and this replica would keep a wrapper for a nested table
    /// that no longer exists. Refuse here, before `InterpreterDropQuery` calls `flushAndShutdown` on the
    /// table, so a rejected DROP stays a true no-op and does not stop replication of the nested table.
    /// This guard is reached only for a user-issued DROP/TRUNCATE of an individual table (the per-query
    /// wrapper is built only for non-internal queries); DROP DATABASE runs with an internal context and
    /// operates on the nested tables directly, so it is unaffected.
    if (is_coordinated)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Dropping or truncating an individual table is not supported for a coordinated MaterializedPostgreSQL "
            "setup (materialized_postgresql_keeper_path is set). "
            "Recreate the database with an updated materialized_postgresql_tables_list instead");

    /// The same applies to a plain (non-coordinated) MaterializedPostgreSQL database: dropping an individual
    /// table would only remove the local nested table, while the table stays in the persisted
    /// materialized_postgresql_tables_list, in the PostgreSQL publication and in the replication handler's
    /// state, so the consumer would keep receiving its changes, mark it as skipped and silently advance the
    /// replication slot past them. DETACH TABLE ... PERMANENTLY is the supported way to remove one table:
    /// it updates the tables list, removes the table from the publication and drops the local nested table.
    /// (This check does not affect the standalone MaterializedPostgreSQL table engine, for which DROP TABLE
    /// is the regular way to remove the whole replication setup.)
    if (is_materialized_postgresql_database)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Dropping or truncating an individual table is not supported for a MaterializedPostgreSQL database: "
            "it would remove only the local nested table, while the table remains in "
            "materialized_postgresql_tables_list and in the PostgreSQL publication, so its replication would "
            "silently stop. Use DETACH TABLE ... PERMANENTLY to remove the table from replication");
}


void StorageMaterializedPostgreSQL::dropInnerTableIfAny(bool sync, ContextPtr local_context)
{
    /// If it is a table with database engine MaterializedPostgreSQL - return, because delition of
    /// internal tables is managed there.
    if (is_materialized_postgresql_database)
        return;

    if (replication_handler->isCoordinated())
    {
        /// Coordinated single-table engine: make the fail-close last-replica decision - and, if this is the
        /// last replica, remove the shared slot/publication/marker - BEFORE dropping the local nested table,
        /// so a Keeper outage can never delete the last copy while the shared state survives. For the non-last
        /// case this keeps /replicas/<name> registered until the nested table has actually been dropped, so a
        /// failure while dropping it never leaves this replica unregistered while it still holds a live copy of
        /// the shared data (which a peer's later last-replica drop would then tear down around).
        ///
        /// The regular DROP TABLE path has already run the teardown in `shutdown(/* is_drop */ true)` - while
        /// the handler and the nested table were still alive, so a refusal there leaves the table consuming.
        /// This is a fallback for drop paths that do not go through it; the handler is typically already
        /// stopped here, so on failure re-arm its retrying startup path rather than leaving the table dead.
        if (!coordinated_teardown_done)
        {
            try
            {
                replication_handler->coordinatedTeardownBeforeDataDrop();
                coordinated_teardown_done = true;
            }
            catch (...)
            {
                if (replication_handler->isStopped())
                    replication_handler->restartCoordinatedReplicationAfterFailedTeardown();
                throw;
            }
        }

        /// The teardown above has already stopped the handler. Dropping the local nested table (and the
        /// authoritative `shutdownFinal` that follows) can still throw - for example if Keeper disappears while
        /// the nested ReplicatedReplacingMergeTree is deleting its own Keeper metadata. In that case the DROP is
        /// refused, so recover the stopped handler exactly as for a post-shutdown teardown failure above, instead
        /// of leaving the table mounted but permanently not consuming until a server restart.
        try
        {
            /// Simulates the local nested-table drop below failing (e.g. Keeper disappearing while the nested
            /// ReplicatedReplacingMergeTree deletes its own Keeper metadata), to test recovery of the handler
            /// that the teardown has already stopped.
            fiu_do_on(FailPoints::materialized_postgresql_fail_nested_table_drop,
            {
                throw Exception(ErrorCodes::FAULT_INJECTED,
                    "Injected failure while dropping the local nested table of a coordinated MaterializedPostgreSQL table");
            });

            /// Drop the nested replicated table synchronously (ignoring the delayed-drop window): the shared
            /// teardown above already runs now, so leaving the nested table's Keeper tree behind for
            /// `database_atomic_delay_before_drop_table_sec` would let a prompt CREATE on the same keeper path
            /// adopt a half-dead shared tree (a ghost replica and stale block deduplication hashes).
            if (tryGetNested())
                InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, getContext(), local_context, getNestedStorageID(), /* sync */ true, /* ignore_sync_setting */ true);

            /// The local copy is gone: finalize the teardown authoritatively (drops this replica's registration
            /// for the non-last case; a no-op for the last case, whose registration was already removed above).
            replication_handler->shutdownFinal();
        }
        catch (...)
        {
            if (replication_handler->isStopped())
            {
                /// The teardown already committed its last-replica decision, but the local nested table was not
                /// dropped. Re-arm the retrying startup path (it rebuilds and re-registers this replica) and
                /// clear `coordinated_teardown_done` so a retried drop re-runs the teardown - and its race-free
                /// last-replica check - from scratch rather than reusing the now-stale earlier decision.
                coordinated_teardown_done = false;
                replication_handler->restartCoordinatedReplicationAfterFailedTeardown();
            }
            throw;
        }
        replication_handler.reset();
        return;
    }

    /// Plain single-table engine: drop the local nested table BEFORE the authoritative PostgreSQL teardown.
    /// `shutdownFinal` removes the shared publication/slot (and the handler is destroyed right after), so
    /// running it first would make a refused (thrown) nested-table drop unrecoverable: `DatabaseAtomic::dropTable`
    /// aborts before removing the outer table's metadata, leaving the table mounted but already shut down,
    /// with no replication handler and no PostgreSQL objects left to resume from. In this order a refused
    /// nested drop keeps the slot/publication, and the handler is re-armed below to resume consuming from
    /// the slot's confirmed_flush_lsn; `shutdownFinal` itself reports PostgreSQL cleanup errors instead of
    /// throwing, so once the nested table is gone the drop always completes.
    ///
    /// Stop the handler first (idempotent - the regular DROP path has already done it in `flushAndShutdown`),
    /// preserving the previous order's guarantee that the consumer no longer writes while the nested table
    /// is being dropped.
    replication_handler->shutdown();
    try
    {
        /// Simulates the local nested-table drop below failing (e.g. a filesystem error), to test recovery of
        /// the handler that `flushAndShutdown` has already stopped on the DROP path.
        fiu_do_on(FailPoints::materialized_postgresql_fail_nested_table_drop,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED,
                "Injected failure while dropping the local nested table of a plain MaterializedPostgreSQL table");
        });

        if (tryGetNested())
            InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, getContext(), local_context, getNestedStorageID(), sync, /* ignore_sync_setting */ true);
    }
    catch (...)
    {
        if (replication_handler->isStopped())
        {
            /// A plain handler discards its storage pointers after the first successful start (see the end of
            /// `startSynchronization`), so re-add this storage or the restarted handler would rebuild a
            /// consumer with no tables and silently apply nothing.
            replication_handler->addStorage(remote_table_name, this);
            replication_handler->restartReplicationAfterFailedDrop();
        }
        throw;
    }

    replication_handler->shutdownFinal();
    replication_handler.reset();
}


void StorageMaterializedPostgreSQL::checkTableSizeBelowDropLimit(ContextPtr query_context) const
{
    /// In MaterializedPostgreSQL database engine mode there is no per-table nested storage
    /// to size-check (the database engine owns the nested tables); mirror `dropInnerTableIfAny`.
    if (is_materialized_postgresql_database)
        return;

    /// Mirror `dropInnerTableIfAny`'s tolerance: if the nested table is missing for any reason
    /// the drop is a no-op, so the size check is too.
    if (auto nested = tryGetNested())
        nested->checkTableSizeBelowDropLimit(query_context);
}


bool StorageMaterializedPostgreSQL::needRewriteQueryWithFinal(const Names & column_names) const
{
    return needRewriteQueryWithFinalForStorage(column_names, getNested());
}


bool StorageMaterializedPostgreSQL::supportsPrewhere() const
{
    /// `read` hands the `SelectQueryInfo` over to the nested table untouched, so whatever the nested table
    /// can do with `PREWHERE` the wrapper can do too. While the nested table is not created yet there is
    /// nothing to read from anyway, so the default is good enough.
    if (auto nested = tryGetNested())
        return nested->supportsPrewhere();
    return false;
}


bool StorageMaterializedPostgreSQL::canMoveConditionsToPrewhere() const
{
    if (auto nested = tryGetNested())
        return nested->canMoveConditionsToPrewhere();
    return false;
}


bool StorageMaterializedPostgreSQL::supportedPrewhereColumnsIncludeSubcolumns() const
{
    /// `StorageMerge` ANDs this bit across its children, so leaving it at the `IStorage` default would
    /// silently drop a subcolumn condition from `PREWHERE` for the whole `Merge` table.
    if (auto nested = tryGetNested())
        return nested->supportedPrewhereColumnsIncludeSubcolumns();
    return false;
}


bool StorageMaterializedPostgreSQL::supportsSubcolumns() const
{
    if (auto nested = tryGetNested())
        return nested->supportsSubcolumns();
    return false;
}


bool StorageMaterializedPostgreSQL::supportsOptimizationToSubcolumns() const
{
    if (auto nested = tryGetNested())
        return nested->supportsOptimizationToSubcolumns();
    return false;
}


IStorage::ColumnSizeByName StorageMaterializedPostgreSQL::getColumnSizes() const
{
    if (auto nested = tryGetNested())
        return nested->getColumnSizes();
    return {};
}


IStorage::ColumnSizeByName StorageMaterializedPostgreSQL::getColumnSizes(const Names & columns) const
{
    if (auto nested = tryGetNested())
        return nested->getColumnSizes(columns);
    return {};
}


std::optional<UInt64> StorageMaterializedPostgreSQL::totalRows(ContextPtr query_context) const
{
    /// An estimate: as for any `ReplacingMergeTree`, the deleted rows and the superseded versions of the
    /// updated rows are still counted until the parts are merged.
    if (auto nested = tryGetNested())
        return nested->totalRows(query_context);
    return {};
}


std::optional<UInt64> StorageMaterializedPostgreSQL::totalBytes(ContextPtr query_context) const
{
    if (auto nested = tryGetNested())
        return nested->totalBytes(query_context);
    return {};
}


void StorageMaterializedPostgreSQL::read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams)
{
    auto nested_table = getNested();

    readFinalFromNestedStorage(query_plan, nested_table, column_names,
            query_info, context_, processed_stage, max_block_size, num_streams);

    auto lock = lockForShare(context_->getCurrentQueryId(), context_->getSettingsRef()[Setting::lock_acquire_timeout]);
    query_plan.addTableLock(lock);
    query_plan.addStorageHolder(shared_from_this());
}


void StorageMaterializedPostgreSQL::backupData(
    BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions)
{
    /// The data lives in the nested ReplacingMergeTree table, delegate the backup to it.
    auto nested = tryGetNested();
    if (!nested)
        throw Exception(
            ErrorCodes::CANNOT_BACKUP_TABLE,
            "Cannot back up table {}: its nested ReplacingMergeTree table does not exist (the table may not have "
            "finished its initial synchronization from PostgreSQL yet). Failing closed instead of producing a "
            "backup with table metadata but no data.",
            getStorageID().getNameForLogs());

    nested->backupData(backup_entries_collector, data_path_in_backup, partitions);
}


bool StorageMaterializedPostgreSQL::supportsBackupPartition() const
{
    /// Partition backups are delegated to the nested ReplacingMergeTree (see `backupData`), so report whatever
    /// it supports. If the nested table does not exist yet, fall back to the default (no partition support);
    /// `backupData` will then fail closed with a clear message anyway.
    if (auto nested = tryGetNested())
        return nested->supportsBackupPartition();
    return false;
}


void StorageMaterializedPostgreSQL::restoreDataFromBackup(
    RestorerFromBackup &, const String &, const std::optional<ASTs> &)
{
    /// A MaterializedPostgreSQL table starts replicating from the live PostgreSQL source as soon as it is
    /// created (the constructor calls `replication_handler->startup`). Restoring the backed-up parts of the
    /// nested ReplacingMergeTree on top of that freshly replicated data would mix the backup snapshot with the
    /// current remote state, so an in-place restore into an already-existing MaterializedPostgreSQL table cannot
    /// faithfully represent the backup. Fail closed and direct the user to restore the data into a separately
    /// pre-created ReplacingMergeTree table instead - the data is still captured by `backupData`. Restoring
    /// `... AS <new_table>` into a not-yet-existing table never reaches this point: that path would recreate the
    /// table from the backup definition (again as MaterializedPostgreSQL) and is rejected earlier, in the storage
    /// factory, before the table is created (see `registerStorageMaterializedPostgreSQL`). `allow_different_table_def`
    /// only skips the definition comparison against an already existing target.
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Restoring a MaterializedPostgreSQL table ({}) in place is not supported, because the table would start "
        "replicating from the live PostgreSQL source and mix it with the backup snapshot. "
        "Restore the data into a separate ReplacingMergeTree table that you have created beforehand with the same "
        "structure as the nested table (including the _sign and _version columns), e.g.: "
        "RESTORE TABLE <src> AS <existing_replacing_mergetree> SETTINGS allow_different_table_def = 1.",
        getStorageID().getNameForLogs());
}


boost::intrusive_ptr<ASTColumnDeclaration> StorageMaterializedPostgreSQL::getMaterializedColumnsDeclaration(
        String name, String type, UInt64 default_value)
{
    auto column_declaration = make_intrusive<ASTColumnDeclaration>();

    column_declaration->name = std::move(name);
    column_declaration->setType(makeASTDataType(type));

    column_declaration->default_specifier = ColumnDefaultSpecifier::Materialized;
    column_declaration->setDefaultExpression(make_intrusive<ASTLiteral>(default_value));

    return column_declaration;
}


boost::intrusive_ptr<ASTExpressionList>
StorageMaterializedPostgreSQL::getColumnsExpressionList(const NamesAndTypesList & columns, std::unordered_map<std::string, ASTPtr> defaults) const
{
    auto columns_expression_list = make_intrusive<ASTExpressionList>();
    for (const auto & [name, type] : columns)
    {
        const auto & column_declaration = make_intrusive<ASTColumnDeclaration>();

        column_declaration->name = name;
        column_declaration->setType(dataTypeToAST(type));

        if (auto it = defaults.find(name); it != defaults.end())
        {
            column_declaration->setDefaultExpression(std::move(it->second));
            column_declaration->default_specifier = ColumnDefaultSpecifier::Default;
        }

        columns_expression_list->children.emplace_back(column_declaration);
    }
    return columns_expression_list;
}


/// For single storage MaterializedPostgreSQL get columns and primary key columns from storage definition.
/// For database engine MaterializedPostgreSQL get columns and primary key columns by fetching from PostgreSQL, also using the same
/// transaction with snapshot, which is used for initial tables dump.
ASTPtr StorageMaterializedPostgreSQL::getCreateNestedTableQuery(
    const NestedTableEngineSpec & engine_spec, PostgreSQLTableStructurePtr table_structure, const ASTTableOverride * table_override)
{
    auto create_table_query = make_intrusive<ASTCreateQuery>();

    auto table_id = getStorageID();
    create_table_query->setTable(getNestedTableName());
    create_table_query->setDatabase(table_id.database_name);
    if (is_materialized_postgresql_database)
        create_table_query->uuid = table_id.uuid;

    auto storage = make_intrusive<ASTStorage>();
    if (engine_spec.replicated)
    {
        /// Replicated/Shared ReplacingMergeTree: the first two arguments are the (already fully
        /// macro-expanded) zookeeper path and replica name, followed by the `_version` column. The path
        /// is passed as an explicit literal so that ReplicatedMergeTree does not re-resolve {uuid} per
        /// nested table, which would put each replica's table on a different path.
        storage->set(storage->engine, makeASTFunction(
            engine_spec.engine_name,
            make_intrusive<ASTLiteral>(engine_spec.zookeeper_path),
            make_intrusive<ASTLiteral>(engine_spec.replica_name),
            make_intrusive<ASTIdentifier>("_version")));
    }
    else
    {
        storage->set(storage->engine, makeASTFunction("ReplacingMergeTree", make_intrusive<ASTIdentifier>("_version")));
    }

    auto columns_declare_list = make_intrusive<ASTColumns>();
    auto order_by_expression = make_intrusive<ASTFunction>();

    auto metadata_snapshot = getInMemoryMetadataPtr(getContext(), false);

    ConstraintsDescription constraints;
    NamesAndTypesList ordinary_columns_and_types;

    if (is_materialized_postgresql_database)
    {
        if (!table_structure && !table_override)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No table structure returned for table {}.{}",
                            table_id.database_name, table_id.table_name);
        }

        if (!table_structure->physical_columns && (!table_override || !table_override->columns))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No columns returned for table {}.{}",
                            table_id.database_name, table_id.table_name);
        }

        bool has_order_by_override = table_override && table_override->storage && table_override->storage->order_by;
        if (has_order_by_override && !table_structure->replica_identity_columns)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Having PRIMARY KEY OVERRIDE is allowed only if there is "
                            "replica identity index for PostgreSQL table. (table {}.{})",
                            table_id.database_name, table_id.table_name);
        }

        if (!table_structure->primary_key_columns && !table_structure->replica_identity_columns)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Table {}.{} has no primary key and no replica identity index",
                            table_id.database_name, table_id.table_name);
        }

        if (table_override && table_override->columns)
        {
            if (table_override->columns)
            {
                auto children = table_override->columns->children;
                const auto & columns = children[0]->as<ASTExpressionList>();
                if (columns)
                {
                    for (const auto & child : columns->children)
                    {
                        const auto * column_declaration = child->as<ASTColumnDeclaration>();
                        auto type = DataTypeFactory::instance().get(column_declaration->getType());
                        ordinary_columns_and_types.emplace_back(NameAndTypePair(column_declaration->name, type));
                    }
                }

                columns_declare_list->set(columns_declare_list->columns, children[0]);
            }
            else
            {
                ordinary_columns_and_types = table_structure->physical_columns->columns;
                columns_declare_list->set(columns_declare_list->columns, getColumnsExpressionList(ordinary_columns_and_types));
            }

            auto * columns = table_override->columns;
            if (columns && columns->constraints)
                constraints = ConstraintsDescription(columns->constraints->children);
        }
        else
        {
            const auto columns = table_structure->physical_columns;
            std::unordered_map<std::string, ASTPtr> defaults;
            for (const auto & col : columns->columns)
            {
                const auto & attr = columns->attributes.at(col.name);
                if (!attr.attr_def.empty())
                {
                    ParserExpression expr_parser;
                    Expected expected;
                    ASTPtr result;

                    Tokens tokens(attr.attr_def.data(), attr.attr_def.data() + attr.attr_def.size());
                    IParser::Pos pos(tokens, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
                    if (!expr_parser.parse(pos, result, expected))
                    {
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to parse default expression: {}", attr.attr_def);
                    }
                    defaults.emplace(col.name, result);
                }
            }
            ordinary_columns_and_types = columns->columns;
            columns_declare_list->set(columns_declare_list->columns, getColumnsExpressionList(ordinary_columns_and_types, defaults));
        }

        if (ordinary_columns_and_types.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Table {}.{} has no columns", table_id.database_name, table_id.table_name);

        NamesAndTypesList merging_columns;
        if (table_structure->primary_key_columns)
            merging_columns = table_structure->primary_key_columns->columns;
        else
            merging_columns = table_structure->replica_identity_columns->columns;

        order_by_expression->name = "tuple";
        order_by_expression->arguments = make_intrusive<ASTExpressionList>();
        for (const auto & column : merging_columns)
            order_by_expression->arguments->children.emplace_back(make_intrusive<ASTIdentifier>(column.name));

        storage->set(storage->order_by, order_by_expression);
    }
    else
    {
        ordinary_columns_and_types = metadata_snapshot->getColumns().getOrdinary();
        columns_declare_list->set(columns_declare_list->columns, getColumnsExpressionList(ordinary_columns_and_types));

        auto primary_key_ast = metadata_snapshot->getPrimaryKeyAST();
        /// Once the nested table has been attached (`set`), this wrapper carries the nested table's metadata,
        /// where the primary key is implicit in the sorting key: the nested (Replicated)ReplacingMergeTree is
        /// created with only an ORDER BY clause. A re-creation of the nested table from such metadata (the
        /// refused-drop recovery drops a shut-down nested table and rebuilds it here) must fall back to the
        /// sorting key, which is the same expression the original nested table was created with.
        if (!primary_key_ast)
            primary_key_ast = metadata_snapshot->getSortingKeyAST();
        if (!primary_key_ast)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage MaterializedPostgreSQL must have primary key");
        storage->set(storage->order_by, primary_key_ast);

        constraints = metadata_snapshot->getConstraints();
    }

    create_table_query->set(create_table_query->storage, storage);

    if (table_override)
    {
        if (auto * columns = table_override->columns)
        {
            if (columns->columns)
            {
                for (const auto & override_column_ast : columns->columns->children)
                {
                    auto * override_column = override_column_ast->as<ASTColumnDeclaration>();
                    if (override_column->name == "_sign" || override_column->name == "_version")
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot override _sign and _version column");
                }
            }
        }

        create_table_query->set(create_table_query->columns_list, columns_declare_list);

        applyTableOverrideToCreateQuery(*table_override, create_table_query.get());

        create_table_query->columns_list->columns->children.emplace_back(getMaterializedColumnsDeclaration("_sign", "Int8", 1));
        create_table_query->columns_list->columns->children.emplace_back(getMaterializedColumnsDeclaration("_version", "UInt64", 1));
    }
    else
    {
        columns_declare_list->columns->children.emplace_back(getMaterializedColumnsDeclaration("_sign", "Int8", 1));
        columns_declare_list->columns->children.emplace_back(getMaterializedColumnsDeclaration("_version", "UInt64", 1));
        create_table_query->set(create_table_query->columns_list, columns_declare_list);
    }

    /// Add columns _sign and _version, so that they can be accessed from nested ReplacingMergeTree table if needed.
    ordinary_columns_and_types.push_back({"_sign", std::make_shared<DataTypeInt8>()});
    ordinary_columns_and_types.push_back({"_version", std::make_shared<DataTypeUInt64>()});

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(ordinary_columns_and_types));
    storage_metadata.setConstraints(constraints);
    setInMemoryMetadata(storage_metadata);

    return create_table_query;
}


void registerStorageMaterializedPostgreSQL(StorageFactory & factory);
void registerStorageMaterializedPostgreSQL(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        /// Restoring a MaterializedPostgreSQL table from a backup is not supported, and must be rejected here -
        /// before the storage is constructed - to fail closed. The constructor calls `replication_handler->startup`,
        /// so by the time `restoreDataFromBackup` could reject the restore the table would already exist and be
        /// replicating from the live PostgreSQL source (`RestorerFromBackup` does not roll back the created table on
        /// a later failure). The table data is still captured by the backup (delegated to the nested
        /// ReplacingMergeTree, see `backupData`); restore it into a ReplacingMergeTree created beforehand instead.
        if (args.is_restore_from_backup)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Restoring a MaterializedPostgreSQL table ({}) from a backup is not supported, because the table "
                "would start replicating from the live PostgreSQL source as soon as it is created, mixing the backup "
                "snapshot with the current remote state. The table data is still captured by the backup (delegated "
                "to the nested ReplacingMergeTree); restore it into a ReplacingMergeTree table that you have created "
                "beforehand with the same structure as the nested table (including the _sign and _version columns), "
                "e.g.: RESTORE TABLE <src> AS <existing_replacing_mergetree> SETTINGS allow_different_table_def = 1.",
                args.table_id.getNameForLogs());

        StorageInMemoryMetadata metadata;
        metadata.setColumns(args.columns);
        metadata.setConstraints(args.constraints);
        metadata.setComment(args.comment);

        if (args.mode <= LoadingStrictnessLevel::CREATE
            && !args.getLocalContext()->getSettingsRef()[Setting::allow_experimental_materialized_postgresql_table])
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaterializedPostgreSQL is an experimental table engine."
                                " You can enable it with the `allow_experimental_materialized_postgresql_table` setting");

        if (!args.storage_def->order_by && args.storage_def->primary_key)
            args.storage_def->set(args.storage_def->order_by, args.storage_def->primary_key->clone());

        if (!args.storage_def->order_by)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage MaterializedPostgreSQL needs order by key or primary key");

        if (args.storage_def->primary_key)
            metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, {}, args.getContext());
        else
            metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->order_by->ptr(), metadata.columns, {}, args.getContext());

        /// The `PostgreSQLSettings` are not passed: this engine does not use a connection pool,
        /// so the `postgresql_*` pool settings are rejected instead of being silently ignored.
        auto configuration = StoragePostgreSQL::getConfiguration(args.engine_args, args.getContext(), /*storage_settings=*/ nullptr);

        /// A named collection may specify the endpoint as `addresses_expr`, which fills only
        /// `configuration.addresses` and leaves `host` / `port` empty, while the connection string
        /// below is built from `host` / `port`. This engine keeps a single replication connection,
        /// so exactly one address is accepted; canonicalize it back into `host` / `port`. This mirrors
        /// `registerDatabaseMaterializedPostgreSQL`.
        if (configuration.host.empty())
        {
            if (configuration.addresses.size() == 1)
            {
                configuration.host = configuration.addresses.front().first;
                configuration.port = configuration.addresses.front().second;
            }
            else if (!isLoadingFromExistingMetadata(args.mode) && !args.query.attach_short_syntax)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Engine `MaterializedPostgreSQL` requires a single `host:port` address, but `addresses_expr` defines {} addresses",
                                configuration.addresses.size());
            }
            /// When replaying previously persisted metadata (server startup, and DETACH / ATTACH
            /// with the short syntax, which re-reads the stored definition) a legacy multi-address
            /// definition keeps its historical behavior: it could be created before this validation
            /// existed (replication starts asynchronously, so the broken connection string never
            /// aborted the CREATE), and the table must keep loading with replication failing and
            /// retrying in the background rather than abort startup. A user ATTACH with a full
            /// table definition is a fresh definition, not a replay, and stays fail-closed
            /// (the same distinction `StorageDistributed` draws for its skipping-indices check).
        }

        auto connection_info = postgres::formatConnectionString(
            configuration.database,
            configuration.host,
            configuration.port,
            configuration.username,
            configuration.password,
            args.getContext()->getSettingsRef()[Setting::postgresql_connection_attempt_timeout]);

        bool has_settings = args.storage_def->settings;
        auto postgresql_replication_settings = std::make_unique<MaterializedPostgreSQLSettings>();

        if (has_settings)
            postgresql_replication_settings->loadFromQuery(*args.storage_def);

        if (args.mode <= LoadingStrictnessLevel::CREATE)
        {
            /// `{uuid}` in the coordination path is only safe when every replica ends up with the same
            /// UUID, which is the case exactly when the DDL carries it: an ON CLUSTER query, a table
            /// inside a `Replicated` database, or an explicit `UUID '...'` clause. Otherwise each server
            /// generates its own UUID. Same rule as `TableZnodeInfo::resolve` applies to a
            /// ReplicatedMergeTree path.
            const bool is_on_cluster = args.getContext()->isDDLOrOnClusterInternal();
            const bool is_replicated_database = is_on_cluster
                && DatabaseCatalog::instance().getDatabase(args.table_id.database_name)->getEngineName() == "Replicated";
            const bool allow_uuid_macro = is_on_cluster || is_replicated_database || args.query.has_uuid;
            validateMaterializedPostgreSQLCoordinationSettings(
                *postgresql_replication_settings, args.getContext(), args.table_id.database_name, args.table_id.uuid,
                configuration.database, configuration.table_or_query.getTableName(), allow_uuid_macro);
        }

        /// For the table engine the user declares the column types explicitly, so this setting cannot
        /// affect anything (it would be a silent no-op). It is only meaningful for the database engine,
        /// where the nested table structure is derived from PostgreSQL.
        if (args.mode <= LoadingStrictnessLevel::CREATE
            && (*postgresql_replication_settings)[MaterializedPostgreSQLSetting::materialized_postgresql_use_extended_date_and_time_types].isChanged())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Setting `materialized_postgresql_use_extended_date_and_time_types` is not applicable to the "
                            "MaterializedPostgreSQL table engine, where column types are declared explicitly. "
                            "It only affects the MaterializedPostgreSQL database engine. "
                            "Declare the desired `Date`/`DateTime` or `Date32`/`DateTime64` types directly in the table definition.");

        return std::make_shared<StorageMaterializedPostgreSQL>(
                args.table_id, args.mode, configuration.database, configuration.table_or_query.getTableName(), connection_info,
                metadata, args.getContext(),
                std::move(postgresql_replication_settings));
    };

    factory.registerStorage(
        "MaterializedPostgreSQL",
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
            .supports_sort_order = true,
            .source_access_type = AccessTypeObjects::Source::POSTGRES,
            .has_builtin_setting_fn = MaterializedPostgreSQLSettings::hasBuiltin,
        },
        Documentation{
            .description = R"DOCS_MD(
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# MaterializedPostgreSQL table engine

<ExperimentalBadge/>
<CloudNotSupportedBadge/>

:::note
ClickHouse Cloud users are recommended to use [ClickPipes](/integrations/clickpipes) for PostgreSQL replication to ClickHouse. This natively supports high-performance Change Data Capture (CDC) for PostgreSQL.
:::

Creates ClickHouse table with an initial data dump of PostgreSQL table and starts the replication process, i.e. it executes a background job to apply new changes as they happen on PostgreSQL table in the remote PostgreSQL database.

:::note
This table engine is experimental. To use it, set `allow_experimental_materialized_postgresql_table` to 1 in your configuration files or by using the `SET` command:

```sql
SET allow_experimental_materialized_postgresql_table=1
```
:::

If more than one table is required, it is highly recommended to use the [MaterializedPostgreSQL](/reference/engines/database-engines/materialized-postgresql) database engine instead of the table engine and use the `materialized_postgresql_tables_list` setting, which specifies the tables to be replicated (will also be possible to add database `schema`). It will be much better in terms of CPU, fewer connections and fewer replication slots inside the remote PostgreSQL database.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_table', 'postgres_user', 'postgres_password')
PRIMARY KEY key;
```

**Engine Parameters**

- `host:port` — PostgreSQL server address.
- `database` — Remote database name.
- `table` — Remote table name.
- `user` — PostgreSQL user.
- `password` — User password.

## Requirements {#requirements}

1. The [wal_level](https://www.postgresql.org/docs/current/runtime-config-wal.html) setting must have a value `logical` and `max_replication_slots` parameter must have a value at least `2` in the PostgreSQL config file.

2. A table with `MaterializedPostgreSQL` engine must have a primary key — the same as a replica identity index (by default: primary key) of a PostgreSQL table (see [details on replica identity index](/reference/engines/database-engines/materialized-postgresql#requirements)).

3. Only database [Atomic](https://en.wikipedia.org/wiki/Atomicity_(database_systems)) is allowed.

4. The `MaterializedPostgreSQL` table engine only works for PostgreSQL versions >= 11 as the implementation requires the [pg_replication_slot_advance](https://pgpedia.info/p/pg_replication_slot_advance.html) PostgreSQL function.

## High availability with Keeper coordination {#high-availability-with-keeper-coordination}

A PostgreSQL logical replication slot allows only one active consumer, so by default a `MaterializedPostgreSQL` table lives on a single ClickHouse server: its nested table is a plain `ReplacingMergeTree` and nothing takes over if that server goes away. Keeper coordination removes that limitation - several ClickHouse replicas create the same table on a shared Keeper path, exactly one of them (the "active worker") consumes the shared replication slot at a time, and the others stand by and take over automatically once the active worker's Keeper session ends. The standby replicas are not idle copies: the nested table is a replicated table engine, so they receive both the initial snapshot and the ongoing changes through ClickHouse replication and can be queried like the active worker. A takeover is snapshot-safe: a durable marker in Keeper records that the initial snapshot completed, a new active worker redoes the snapshot when the marker is absent, and the marker itself can only be published over the live leadership session - a worker deposed mid-snapshot aborts instead of masking its successor's replacement snapshot with a stale marker. The redo itself is fenced the same way: a deposed worker aborts before truncating the nested table and before dropping or recreating the shared slot, so it cannot wipe the data its successor has already reloaded or discard the slot the successor just created. An elected worker whose snapshot load fails aborts the whole startup - it never starts a consumer that would advance the shared replication slot without applying any data - and releases the leadership so a healthy replica can retry.

To enable it, set [`materialized_postgresql_keeper_path`](#materialized-postgresql-keeper-path) together with a replicated nested table engine and create the same table on every participating replica:

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_table', 'postgres_user', 'postgres_password')
PRIMARY KEY key
SETTINGS materialized_postgresql_table_engine = 'ReplicatedReplacingMergeTree',
         materialized_postgresql_keeper_path = '/clickhouse/materialized_postgresql/{shard}/postgresql_replica';
```

### `materialized_postgresql_table_engine` {#materialized-postgresql-table-engine}

Engine used for the nested table that stores the replicated data. One of `ReplacingMergeTree` (default), `ReplicatedReplacingMergeTree`, `SharedReplacingMergeTree`. The replicated and shared variants require `materialized_postgresql_keeper_path` to be set, and coordination in turn requires one of them: with a plain `ReplacingMergeTree` the standby replicas would hold no data, so a takeover would lose every row replicated before the failover. `SharedReplacingMergeTree` is only available in ClickHouse Cloud. It must be specified at `CREATE` time and cannot be changed afterwards.

### `materialized_postgresql_keeper_path` {#materialized-postgresql-keeper-path}

Keeper (or ZooKeeper) path used to coordinate the PostgreSQL replication slot across ClickHouse replicas. Default: empty (coordination disabled). Keeper must be configured on the server; a coordinated `CREATE TABLE` without it is rejected at `CREATE` time rather than left retrying in the background.

The path supports the `{shard}` macro and **must resolve to the same value on every participating replica**, so a per-replica or per-server macro such as `{replica}` or `{server_uuid}` is rejected at `CREATE` time - put the per-replica part in [`materialized_postgresql_replica_name`](#materialized-postgresql-replica-name) instead. The `{uuid}` macro is accepted only when the UUID is guaranteed to be identical on every replica - an `ON CLUSTER` query, a table inside a `Replicated` database, or an explicit `UUID '...'` clause - because a plain `CREATE` generates a different UUID on every server, which would leave the replicas on disjoint Keeper subtrees while still contending for the same PostgreSQL replication slot and publication. Coordination owns the shared slot and publication, so it cannot be combined with `materialized_postgresql_use_unique_replication_consumer_identifier` or with a user-managed `materialized_postgresql_replication_slot` / `materialized_postgresql_snapshot`.

All engines sharing a keeper path must agree on the settings that determine the derived names of the nested table, the shared replication slot and the publication, and must replicate the same PostgreSQL source database and table; the first one publishes that identity under the keeper path and a disagreeing engine is rejected. In particular a `MaterializedPostgreSQL` **table** and a `MaterializedPostgreSQL` **database** can never share one keeper path, because they derive different slot and publication names even for the same source table.

`DROP TABLE` on a coordinated table only removes the shared replication slot and publication from PostgreSQL together with the last remaining replica, and that last-replica decision is made in Keeper *before* the local data is deleted: a `DROP TABLE` while Keeper is unreachable fails instead of deleting the last copy of the data (retry it once Keeper is reachable), and a drop that is refused after replication has already been stopped rebuilds replication in the background so the replica rejoins the setup. (`TRUNCATE TABLE` is not supported by this table engine in any mode.)

See the [`MaterializedPostgreSQL` database engine](/reference/engines/database-engines/materialized-postgresql) for the full description of the coordinated mode, including the leftover-state and schema-drift rules that apply here as well.

### `materialized_postgresql_replica_name` {#materialized-postgresql-replica-name}

Replica identity used for the coordination node and for the nested replicated table engine. Default: `{replica}`. Supports the `{uuid}`, `{shard}` and `{replica}` macros. It **must resolve to a distinct value on every replica** (a name already registered by another replica is rejected) and the expanded value must be a single Keeper node name - an empty value, or one containing `/`, is rejected.

Together with `materialized_postgresql_keeper_path` it forms the **coordination identity** of the replica, which must stay the same for the lifetime of the coordinated setup: both settings are re-expanded from the current server configuration on every startup, while the nested table keeps the expansion it was created with. A configuration-only change of a macro they expand through is therefore refused when the replica starts up, with an error naming both identities; restore the configuration, or drop the table on that replica and recreate it on the new coordination path. The table stays droppable in that state, and the drop tears down the coordination state the nested table was actually created with.

## Virtual columns {#virtual-columns}

- `_version` — Transaction counter. Type: [UInt64](/reference/data-types/int-uint).

- `_sign` — Deletion mark. Type: [Int8](/reference/data-types/int-uint). Possible values:
  - `1` — Row is not deleted,
  - `-1` — Row is deleted.

These columns do not need to be added when a table is created. They are always accessible in `SELECT` query.
`_version` column equals `LSN` position in `WAL`, so it might be used to check how up-to-date replication is.

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;

SELECT key, value, _version FROM postgresql_db.postgresql_replica;
```

:::note
Replication of [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) values is not supported. The default value for the data type will be used.
:::
)DOCS_MD",
            .syntax = "ENGINE = MaterializedPostgreSQL('host:port', 'database', 'table', 'user', 'password') ORDER BY key",
            .related = {"PostgreSQL"}});
}

}

#endif
