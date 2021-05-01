#include "PostgreSQLReplicationHandler.h"

#if USE_LIBPQXX
#include <DataStreams/PostgreSQLBlockInputStream.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>
#include <Storages/PostgreSQL/StorageMaterializePostgreSQL.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Common/setThreadName.h>
#include <DataStreams/copyData.h>
#include <Poco/File.h>


namespace DB
{

static const auto reschedule_ms = 500;
static const auto TMP_SUFFIX = "_tmp";


PostgreSQLReplicationHandler::PostgreSQLReplicationHandler(
    const String & remote_database_name_,
    const String & current_database_name_,
    const postgres::ConnectionInfo & connection_info_,
    const std::string & metadata_path_,
    ContextPtr context_,
    const size_t max_block_size_,
    bool allow_minimal_ddl_,
    bool is_postgresql_replica_database_engine_,
    const String tables_list_)
    : log(&Poco::Logger::get("PostgreSQLReplicationHandler"))
    , context(context_)
    , remote_database_name(remote_database_name_)
    , current_database_name(current_database_name_)
    , metadata_path(metadata_path_)
    , connection_info(connection_info_)
    , max_block_size(max_block_size_)
    , allow_minimal_ddl(allow_minimal_ddl_)
    , is_postgresql_replica_database_engine(is_postgresql_replica_database_engine_)
    , tables_list(tables_list_)
    , connection(std::make_shared<postgres::Connection>(connection_info_))
{
    replication_slot = fmt::format("{}_ch_replication_slot", current_database_name);
    publication_name = fmt::format("{}_ch_publication", current_database_name);

    startup_task = context->getSchedulePool().createTask("PostgreSQLReplicaStartup", [this]{ waitConnectionAndStart(); });
    consumer_task = context->getSchedulePool().createTask("PostgreSQLReplicaStartup", [this]{ consumerFunc(); });
}


void PostgreSQLReplicationHandler::addStorage(const std::string & table_name, StorageMaterializePostgreSQL * storage)
{
    materialized_storages[table_name] = storage;
}


void PostgreSQLReplicationHandler::startup()
{
    startup_task->activateAndSchedule();
}


void PostgreSQLReplicationHandler::waitConnectionAndStart()
{
    try
    {
        /// Will throw pqxx::broken_connection if no connection at the moment
        connection->get();
        startSynchronization();
    }
    catch (const pqxx::broken_connection & pqxx_error)
    {
        LOG_ERROR(log, "Unable to set up connection. Reconnection attempt will continue. Error message: {}", pqxx_error.what());
        startup_task->scheduleAfter(reschedule_ms);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void PostgreSQLReplicationHandler::shutdown()
{
    stop_synchronization.store(true);
    startup_task->deactivate();
    consumer_task->deactivate();
}


void PostgreSQLReplicationHandler::startSynchronization()
{
    {
        postgres::Transaction<pqxx::work> tx(connection->getRef());
        createPublicationIfNeeded(tx.getRef());
    }

    auto replication_connection = postgres::createReplicationConnection(connection_info);
    postgres::Transaction<pqxx::nontransaction> tx(replication_connection->getRef());

    /// List of nested tables (table_name -> nested_storage), which is passed to replication consumer.
    std::unordered_map<String, StoragePtr> nested_storages;
    std::string snapshot_name, start_lsn;

    auto initial_sync = [&]()
    {
        createReplicationSlot(tx.getRef(), start_lsn, snapshot_name);

        for (const auto & [table_name, storage] : materialized_storages)
        {
            try
            {
                nested_storages[table_name] = loadFromSnapshot(snapshot_name, table_name, storage->as <StorageMaterializePostgreSQL>());
            }
            catch (Exception & e)
            {
                e.addMessage("while loading table {}.{}", remote_database_name, table_name);
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    };

    /// There is one replication slot for each replication handler. In case of MaterializePostgreSQL database engine,
    /// there is one replication slot per database. Its lifetime must be equal to the lifetime of replication handler.
    /// Recreation of a replication slot imposes reloading of all tables.
    if (!isReplicationSlotExist(tx.getRef(), replication_slot))
    {
        initial_sync();
    }
    else if (!Poco::File(metadata_path).exists() || new_publication_created)
    {
        /// There are the following cases, which mean that something non-intentioanal happened.
        /// 1. If replication slot exists and metadata file does not exist, it is not ok.
        /// 2. If replication slot exists before publication is created.
        dropReplicationSlot(tx.getRef());
        initial_sync();
    }
    else
    {
        /// Synchronization and initial load already took place.c
        LOG_TRACE(log, "Loading {} tables...", materialized_storages.size());
        for (const auto & [table_name, storage] : materialized_storages)
        {
            auto materialized_storage = storage->as <StorageMaterializePostgreSQL>();
            try
            {
                nested_storages[table_name] = materialized_storage->getNested();
                materialized_storage->setStorageMetadata();
                materialized_storage->setNestedStatus(true);
            }
            catch (Exception & e)
            {
                e.addMessage("while loading table {}.{}", remote_database_name, table_name);
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }

    consumer = std::make_shared<MaterializePostgreSQLConsumer>(
            context,
            connection,
            replication_slot,
            publication_name,
            metadata_path,
            start_lsn,
            max_block_size,
            allow_minimal_ddl,
            nested_storages);

    consumer_task->activateAndSchedule();

    /// Do not rely anymore on saved storage pointers.
    materialized_storages.clear();
}


StoragePtr PostgreSQLReplicationHandler::loadFromSnapshot(std::string & snapshot_name, const String & table_name, StorageMaterializePostgreSQL * materialized_storage)
{
    auto tx = std::make_shared<pqxx::ReplicationTransaction>(connection->getRef());

    std::string query_str = fmt::format("SET TRANSACTION SNAPSHOT '{}'", snapshot_name);
    tx->exec(query_str);

    /// Load from snapshot, which will show table state before creation of replication slot.
    /// Already connected to needed database, no need to add it to query.
    query_str = fmt::format("SELECT * FROM {}", table_name);

    /// If table schema has changed, the table stops consuming changed from replication stream.
    /// If `allow_minimal_ddl` is true, create a new table in the background, load new table schema
    /// and all data from scratch. Then execute REPLACE query with Nested table.
    /// This is only allowed for MaterializePostgreSQL database engine.
    materialized_storage->createNestedIfNeeded(fetchTableStructure(*tx, table_name));
    auto nested_storage = materialized_storage->getNested();
    auto insert_context = materialized_storage->getNestedTableContext();

    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = nested_storage->getStorageID();

    InterpreterInsertQuery interpreter(insert, insert_context);
    auto block_io = interpreter.execute();

    const StorageInMemoryMetadata & storage_metadata = nested_storage->getInMemoryMetadata();
    auto sample_block = storage_metadata.getSampleBlockNonMaterialized();

    PostgreSQLTransactionBlockInputStream<pqxx::ReplicationTransaction> input(tx, query_str, sample_block, DEFAULT_BLOCK_SIZE);
    assertBlocksHaveEqualStructure(input.getHeader(), block_io.out->getHeader(), "postgresql replica load from snapshot");
    copyData(input, *block_io.out);

    materialized_storage->setNestedStatus(true);

    return nested_storage;
}


void PostgreSQLReplicationHandler::consumerFunc()
{
    std::vector<std::pair<Int32, String>> skipped_tables;

    bool schedule_now = consumer->consume(skipped_tables);

    if (!skipped_tables.empty())
        reloadFromSnapshot(skipped_tables);

    if (stop_synchronization)
        return;

    if (schedule_now)
        consumer_task->schedule();
    else
        consumer_task->scheduleAfter(reschedule_ms);
}


bool PostgreSQLReplicationHandler::isPublicationExist(pqxx::work & tx)
{
    std::string query_str = fmt::format("SELECT exists (SELECT 1 FROM pg_publication WHERE pubname = '{}')", publication_name);
    pqxx::result result{tx.exec(query_str)};
    assert(!result.empty());
    bool publication_exists = (result[0][0].as<std::string>() == "t");

    if (publication_exists)
        LOG_INFO(log, "Publication {} already exists. Using existing version", publication_name);

    return publication_exists;
}


void PostgreSQLReplicationHandler::createPublicationIfNeeded(pqxx::work & tx, bool create_without_check)
{
    if (new_publication_created)
        return;

    if (create_without_check || !isPublicationExist(tx))
    {
        if (tables_list.empty())
        {
            for (const auto & storage_data : materialized_storages)
            {
                if (!tables_list.empty())
                    tables_list += ", ";
                tables_list += storage_data.first;
            }
        }

        /// 'ONLY' means just a table, without descendants.
        std::string query_str = fmt::format("CREATE PUBLICATION {} FOR TABLE ONLY {}", publication_name, tables_list);
        try
        {
            tx.exec(query_str);
            new_publication_created = true;
            LOG_TRACE(log, "Created publication {} with tables list: {}", publication_name, tables_list);
        }
        catch (Exception & e)
        {
            e.addMessage("while creating pg_publication");
            throw;
        }
    }
}


bool PostgreSQLReplicationHandler::isReplicationSlotExist(pqxx::nontransaction & tx, std::string & slot_name)
{
    std::string query_str = fmt::format("SELECT active, restart_lsn FROM pg_replication_slots WHERE slot_name = '{}'", slot_name);
    pqxx::result result{tx.exec(query_str)};

    /// Replication slot does not exist
    if (result.empty())
        return false;

    LOG_TRACE(log, "Replication slot {} already exists (active: {}). Restart lsn position is {}",
            slot_name, result[0][0].as<bool>(), result[0][0].as<std::string>());

    return true;
}


void PostgreSQLReplicationHandler::createReplicationSlot(
        pqxx::nontransaction & tx, std::string & start_lsn, std::string & snapshot_name, bool temporary)
{
    std::string query_str;

    std::string slot_name;
    if (temporary)
        slot_name = replication_slot + "_tmp";
    else
        slot_name = replication_slot;

    query_str = fmt::format("CREATE_REPLICATION_SLOT {} LOGICAL pgoutput EXPORT_SNAPSHOT", slot_name);

    try
    {
        pqxx::result result{tx.exec(query_str)};
        start_lsn = result[0][1].as<std::string>();
        snapshot_name = result[0][2].as<std::string>();
        LOG_TRACE(log, "Created replication slot: {}, start lsn: {}", replication_slot, start_lsn);
    }
    catch (Exception & e)
    {
        e.addMessage("while creating PostgreSQL replication slot {}", slot_name);
        throw;
    }
}


void PostgreSQLReplicationHandler::dropReplicationSlot(pqxx::nontransaction & tx, bool temporary)
{
    std::string slot_name;
    if (temporary)
        slot_name = replication_slot + "_tmp";
    else
        slot_name = replication_slot;

    std::string query_str = fmt::format("SELECT pg_drop_replication_slot('{}')", slot_name);

    tx.exec(query_str);
    LOG_TRACE(log, "Dropped replication slot: {}", slot_name);
}


void PostgreSQLReplicationHandler::dropPublication(pqxx::nontransaction & tx)
{
    std::string query_str = fmt::format("DROP PUBLICATION IF EXISTS {}", publication_name);
    tx.exec(query_str);
}


void PostgreSQLReplicationHandler::shutdownFinal()
{
    if (Poco::File(metadata_path).exists())
        Poco::File(metadata_path).remove();

    connection = std::make_shared<postgres::Connection>(connection_info);
    postgres::Transaction<pqxx::nontransaction> tx(connection->getRef());

    dropPublication(tx.getRef());
    if (isReplicationSlotExist(tx.getRef(), replication_slot))
        dropReplicationSlot(tx.getRef());
}


/// Used by MaterializePostgreSQL database engine.
NameSet PostgreSQLReplicationHandler::fetchRequiredTables(pqxx::connection & connection_)
{
    postgres::Transaction<pqxx::work> tx(connection_);
    bool publication_exists = isPublicationExist(tx.getRef());

    if (tables_list.empty() && !publication_exists)
    {
        /// Fetch all tables list from database. Publication does not exist yet, which means
        /// that no replication took place. Publication will be created in
        /// startSynchronization method.
        return fetchPostgreSQLTablesList(tx.getRef());
    }

    if (!publication_exists)
        createPublicationIfNeeded(tx.getRef(), /* create_without_check = */ true);

    return fetchTablesFromPublication(tx.getRef());
}


NameSet PostgreSQLReplicationHandler::fetchTablesFromPublication(pqxx::work & tx)
{
    std::string query = fmt::format("SELECT tablename FROM pg_publication_tables WHERE pubname = '{}'", publication_name);
    std::unordered_set<std::string> tables;

    for (auto table_name : tx.stream<std::string>(query))
        tables.insert(std::get<0>(table_name));

    return tables;
}


PostgreSQLTableStructurePtr PostgreSQLReplicationHandler::fetchTableStructure(
        pqxx::ReplicationTransaction & tx, const std::string & table_name)
{
    if (!is_postgresql_replica_database_engine)
        return nullptr;

    auto use_nulls = context->getSettingsRef().external_databases_use_nulls;
    return std::make_unique<PostgreSQLTableStructure>(fetchPostgreSQLTableStructure(tx, table_name, use_nulls, true, true));
}


void PostgreSQLReplicationHandler::reloadFromSnapshot(const std::vector<std::pair<Int32, String>> & relation_data)
{
    try
    {
        auto replication_connection = postgres::createReplicationConnection(connection_info);
        postgres::Transaction<pqxx::nontransaction> tx(replication_connection->getRef());

        std::string snapshot_name, start_lsn;
        createReplicationSlot(tx.getRef(), start_lsn, snapshot_name, true);

        for (const auto & [table_id, table_name] : relation_data)
        {
            auto materialized_storage = DatabaseCatalog::instance().getTable(StorageID(current_database_name, table_name), context);
            StoragePtr temp_materialized_storage = materialized_storage->as <StorageMaterializePostgreSQL>()->createTemporary();

            /// This snapshot is valid up to the end of the transaction, which exported it.
            StoragePtr nested_storage = loadFromSnapshot(snapshot_name, table_name,
                                                         temp_materialized_storage->as <StorageMaterializePostgreSQL>());
            consumer->updateNested(table_name, nested_storage);
            consumer->updateSkipList(table_id, start_lsn);
            replaceMaterializedTable(table_name);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void PostgreSQLReplicationHandler::replaceMaterializedTable(const String & table_name)
{
    auto ast_replace = std::make_shared<ASTCreateQuery>();

    auto outdated_storage = materialized_storages[table_name];
    auto table_id = outdated_storage->getStorageID();

    ast_replace->replace_table = true;

    ast_replace->table = table_id.table_name;
    ast_replace->database = table_id.database_name;

    ast_replace->as_table = table_id.table_name + TMP_SUFFIX;
    ast_replace->as_database = table_id.database_name;

    InterpreterCreateQuery interpreter(ast_replace, context);
    interpreter.execute();
}

}

#endif
