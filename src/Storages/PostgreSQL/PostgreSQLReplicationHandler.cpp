#include "PostgreSQLReplicationHandler.h"

#if USE_LIBPQXX
#include <DataStreams/PostgreSQLBlockInputStream.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>
#include <Storages/PostgreSQL/StorageMaterializePostgreSQL.h>

#include <Common/setThreadName.h>
#include <DataStreams/copyData.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Poco/File.h>


namespace DB
{

static const auto reschedule_ms = 500;


/// TODO: fetch replica identity index

PostgreSQLReplicationHandler::PostgreSQLReplicationHandler(
    const std::string & database_name_,
    const postgres::ConnectionInfo & connection_info_,
    const std::string & metadata_path_,
    const Context & context_,
    const size_t max_block_size_,
    bool allow_minimal_ddl_,
    bool is_postgresql_replica_database_engine_,
    const String tables_list_)
    : log(&Poco::Logger::get("PostgreSQLReplicationHandler"))
    , context(context_)
    , database_name(database_name_)
    , metadata_path(metadata_path_)
    , connection_info(connection_info_)
    , max_block_size(max_block_size_)
    , allow_minimal_ddl(allow_minimal_ddl_)
    , is_postgresql_replica_database_engine(is_postgresql_replica_database_engine_)
    , tables_list(tables_list_)
    , connection(std::make_shared<postgres::Connection>(connection_info_))
{
    replication_slot = fmt::format("{}_ch_replication_slot", database_name);
    publication_name = fmt::format("{}_ch_publication", database_name);

    startup_task = context.getSchedulePool().createTask("PostgreSQLReplicaStartup", [this]{ waitConnectionAndStart(); });
    consumer_task = context.getSchedulePool().createTask("PostgreSQLReplicaStartup", [this]{ consumerFunc(); });
}


void PostgreSQLReplicationHandler::addStorage(const std::string & table_name, StorageMaterializePostgreSQL * storage)
{
    storages[table_name] = storage;
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
    createPublicationIfNeeded(connection->getRef());

    auto replication_connection = postgres::createReplicationConnection(connection_info);
    postgres::Transaction<pqxx::nontransaction> tx(replication_connection->getRef());

    std::string snapshot_name, start_lsn;

    auto initial_sync = [&]()
    {
        createReplicationSlot(tx.getRef(), start_lsn, snapshot_name);
        loadFromSnapshot(snapshot_name, storages);
    };

    /// Replication slot should be deleted with drop table only and created only once, reused after detach.
    if (!isReplicationSlotExist(tx.getRef(), replication_slot))
    {
        initial_sync();
    }
    else if (!Poco::File(metadata_path).exists() || new_publication_created)
    {
        /// In case of some failure, the following cases are possible (since publication and replication slot are reused):
        /// 1. If replication slot exists and metadata file (where last synced version is written) does not exist, it is not ok.
        /// 2. If created a new publication and replication slot existed before it was created, it is not ok.
        dropReplicationSlot(tx.getRef());
        initial_sync();
    }
    else
    {
        LOG_TRACE(log, "Restoring {} tables...", storages.size());
        for (const auto & [table_name, storage] : storages)
        {
            try
            {
                nested_storages[table_name] = storage->getNested();
                storage->setStorageMetadata();
                storage->setNestedLoaded();
            }
            catch (...)
            {
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
            is_postgresql_replica_database_engine,
            nested_storages);

    consumer_task->activateAndSchedule();
}


NameSet PostgreSQLReplicationHandler::loadFromSnapshot(std::string & snapshot_name, Storages & sync_storages)
{
    NameSet success_tables;
    for (const auto & storage_data : sync_storages)
    {
        try
        {
            auto tx = std::make_shared<pqxx::ReplicationTransaction>(connection->getRef());
            const auto & table_name = storage_data.first;

            std::string query_str = fmt::format("SET TRANSACTION SNAPSHOT '{}'", snapshot_name);
            tx->exec(query_str);

            storage_data.second->createNestedIfNeeded(fetchTableStructure(tx, table_name));
            auto nested_storage = storage_data.second->getNested();

            /// Load from snapshot, which will show table state before creation of replication slot.
            /// Already connected to needed database, no need to add it to query.
            query_str = fmt::format("SELECT * FROM {}", storage_data.first);

            const StorageInMemoryMetadata & storage_metadata = nested_storage->getInMemoryMetadata();
            auto insert_context = storage_data.second->makeNestedTableContext();

            auto insert = std::make_shared<ASTInsertQuery>();
            insert->table_id = nested_storage->getStorageID();

            InterpreterInsertQuery interpreter(insert, insert_context);
            auto block_io = interpreter.execute();

            auto sample_block = storage_metadata.getSampleBlockNonMaterialized();
            PostgreSQLTransactionBlockInputStream<pqxx::ReplicationTransaction> input(tx, query_str, sample_block, DEFAULT_BLOCK_SIZE);

            assertBlocksHaveEqualStructure(input.getHeader(), block_io.out->getHeader(), "postgresql replica load from snapshot");
            copyData(input, *block_io.out);

            storage_data.second->setNestedLoaded();
            nested_storages[table_name] = nested_storage;

            /// This is needed if this method is called from reloadFromSnapshot() method below.
            success_tables.insert(table_name);
            if (consumer)
                consumer->updateNested(table_name, nested_storage);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    LOG_DEBUG(log, "Table dump end");
    return success_tables;
}


void PostgreSQLReplicationHandler::consumerFunc()
{
    std::vector<std::pair<Int32, String>> skipped_tables;

    bool schedule_now = consumer->consume(skipped_tables);

    if (!skipped_tables.empty())
        consumer->updateSkipList(reloadFromSnapshot(skipped_tables));

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


void PostgreSQLReplicationHandler::createPublicationIfNeeded(pqxx::connection & connection_)
{
    if (new_publication_created)
        return;

    postgres::Transaction<pqxx::work> tx(connection_);

    if (!isPublicationExist(tx.getRef()))
    {
        if (tables_list.empty())
        {
            for (const auto & storage_data : storages)
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


NameSet PostgreSQLReplicationHandler::fetchRequiredTables(pqxx::connection & connection_)
{
    if (tables_list.empty())
    {
        return fetchPostgreSQLTablesList(connection_);
    }
    else
    {
        createPublicationIfNeeded(connection_);
        return fetchTablesFromPublication(connection_);
    }
}


NameSet PostgreSQLReplicationHandler::fetchTablesFromPublication(pqxx::connection & connection_)
{
    std::string query = fmt::format("SELECT tablename FROM pg_publication_tables WHERE pubname = '{}'", publication_name);
    std::unordered_set<std::string> tables;
    postgres::Transaction<pqxx::read_transaction> tx(connection_);

    for (auto table_name : tx.getRef().stream<std::string>(query))
        tables.insert(std::get<0>(table_name));

    return tables;
}


PostgreSQLTableStructurePtr PostgreSQLReplicationHandler::fetchTableStructure(
        std::shared_ptr<pqxx::ReplicationTransaction> tx, const std::string & table_name)
{
    if (!is_postgresql_replica_database_engine)
        return nullptr;

    auto use_nulls = context.getSettingsRef().external_databases_use_nulls;
    return std::make_unique<PostgreSQLTableStructure>(fetchPostgreSQLTableStructure(tx, table_name, use_nulls, true));
}


std::unordered_map<Int32, String> PostgreSQLReplicationHandler::reloadFromSnapshot(
        const std::vector<std::pair<Int32, String>> & relation_data)
{
    std::unordered_map<Int32, String> tables_start_lsn;
    try
    {
        Storages sync_storages;
        for (const auto & relation : relation_data)
        {
            const auto & table_name = relation.second;
            auto * storage = storages[table_name];
            sync_storages[table_name] = storage;
            storage->dropNested();
        }

        auto replication_connection = postgres::createReplicationConnection(connection_info);
        postgres::Transaction<pqxx::nontransaction> tx(replication_connection->getRef());

        std::string snapshot_name, start_lsn;
        createReplicationSlot(tx.getRef(), start_lsn, snapshot_name, true);
        /// This snapshot is valid up to the end of the transaction, which exported it.
        auto success_tables = loadFromSnapshot(snapshot_name, sync_storages);

        for (const auto & relation : relation_data)
        {
            if (success_tables.find(relation.second) != success_tables.end())
                tables_start_lsn[relation.first] = start_lsn;
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    return tables_start_lsn;
}

}

#endif
