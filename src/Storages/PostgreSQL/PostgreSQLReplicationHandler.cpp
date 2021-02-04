#include "PostgreSQLReplicationHandler.h"
#include "PostgreSQLReplicaConsumer.h"
#include <Interpreters/InterpreterInsertQuery.h>

#include <Poco/File.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <DataStreams/PostgreSQLBlockInputStream.h>
#include <DataStreams/CountingBlockOutputStream.h>
#include <Common/CurrentThread.h>
#include <Storages/IStorage.h>
#include <DataStreams/copyData.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
}

static const auto reschedule_ms = 500;

PostgreSQLReplicationHandler::PostgreSQLReplicationHandler(
    const std::string & database_name_,
    const std::string & table_name_,
    const std::string & conn_str,
    std::shared_ptr<Context> context_,
    const std::string & publication_name_,
    const std::string & replication_slot_name_,
    const size_t max_block_size_)
    : log(&Poco::Logger::get("PostgreSQLReplicaHandler"))
    , context(context_)
    , database_name(database_name_)
    , table_name(table_name_)
    , connection_str(conn_str)
    , publication_name(publication_name_)
    , replication_slot(replication_slot_name_)
    , max_block_size(max_block_size_)
    , connection(std::make_shared<PostgreSQLConnection>(conn_str))
    , replication_connection(std::make_shared<PostgreSQLConnection>(fmt::format("{} replication=database", connection->conn_str())))
    , metadata_path(DatabaseCatalog::instance().getDatabase(database_name)->getMetadataPath() + "/.metadata")
{
    if (replication_slot.empty())
        replication_slot = fmt::format("{}_{}_ch_replication_slot", database_name, table_name);

    /// Temporary replication slot is used to acquire a snapshot for initial table synchronization and to determine starting lsn position.
    tmp_replication_slot = replication_slot + "_temp";

    startup_task = context->getSchedulePool().createTask("PostgreSQLReplicaStartup", [this]{ waitConnectionAndStart(); });
    startup_task->deactivate();
}


void PostgreSQLReplicationHandler::startup(StoragePtr storage)
{
    nested_storage = storage;
    startup_task->activateAndSchedule();
}


void PostgreSQLReplicationHandler::waitConnectionAndStart()
{
    try
    {
        connection->conn();
    }
    catch (pqxx::broken_connection const & pqxx_error)
    {
        LOG_ERROR(log, "Unable to set up connection for table {}.{}. Reconnection attempt continues. Error message: {}",
                database_name, table_name, pqxx_error.what());

        startup_task->scheduleAfter(reschedule_ms);
    }
    catch (Exception & e)
    {
        e.addMessage("while setting up connection for {}.{}", database_name, table_name);
        throw;
    }

    startReplication();
}


void PostgreSQLReplicationHandler::shutdown()
{
    if (consumer)
        consumer->stopSynchronization();
}


bool PostgreSQLReplicationHandler::isPublicationExist(std::shared_ptr<pqxx::work> tx)
{
    std::string query_str = fmt::format("SELECT exists (SELECT 1 FROM pg_publication WHERE pubname = '{}')", publication_name);
    pqxx::result result{tx->exec(query_str)};
    assert(!result.empty());
    bool publication_exists = (result[0][0].as<std::string>() == "t");

    /// TODO: check if publication is still valid?
    if (publication_exists)
        LOG_TRACE(log, "Publication {} already exists. Using existing version", publication_name);

    return publication_exists;
}


void PostgreSQLReplicationHandler::createPublication(std::shared_ptr<pqxx::work> tx)
{
    /// 'ONLY' means just a table, without descendants.
    std::string query_str = fmt::format("CREATE PUBLICATION {} FOR TABLE ONLY {}", publication_name, table_name);
    try
    {
        tx->exec(query_str);
        LOG_TRACE(log, "Created publication {}", publication_name);
    }
    catch (pqxx::undefined_table const &)
    {
        throw Exception(fmt::format("PostgreSQL table {}.{} does not exist", database_name, table_name), ErrorCodes::UNKNOWN_TABLE);
    }

    /// TODO: check replica identity?
    /// Requires changed replica identity for included table to be able to receive old values of updated rows.
}


void PostgreSQLReplicationHandler::startReplication()
{
    LOG_DEBUG(log, "PostgreSQLReplica starting replication proccess");

    /// used commands require a specific transaction isolation mode.
    replication_connection->conn()->set_variable("default_transaction_isolation", "'repeatable read'");

    auto tx = std::make_shared<pqxx::work>(*replication_connection->conn());
    if (publication_name.empty())
    {
        publication_name = fmt::format("{}_{}_ch_publication", database_name, table_name);

        /// Publication defines what tables are included into replication stream. Should be deleted only if MaterializePostgreSQL
        /// table is dropped.
        if (!isPublicationExist(tx))
            createPublication(tx);
    }
    else if (!isPublicationExist(tx))
    {
        throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Publication name '{}' is spesified in table arguments, but it does not exist", publication_name);
    }
    tx->commit();

    auto ntx = std::make_shared<pqxx::nontransaction>(*replication_connection->conn());

    /// Normally temporary replication slot should not exist.
    if (isReplicationSlotExist(ntx, tmp_replication_slot))
        dropReplicationSlot(ntx, tmp_replication_slot);

    std::string snapshot_name;
    LSNPosition start_lsn;

    auto initial_sync = [&]()
    {
        /// Temporary replication slot
        createTempReplicationSlot(ntx, start_lsn, snapshot_name);
        /// Initial table synchronization from created snapshot
        loadFromSnapshot(snapshot_name);
        /// Do not need this replication slot anymore (snapshot loaded and start lsn determined
        dropReplicationSlot(ntx, tmp_replication_slot);
        /// Non-temporary replication slot
        createReplicationSlot(ntx);
    };

    /// Non temporary replication slot should be deleted with drop table only and created only once, reused after detach.
    if (!isReplicationSlotExist(ntx, replication_slot))
    {
        initial_sync();
    }
    else if (!Poco::File(metadata_path).exists())
    {
        /// If non-temporary slot exists and metadata file (where last synced version is written) does not exist, it is not normal.
        dropReplicationSlot(ntx, replication_slot);
        initial_sync();
    }

    ntx->commit();

    LOG_DEBUG(&Poco::Logger::get("StoragePostgreSQLMetadata"), "Creating replication consumer");
    consumer = std::make_shared<PostgreSQLReplicaConsumer>(
            context,
            table_name,
            std::move(connection),
            replication_slot,
            publication_name,
            metadata_path,
            start_lsn,
            max_block_size,
            nested_storage);

    LOG_DEBUG(&Poco::Logger::get("StoragePostgreSQLMetadata"), "Successfully created replication consumer");

    consumer->startSynchronization();

    /// Takes time to close
    replication_connection->conn()->close();
}


void PostgreSQLReplicationHandler::loadFromSnapshot(std::string & snapshot_name)
{
    LOG_DEBUG(log, "Creating transaction snapshot");

    try
    {
        auto stx = std::make_unique<pqxx::work>(*connection->conn());

        /// Specific isolation level is required to read from snapshot.
        stx->set_variable("transaction_isolation", "'repeatable read'");

        std::string query_str = fmt::format("SET TRANSACTION SNAPSHOT '{}'", snapshot_name);
        stx->exec(query_str);

        /// Load from snapshot, which will show table state before creation of replication slot.
        query_str = fmt::format("SELECT * FROM {}", table_name);

        Context insert_context(*context);
        insert_context.makeQueryContext();

        auto insert = std::make_shared<ASTInsertQuery>();
        insert->table_id = nested_storage->getStorageID();

        InterpreterInsertQuery interpreter(insert, insert_context);
        auto block_io = interpreter.execute();

        const StorageInMemoryMetadata & storage_metadata = nested_storage->getInMemoryMetadata();
        auto sample_block = storage_metadata.getSampleBlockNonMaterialized();

        PostgreSQLBlockInputStream input(std::move(stx), query_str, sample_block, DEFAULT_BLOCK_SIZE);

        copyData(input, *block_io.out);
    }
    catch (Exception & e)
    {
        e.addMessage("while initial data sync for table {}.{}", database_name, table_name);
        throw;
    }

    LOG_DEBUG(log, "Done loading from snapshot");
}


bool PostgreSQLReplicationHandler::isReplicationSlotExist(NontransactionPtr ntx, std::string & slot_name)
{
    std::string query_str = fmt::format("SELECT active, restart_lsn FROM pg_replication_slots WHERE slot_name = '{}'", slot_name);
    pqxx::result result{ntx->exec(query_str)};

    /// Replication slot does not exist
    if (result.empty())
        return false;

    bool is_active = result[0][0].as<bool>();
    LOG_TRACE(log, "Replication slot {} already exists (active: {}). Restart lsn position is {}",
            slot_name, is_active, result[0][0].as<std::string>());

    return true;
}


void PostgreSQLReplicationHandler::createTempReplicationSlot(NontransactionPtr ntx, LSNPosition & start_lsn, std::string & snapshot_name)
{
    std::string query_str = fmt::format("CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL pgoutput EXPORT_SNAPSHOT", tmp_replication_slot);
    try
    {
        pqxx::result result{ntx->exec(query_str)};
        start_lsn.lsn = result[0][1].as<std::string>();
        snapshot_name = result[0][2].as<std::string>();
        LOG_TRACE(log, "Created temporary replication slot: {}, start lsn: {}, snapshot: {}",
                tmp_replication_slot, start_lsn.lsn, snapshot_name);
    }
    catch (Exception & e)
    {
        e.addMessage("while creating PostgreSQL replication slot {}", tmp_replication_slot);
        throw;
    }
}


void PostgreSQLReplicationHandler::createReplicationSlot(NontransactionPtr ntx)
{
    std::string query_str = fmt::format("CREATE_REPLICATION_SLOT {} LOGICAL pgoutput", replication_slot);
    try
    {
        pqxx::result result{ntx->exec(query_str)};
        LOG_TRACE(log, "Created replication slot: {}, start lsn: {}", replication_slot, result[0][1].as<std::string>());
    }
    catch (Exception & e)
    {
        e.addMessage("while creating PostgreSQL replication slot {}", replication_slot);
        throw;
    }
}


void PostgreSQLReplicationHandler::dropReplicationSlot(NontransactionPtr ntx, std::string & slot_name)
{
    std::string query_str = fmt::format("SELECT pg_drop_replication_slot('{}')", slot_name);
    ntx->exec(query_str);
    LOG_TRACE(log, "Replication slot {} is dropped", slot_name);
}


void PostgreSQLReplicationHandler::dropPublication(NontransactionPtr ntx)
{
    if (publication_name.empty())
        return;

    std::string query_str = fmt::format("DROP PUBLICATION IF EXISTS {}", publication_name);
    ntx->exec(query_str);
}


void PostgreSQLReplicationHandler::shutdownFinal()
{
    if (Poco::File(metadata_path).exists())
        Poco::File(metadata_path).remove();

    connection = std::make_shared<PostgreSQLConnection>(connection_str);
    auto ntx = std::make_shared<pqxx::nontransaction>(*connection->conn());

    dropPublication(ntx);
    if (isReplicationSlotExist(ntx, replication_slot))
        dropReplicationSlot(ntx, replication_slot);

    ntx->commit();
}

}
