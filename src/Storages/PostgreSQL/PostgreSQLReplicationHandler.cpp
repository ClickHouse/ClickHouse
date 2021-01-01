#include "PostgreSQLReplicationHandler.h"
#include "PostgreSQLReplicaConsumer.h"

#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
}

PostgreSQLReplicationHandler::PostgreSQLReplicationHandler(
    const std::string & database_name_,
    const std::string & table_name_,
    const std::string & conn_str,
    const std::string & replication_slot_,
    const std::string & publication_name_)
    : log(&Poco::Logger::get("PostgreSQLReplicaHandler"))
    , database_name(database_name_)
    , table_name(table_name_)
    , replication_slot(replication_slot_)
    , publication_name(publication_name_)
    , connection(std::make_shared<PostgreSQLConnection>(conn_str))
{
    /// Create a replication connection, through which it is possible to execute only commands from streaming replication protocol
    /// interface. Passing 'database' as the value instructs walsender to connect to the database specified in the dbname parameter,
    /// which will allow the connection to be used for logical replication from that database.
    replication_connection = std::make_shared<PostgreSQLConnection>(fmt::format("{} replication=database", conn_str));

    /// Used commands require a specific transaction isolation mode.
    replication_connection->conn()->set_variable("default_transaction_isolation", "'repeatable read'");

    /// Non temporary replication slot should be the same at restart.
    if (replication_slot.empty())
        replication_slot = fmt::format("{}_{}_ch_replication_slot", database_name, table_name);

    /// Temporary replication slot is used to determine a start lsn position and to acquire a snapshot for initial table synchronization.
    temp_replication_slot = replication_slot + "_temp";
}


void PostgreSQLReplicationHandler::startup()
{
    tx = std::make_shared<pqxx::work>(*connection->conn());
    if (publication_name.empty())
    {
        publication_name = fmt::format("{}_{}_ch_publication", database_name, table_name);

        if (!isPublicationExist())
            createPublication();
    }
    else if (!isPublicationExist())
    {
        throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Publication name '{}' is spesified in table arguments, but it does not exist", publication_name);
    }
    tx->commit();

    startReplication();
}


bool PostgreSQLReplicationHandler::isPublicationExist()
{
    std::string query_str = fmt::format("SELECT exists (SELECT 1 FROM pg_publication WHERE pubname = '{}')", publication_name);
    pqxx::result result{tx->exec(query_str)};
    bool publication_exists = (result[0][0].as<std::string>() == "t");

    if (publication_exists)
        LOG_TRACE(log, "Publication {} already exists. Using existing version", publication_name);

    return publication_exists;
}


void PostgreSQLReplicationHandler::createPublication()
{
    /* * It is also important that change replica identity for this table to be able to receive old values of updated rows:
     *   ALTER TABLE pgbench_accounts REPLICA IDENTITY FULL;
     * * TRUNCATE and DDL are not included in PUBLICATION.
     * * 'ONLY' means just a table, without descendants.
     */
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
}


void PostgreSQLReplicationHandler::startReplication()
{
    auto ntx = std::make_shared<pqxx::nontransaction>(*replication_connection->conn());

    /// But it should not actually exist. May exist if failed to drop it before.
    if (isReplicationSlotExist(ntx, temp_replication_slot))
        dropReplicationSlot(ntx, temp_replication_slot, true);

    std::string snapshot_name;
    LSNPosition start_lsn;

    createTempReplicationSlot(ntx, start_lsn, snapshot_name);

    loadFromSnapshot(snapshot_name);

    /// Do not need this replication slot anymore (snapshot loaded and start lsn determined, will continue replication protocol
    /// with another slot, which should be the same at restart (and reused) to minimize memory usage)
    /// Non temporary replication slot should be deleted with drop table only.
    LOG_DEBUG(log, "Dropping temporaty replication slot");
    dropReplicationSlot(ntx, temp_replication_slot, true);

    if (!isReplicationSlotExist(ntx, replication_slot))
        createReplicationSlot(ntx);

    PostgreSQLReplicaConsumer consumer(
            table_name,
            connection->conn_str(),
            replication_slot,
            publication_name,
            start_lsn);

    LOG_DEBUG(log, "Commiting replication transaction");
    ntx->commit();

    consumer.run();
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
    std::string query_str = fmt::format("CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL pgoutput EXPORT_SNAPSHOT", temp_replication_slot);
    try
    {
        pqxx::result result{ntx->exec(query_str)};
        start_lsn.lsn = result[0][1].as<std::string>();
        snapshot_name = result[0][2].as<std::string>();
        LOG_TRACE(log, "Created temporary replication slot: {}, start lsn: {}, snapshot: {}",
                temp_replication_slot, start_lsn.lsn, snapshot_name);
    }
    catch (Exception & e)
    {
        e.addMessage("while creating PostgreSQL replication slot {}", temp_replication_slot);
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


void PostgreSQLReplicationHandler::dropReplicationSlot(NontransactionPtr ntx, std::string & slot_name, bool use_replication_api)
{
    if (use_replication_api)
    {
        std::string query_str = fmt::format("DROP_REPLICATION_SLOT {}", slot_name);
        ntx->exec(query_str);
    }
    else
    {
        pqxx::work work(*connection->conn());
        std::string query_str = fmt::format("SELECT pg_drop_replication_slot('{}')", slot_name);
        work.exec(query_str);
        work.commit();
    }
}


/// Only used when MaterializePostgreSQL table is dropped.
void PostgreSQLReplicationHandler::checkAndDropReplicationSlot()
{
    auto ntx = std::make_shared<pqxx::nontransaction>(*replication_connection->conn());
    if (isReplicationSlotExist(ntx, replication_slot))
        dropReplicationSlot(ntx, replication_slot, false);
    ntx->commit();
}


void PostgreSQLReplicationHandler::loadFromSnapshot(std::string & snapshot_name)
{
    auto stx = std::make_unique<pqxx::work>(*connection->conn());
    /// Required to execute the following command.
    stx->set_variable("transaction_isolation", "'repeatable read'");

    std::string query_str = fmt::format("SET TRANSACTION SNAPSHOT '{}'", snapshot_name);
    stx->exec(query_str);

    LOG_DEBUG(log, "Created transaction snapshot");
    query_str = fmt::format("SELECT * FROM {}", table_name);
    pqxx::result result{stx->exec(query_str)};
    if (!result.empty())
    {
        pqxx::row row{result[0]};
        for (auto res : row)
        {
            if (std::size(res))
                LOG_TRACE(log, "GOT {}", res.as<std::string>());
            else
                LOG_TRACE(log, "GOT NULL");
        }
    }
    LOG_DEBUG(log, "Done loading from snapshot");
    stx->commit();
}


}
