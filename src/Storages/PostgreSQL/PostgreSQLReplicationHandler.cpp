#include "PostgreSQLReplicationHandler.h"

#include <DataStreams/PostgreSQLBlockInputStream.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>
#include <Storages/PostgreSQL/StorageMaterializedPostgreSQL.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Common/setThreadName.h>
#include <Interpreters/Context.h>
#include <DataStreams/copyData.h>


namespace DB
{

static const auto RESCHEDULE_MS = 500;
static const auto BACKOFF_TRESHOLD_MS = 10000;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

PostgreSQLReplicationHandler::PostgreSQLReplicationHandler(
    const String & replication_identifier,
    const String & remote_database_name_,
    const String & current_database_name_,
    const postgres::ConnectionInfo & connection_info_,
    ContextPtr context_,
    bool is_attach_,
    const MaterializedPostgreSQLSettings & replication_settings,
    bool is_materialized_postgresql_database_)
    : log(&Poco::Logger::get("PostgreSQLReplicationHandler"))
    , context(context_)
    , is_attach(is_attach_)
    , remote_database_name(remote_database_name_)
    , current_database_name(current_database_name_)
    , connection_info(connection_info_)
    , max_block_size(replication_settings.materialized_postgresql_max_block_size)
    , allow_automatic_update(replication_settings.materialized_postgresql_allow_automatic_update)
    , is_materialized_postgresql_database(is_materialized_postgresql_database_)
    , tables_list(replication_settings.materialized_postgresql_tables_list)
    , user_provided_snapshot(replication_settings.materialized_postgresql_snapshot)
    , connection(std::make_shared<postgres::Connection>(connection_info_))
    , milliseconds_to_wait(RESCHEDULE_MS)
{
    replication_slot = replication_settings.materialized_postgresql_replication_slot;
    if (replication_slot.empty())
    {
        user_managed_slot = false;
        replication_slot = fmt::format("{}_ch_replication_slot", replication_identifier);
    }
    publication_name = fmt::format("{}_ch_publication", replication_identifier);

    startup_task = context->getSchedulePool().createTask("PostgreSQLReplicaStartup", [this]{ waitConnectionAndStart(); });
    consumer_task = context->getSchedulePool().createTask("PostgreSQLReplicaStartup", [this]{ consumerFunc(); });
}


void PostgreSQLReplicationHandler::addStorage(const std::string & table_name, StorageMaterializedPostgreSQL * storage)
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
        connection->connect(); /// Will throw pqxx::broken_connection if no connection at the moment
        startSynchronization(false);
    }
    catch (const pqxx::broken_connection & pqxx_error)
    {
        LOG_ERROR(log, "Unable to set up connection. Reconnection attempt will continue. Error message: {}", pqxx_error.what());
        startup_task->scheduleAfter(RESCHEDULE_MS);
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


void PostgreSQLReplicationHandler::startSynchronization(bool throw_on_error)
{
    {
        pqxx::work tx(connection->getRef());
        createPublicationIfNeeded(tx);
        tx.commit();
    }

    postgres::Connection replication_connection(connection_info, /* replication */true);
    pqxx::nontransaction tx(replication_connection.getRef());

    /// List of nested tables (table_name -> nested_storage), which is passed to replication consumer.
    std::unordered_map<String, StoragePtr> nested_storages;

    /// snapshot_name is initialized only if a new replication slot is created.
    /// start_lsn is initialized in two places:
    /// 1. if replication slot does not exist, start_lsn will be returned with its creation return parameters;
    /// 2. if replication slot already exist, start_lsn is read from pg_replication_slots as
    ///    `confirmed_flush_lsn` - the address (LSN) up to which the logical slot's consumer has confirmed receiving data.
    ///    Data older than this is not available anymore.
    ///    TODO: more tests
    String snapshot_name, start_lsn;

    auto initial_sync = [&]()
    {
        LOG_TRACE(log, "Starting tables sync load");

        if (user_managed_slot)
        {
            if (user_provided_snapshot.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Using a user-defined replication slot must be provided with a snapshot from EXPORT SNAPSHOT when the slot is created."
                                "Pass it to `materialized_postgresql_snapshot` setting");
            snapshot_name = user_provided_snapshot;
        }
        else
        {
            createReplicationSlot(tx, start_lsn, snapshot_name);
        }

        for (const auto & [table_name, storage] : materialized_storages)
        {
            try
            {
                nested_storages[table_name] = loadFromSnapshot(snapshot_name, table_name, storage->as <StorageMaterializedPostgreSQL>());
            }
            catch (Exception & e)
            {
                e.addMessage("while loading table {}.{}", remote_database_name, table_name);
                tryLogCurrentException(__PRETTY_FUNCTION__);

                /// Throw in case of single MaterializedPostgreSQL storage, because initial setup is done immediately
                /// (unlike database engine where it is done in a separate thread).
                if (throw_on_error)
                    throw;
            }
        }
    };

    /// There is one replication slot for each replication handler. In case of MaterializedPostgreSQL database engine,
    /// there is one replication slot per database. Its lifetime must be equal to the lifetime of replication handler.
    /// Recreation of a replication slot imposes reloading of all tables.
    if (!isReplicationSlotExist(tx, start_lsn, /* temporary */false))
    {
        if (user_managed_slot)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Having replication slot `{}` from settings, but it does not exist", replication_slot);

        initial_sync();
    }
    /// Always drop replication slot if it is CREATE query and not ATTACH.
    else if (!is_attach)
    {
        if (!user_managed_slot)
            dropReplicationSlot(tx);

        initial_sync();
    }
    /// Synchronization and initial load already took place - do not create any new tables, just fetch StoragePtr's
    /// and pass them to replication consumer.
    else
    {
        for (const auto & [table_name, storage] : materialized_storages)
        {
            auto * materialized_storage = storage->as <StorageMaterializedPostgreSQL>();
            try
            {
                /// Try load nested table, set materialized table metadata.
                nested_storages[table_name] = materialized_storage->prepare();
            }
            catch (Exception & e)
            {
                e.addMessage("while loading table {}.{}", remote_database_name, table_name);
                tryLogCurrentException(__PRETTY_FUNCTION__);

                if (throw_on_error)
                    throw;
            }
        }
        LOG_TRACE(log, "Loaded {} tables", nested_storages.size());
    }

    tx.commit();

    /// Pass current connection to consumer. It is not std::moved implicitly, but a shared_ptr is passed.
    /// Consumer and replication handler are always executed one after another (not concurrently) and share the same connection.
    /// (Apart from the case, when shutdownFinal is called).
    /// Handler uses it only for loadFromSnapshot and shutdown methods.
    consumer = std::make_shared<MaterializedPostgreSQLConsumer>(
            context,
            connection,
            replication_slot,
            publication_name,
            start_lsn,
            max_block_size,
            allow_automatic_update,
            nested_storages);

    consumer_task->activateAndSchedule();

    /// Do not rely anymore on saved storage pointers.
    materialized_storages.clear();
}


StoragePtr PostgreSQLReplicationHandler::loadFromSnapshot(String & snapshot_name, const String & table_name,
                                                          StorageMaterializedPostgreSQL * materialized_storage)
{
    auto tx = std::make_shared<pqxx::ReplicationTransaction>(connection->getRef());

    std::string query_str = fmt::format("SET TRANSACTION SNAPSHOT '{}'", snapshot_name);
    tx->exec(query_str);

    /// Load from snapshot, which will show table state before creation of replication slot.
    /// Already connected to needed database, no need to add it to query.
    query_str = fmt::format("SELECT * FROM {}", doubleQuoteString(table_name));

    materialized_storage->createNestedIfNeeded(fetchTableStructure(*tx, table_name));
    auto nested_storage = materialized_storage->getNested();

    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = nested_storage->getStorageID();

    auto insert_context = materialized_storage->getNestedTableContext();

    InterpreterInsertQuery interpreter(insert, insert_context);
    auto block_io = interpreter.execute();

    const StorageInMemoryMetadata & storage_metadata = nested_storage->getInMemoryMetadata();
    auto sample_block = storage_metadata.getSampleBlockNonMaterialized();

    PostgreSQLTransactionBlockInputStream<pqxx::ReplicationTransaction> input(tx, query_str, sample_block, DEFAULT_BLOCK_SIZE);
    assertBlocksHaveEqualStructure(input.getHeader(), block_io.out->getHeader(), "postgresql replica load from snapshot");
    copyData(input, *block_io.out);

    nested_storage = materialized_storage->prepare();
    auto nested_table_id = nested_storage->getStorageID();
    LOG_TRACE(log, "Loaded table {}.{} (uuid: {})", nested_table_id.database_name, nested_table_id.table_name, toString(nested_table_id.uuid));

    return nested_storage;
}


void PostgreSQLReplicationHandler::consumerFunc()
{
    std::vector<std::pair<Int32, String>> skipped_tables;

    bool schedule_now = consumer->consume(skipped_tables);

    if (!skipped_tables.empty())
    {
        try
        {
            reloadFromSnapshot(skipped_tables);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    if (stop_synchronization)
    {
        LOG_TRACE(log, "Replication thread is stopped");
        return;
    }

    if (schedule_now)
    {
        milliseconds_to_wait = RESCHEDULE_MS;
        consumer_task->schedule();

        LOG_DEBUG(log, "Scheduling replication thread: now");
    }
    else
    {
        consumer_task->scheduleAfter(milliseconds_to_wait);
        if (milliseconds_to_wait < BACKOFF_TRESHOLD_MS)
            milliseconds_to_wait *= 2;

        LOG_TRACE(log, "Scheduling replication thread: after {} ms", milliseconds_to_wait);
    }
}


bool PostgreSQLReplicationHandler::isPublicationExist(pqxx::work & tx)
{
    std::string query_str = fmt::format("SELECT exists (SELECT 1 FROM pg_publication WHERE pubname = '{}')", publication_name);
    pqxx::result result{tx.exec(query_str)};
    assert(!result.empty());
    return result[0][0].as<std::string>() == "t";
}


void PostgreSQLReplicationHandler::createPublicationIfNeeded(pqxx::work & tx)
{
    auto publication_exists = isPublicationExist(tx);

    if (!is_attach && publication_exists)
    {
        /// This is a case for single Materialized storage. In case of database engine this check is done in advance.
        LOG_WARNING(log,
                    "Publication {} already exists, but it is a CREATE query, not ATTACH. Publication will be dropped",
                    publication_name);

        connection->execWithRetry([&](pqxx::nontransaction & tx_){ dropPublication(tx_); });
    }

    if (!is_attach || !publication_exists)
    {
        if (tables_list.empty())
        {
            for (const auto & storage_data : materialized_storages)
            {
                if (!tables_list.empty())
                    tables_list += ", ";
                tables_list += doubleQuoteString(storage_data.first);
            }
        }

        if (tables_list.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No table found to be replicated");

        /// 'ONLY' means just a table, without descendants.
        std::string query_str = fmt::format("CREATE PUBLICATION {} FOR TABLE ONLY {}", publication_name, tables_list);
        try
        {
            tx.exec(query_str);
            LOG_TRACE(log, "Created publication {} with tables list: {}", publication_name, tables_list);
        }
        catch (Exception & e)
        {
            e.addMessage("while creating pg_publication");
            throw;
        }
    }
    else
    {
        LOG_TRACE(log, "Using existing publication ({}) version", publication_name);
    }
}


bool PostgreSQLReplicationHandler::isReplicationSlotExist(pqxx::nontransaction & tx, String & start_lsn, bool temporary)
{
    String slot_name;
    if (temporary)
        slot_name = replication_slot + "_tmp";
    else
        slot_name = replication_slot;

    String query_str = fmt::format("SELECT active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{}'", slot_name);
    pqxx::result result{tx.exec(query_str)};

    /// Replication slot does not exist
    if (result.empty())
        return false;

    start_lsn = result[0][2].as<std::string>();

    LOG_TRACE(log, "Replication slot {} already exists (active: {}). Restart lsn position: {}, confirmed flush lsn: {}",
            slot_name, result[0][0].as<bool>(), result[0][1].as<std::string>(), start_lsn);

    return true;
}


void PostgreSQLReplicationHandler::createReplicationSlot(
        pqxx::nontransaction & tx, String & start_lsn, String & snapshot_name, bool temporary)
{
    assert(temporary || !user_managed_slot);

    String query_str, slot_name;
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
    assert(temporary || !user_managed_slot);

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
    LOG_TRACE(log, "Dropped publication: {}", publication_name);
}


void PostgreSQLReplicationHandler::shutdownFinal()
{
    try
    {
        shutdown();

        connection->execWithRetry([&](pqxx::nontransaction & tx){ dropPublication(tx); });
        String last_committed_lsn;

        connection->execWithRetry([&](pqxx::nontransaction & tx)
        {
            if (isReplicationSlotExist(tx, last_committed_lsn, /* temporary */true))
                dropReplicationSlot(tx, /* temporary */true);
        });

        if (user_managed_slot)
            return;

        connection->execWithRetry([&](pqxx::nontransaction & tx)
        {
            if (isReplicationSlotExist(tx, last_committed_lsn, /* temporary */false))
                dropReplicationSlot(tx, /* temporary */false);
        });
    }
    catch (Exception & e)
    {
        e.addMessage("while dropping replication slot: {}", replication_slot);
        LOG_ERROR(log, "Failed to drop replication slot: {}. It must be dropped manually.", replication_slot);
        throw;
    }
}


/// Used by MaterializedPostgreSQL database engine.
std::set<String> PostgreSQLReplicationHandler::fetchRequiredTables(postgres::Connection & connection_)
{
    pqxx::work tx(connection_.getRef());
    std::set<String> result_tables;

    bool publication_exists_before_startup = isPublicationExist(tx);
    LOG_DEBUG(log, "Publication exists: {}, is attach: {}", publication_exists_before_startup, is_attach);

    Strings expected_tables;
    if (!tables_list.empty())
    {
         splitInto<','>(expected_tables, tables_list);
         if (expected_tables.empty())
             throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse tables list: {}", tables_list);
         for (auto & table_name : expected_tables)
             boost::trim(table_name);
    }

    if (publication_exists_before_startup)
    {
        if (!is_attach)
        {
            LOG_WARNING(log,
                        "Publication {} already exists, but it is a CREATE query, not ATTACH. Publication will be dropped",
                        publication_name);

            connection->execWithRetry([&](pqxx::nontransaction & tx_){ dropPublication(tx_); });
        }
        else
        {
            if (tables_list.empty())
            {
                LOG_WARNING(log,
                            "Publication {} already exists and tables list is empty. Assuming publication is correct.",
                            publication_name);

                result_tables = fetchPostgreSQLTablesList(tx);
            }
            /// Check tables list from publication is the same as expected tables list.
            /// If not - drop publication and return expected tables list.
            else
            {
                result_tables = fetchTablesFromPublication(tx);
                NameSet diff;
                std::sort(expected_tables.begin(), expected_tables.end());
                std::set_symmetric_difference(expected_tables.begin(), expected_tables.end(),
                                              result_tables.begin(), result_tables.end(),
                                              std::inserter(diff, diff.begin()));
                if (!diff.empty())
                {
                    String diff_tables;
                    for (const auto & table_name : diff)
                    {
                        if (!diff_tables.empty())
                            diff_tables += ", ";
                        diff_tables += table_name;
                    }
                    String publication_tables;
                    for (const auto & table_name : result_tables)
                    {
                        if (!publication_tables.empty())
                            publication_tables += ", ";
                        publication_tables += table_name;
                    }
                    String listed_tables;
                    for (const auto & table_name : expected_tables)
                    {
                        if (!listed_tables.empty())
                            listed_tables += ", ";
                        listed_tables += table_name;
                    }

                    LOG_ERROR(log,
                              "Publication {} already exists, but specified tables list differs from publication tables list in tables: {}. ",
                              "Will use tables list from setting. "
                              "To avoid redundant work, you can try ALTER PUBLICATION query to remove redundant tables. "
                              "Or you can you ALTER SETTING. "
                              "\nPublication tables: {}.\nTables list: {}",
                              publication_name, diff_tables, publication_tables, listed_tables);

                    return std::set(expected_tables.begin(), expected_tables.end());
                }
            }
        }
    }

    if (result_tables.empty())
    {
        if (!tables_list.empty())
        {
            result_tables = std::set(expected_tables.begin(), expected_tables.end());
        }
        else
        {
            /// Fetch all tables list from database. Publication does not exist yet, which means
            /// that no replication took place. Publication will be created in
            /// startSynchronization method.
            result_tables = fetchPostgreSQLTablesList(tx);
        }
    }

    tx.commit();
    return result_tables;
}


std::set<String> PostgreSQLReplicationHandler::fetchTablesFromPublication(pqxx::work & tx)
{
    std::string query = fmt::format("SELECT tablename FROM pg_publication_tables WHERE pubname = '{}'", publication_name);
    std::set<String> tables;

    for (auto table_name : tx.stream<std::string>(query))
        tables.insert(std::get<0>(table_name));

    return tables;
}


PostgreSQLTableStructurePtr PostgreSQLReplicationHandler::fetchTableStructure(
        pqxx::ReplicationTransaction & tx, const std::string & table_name) const
{
    if (!is_materialized_postgresql_database)
        return nullptr;

    PostgreSQLTableStructure structure;
    try
    {
        structure = fetchPostgreSQLTableStructure(tx, table_name, "", true, true, true);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    return std::make_unique<PostgreSQLTableStructure>(std::move(structure));
}


void PostgreSQLReplicationHandler::reloadFromSnapshot(const std::vector<std::pair<Int32, String>> & relation_data)
{
    /// If table schema has changed, the table stops consuming changes from replication stream.
    /// If `allow_automatic_update` is true, create a new table in the background, load new table schema
    /// and all data from scratch. Then execute REPLACE query.
    /// This is only allowed for MaterializedPostgreSQL database engine.
    try
    {
        postgres::Connection replication_connection(connection_info, /* replication */true);
        pqxx::nontransaction tx(replication_connection.getRef());

        String snapshot_name, start_lsn;

        if (isReplicationSlotExist(tx, start_lsn, /* temporary */true))
            dropReplicationSlot(tx, /* temporary */true);

        createReplicationSlot(tx, start_lsn, snapshot_name, /* temporary */true);

        for (const auto & [relation_id, table_name] : relation_data)
        {
            auto storage = DatabaseCatalog::instance().getTable(StorageID(current_database_name, table_name), context);
            auto * materialized_storage = storage->as <StorageMaterializedPostgreSQL>();

            /// If for some reason this temporary table already exists - also drop it.
            auto temp_materialized_storage = materialized_storage->createTemporary();

            /// This snapshot is valid up to the end of the transaction, which exported it.
            StoragePtr temp_nested_storage = loadFromSnapshot(snapshot_name, table_name,
                                                              temp_materialized_storage->as <StorageMaterializedPostgreSQL>());

            auto table_id = materialized_storage->getNestedStorageID();
            auto temp_table_id = temp_nested_storage->getStorageID();

            LOG_TRACE(log, "Starting background update of table {} with table {}",
                      table_id.getNameForLogs(), temp_table_id.getNameForLogs());

            auto ast_rename = std::make_shared<ASTRenameQuery>();
            ASTRenameQuery::Element elem
            {
                ASTRenameQuery::Table{table_id.database_name, table_id.table_name},
                ASTRenameQuery::Table{temp_table_id.database_name, temp_table_id.table_name}
            };
            ast_rename->elements.push_back(std::move(elem));
            ast_rename->exchange = true;

            auto nested_context = materialized_storage->getNestedTableContext();

            try
            {
                auto materialized_table_lock = materialized_storage->lockForShare(String(), context->getSettingsRef().lock_acquire_timeout);
                InterpreterRenameQuery(ast_rename, nested_context).execute();

                {
                    auto nested_storage = DatabaseCatalog::instance().getTable(StorageID(table_id.database_name, table_id.table_name),
                                                                               nested_context);
                    auto nested_table_lock = nested_storage->lockForShare(String(), context->getSettingsRef().lock_acquire_timeout);
                    auto nested_table_id = nested_storage->getStorageID();

                    materialized_storage->setNestedStorageID(nested_table_id);
                    nested_storage = materialized_storage->prepare();

                    auto nested_storage_metadata = nested_storage->getInMemoryMetadataPtr();
                    auto nested_sample_block = nested_storage_metadata->getSampleBlock();
                    LOG_TRACE(log, "Updated table {}. New structure: {}",
                              nested_table_id.getNameForLogs(), nested_sample_block.dumpStructure());

                    auto materialized_storage_metadata = nested_storage->getInMemoryMetadataPtr();
                    auto materialized_sample_block = materialized_storage_metadata->getSampleBlock();

                    assertBlocksHaveEqualStructure(nested_sample_block, materialized_sample_block, "while reloading table in the background");

                    /// Pass pointer to new nested table into replication consumer, remove current table from skip list and set start lsn position.
                    consumer->updateNested(table_name, nested_storage, relation_id, start_lsn);
                }

                LOG_DEBUG(log, "Dropping table {}", temp_table_id.getNameForLogs());
                InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, nested_context, nested_context, temp_table_id, true);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        dropReplicationSlot(tx, /* temporary */true);
        tx.commit();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
