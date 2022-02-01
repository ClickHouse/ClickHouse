#include "PostgreSQLReplicationHandler.h"

#include <base/sort.h>

#include <Common/setThreadName.h>
#include <Parsers/ASTTableOverrides.h>
#include <Processors/Transforms/PostgreSQLSource.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>
#include <Storages/PostgreSQL/StorageMaterializedPostgreSQL.h>
#include <Interpreters/getTableOverride.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/Context.h>
#include <Databases/DatabaseOnDisk.h>
#include <boost/algorithm/string/trim.hpp>


namespace DB
{

static const auto RESCHEDULE_MS = 1000;
static const auto BACKOFF_TRESHOLD_MS = 10000;
static const auto CLEANUP_RESCHEDULE_MS = 600000 * 3; /// 30 min

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int POSTGRESQL_REPLICATION_INTERNAL_ERROR;
}

class TemporaryReplicationSlot
{
public:
    TemporaryReplicationSlot(
        PostgreSQLReplicationHandler * handler_,
        std::shared_ptr<pqxx::nontransaction> tx_,
        String & start_lsn,
        String & snapshot_name)
        : handler(handler_), tx(tx_)
    {
        handler->createReplicationSlot(*tx, start_lsn, snapshot_name, /* temporary */true);
    }

    ~TemporaryReplicationSlot()
    {
        handler->dropReplicationSlot(*tx, /* temporary */true);
    }

private:
    PostgreSQLReplicationHandler * handler;
    std::shared_ptr<pqxx::nontransaction> tx;
};


PostgreSQLReplicationHandler::PostgreSQLReplicationHandler(
    const String & replication_identifier,
    const String & postgres_database_,
    const String & current_database_name_,
    const postgres::ConnectionInfo & connection_info_,
    ContextPtr context_,
    bool is_attach_,
    const MaterializedPostgreSQLSettings & replication_settings,
    bool is_materialized_postgresql_database_)
    : log(&Poco::Logger::get("PostgreSQLReplicationHandler"))
    , context(context_)
    , is_attach(is_attach_)
    , postgres_database(postgres_database_)
    , postgres_schema(replication_settings.materialized_postgresql_schema)
    , current_database_name(current_database_name_)
    , connection_info(connection_info_)
    , max_block_size(replication_settings.materialized_postgresql_max_block_size)
    , allow_automatic_update(replication_settings.materialized_postgresql_allow_automatic_update)
    , is_materialized_postgresql_database(is_materialized_postgresql_database_)
    , tables_list(replication_settings.materialized_postgresql_tables_list)
    , schema_list(replication_settings.materialized_postgresql_schema_list)
    , schema_as_a_part_of_table_name(!schema_list.empty() || replication_settings.materialized_postgresql_tables_list_with_schema)
    , user_provided_snapshot(replication_settings.materialized_postgresql_snapshot)
    , milliseconds_to_wait(RESCHEDULE_MS)
{
    if (!schema_list.empty() && !tables_list.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot have schema list and tables list at the same time");

    if (!schema_list.empty() && !postgres_schema.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot have schema list and common schema at the same time");

    replication_slot = replication_settings.materialized_postgresql_replication_slot;
    if (replication_slot.empty())
    {
        user_managed_slot = false;
        replication_slot = fmt::format("{}_ch_replication_slot", replication_identifier);
    }
    publication_name = fmt::format("{}_ch_publication", replication_identifier);

    startup_task = context->getSchedulePool().createTask("PostgreSQLReplicaStartup", [this]{ checkConnectionAndStart(); });
    consumer_task = context->getSchedulePool().createTask("PostgreSQLReplicaStartup", [this]{ consumerFunc(); });
    cleanup_task = context->getSchedulePool().createTask("PostgreSQLReplicaStartup", [this]{ cleanupFunc(); });
}


void PostgreSQLReplicationHandler::addStorage(const std::string & table_name, StorageMaterializedPostgreSQL * storage)
{
    materialized_storages[table_name] = storage;
}


void PostgreSQLReplicationHandler::startup(bool delayed)
{
    if (delayed)
    {
        startup_task->activateAndSchedule();
    }
    else
    {
        startSynchronization(/* throw_on_error */ true);
    }
}


std::pair<String, String> PostgreSQLReplicationHandler::getSchemaAndTableName(const String & table_name) const
{
    /// !schema_list.empty() -- We replicate all tables from specifies schemas.
    /// In this case when tables list is fetched, we append schema with dot. But without quotes.

    /// If there is a setting `tables_list`, then table names can be put there along with schema,
    /// separated by dot and with no quotes. We add double quotes in this case.

    if (!postgres_schema.empty())
        return std::make_pair(postgres_schema, table_name);

    if (auto pos = table_name.find('.'); schema_as_a_part_of_table_name && pos != std::string::npos)
        return std::make_pair(table_name.substr(0, pos), table_name.substr(pos + 1));

    return std::make_pair("", table_name);
}


String PostgreSQLReplicationHandler::doubleQuoteWithSchema(const String & table_name) const
{
    auto [schema, table] = getSchemaAndTableName(table_name);

    if (schema.empty())
        return doubleQuoteString(table);

    return doubleQuoteString(schema) + '.' + doubleQuoteString(table);
}


void PostgreSQLReplicationHandler::checkConnectionAndStart()
{
    try
    {
        postgres::Connection connection(connection_info);
        connection.connect(); /// Will throw pqxx::broken_connection if no connection at the moment
        startSynchronization(is_attach);
    }
    catch (const pqxx::broken_connection & pqxx_error)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        if (!is_attach)
            throw;

        LOG_ERROR(log, "Unable to set up connection. Reconnection attempt will continue. Error message: {}", pqxx_error.what());
        startup_task->scheduleAfter(RESCHEDULE_MS);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        if (!is_attach)
            throw;
    }
}


void PostgreSQLReplicationHandler::shutdown()
{
    stop_synchronization.store(true);
    startup_task->deactivate();
    consumer_task->deactivate();
    cleanup_task->deactivate();
    consumer.reset(); /// Clear shared pointers to inner storages.
}


void PostgreSQLReplicationHandler::startSynchronization(bool throw_on_error)
{
    postgres::Connection replication_connection(connection_info, /* replication */true);
    pqxx::nontransaction tx(replication_connection.getRef());
    createPublicationIfNeeded(tx);

    /// List of nested tables (table_name -> nested_storage), which is passed to replication consumer.
    std::unordered_map<String, StorageInfo> nested_storages;

    /// snapshot_name is initialized only if a new replication slot is created.
    /// start_lsn is initialized in two places:
    /// 1. if replication slot does not exist, start_lsn will be returned with its creation return parameters;
    /// 2. if replication slot already exist, start_lsn is read from pg_replication_slots as
    ///    `confirmed_flush_lsn` - the address (LSN) up to which the logical slot's consumer has confirmed receiving data.
    ///    Data older than this is not available anymore.
    String snapshot_name, start_lsn;

    /// Also lets have a separate non-replication connection, because we need two parallel transactions and
    /// one connection can have one transaction at a time.
    auto tmp_connection = std::make_shared<postgres::Connection>(connection_info);

    auto initial_sync = [&]()
    {
        LOG_DEBUG(log, "Starting tables sync load");

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
                nested_storages.emplace(table_name, loadFromSnapshot(*tmp_connection, snapshot_name, table_name, storage->as<StorageMaterializedPostgreSQL>()));
            }
            catch (Exception & e)
            {
                e.addMessage("while loading table `{}`.`{}`", postgres_database, table_name);
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
                auto [postgres_table_schema, postgres_table_name] = getSchemaAndTableName(table_name);
                auto table_structure = fetchPostgreSQLTableStructure(tx, postgres_table_name, postgres_table_schema, true, true, true);
                if (!table_structure.physical_columns)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "No columns");
                auto storage_info = StorageInfo(materialized_storage->getNested(), table_structure.physical_columns->attributes);
                nested_storages.emplace(table_name, std::move(storage_info));
            }
            catch (Exception & e)
            {
                e.addMessage("while loading table {}.{}", postgres_database, table_name);
                tryLogCurrentException(__PRETTY_FUNCTION__);

                if (throw_on_error)
                    throw;
            }
        }
        LOG_DEBUG(log, "Loaded {} tables", nested_storages.size());
    }

    tx.commit();

    /// Pass current connection to consumer. It is not std::moved implicitly, but a shared_ptr is passed.
    /// Consumer and replication handler are always executed one after another (not concurrently) and share the same connection.
    /// (Apart from the case, when shutdownFinal is called).
    /// Handler uses it only for loadFromSnapshot and shutdown methods.
    consumer = std::make_shared<MaterializedPostgreSQLConsumer>(
            context,
            std::move(tmp_connection),
            replication_slot,
            publication_name,
            start_lsn,
            max_block_size,
            schema_as_a_part_of_table_name,
            allow_automatic_update,
            nested_storages,
            (is_materialized_postgresql_database ? postgres_database : postgres_database + '.' + tables_list));

    consumer_task->activateAndSchedule();
    cleanup_task->activateAndSchedule();

    /// Do not rely anymore on saved storage pointers.
    materialized_storages.clear();
}


ASTPtr PostgreSQLReplicationHandler::getCreateNestedTableQuery(StorageMaterializedPostgreSQL * storage, const String & table_name)
{
    postgres::Connection connection(connection_info);
    pqxx::nontransaction tx(connection.getRef());

    auto [postgres_table_schema, postgres_table_name] = getSchemaAndTableName(table_name);
    auto table_structure = std::make_unique<PostgreSQLTableStructure>(fetchPostgreSQLTableStructure(tx, postgres_table_name, postgres_table_schema, true, true, true));

    auto table_override = tryGetTableOverride(current_database_name, table_name);
    return storage->getCreateNestedTableQuery(std::move(table_structure), table_override ? table_override->as<ASTTableOverride>() : nullptr);
}


StorageInfo PostgreSQLReplicationHandler::loadFromSnapshot(postgres::Connection & connection, String & snapshot_name, const String & table_name,
                                                          StorageMaterializedPostgreSQL * materialized_storage)
{
    auto tx = std::make_shared<pqxx::ReplicationTransaction>(connection.getRef());

    std::string query_str = fmt::format("SET TRANSACTION SNAPSHOT '{}'", snapshot_name);
    tx->exec(query_str);

    /// Load from snapshot, which will show table state before creation of replication slot.
    /// Already connected to needed database, no need to add it to query.
    auto quoted_name = doubleQuoteWithSchema(table_name);
    query_str = fmt::format("SELECT * FROM {}", quoted_name);
    LOG_DEBUG(log, "Loading PostgreSQL table {}.{}", postgres_database, quoted_name);

    auto table_structure = fetchTableStructure(*tx, table_name);
    if (!table_structure->physical_columns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No table attributes");
    auto table_attributes = table_structure->physical_columns->attributes;

    auto table_override = tryGetTableOverride(current_database_name, table_name);
    materialized_storage->createNestedIfNeeded(std::move(table_structure), table_override ? table_override->as<ASTTableOverride>() : nullptr);
    auto nested_storage = materialized_storage->getNested();

    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = nested_storage->getStorageID();

    auto insert_context = materialized_storage->getNestedTableContext();

    InterpreterInsertQuery interpreter(insert, insert_context);
    auto block_io = interpreter.execute();

    const StorageInMemoryMetadata & storage_metadata = nested_storage->getInMemoryMetadata();
    auto sample_block = storage_metadata.getSampleBlockNonMaterialized();

    auto input = std::make_unique<PostgreSQLTransactionSource<pqxx::ReplicationTransaction>>(tx, query_str, sample_block, DEFAULT_BLOCK_SIZE);
    assertBlocksHaveEqualStructure(input->getPort().getHeader(), block_io.pipeline.getHeader(), "postgresql replica load from snapshot");
    block_io.pipeline.complete(Pipe(std::move(input)));

    CompletedPipelineExecutor executor(block_io.pipeline);
    executor.execute();

    materialized_storage->set(nested_storage);
    auto nested_table_id = nested_storage->getStorageID();
    LOG_DEBUG(log, "Loaded table {}.{} (uuid: {})", nested_table_id.database_name, nested_table_id.table_name, toString(nested_table_id.uuid));

    return StorageInfo(nested_storage, std::move(table_attributes));
}


void PostgreSQLReplicationHandler::cleanupFunc()
{
    /// It is very important to make sure temporary replication slots are removed!
    /// So just in case every 30 minutes check if one still exists.
    postgres::Connection connection(connection_info);
    String last_committed_lsn;
    connection.execWithRetry([&](pqxx::nontransaction & tx)
    {
        if (isReplicationSlotExist(tx, last_committed_lsn, /* temporary */true))
            dropReplicationSlot(tx, /* temporary */true);
    });
    cleanup_task->scheduleAfter(CLEANUP_RESCHEDULE_MS);
}


void PostgreSQLReplicationHandler::consumerFunc()
{
    std::vector<std::pair<Int32, String>> skipped_tables;

    bool schedule_now = consumer->consume(skipped_tables);

    LOG_DEBUG(log, "checking for skipped tables: {}", skipped_tables.size());
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
        LOG_DEBUG(log, "Replication thread is stopped");
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

        LOG_DEBUG(log, "Scheduling replication thread: after {} ms", milliseconds_to_wait);
    }
}


bool PostgreSQLReplicationHandler::isPublicationExist(pqxx::nontransaction & tx)
{
    std::string query_str = fmt::format("SELECT exists (SELECT 1 FROM pg_publication WHERE pubname = '{}')", publication_name);
    pqxx::result result{tx.exec(query_str)};
    assert(!result.empty());
    return result[0][0].as<std::string>() == "t";
}


void PostgreSQLReplicationHandler::createPublicationIfNeeded(pqxx::nontransaction & tx)
{
    auto publication_exists = isPublicationExist(tx);

    if (!is_attach && publication_exists)
    {
        /// This is a case for single Materialized storage. In case of database engine this check is done in advance.
        LOG_WARNING(log,
                    "Publication {} already exists, but it is a CREATE query, not ATTACH. Publication will be dropped",
                    publication_name);

        dropPublication(tx);
    }

    if (!is_attach || !publication_exists)
    {
        if (tables_list.empty())
        {
            if (materialized_storages.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "No tables to replicate");

            WriteBufferFromOwnString buf;
            for (const auto & storage_data : materialized_storages)
            {
                buf << doubleQuoteWithSchema(storage_data.first);
                buf << ",";
            }
            tables_list = buf.str();
            tables_list.resize(tables_list.size() - 1);
        }

        if (tables_list.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No table found to be replicated");

        /// 'ONLY' means just a table, without descendants.
        std::string query_str = fmt::format("CREATE PUBLICATION {} FOR TABLE ONLY {}", publication_name, tables_list);
        try
        {
            tx.exec(query_str);
            LOG_DEBUG(log, "Created publication {} with tables list: {}", publication_name, tables_list);
        }
        catch (Exception & e)
        {
            e.addMessage("while creating pg_publication");
            throw;
        }
    }
    else
    {
        LOG_DEBUG(log, "Using existing publication ({}) version", publication_name);
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

    LOG_DEBUG(log, "Replication slot {} already exists (active: {}). Restart lsn position: {}, confirmed flush lsn: {}",
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
        LOG_TRACE(log, "Created replication slot: {}, start lsn: {}, snapshot: {}", replication_slot, start_lsn, snapshot_name);
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
    LOG_DEBUG(log, "Dropped replication slot: {}", slot_name);
}


void PostgreSQLReplicationHandler::dropPublication(pqxx::nontransaction & tx)
{
    std::string query_str = fmt::format("DROP PUBLICATION IF EXISTS {}", publication_name);
    tx.exec(query_str);
    LOG_DEBUG(log, "Dropped publication: {}", publication_name);
}


void PostgreSQLReplicationHandler::addTableToPublication(pqxx::nontransaction & ntx, const String & table_name)
{
    std::string query_str = fmt::format("ALTER PUBLICATION {} ADD TABLE ONLY {}", publication_name, doubleQuoteWithSchema(table_name));
    ntx.exec(query_str);
    LOG_TRACE(log, "Added table {} to publication `{}`", doubleQuoteWithSchema(table_name), publication_name);
}


void PostgreSQLReplicationHandler::removeTableFromPublication(pqxx::nontransaction & ntx, const String & table_name)
{
    try
    {
        std::string query_str = fmt::format("ALTER PUBLICATION {} DROP TABLE ONLY {}", publication_name, doubleQuoteWithSchema(table_name));
        ntx.exec(query_str);
        LOG_TRACE(log, "Removed table `{}` from publication `{}`", doubleQuoteWithSchema(table_name), publication_name);
    }
    catch (const pqxx::undefined_table &)
    {
        /// Removing table from replication must succeed even if table does not exist in PostgreSQL.
        LOG_WARNING(log, "Did not remove table {} from publication, because table does not exist in PostgreSQL", doubleQuoteWithSchema(table_name), publication_name);
    }
}


void PostgreSQLReplicationHandler::setSetting(const SettingChange & setting)
{
    consumer_task->deactivate();
    consumer->setSetting(setting);
    consumer_task->activateAndSchedule();
}


void PostgreSQLReplicationHandler::shutdownFinal()
{
    try
    {
        shutdown();

        postgres::Connection connection(connection_info);
        connection.execWithRetry([&](pqxx::nontransaction & tx){ dropPublication(tx); });
        String last_committed_lsn;

        connection.execWithRetry([&](pqxx::nontransaction & tx)
        {
            if (isReplicationSlotExist(tx, last_committed_lsn, /* temporary */true))
                dropReplicationSlot(tx, /* temporary */true);
        });

        if (user_managed_slot)
            return;

        connection.execWithRetry([&](pqxx::nontransaction & tx)
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
std::set<String> PostgreSQLReplicationHandler::fetchRequiredTables()
{
    postgres::Connection connection(connection_info);
    std::set<String> result_tables;
    bool publication_exists_before_startup;

    {
        pqxx::nontransaction tx(connection.getRef());
        publication_exists_before_startup = isPublicationExist(tx);
    }

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

    /// Try to fetch tables list from publication if there is not tables list.
    /// If there is a tables list -- check that lists are consistent and if not -- remove publication, it will be recreated.
    if (publication_exists_before_startup)
    {
        if (!is_attach)
        {
            LOG_WARNING(log,
                        "Publication {} already exists, but it is a CREATE query, not ATTACH. Publication will be dropped",
                        publication_name);

            connection.execWithRetry([&](pqxx::nontransaction & tx_){ dropPublication(tx_); });
        }
        else
        {
            if (tables_list.empty())
            {
                LOG_WARNING(log,
                            "Publication {} already exists and tables list is empty. Assuming publication is correct.",
                            publication_name);

                {
                    pqxx::nontransaction tx(connection.getRef());
                    result_tables = fetchPostgreSQLTablesList(tx, schema_list.empty() ? postgres_schema : schema_list);
                }
            }
            /// Check tables list from publication is the same as expected tables list.
            /// If not - drop publication and return expected tables list.
            else
            {
                {
                    pqxx::work tx(connection.getRef());
                    result_tables = fetchTablesFromPublication(tx);
                }

                NameSet diff;
                ::sort(expected_tables.begin(), expected_tables.end());
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
            {
                pqxx::nontransaction tx(connection.getRef());
                result_tables = fetchPostgreSQLTablesList(tx, schema_list.empty() ? postgres_schema : schema_list);
            }
        }
    }


    /// `schema1.table1, schema2.table2, ...` -> `"schema1"."table1", "schema2"."table2", ...`
    /// or
    /// `table1, table2, ...` + setting `schema` -> `"schema"."table1", "schema"."table2", ...`
    if (!tables_list.empty())
    {
        Strings tables_names;
        splitInto<','>(tables_names, tables_list);
        if (tables_names.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty list of tables");

        WriteBufferFromOwnString buf;
        for (auto & table_name : tables_names)
        {
            boost::trim(table_name);
            buf << doubleQuoteWithSchema(table_name);
            buf << ",";
        }
        tables_list = buf.str();
        tables_list.resize(tables_list.size() - 1);
    }
    /// Also we make sure that queries in postgres always use quoted version "table_schema"."table_name".
    /// But tables in ClickHouse in case of multi-schame database are never double-quoted.
    /// It is ok, because they are accessed with backticks: postgres_database.`table_schema.table_name`.
    /// We do quote tables_list table AFTER collected expected_tables, because expected_tables are future clickhouse tables.

    return result_tables;
}


std::set<String> PostgreSQLReplicationHandler::fetchTablesFromPublication(pqxx::work & tx)
{
    std::string query = fmt::format("SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = '{}'", publication_name);
    std::set<String> tables;

    for (const auto & [schema, table] : tx.stream<std::string, std::string>(query))
        tables.insert(schema_as_a_part_of_table_name ? schema + '.' + table : table);

    return tables;
}


PostgreSQLTableStructurePtr PostgreSQLReplicationHandler::fetchTableStructure(
        pqxx::ReplicationTransaction & tx, const std::string & table_name) const
{
    PostgreSQLTableStructure structure;
    try
    {
        auto [schema, table] = getSchemaAndTableName(table_name);
        structure = fetchPostgreSQLTableStructure(tx, table, schema, true, true, true);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    return std::make_unique<PostgreSQLTableStructure>(std::move(structure));
}


void PostgreSQLReplicationHandler::addTableToReplication(StorageMaterializedPostgreSQL * materialized_storage, const String & postgres_table_name)
{
    /// Note: we have to ensure that replication consumer task is stopped when we reload table, because otherwise
    /// it can read wal beyond start lsn position (from which this table is being loaded), which will result in losing data.
    consumer_task->deactivate();
    try
    {
        LOG_TRACE(log, "Adding table `{}` to replication", postgres_table_name);
        postgres::Connection replication_connection(connection_info, /* replication */true);
        String snapshot_name, start_lsn;
        StorageInfo nested_storage_info{ nullptr, {} };

        {
            auto tx = std::make_shared<pqxx::nontransaction>(replication_connection.getRef());

            if (isReplicationSlotExist(*tx, start_lsn, /* temporary */true))
                dropReplicationSlot(*tx, /* temporary */true);

            TemporaryReplicationSlot temporary_slot(this, tx, start_lsn, snapshot_name);

            /// Protect against deadlock.
            auto nested = DatabaseCatalog::instance().tryGetTable(materialized_storage->getNestedStorageID(), materialized_storage->getNestedTableContext());
            if (!nested)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Internal table was not created");

            postgres::Connection tmp_connection(connection_info);
            nested_storage_info = loadFromSnapshot(tmp_connection, snapshot_name, postgres_table_name, materialized_storage);
            materialized_storage->set(nested_storage_info.storage);
        }

        {
            pqxx::nontransaction tx(replication_connection.getRef());
            addTableToPublication(tx, postgres_table_name);
        }

        /// Pass storage to consumer and lsn position, from which to start receiving replication messages for this table.
        consumer->addNested(postgres_table_name, nested_storage_info, start_lsn);
        LOG_TRACE(log, "Table `{}` successfully added to replication", postgres_table_name);
    }
    catch (...)
    {
        consumer_task->activate();
        consumer_task->scheduleAfter(RESCHEDULE_MS);

        auto error_message = getCurrentExceptionMessage(false);
        throw Exception(ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                        "Failed to add table `{}` to replication. Info: {}", postgres_table_name, error_message);
    }
    consumer_task->activateAndSchedule();
}


void PostgreSQLReplicationHandler::removeTableFromReplication(const String & postgres_table_name)
{
    consumer_task->deactivate();
    try
    {
        postgres::Connection replication_connection(connection_info, /* replication */true);

        {
            pqxx::nontransaction tx(replication_connection.getRef());
            removeTableFromPublication(tx, postgres_table_name);
        }

        /// Pass storage to consumer and lsn position, from which to start receiving replication messages for this table.
        consumer->removeNested(postgres_table_name);
    }
    catch (...)
    {
        consumer_task->activate();
        consumer_task->scheduleAfter(RESCHEDULE_MS);

        auto error_message = getCurrentExceptionMessage(false);
        throw Exception(ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                        "Failed to remove table `{}` from replication. Info: {}", postgres_table_name, error_message);
    }
    consumer_task->activateAndSchedule();
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
        auto tx = std::make_shared<pqxx::nontransaction>(replication_connection.getRef());

        {
            String snapshot_name, start_lsn;
            if (isReplicationSlotExist(*tx, start_lsn, /* temporary */true))
                dropReplicationSlot(*tx, /* temporary */true);

            TemporaryReplicationSlot temporary_slot(this, tx, start_lsn, snapshot_name);
            postgres::Connection tmp_connection(connection_info);

            for (const auto & [relation_id, table_name] : relation_data)
            {
                auto storage = DatabaseCatalog::instance().getTable(StorageID(current_database_name, table_name), context);
                auto * materialized_storage = storage->as <StorageMaterializedPostgreSQL>();
                auto materialized_table_lock = materialized_storage->lockForShare(String(), context->getSettingsRef().lock_acquire_timeout);

                /// If for some reason this temporary table already exists - also drop it.
                auto temp_materialized_storage = materialized_storage->createTemporary();

                /// This snapshot is valid up to the end of the transaction, which exported it.
                auto [temp_nested_storage, table_attributes] = loadFromSnapshot(
                    tmp_connection, snapshot_name, table_name, temp_materialized_storage->as <StorageMaterializedPostgreSQL>());

                auto table_id = materialized_storage->getNestedStorageID();
                auto temp_table_id = temp_nested_storage->getStorageID();

                LOG_DEBUG(log, "Starting background update of table {} ({} with {})",
                        table_name, table_id.getNameForLogs(), temp_table_id.getNameForLogs());

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
                    InterpreterRenameQuery(ast_rename, nested_context).execute();

                    auto nested_storage = DatabaseCatalog::instance().getTable(StorageID(table_id.database_name, table_id.table_name, temp_table_id.uuid), nested_context);
                    materialized_storage->set(nested_storage);

                    auto nested_sample_block = nested_storage->getInMemoryMetadataPtr()->getSampleBlock();
                    auto materialized_sample_block = materialized_storage->getInMemoryMetadataPtr()->getSampleBlock();
                    assertBlocksHaveEqualStructure(nested_sample_block, materialized_sample_block, "while reloading table in the background");

                    LOG_INFO(log, "Updated table {}. New structure: {}",
                            nested_storage->getStorageID().getNameForLogs(), nested_sample_block.dumpStructure());

                    /// Pass pointer to new nested table into replication consumer, remove current table from skip list and set start lsn position.
                    consumer->updateNested(table_name, StorageInfo(nested_storage, std::move(table_attributes)), relation_id, start_lsn);

                    auto table_to_drop = DatabaseCatalog::instance().getTable(StorageID(temp_table_id.database_name, temp_table_id.table_name, table_id.uuid), nested_context);
                    auto drop_table_id = table_to_drop->getStorageID();

                    if (drop_table_id == nested_storage->getStorageID())
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                                        "Cannot drop table because is has the same uuid as new table: {}", drop_table_id.getNameForLogs());

                    LOG_DEBUG(log, "Dropping table {}", drop_table_id.getNameForLogs());
                    InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, nested_context, nested_context, drop_table_id, true);
                }
                catch (...)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
            }
        }

        tx->commit();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
