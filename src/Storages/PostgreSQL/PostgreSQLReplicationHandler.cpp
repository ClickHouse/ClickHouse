
#include <base/sort.h>

#include <Common/setThreadName.h>
#include <Parsers/ASTTableOverrides.h>
#include <Processors/Sources/PostgreSQLSource.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/Pipe.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>
#include <Storages/PostgreSQL/MaterializedPostgreSQLSettings.h>
#include <Storages/PostgreSQL/PostgreSQLReplicationHandler.h>
#include <Storages/PostgreSQL/StorageMaterializedPostgreSQL.h>
#include <Interpreters/getTableOverride.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/Context.h>
#include <Databases/DatabaseOnDisk.h>
#include <boost/algorithm/string/trim.hpp>
#include <Poco/String.h>


namespace DB
{

static const auto CLEANUP_RESCHEDULE_MS = 600000 * 3; /// 30 min
static constexpr size_t replication_slot_name_max_size = 64;

namespace MaterializedPostgreSQLSetting
{
    extern const MaterializedPostgreSQLSettingsUInt64 materialized_postgresql_backoff_factor;
    extern const MaterializedPostgreSQLSettingsUInt64 materialized_postgresql_backoff_max_ms;
    extern const MaterializedPostgreSQLSettingsUInt64 materialized_postgresql_backoff_min_ms;
    extern const MaterializedPostgreSQLSettingsUInt64 materialized_postgresql_max_block_size;
    extern const MaterializedPostgreSQLSettingsString materialized_postgresql_replication_slot;
    extern const MaterializedPostgreSQLSettingsString materialized_postgresql_schema;
    extern const MaterializedPostgreSQLSettingsString materialized_postgresql_schema_list;
    extern const MaterializedPostgreSQLSettingsString materialized_postgresql_snapshot;
    extern const MaterializedPostgreSQLSettingsString materialized_postgresql_tables_list;
    extern const MaterializedPostgreSQLSettingsBool materialized_postgresql_tables_list_with_schema;
    extern const MaterializedPostgreSQLSettingsBool materialized_postgresql_use_unique_replication_consumer_identifier;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int POSTGRESQL_REPLICATION_INTERNAL_ERROR;
    extern const int QUERY_NOT_ALLOWED;
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


namespace
{
    /// There can be several replication slots per publication, but one publication per table/database replication.
    /// Replication slot might be unique (contain uuid) to allow have multiple replicas for the same PostgreSQL table/database.

    String getPublicationName(const String & postgres_database, const String & postgres_table)
    {
        return fmt::format(
            "{}_ch_publication",
            postgres_table.empty() ? postgres_database : fmt::format("{}_{}", postgres_database, postgres_table));
    }

    void checkReplicationSlot(String name)
    {
        for (const auto & c : name)
        {
            const bool ok = (std::isalpha(c) && std::islower(c)) || std::isdigit(c) || c == '_';
            if (!ok)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Replication slot can contain lower-case letters, numbers, and the underscore character. "
                    "Got: {}", name);
            }
        }

        if (name.size() > replication_slot_name_max_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Too big replication slot size: {}", name);
    }

    String normalizeReplicationSlot(String name)
    {
        name = Poco::toLower(name);
        for (auto & c : name)
            if (c == '-')
                c = '_';
        return name;
    }

    String getReplicationSlotName(
        const String & postgres_database,
        const String & postgres_table,
        const String & clickhouse_uuid,
        const MaterializedPostgreSQLSettings & replication_settings)
    {
        String slot_name = replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_replication_slot];
        if (slot_name.empty())
        {
            if (replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_use_unique_replication_consumer_identifier])
                slot_name = clickhouse_uuid;
            else
                slot_name = postgres_table.empty() ? postgres_database : fmt::format("{}_{}_ch_replication_slot", postgres_database, postgres_table);

            slot_name = normalizeReplicationSlot(slot_name);
        }
        return slot_name;
    }
}

PostgreSQLReplicationHandler::PostgreSQLReplicationHandler(
    const String & postgres_database_,
    const String & postgres_table_,
    const String & clickhouse_database_,
    const String & clickhouse_uuid_,
    const postgres::ConnectionInfo & connection_info_,
    ContextPtr context_,
    bool is_attach_,
    const MaterializedPostgreSQLSettings & replication_settings,
    bool is_materialized_postgresql_database_)
    : WithContext(context_->getGlobalContext())
    , log(getLogger("PostgreSQLReplicationHandler"))
    , is_attach(is_attach_)
    , postgres_database(postgres_database_)
    , postgres_schema(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_schema])
    , current_database_name(clickhouse_database_)
    , connection_info(connection_info_)
    , max_block_size(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_max_block_size])
    , is_materialized_postgresql_database(is_materialized_postgresql_database_)
    , tables_list(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_tables_list])
    , schema_list(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_schema_list])
    , schema_as_a_part_of_table_name(!schema_list.empty() || replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_tables_list_with_schema])
    , user_managed_slot(!replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_replication_slot].value.empty())
    , user_provided_snapshot(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_snapshot])
    , replication_slot(getReplicationSlotName(postgres_database_, postgres_table_, clickhouse_uuid_, replication_settings))
    , tmp_replication_slot(replication_slot + "_tmp")
    , publication_name(getPublicationName(postgres_database_, postgres_table_))
    , reschedule_backoff_min_ms(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_backoff_min_ms])
    , reschedule_backoff_max_ms(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_backoff_max_ms])
    , reschedule_backoff_factor(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_backoff_factor])
    , milliseconds_to_wait(reschedule_backoff_min_ms)
{
    if (!schema_list.empty() && !tables_list.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot have schema list and tables list at the same time");

    if (!schema_list.empty() && !postgres_schema.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot have schema list and common schema at the same time");

    checkReplicationSlot(replication_slot);

    LOG_INFO(log, "Using replication slot {} and publication {}", replication_slot, doubleQuoteString(publication_name));

    startup_task = getContext()->getSchedulePool().createTask("PostgreSQLReplicaStartup", [this]{ checkConnectionAndStart(); });
    consumer_task = getContext()->getSchedulePool().createTask("PostgreSQLReplicaConsume", [this]{ consumerFunc(); });
    cleanup_task = getContext()->getSchedulePool().createTask("PostgreSQLReplicaCleanup", [this]{ cleanupFunc(); });
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
        startup_task->scheduleAfter(milliseconds_to_wait);
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

    LOG_TRACE(log, "Deactivating startup task");
    startup_task->deactivate();

    LOG_TRACE(log, "Deactivating consumer task");
    consumer_task->deactivate();

    LOG_TRACE(log, "Deactivating cleanup task");
    cleanup_task->deactivate();

    LOG_TRACE(log, "Resetting consumer");
    consumer.reset(); /// Clear shared pointers to inner storages.
}


void PostgreSQLReplicationHandler::assertInitialized() const
{
    if (!replication_handler_initialized)
    {
        throw Exception(
            ErrorCodes::QUERY_NOT_ALLOWED,
            "PostgreSQL replication initialization did not finish successfully. Please check logs for error messages");
    }
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
                                "Using a user-defined replication slot must "
                                "be provided with a snapshot from EXPORT SNAPSHOT when the slot is created."
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
                if (throw_on_error && !is_materialized_postgresql_database)
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
        LOG_DEBUG(log, "Loaded {} tables", nested_storages.size());
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
                auto table_structure = fetchTableStructure(tx, table_name);
                if (!table_structure->physical_columns)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "No columns");
                auto storage_info = StorageInfo(materialized_storage->getNested(), table_structure->physical_columns->attributes);
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
            getContext(),
            std::move(tmp_connection),
            replication_slot,
            publication_name,
            start_lsn,
            max_block_size,
            schema_as_a_part_of_table_name,
            nested_storages,
            (is_materialized_postgresql_database ? postgres_database : postgres_database + '.' + tables_list));

    replication_handler_initialized = true;

    consumer_task->activateAndSchedule();
    cleanup_task->activateAndSchedule();

    /// Do not rely anymore on saved storage pointers.
    materialized_storages.clear();
}


ASTPtr PostgreSQLReplicationHandler::getCreateNestedTableQuery(StorageMaterializedPostgreSQL * storage, const String & table_name)
{
    postgres::Connection connection(connection_info);
    pqxx::nontransaction tx(connection.getRef());

    auto table_structure = fetchTableStructure(tx, table_name);
    auto table_override = tryGetTableOverride(current_database_name, table_name);
    return storage->getCreateNestedTableQuery(std::move(table_structure), table_override ? table_override->as<ASTTableOverride>() : nullptr);
}


StorageInfo PostgreSQLReplicationHandler::loadFromSnapshot(postgres::Connection & connection, String & snapshot_name, const String & table_name,
                                                          StorageMaterializedPostgreSQL * materialized_storage)
{
    auto tx = std::make_shared<pqxx::ReplicationTransaction>(connection.getRef());

    std::string query_str = fmt::format("SET TRANSACTION SNAPSHOT '{}'", snapshot_name);
    tx->exec(query_str);

    PostgreSQLTableStructurePtr table_structure;
    try
    {
        table_structure = fetchTableStructure(*tx, table_name);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        table_structure = std::make_unique<PostgreSQLTableStructure>();
    }
    if (!table_structure->physical_columns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No table attributes");

    auto table_attributes = table_structure->physical_columns->attributes;
    auto columns = getTableAllowedColumns(table_name);

    /// Load from snapshot, which will show table state before creation of replication slot.
    /// Already connected to needed database, no need to add it to query.
    auto quoted_name = doubleQuoteWithSchema(table_name);
    if (columns.empty())
        query_str = fmt::format("SELECT * FROM ONLY {}", quoted_name);
    else
    {
        /// We should not use columns list from getTableAllowedColumns because it may have broken columns order
        Strings allowed_columns;
        for (const auto & column : table_structure->physical_columns->columns)
            allowed_columns.push_back(column.name);
        query_str = fmt::format("SELECT {} FROM ONLY {}", boost::algorithm::join(allowed_columns, ","), quoted_name);
    }

    LOG_DEBUG(log, "Loading PostgreSQL table {}.{}", postgres_database, quoted_name);

    auto table_override = tryGetTableOverride(current_database_name, table_name);
    materialized_storage->createNestedIfNeeded(std::move(table_structure), table_override ? table_override->as<ASTTableOverride>() : nullptr);
    auto nested_storage = materialized_storage->getNested();

    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = nested_storage->getStorageID();

    auto insert_context = materialized_storage->getNestedTableContext();

    InterpreterInsertQuery interpreter(
        insert,
        insert_context,
        /* allow_materialized */ false,
        /* no_squash */ false,
        /* no_destination */ false,
        /* async_isnert */ false);
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

    LOG_DEBUG(log, "Loaded table {}.{} (uuid: {})",
              nested_table_id.database_name, nested_table_id.table_name, toString(nested_table_id.uuid));

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

    if (!stop_synchronization)
        cleanup_task->scheduleAfter(CLEANUP_RESCHEDULE_MS);
}

PostgreSQLReplicationHandler::ConsumerPtr PostgreSQLReplicationHandler::getConsumer()
{
    if (!consumer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Consumer not initialized");
    return consumer;
}

void PostgreSQLReplicationHandler::consumerFunc()
{
    assertInitialized();

    bool schedule_now = true;
    try
    {
        schedule_now = getConsumer()->consume();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    if (stop_synchronization)
    {
        LOG_DEBUG(log, "Replication thread is stopped");
        return;
    }

    if (schedule_now)
    {
        milliseconds_to_wait = reschedule_backoff_min_ms;
        consumer_task->schedule();

        LOG_DEBUG(log, "Scheduling replication thread: now");
    }
    else
    {
        if (milliseconds_to_wait < reschedule_backoff_max_ms)
            milliseconds_to_wait = std::min(milliseconds_to_wait * reschedule_backoff_factor, reschedule_backoff_max_ms);

        LOG_DEBUG(log, "Scheduling replication thread: after {} ms", milliseconds_to_wait);
        consumer_task->scheduleAfter(milliseconds_to_wait);
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
                    doubleQuoteString(publication_name));

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
        std::string query_str = fmt::format("CREATE PUBLICATION {} FOR TABLE ONLY {}", doubleQuoteString(publication_name), tables_list);
        try
        {
            tx.exec(query_str);
            LOG_DEBUG(log, "Created publication {} with tables list: {}", doubleQuoteString(publication_name), tables_list);
        }
        catch (Exception & e)
        {
            e.addMessage("while creating pg_publication");
            throw;
        }
    }
    else
    {
        LOG_DEBUG(log, "Using existing publication ({}) version", doubleQuoteString(publication_name));
    }
}


bool PostgreSQLReplicationHandler::isReplicationSlotExist(pqxx::nontransaction & tx, String & start_lsn, bool temporary)
{
    String slot_name;
    if (temporary)
        slot_name = tmp_replication_slot;
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
        slot_name = tmp_replication_slot;
    else
        slot_name = replication_slot;

    query_str = fmt::format("CREATE_REPLICATION_SLOT {} LOGICAL pgoutput EXPORT_SNAPSHOT", doubleQuoteString(slot_name));

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
        slot_name = tmp_replication_slot;
    else
        slot_name = replication_slot;

    std::string query_str = fmt::format("SELECT pg_drop_replication_slot('{}')", slot_name);

    tx.exec(query_str);
    LOG_DEBUG(log, "Dropped replication slot: {}", slot_name);
}


void PostgreSQLReplicationHandler::dropPublication(pqxx::nontransaction & tx)
{
    std::string query_str = fmt::format("DROP PUBLICATION IF EXISTS {}", doubleQuoteString(publication_name));
    tx.exec(query_str);
    LOG_DEBUG(log, "Dropped publication: {}", doubleQuoteString(publication_name));
}


void PostgreSQLReplicationHandler::addTableToPublication(pqxx::nontransaction & ntx, const String & table_name)
{
    std::string query_str = fmt::format("ALTER PUBLICATION {} ADD TABLE ONLY {}", doubleQuoteString(publication_name), doubleQuoteWithSchema(table_name));
    ntx.exec(query_str);
    LOG_TRACE(log, "Added table {} to publication `{}`", doubleQuoteWithSchema(table_name), publication_name);
}


void PostgreSQLReplicationHandler::removeTableFromPublication(pqxx::nontransaction & ntx, const String & table_name)
{
    try
    {
        std::string query_str = fmt::format("ALTER PUBLICATION {} DROP TABLE ONLY {}", doubleQuoteString(publication_name), doubleQuoteWithSchema(table_name));
        ntx.exec(query_str);
        LOG_TRACE(log, "Removed table `{}` from publication `{}`", doubleQuoteWithSchema(table_name), publication_name);
    }
    catch (const pqxx::undefined_table &)
    {
        /// Removing table from replication must succeed even if table does not exist in PostgreSQL.
        LOG_WARNING(log, "Did not remove table {} from publication, because table does not exist in PostgreSQL (publication: {})",
                    doubleQuoteWithSchema(table_name), publication_name);
    }
}


void PostgreSQLReplicationHandler::setSetting(const SettingChange & setting)
{
    assertInitialized();

    consumer_task->deactivate();
    getConsumer()->setSetting(setting);
    consumer_task->activateAndSchedule();
}


/// Allowed columns for table from materialized_postgresql_tables_list setting
Strings PostgreSQLReplicationHandler::getTableAllowedColumns(const std::string & table_name) const
{
    Strings result;
    if (tables_list.empty())
        return result;

    size_t table_pos = 0;
    while (true)
    {
        table_pos = tables_list.find(table_name, table_pos + 1);
        if (table_pos == std::string::npos)
            return result;
        if (table_pos + table_name.length() + 1 > tables_list.length())
            return result;
        if (tables_list[table_pos + table_name.length() + 1] == '(' ||
            tables_list[table_pos + table_name.length() + 1] == ',' ||
            tables_list[table_pos + table_name.length() + 1] == ' '
        )
            break;
    }

    String column_list = tables_list.substr(table_pos + table_name.length() + 1);
    column_list.erase(std::remove(column_list.begin(), column_list.end(), '"'), column_list.end());
    boost::trim(column_list);
    if (column_list.empty() || column_list[0] != '(')
        return result;

    size_t end_bracket_pos = column_list.find(')');
    column_list = column_list.substr(1, end_bracket_pos - 1);
    splitInto<','>(result, column_list);

    return result;
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
    catch (...)
    {
        LOG_ERROR(log, "Failed to drop replication slot: {}. It must be dropped manually. Error: {}", replication_slot, getCurrentExceptionMessage(true));
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
        /// Removing columns `table(col1, col2)` from tables_list
        String cleared_tables_list = tables_list;
        while (true)
        {
            size_t start_bracket_pos = cleared_tables_list.find('(');
            size_t end_bracket_pos = cleared_tables_list.find(')');
            if (start_bracket_pos == std::string::npos || end_bracket_pos == std::string::npos)
            {
                break;
            }
            cleared_tables_list = cleared_tables_list.substr(0, start_bracket_pos) + cleared_tables_list.substr(end_bracket_pos + 1);
        }

        splitInto<','>(expected_tables, cleared_tables_list);
        if (expected_tables.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse tables list: {}", tables_list);

        for (auto & table_name : expected_tables)
        {
            boost::trim(table_name);
        }
    }

    /// Try to fetch tables list from publication if there is not tables list.
    /// If there is a tables list -- check that lists are consistent and if not -- remove publication, it will be recreated.
    if (publication_exists_before_startup)
    {
        if (!is_attach)
        {
            LOG_WARNING(log,
                        "Publication {} already exists, but it is a CREATE query, not ATTACH. Publication will be dropped",
                        doubleQuoteString(publication_name));

            connection.execWithRetry([&](pqxx::nontransaction & tx_){ dropPublication(tx_); });
        }
        else
        {
            if (tables_list.empty())
            {
                LOG_WARNING(log,
                            "Publication {} already exists and tables list is empty. Assuming publication is correct.",
                            doubleQuoteString(publication_name));

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
                              "Publication {} already exists, but specified tables list differs from publication tables list in tables: {}. "
                              "Will use tables list from setting. "
                              "To avoid redundant work, you can try ALTER PUBLICATION query to remove redundant tables. "
                              "Or you can you ALTER SETTING. "
                              "\nPublication tables: {}.\nTables list: {}",
                              doubleQuoteString(publication_name), diff_tables, publication_tables, listed_tables);

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

                std::string tables_string;
                for (const auto & table : result_tables)
                {
                    if (!tables_string.empty())
                        tables_string += ", ";
                    tables_string += table;
                }
                LOG_DEBUG(log, "Tables list was fetched from PostgreSQL directly: {}", tables_string);
            }
        }
    }


    /// `schema1.table1, schema2.table2, ...` -> `"schema1"."table1", "schema2"."table2", ...`
    /// or
    /// `table1, table2, ...` + setting `schema` -> `"schema"."table1", "schema"."table2", ...`
    /// or
    /// `table1, table2(id,name), ...` + setting `schema` -> `"schema"."table1", "schema"."table2"("id","name"), ...`
    if (!tables_list.empty())
    {
        Strings parts;
        splitInto<','>(parts, tables_list);
        if (parts.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty list of tables");

        bool is_column = false;
        WriteBufferFromOwnString buf;
        for (auto & part : parts)
        {
            boost::trim(part);

            size_t bracket_pos = part.find('(');
            if (bracket_pos != std::string::npos)
            {
                is_column = true;
                std::string table_name = part.substr(0, bracket_pos);
                boost::trim(table_name);
                buf << doubleQuoteWithSchema(table_name);

                part = part.substr(bracket_pos + 1);
                boost::trim(part);
                buf << '(';
                buf << doubleQuoteString(part);
            }
            else if (part.back() == ')')
            {
                is_column = false;
                part = part.substr(0, part.size() - 1);
                boost::trim(part);
                buf << doubleQuoteString(part);
                buf << ')';
            }
            else if (is_column)
            {
                buf << doubleQuoteString(part);
            }
            else
            {
                buf << doubleQuoteWithSchema(part);
            }
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


template<typename T>
PostgreSQLTableStructurePtr PostgreSQLReplicationHandler::fetchTableStructure(
        T & tx, const std::string & table_name) const
{
    PostgreSQLTableStructure structure;
    auto [schema, table] = getSchemaAndTableName(table_name);
    structure = fetchPostgreSQLTableStructure(tx, table, schema, true, true, true, getTableAllowedColumns(table_name));

    return std::make_unique<PostgreSQLTableStructure>(std::move(structure));
}

template
PostgreSQLTableStructurePtr PostgreSQLReplicationHandler::fetchTableStructure(
        pqxx::ReadTransaction & tx, const std::string & table_name) const;

template
PostgreSQLTableStructurePtr PostgreSQLReplicationHandler::fetchTableStructure(
        pqxx::ReplicationTransaction & tx, const std::string & table_name) const;

template
PostgreSQLTableStructurePtr PostgreSQLReplicationHandler::fetchTableStructure(
        pqxx::nontransaction & tx, const std::string & table_name) const;

void PostgreSQLReplicationHandler::addTableToReplication(StorageMaterializedPostgreSQL * materialized_storage, const String & postgres_table_name)
{
    assertInitialized();

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
        getConsumer()->addNested(postgres_table_name, nested_storage_info, start_lsn);
        LOG_TRACE(log, "Table `{}` successfully added to replication", postgres_table_name);
    }
    catch (...)
    {
        consumer_task->activate();
        consumer_task->scheduleAfter(milliseconds_to_wait);

        auto error_message = getCurrentExceptionMessage(false);
        throw Exception(ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                        "Failed to add table `{}` to replication. Info: {}", postgres_table_name, error_message);
    }
    consumer_task->activateAndSchedule();
}


void PostgreSQLReplicationHandler::removeTableFromReplication(const String & postgres_table_name)
{
    assertInitialized();

    consumer_task->deactivate();
    try
    {
        postgres::Connection replication_connection(connection_info, /* replication */true);

        {
            pqxx::nontransaction tx(replication_connection.getRef());
            removeTableFromPublication(tx, postgres_table_name);
        }

        /// Pass storage to consumer and lsn position, from which to start receiving replication messages for this table.
        getConsumer()->removeNested(postgres_table_name);
    }
    catch (...)
    {
        consumer_task->activate();
        consumer_task->scheduleAfter(milliseconds_to_wait);

        auto error_message = getCurrentExceptionMessage(false);
        throw Exception(ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                        "Failed to remove table `{}` from replication. Info: {}", postgres_table_name, error_message);
    }
    consumer_task->activateAndSchedule();
}


}
