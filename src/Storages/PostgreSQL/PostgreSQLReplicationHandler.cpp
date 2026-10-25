#include <optional>
#include <random>
#include <unordered_set>
#include <base/sort.h>

#include <Core/Settings.h>
#include <Core/BackgroundSchedulePool.h>
#include <Common/FailPoint.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/thread_local_rng.h>
#include <Parsers/ASTTableOverrides.h>
#include <Processors/Sources/PostgreSQLSource.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/Pipe.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Storages/PostgreSQL/MaterializedPostgreSQLSettings.h>
#include <Storages/PostgreSQL/PostgreSQLReplicationHandler.h>
#include <Storages/PostgreSQL/StorageMaterializedPostgreSQL.h>
#include <Interpreters/getTableOverride.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabasesCommon.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/replace.hpp>
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
    extern const MaterializedPostgreSQLSettingsBool materialized_postgresql_use_extended_date_and_time_types;
}

namespace Setting
{
    extern const SettingsFloat postgresql_fault_injection_probability;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int POSTGRESQL_REPLICATION_INTERNAL_ERROR;
    extern const int QUERY_NOT_ALLOWED;
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char materialized_postgresql_fail_after_snapshot_on_attach[];
    extern const char materialized_postgresql_pause_before_attach_table_snapshot[];
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
        try
        {
            handler->dropReplicationSlot(*tx, /* temporary */true);
        }
        catch (...)
        {
            tryLogCurrentException("TemporaryReplicationSlot");
        }
    }

private:
    PostgreSQLReplicationHandler * handler;
    std::shared_ptr<pqxx::nontransaction> tx;
};


namespace
{
    /// There can be several replication slots per publication, but one publication per table/database replication.
    /// Replication slot might be unique (contain uuid) to allow have multiple replicas for the same PostgreSQL table/database.

    /// A MaterializedPostgreSQL engine that targets PostgreSQL's default schema — either implicitly (the
    /// `materialized_postgresql_schema` setting is left empty) or explicitly (set to `public`) — must keep
    /// the legacy, schema-unaware publication and default replication-slot names, derived from the database
    /// and (for the single-table engine) the bare table name only. Only a genuinely non-default schema is
    /// included in the generated name. Otherwise the generated default object names would change for
    /// tables/databases created before the identity became schema-aware, and their `ATTACH` would look for a
    /// slot/publication that does not exist, run an initial sync, and reload a snapshot into the
    /// already-existing nested table (duplicating data).
    bool isDefaultPostgreSQLSchema(const String & postgres_schema)
    {
        return postgres_schema.empty() || postgres_schema == "public";
    }

    /// A collision-resistant, fixed-length identity derived from the full (database, schema, table) triple.
    /// It is used in place of a plain `database_schema_table` concatenation in the schema-aware
    /// names below. A plain concatenation with `_` is not injective: `schema = a_b`,
    /// `table = c` and `schema = a`, `table = b_c` both produce `..._a_b_c_...`. The replication slot name
    /// is additionally folded by normalizeReplicationSlot() (lower-cased, `-` mapped to `_`), so even
    /// names PostgreSQL keeps distinct — the schemas `"Foo"` and `"foo"`, or `"a-b"` and `"a_b"` — would
    /// otherwise map to one slot. In either case two distinct source tables would share one publication or
    /// one replication slot and their consumers would cross-talk, the very failure this schema-aware
    /// identity is meant to remove. Hashing a length-prefixed (hence unambiguous) serialization of the
    /// triple keeps the generated name injective in practice, of a fixed 16-character length independent
    /// of the database, schema and table lengths, and inside the `[a-z0-9_]` slot character set.
    String getSchemaAwareIdentityHash(const String & postgres_database, const String & postgres_schema, const String & postgres_table)
    {
        SipHash hash;
        hash.update(static_cast<UInt64>(postgres_database.size()));
        hash.update(postgres_database.data(), postgres_database.size());
        hash.update(static_cast<UInt64>(postgres_schema.size()));
        hash.update(postgres_schema.data(), postgres_schema.size());
        hash.update(static_cast<UInt64>(postgres_table.size()));
        hash.update(postgres_table.data(), postgres_table.size());
        return fmt::format("{:016x}", hash.get64());
    }

    /// The base name of the schema-aware publication and default replication slot — used by the single-table
    /// engine, and by the database engine with a non-default common schema. It keeps a
    /// short, human-readable prefix taken from the PostgreSQL database name purely for recognizability in
    /// `pg_replication_slots`/`pg_publication`, followed by the fixed-length identity hash. Only the prefix
    /// length is bounded here: the full (database, schema, table) identity is carried by the hash, so the
    /// prefix is cosmetic and capping it cannot reintroduce a collision. Bounding the whole base name keeps
    /// the generated publication and replication-slot names within PostgreSQL's identifier length limit
    /// regardless of how long the database name is — otherwise a moderately long database name would push
    /// the slot name over the limit and checkReplicationSlot() would reject the table before replication
    /// starts. The longest fixed suffix appended to this base name is the replication slot's
    /// `_ch_replication_slot` (20 bytes), to which a temporary slot adds `_tmp` (4 bytes); a 16-byte prefix
    /// plus the `_` separator and 16-byte hash therefore yields at most 57 bytes, comfortably inside the
    /// 63-byte limit. The publication's `_ch_publication` suffix is shorter, so it is covered too.
    String getSchemaAwareIdentityName(const String & postgres_database, const String & postgres_schema, const String & postgres_table)
    {
        static constexpr size_t schema_aware_database_prefix_max_size = 16;
        return fmt::format(
            "{}_{}",
            postgres_database.substr(0, schema_aware_database_prefix_max_size),
            getSchemaAwareIdentityHash(postgres_database, postgres_schema, postgres_table));
    }

    String getPublicationName(const String & postgres_database, const String & postgres_schema, const String & postgres_table)
    {
        /// The publication name preserves the case of the database/table name. It is created via
        /// `CREATE PUBLICATION "<name>"` (case-preserving) and looked up by exact `pubname` match,
        /// so it must not be folded to lower case here — otherwise two tables whose names differ
        /// only by case would collide on a single publication. The consumer takes care to quote the
        /// name when it hands it to the `pgoutput` plugin via the `publication_names` option (which
        /// PostgreSQL parses with `SplitIdentifierString`, folding unquoted identifiers to lower
        /// case), so both sides agree even for names with upper-case letters.
        String name;
        if (!isDefaultPostgreSQLSchema(postgres_schema))
            /// A non-default `materialized_postgresql_schema` — either a single-table MaterializedPostgreSQL
            /// engine, or a database engine replicating one common non-default schema. Include the schema so
            /// that two engines replicating from different schemas of the same PostgreSQL database do not
            /// collide on a single publication (which would make their consumers cross-talk, because in
            /// single-schema mode the publication carries only the bare relation name). A plain
            /// `database_schema_table` concatenation is not injective, so a collision-resistant hash of the
            /// full identity is used instead, with a bounded database prefix (see getSchemaAwareIdentityName()).
            /// For the database engine the remote table name is empty, so the identity is over
            /// `(database, schema, "")`, which is still distinct from any single-table identity (non-empty table).
            name = getSchemaAwareIdentityName(postgres_database, postgres_schema, postgres_table);
        else if (postgres_table.empty())
            /// MaterializedPostgreSQL database engine over the default schema: one publication per database.
            name = postgres_database;
        else
            name = fmt::format("{}_{}", postgres_database, postgres_table);
        return fmt::format("{}_ch_publication", name);
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
        const String & postgres_schema,
        const String & postgres_table,
        const String & clickhouse_uuid,
        const MaterializedPostgreSQLSettings & replication_settings)
    {
        String slot_name = replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_replication_slot];
        if (slot_name.empty())
        {
            if (replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_use_unique_replication_consumer_identifier])
                slot_name = clickhouse_uuid;
            else if (!isDefaultPostgreSQLSchema(postgres_schema))
                /// Include the schema for the same reason as in getPublicationName(), via the same
                /// collision-resistant and length-bounded identity: otherwise two engines replicating from
                /// different schemas of the same PostgreSQL database would share the default replication slot
                /// (and normalizeReplicationSlot() would additionally fold case- or hyphen-distinct schema
                /// names together). Covers both the single-table engine and the database engine (whose remote
                /// table name is empty).
                slot_name = fmt::format("{}_ch_replication_slot", getSchemaAwareIdentityName(postgres_database, postgres_schema, postgres_table));
            else if (postgres_table.empty())
                /// MaterializedPostgreSQL database engine over the default schema.
                slot_name = postgres_database;
            else
                slot_name = fmt::format("{}_{}_ch_replication_slot", postgres_database, postgres_table);

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
    bool is_fresh_definition_,
    const MaterializedPostgreSQLSettings & replication_settings,
    bool is_materialized_postgresql_database_)
    : WithContext(context_->getGlobalContext())
    , log(getLogger("PostgreSQLReplicationHandler"))
    , is_attach(is_attach_)
    , is_fresh_definition(is_fresh_definition_)
    , postgres_database(postgres_database_)
    , postgres_schema(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_schema])
    , current_database_name(clickhouse_database_)
    , connection_info(connection_info_)
    , max_block_size(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_max_block_size])
    , is_materialized_postgresql_database(is_materialized_postgresql_database_)
    , tables_list(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_tables_list])
    , has_configured_tables_list(!tables_list.empty())
    , schema_list(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_schema_list])
    , schema_as_a_part_of_table_name(!schema_list.empty() || replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_tables_list_with_schema])
    , use_extended_date_and_time_types(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_use_extended_date_and_time_types])
    , user_managed_slot(!replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_replication_slot].value.empty())
    , user_provided_snapshot(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_snapshot])
    , replication_slot(getReplicationSlotName(postgres_database_, postgres_schema, postgres_table_, clickhouse_uuid_, replication_settings))
    , tmp_replication_slot(replication_slot + "_tmp")
    , publication_name(getPublicationName(postgres_database_, postgres_schema, postgres_table_))
    , legacy_replication_slot(getReplicationSlotName(postgres_database_, /* postgres_schema */ "", postgres_table_, clickhouse_uuid_, replication_settings))
    , legacy_publication_name(getPublicationName(postgres_database_, /* postgres_schema */ "", postgres_table_))
    , reschedule_backoff_min_ms(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_backoff_min_ms])
    , reschedule_backoff_max_ms(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_backoff_max_ms])
    , reschedule_backoff_factor(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_backoff_factor])
    , milliseconds_to_wait(reschedule_backoff_min_ms)
    , fault_injection_probability(getContext()->getSettingsRef()[Setting::postgresql_fault_injection_probability])
{
    if (!schema_list.empty() && !tables_list.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot have schema list and tables list at the same time");

    if (!schema_list.empty() && !postgres_schema.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot have schema list and common schema at the same time");

    checkReplicationSlot(replication_slot);

    /// `table(col1, col2), table2, ...`: commas separate the elements, except inside a column list. The
    /// single-table engine carries its raw remote table name in `tables_list`, which never has a column list.
    if (is_materialized_postgresql_database && !tables_list.empty())
    {
        String element;
        std::optional<std::set<String>> columns;
        String column;
        auto add_column = [&]
        {
            boost::trim(column);
            if (!column.empty())
                columns->insert(column);
            column.clear();
        };
        auto add_element = [&]
        {
            boost::trim(element);
            if (columns && !element.empty())
                configured_column_lists[getNormalizedSchemaAndTableName(element)] = std::move(*columns);
            element.clear();
            columns.reset();
        };
        bool in_columns = false;
        for (const char c : tables_list)
        {
            if (in_columns)
            {
                if (c == ',' || c == ')')
                    add_column();
                else
                    column += c;
                in_columns = c != ')';
            }
            else if (c == '(')
            {
                in_columns = true;
                columns.emplace();
            }
            else if (c == ',')
                add_element();
            else if (!columns)
                element += c;
        }
        add_element();
    }

    LOG_INFO(log, "Using replication slot {} and publication {}", replication_slot, doubleQuoteString(publication_name));

    startup_task = getContext()->getSchedulePool()->createTask(StorageID::createEmpty(), "PostgreSQLReplicaStartup", [this]{ checkConnectionAndStart(); });
    consumer_task = getContext()->getSchedulePool()->createTask(StorageID::createEmpty(), "PostgreSQLReplicaConsume", [this]{ consumerFunc(); });
    cleanup_task = getContext()->getSchedulePool()->createTask(StorageID::createEmpty(), "PostgreSQLReplicaCleanup", [this]{ cleanupFunc(); });
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


std::pair<String, String> PostgreSQLReplicationHandler::getNormalizedSchemaAndTableName(const String & table_name) const
{
    auto [schema, table] = getSchemaAndTableName(table_name);
    return std::make_pair(isDefaultPostgreSQLSchema(schema) ? "public" : schema, table);
}


String PostgreSQLReplicationHandler::collidingForeignPublishedTables(
    const std::set<std::pair<String, String>> & expected,
    const std::set<std::pair<String, String>> & published) const
{
    /// In the multi-schema modes the replication consumer identifies relations by "schema.table",
    /// so a table from another schema never collides with a replicated table.
    if (schema_as_a_part_of_table_name)
        return {};

    std::unordered_set<String> expected_bare_names;
    for (const auto & [schema, table] : expected)
        expected_bare_names.insert(table);

    String colliding;
    for (const auto & [schema, table] : published)
    {
        if (expected.contains(std::make_pair(schema, table)) || !expected_bare_names.contains(table))
            continue;
        if (!colliding.empty())
            colliding += ", ";
        colliding += schema + '.' + table;
    }
    return colliding;
}


void PostgreSQLReplicationHandler::setSnapshotPositionsFile(DiskPtr disk, const String & path)
{
    snapshot_positions_disk = std::move(disk);
    snapshot_positions_path = path;
}


PostgreSQLReplicationHandler::SnapshotPositions PostgreSQLReplicationHandler::loadSnapshotPositions() const
{
    SnapshotPositions positions;
    if (!snapshot_positions_disk || !snapshot_positions_disk->existsFile(snapshot_positions_path))
        return positions;

    const String content = readMetadataFile(snapshot_positions_disk, snapshot_positions_path);
    ReadBufferFromString in(content);
    while (!in.eof())
    {
        String table_name;
        SnapshotPosition position;
        readEscapedString(table_name, in);
        assertChar('\t', in);
        readEscapedString(position.start_lsn, in);
        assertChar('\t', in);
        readBoolText(position.completed, in);
        assertChar('\t', in);
        readBoolText(position.owns_publication_entry, in);
        assertChar('\n', in);
        positions.emplace(std::move(table_name), std::move(position));
    }
    return positions;
}


void PostgreSQLReplicationHandler::saveSnapshotPositions(const SnapshotPositions & positions) const
{
    if (!snapshot_positions_disk)
        return;

    if (positions.empty())
    {
        snapshot_positions_disk->removeFileIfExists(snapshot_positions_path);
        return;
    }

    WriteBufferFromOwnString out;
    for (const auto & [table_name, position] : positions)
    {
        writeEscapedString(table_name, out);
        writeChar('\t', out);
        writeEscapedString(position.start_lsn, out);
        writeChar('\t', out);
        writeBoolText(position.completed, out);
        writeChar('\t', out);
        writeBoolText(position.owns_publication_entry, out);
        writeChar('\n', out);
    }

    const String tmp_path = snapshot_positions_path + ".tmp";
    writeMetadataFile(snapshot_positions_disk, tmp_path, out.str(), /* fsync_metadata */ true);
    snapshot_positions_disk->replaceFile(tmp_path, snapshot_positions_path);
}


void PostgreSQLReplicationHandler::markSnapshotCompleted(const String & table_name) const
{
    auto positions = loadSnapshotPositions();
    auto it = positions.find(table_name);
    if (it == positions.end() || it->second.completed)
        return;

    it->second.completed = true;
    saveSnapshotPositions(positions);
}


void PostgreSQLReplicationHandler::markPublicationEntryOwned(const String & table_name) const
{
    auto positions = loadSnapshotPositions();
    auto it = positions.find(table_name);
    if (it == positions.end() || it->second.owns_publication_entry)
        return;

    it->second.owns_publication_entry = true;
    saveSnapshotPositions(positions);
}


void PostgreSQLReplicationHandler::dropNestedTable(StorageMaterializedPostgreSQL * materialized_storage) const
{
    if (materialized_storage->tryGetNested())
        InterpreterDropQuery::executeDropQuery(
            ASTDropQuery::Kind::Drop, getContext(), materialized_storage->getNestedTableContext(),
            materialized_storage->getNestedStorageID(), /* sync */ true, /* ignore_sync_setting */ true);

    /// The wrapper may stay without a nested table if its replacement snapshot fails: it must not be counted as
    /// a materialized table by the last-table check of `DETACH TABLE ... PERMANENTLY` or by a database backup.
    materialized_storage->resetNested();
}


bool PostgreSQLReplicationHandler::hasNestedStorage() const
{
    for (const auto & [table_name, storage] : materialized_storages)
    {
        if (storage->tryGetNested())
            return true;
    }
    return false;
}


String PostgreSQLReplicationHandler::doubleQuoteWithSchema(const String & table_name) const
{
    auto [schema, table] = getSchemaAndTableName(table_name);

    if (schema.empty())
        return doubleQuoteStringPostgreSQL(table);

    return doubleQuoteStringPostgreSQL(schema) + '.' + doubleQuoteStringPostgreSQL(table);
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
        tryLogCurrentException(log);

        if (!is_attach)
            throw;

        LOG_ERROR(log, "Unable to set up connection. Reconnection attempt will continue. Error message: {}", pqxx_error.what());
        startup_task->scheduleAfter(milliseconds_to_wait);
    }
    catch (const Exception & e)
    {
        tryLogCurrentException(log);

        if (!is_attach)
            throw;

        /// On attach the startup task must keep retrying on any error so replication starts on its own once
        /// a transient condition clears, instead of leaving the attached table permanently unsynchronized
        /// until a server restart or a manual re-attach. Two examples: the attach-time legacy-identity
        /// ownership conflict (see adoptLegacyReplicationIdentityIfNeeded) throws
        /// POSTGRESQL_REPLICATION_INTERNAL_ERROR before anything destructive runs, and clears once an operator
        /// resolves the replication-slot/publication conflict on the PostgreSQL side; a replication slot that
        /// is momentarily still held active by a just-released connection throws instead, and clears as soon
        /// as that connection goes away. Each retry re-checks ownership and refuses again while a conflict
        /// persists, so no re-snapshot can happen in the meantime. This mirrors the database-engine path,
        /// which retries via DatabaseMaterializedPostgreSQL::tryStartSynchronization.
        if (e.code() == ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR)
            LOG_ERROR(log, "Replication cannot start yet. Retry attempt will continue. Error message: {}", e.message());
        else
            LOG_ERROR(log, "Failed to start replication, retry attempt will continue. Error message: {}", e.message());
        startup_task->scheduleAfter(milliseconds_to_wait);
    }
    catch (...)
    {
        tryLogCurrentException(log);

        if (!is_attach)
            throw;

        /// A non-Exception failure on attach (e.g. a pqxx::sql_error such as "replication slot is active for
        /// PID N" when a just-released connection still holds the slot) is transient too - keep retrying so
        /// replication resumes on its own, matching the Exception branch above and the database-engine path.
        LOG_ERROR(log, "Failed to start replication, retry attempt will continue. Error message: {}", getCurrentExceptionMessage(false));
        startup_task->scheduleAfter(milliseconds_to_wait);
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


/// Deployments created before the generated publication and default replication-slot names became
/// schema-aware own the legacy, schema-blind objects on the PostgreSQL side. On attach such a
/// deployment must keep its legacy identity: looking for the schema-aware slot instead would miss the
/// existing slot, run an initial sync and reload a snapshot into the already-existing nested tables,
/// duplicating data. So, on attach, when the schema-aware objects do not exist but the legacy ones do,
/// switch to the legacy names. The legacy names are schema-blind and therefore shared with a
/// same-database deployment over the default schema (or another schema targeting the same bare table),
/// so the existence of the legacy slot alone does not prove the legacy objects belong to this engine —
/// only the legacy publication's table list carries the schema. The legacy identity is therefore only
/// adopted when the legacy publication exists and every table it publishes belongs to this engine's
/// schema. If the legacy publication is missing, empty, or publishes a table from another schema, the
/// legacy slot is ambiguous or foreign, and adopting it (or returning to proceed under the schema-aware
/// identity) would either hijack another engine's slot or, since the schema-aware slot is gone, run an
/// initial sync and reload a snapshot into the already-existing nested tables (duplicating data on disk).
/// In that case the attach fails closed with an exception instead of silently re-snapshotting a populated
/// replica or hijacking another engine's replication slot.
void PostgreSQLReplicationHandler::adoptLegacyReplicationIdentityIfNeeded(pqxx::nontransaction & tx)
{
    if (!is_attach)
        return;

    /// The generated names differ from the legacy ones only for a non-default schema (and, for the slot,
    /// only when it is neither user-managed nor a unique replication consumer identifier). This also
    /// makes the adoption idempotent: once adopted, the names compare equal.
    if (replication_slot == legacy_replication_slot && publication_name == legacy_publication_name)
        return;

    auto slot_exists = [&](const String & name)
    {
        pqxx::result result{tx.exec(fmt::format("SELECT 1 FROM pg_replication_slots WHERE slot_name = {}", quoteStringPostgreSQL(name)))};
        return !result.empty();
    };
    auto publication_exists = [&](const String & name)
    {
        pqxx::result result{tx.exec(fmt::format("SELECT 1 FROM pg_publication WHERE pubname = {}", quoteStringPostgreSQL(name)))};
        return !result.empty();
    };

    if (replication_slot != legacy_replication_slot)
    {
        /// The slot is the object whose loss triggers a destructive re-sync, so it carries the evidence:
        /// adopt only if the schema-aware slot does not exist while the legacy one does.
        if (slot_exists(replication_slot) || !slot_exists(legacy_replication_slot))
            return;
    }
    else
    {
        /// The slot name does not depend on the schema, so the publication is the only renamed object
        /// and carries the evidence instead.
        if (publication_exists(publication_name) || !publication_exists(legacy_publication_name))
            return;
    }

    /// The legacy slot and publication names are schema-blind, so the mere existence of the legacy slot
    /// (Branch A above) does not prove the legacy objects belong to this engine — a same-database
    /// deployment over the default schema (or another schema targeting the same bare table) owns
    /// identically-named objects. The only schema-carrying evidence is the legacy publication's table list,
    /// so the legacy identity is adopted only when the legacy publication exists and every table it
    /// publishes belongs to this engine's schema. If it is missing, empty, or publishes a table from another
    /// schema, ownership cannot be proven: the legacy slot is ambiguous or foreign and must be left
    /// untouched. And since the schema-aware slot is gone, returning here to proceed under the schema-aware
    /// identity would run an initial sync and reload a snapshot into the already-existing nested tables
    /// (createNestedIfNeeded is a no-op once they exist), silently duplicating data on disk; while adopting
    /// the legacy slot regardless would hijack another engine's slot. Fail closed instead: surface the
    /// identity conflict and let an operator resolve it (createNestedIfNeeded, the initial sync, and any
    /// re-snapshot never run).
    String ownership_conflict;
    if (!publication_exists(legacy_publication_name))
        ownership_conflict = fmt::format(
            "the legacy publication {} does not exist, so the schema-blind legacy replication slot cannot be "
            "proven to belong to this engine's schema '{}'",
            doubleQuoteString(legacy_publication_name), postgres_schema);
    else
    {
        pqxx::result result{tx.exec(fmt::format(
            "SELECT DISTINCT schemaname FROM pg_publication_tables WHERE pubname = {}", quoteStringPostgreSQL(legacy_publication_name)))};
        if (result.empty())
            ownership_conflict = fmt::format(
                "the legacy publication {} publishes no tables, so the schema-blind legacy replication slot "
                "cannot be proven to belong to this engine's schema '{}'",
                doubleQuoteString(legacy_publication_name), postgres_schema);
        for (const auto & row : result)
        {
            if (row[0].as<std::string>() != postgres_schema)
            {
                ownership_conflict = fmt::format(
                    "the legacy publication {} publishes a table from schema '{}', not this engine's schema "
                    "'{}', so it belongs to another engine",
                    doubleQuoteString(legacy_publication_name), row[0].as<std::string>(), postgres_schema);
                break;
            }
        }
    }

    if (!ownership_conflict.empty())
        throw Exception(
            ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
            "Cannot start MaterializedPostgreSQL replication on attach: {}, so the legacy replication identity "
            "cannot be adopted. Proceeding would either reload the initial snapshot into the existing nested "
            "tables and duplicate data, or consume another engine's replication slot and publication, so "
            "replication is refused. Resolve the replication-slot/publication conflict on the PostgreSQL side "
            "(or recreate this table): startup keeps retrying and replication starts automatically once the "
            "conflict is resolved, without a server restart or a manual re-attach.",
            ownership_conflict);

    LOG_INFO(
        log,
        "Adopting the legacy replication identity of a deployment created before the generated names became "
        "schema-aware: replication slot {} (instead of {}) and publication {} (instead of {})",
        legacy_replication_slot, replication_slot, doubleQuoteString(legacy_publication_name), doubleQuoteString(publication_name));

    replication_slot = legacy_replication_slot;
    tmp_replication_slot = replication_slot + "_tmp";
    publication_name = legacy_publication_name;
}


/// On attach, replication resumes from the existing replication slot, through the existing publication, without
/// a new snapshot. Several states of the PostgreSQL side cannot be resumed from without silently losing changes:
///
/// 1. The replication slot exists, but the publication does not. Changes committed while the publication did
///    not exist are skipped by `pgoutput` even after the publication is recreated, because it resolves publication
///    membership from the historic catalog snapshot at the LSN of each change.
///
/// 2. The replication slot does not exist (for example, after `pg_upgrade`, which keeps publications but not
///    replication slots), while this replica already holds data. A new snapshot into the existing nested tables
///    would leave the replica stale: the snapshot rows are materialized with `_sign = 1` and `_version = 1`, so they
///    neither delete the rows deleted in PostgreSQL in the meantime nor override the rows with a higher version.
///
/// 3. The publication no longer publishes a replicated table (`ALTER PUBLICATION ... DROP TABLE`), publishes a table
///    from another schema with the same bare name as a replicated table, or its definition is altered (for example,
///    `publish = 'insert'`, or a row filter). The changes filtered out in the meantime cannot be recovered.
///
/// In these cases the attach fails, and startup keeps retrying until an operator resolves the conflict or recreates
/// the object, which takes a fresh snapshot.
///
/// A replica that did not materialize any table yet (for example, the first synchronization was interrupted)
/// has nothing that could become stale. If its replication slot survived, it is dropped, and the initial
/// synchronization runs from scratch, otherwise resuming from that slot would skip the initial snapshot.
bool PostgreSQLReplicationHandler::checkReplicationStateOnAttach(pqxx::nontransaction & tx)
{
    String slot_lsn;
    const bool slot_exists = isReplicationSlotExist(tx, slot_lsn, /* temporary */ false);
    const bool publication_exists = isPublicationExist(tx);

    /// The database engine holds data from a previous run if it has nested tables. The single-table engine is kept
    /// in the metadata only after its initial synchronization succeeded, so a replay of its stored definition implies
    /// that it holds data. A full-definition `ATTACH TABLE` introduces a new object, unless its nested table exists.
    const bool has_previously_replicated_data = hasNestedStorage() || (!is_materialized_postgresql_database && !is_fresh_definition);

    if (!has_previously_replicated_data)
    {
        if (!slot_exists)
            return false;

        if (user_managed_slot)
            throw Exception(
                ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                "Cannot start MaterializedPostgreSQL replication on attach: the user-managed replication slot {} exists, "
                "but this replica never finished its initial synchronization. Resuming from this slot would skip the initial "
                "snapshot, and a user-managed replication slot cannot be recreated by ClickHouse. Recreate this object with a "
                "new replication slot and the snapshot exported for it (`materialized_postgresql_snapshot`)",
                replication_slot);

        LOG_INFO(
            log,
            "Replication slot {} exists, but no table has been materialized yet: the previous run did not finish its "
            "initial synchronization. The replication slot will be dropped, and the initial synchronization will start "
            "from scratch",
            replication_slot);
        return true;
    }

    if (slot_exists && !publication_exists)
        throw Exception(
            ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
            "Cannot start MaterializedPostgreSQL replication on attach: replication slot {} exists, but publication {} "
            "does not. Resuming from the replication slot through a recreated publication would silently lose every change "
            "written while the publication did not exist. Recreate this object to replicate from a fresh snapshot. "
            "Startup will be retried",
            replication_slot, doubleQuoteString(publication_name));

    if (!slot_exists)
    {
        /// A missing user-managed replication slot is reported as a configuration error in startSynchronization.
        if (user_managed_slot)
            return false;

        throw Exception(
            ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
            "Cannot start MaterializedPostgreSQL replication on attach: replication slot {} does not exist{}, but this "
            "replica already holds data from a previous run. A new snapshot into the existing nested tables would silently "
            "leave the replica stale, because it neither deletes the rows that were deleted in PostgreSQL nor overrides "
            "the rows with a higher version. Recreate this object to replicate from a fresh snapshot. Startup will be "
            "retried",
            replication_slot,
            publication_exists ? "" : fmt::format(", and publication {} does not exist either", doubleQuoteString(publication_name)));
    }

    /// In the single-schema modes the comparison is by exact (schema, table) pair rather than by bare name,
    /// so that a publication rewritten from `foo.a` to `bar.a` is not taken for the same table.
    std::set<std::pair<String, String>> expected;
    for (const auto & [table_name, storage] : materialized_storages)
    {
        if (has_configured_tables_list || storage->tryGetNested())
            expected.insert(getNormalizedSchemaAndTableName(table_name));
    }

    const auto published = fetchPublishedTablePairs(tx);

    String missing;
    for (const auto & [schema, table] : expected)
    {
        if (published.contains(std::make_pair(schema, table)))
            continue;
        if (!missing.empty())
            missing += ", ";
        missing += schema + '.' + table;
    }

    if (!missing.empty())
        throw Exception(
            ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
            "Cannot start MaterializedPostgreSQL replication on attach: publication {} no longer publishes the following "
            "replicated tables: {}. Resuming through this publication would silently skip the changes of these tables, "
            "and the changes committed while they were not published cannot be recovered. Recreate this object to "
            "replicate from a fresh snapshot. Startup will be retried",
            doubleQuoteString(publication_name), missing);

    const String colliding = collidingForeignPublishedTables(expected, published);
    if (!colliding.empty())
        throw Exception(
            ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
            "Cannot start MaterializedPostgreSQL replication on attach: publication {} publishes the following tables "
            "from other schemas with the same names as replicated tables: {}. Their changes would be applied to the "
            "replicated tables. Remove them from the publication, or recreate this object. Startup will be retried",
            doubleQuoteString(publication_name), colliding);

    const String definition_conflict = publicationDefinitionConflict(tx);
    if (!definition_conflict.empty())
        throw Exception(
            ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
            "Cannot start MaterializedPostgreSQL replication on attach: {}. Resuming through this publication would "
            "silently filter the replication stream, and the filtered-out changes cannot be recovered. Recreate this "
            "object to replicate from a fresh snapshot. Startup will be retried",
            definition_conflict);

    return false;
}


void PostgreSQLReplicationHandler::startSynchronization(bool throw_on_error)
{
    postgres::Connection replication_connection(connection_info, /* replication */true);
    pqxx::nontransaction tx(replication_connection.getRef());
    adoptLegacyReplicationIdentityIfNeeded(tx);
    const bool bootstrap_after_never_synchronized_run = is_attach && checkReplicationStateOnAttach(tx);
    createPublicationIfNeeded(tx);

    /// List of nested tables (table_name -> nested_storage), which is passed to replication consumer.
    std::unordered_map<String, StorageInfo> nested_storages;

    /// Tables whose snapshot position is ahead of the existing replication slot on resume, with that position:
    /// the consumer applies their changes only after it. These are the tables that take their first snapshot now,
    /// and the ones that took it in a previous run, which the replication slot has not passed yet.
    std::unordered_map<String, std::pair<StorageInfo, String>> tables_after_snapshot_position;

    /// snapshot_name is initialized only if a new replication slot is created.
    /// start_lsn is initialized in two places:
    /// 1. if replication slot does not exist, start_lsn will be returned with its creation return parameters;
    /// 2. if replication slot already exist, start_lsn is read from pg_replication_slots as
    ///    `confirmed_flush_lsn` - the address (LSN) up to which the logical slot's consumer has confirmed receiving data.
    ///    Data older than this is not available anymore.
    String snapshot_name;
    String start_lsn;

    /// Also lets have a separate non-replication connection, because we need two parallel transactions and
    /// one connection can have one transaction at a time.
    auto tmp_connection = std::make_shared<postgres::Connection>(connection_info);

    auto initial_sync = [&]()
    {
        LOG_DEBUG(log, "Starting tables sync load");

        /// All tables are loaded from the snapshot of the new replication slot.
        saveSnapshotPositions({});

        if (user_managed_slot)
        {
            if (user_provided_snapshot.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Using a user-defined replication slot must "
                                "be provided with a snapshot from EXPORT SNAPSHOT when the slot is created. "
                                "Pass it to `materialized_postgresql_snapshot` setting");
            snapshot_name = user_provided_snapshot;
        }
        else
        {
            /// On attach, the publication may survive from a previous run that did not materialize any table, and miss
            /// some of the tables to replicate. Add them before the replication slot is created, so that all changes
            /// after the snapshot are streamed for them.
            if (is_attach)
            {
                for (const auto & storage_data : materialized_storages)
                    addTableToPublication(tx, storage_data.first);
            }

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
                tryLogCurrentException(log);

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
    /// Always drop replication slot if it is CREATE query and not ATTACH, or if the previous run
    /// did not materialize any table (see checkReplicationStateOnAttach).
    else if (!is_attach || bootstrap_after_never_synchronized_run)
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
        /// A published table may have no nested table: in `materialized_postgresql_tables_list` mode, a table whose
        /// first snapshot failed (for example, it had no primary key) is skipped by the initial synchronization, while
        /// the other tables are materialized. Take its first snapshot now from a temporary replication slot, as
        /// `ATTACH TABLE` does. Otherwise every attach would fail on the missing nested table.
        std::vector<std::pair<String, StorageMaterializedPostgreSQL *>> tables_without_nested;
        std::optional<std::set<std::pair<String, String>>> published;

        auto snapshot_positions = loadSnapshotPositions();

        /// A snapshot position of a table that is not replicated is left by an `ATTACH TABLE` that did not complete (the
        /// server was terminated before `materialized_postgresql_tables_list` was updated), or by a `DETACH TABLE ...
        /// PERMANENTLY` that did not complete after it was updated. Finish the rollback, or the detach: the nested table
        /// would otherwise hide the table from a retried `ATTACH TABLE`, and the publication would stream its changes.
        for (const auto & [table_name, position] : snapshot_positions)
        {
            if (materialized_storages.contains(table_name))
                continue;

            auto nested_context = StorageMaterializedPostgreSQL::makeNestedTableContext(getContext()->getGlobalContext());
            const StorageID nested_table_id(current_database_name, table_name);
            if (DatabaseCatalog::instance().tryGetTable(nested_table_id, nested_context))
            {
                LOG_INFO(log, "Table {}.{} is not replicated, but has a nested table left by an interrupted `ATTACH TABLE` "
                         "or `DETACH TABLE ... PERMANENTLY`, it will be dropped", postgres_database, table_name);
                /// The drop query is executed in a copy of the first context, which must be internal too: otherwise the
                /// database does not find the nested table of a table that it does not replicate.
                InterpreterDropQuery::executeDropQuery(
                    ASTDropQuery::Kind::Drop, nested_context, nested_context, nested_table_id,
                    /* sync */ true, /* ignore_sync_setting */ true);
            }

            /// A publication entry that the interrupted `ATTACH TABLE` reused is kept, as the rollback of a failed
            /// `ATTACH TABLE` does: other consumers of the publication may rely on it.
            if (!position.owns_publication_entry)
            {
                LOG_INFO(log, "The publication entry of table {}.{} existed before the interrupted `ATTACH TABLE`, it is kept",
                         postgres_database, table_name);
                continue;
            }

            if (!published)
                published = fetchPublishedTablePairs(tx);
            if (published->contains(getNormalizedSchemaAndTableName(table_name)))
            {
                removeTableFromPublication(tx, table_name);
                published->erase(getNormalizedSchemaAndTableName(table_name));
            }
        }

        /// Forget the snapshot positions of the completed snapshots that the replication slot has durably passed
        /// (`start_lsn` is its `confirmed_flush_lsn`), and the ones of the tables that are not replicated anymore.
        /// The position of an interrupted snapshot is kept regardless of the replication slot: its table is loaded again.
        if (std::erase_if(snapshot_positions, [&](const auto & position)
            {
                return (position.second.completed
                        && MaterializedPostgreSQLConsumer::getLSNValue(position.second.start_lsn)
                            <= MaterializedPostgreSQLConsumer::getLSNValue(start_lsn))
                    || !materialized_storages.contains(position.first);
            }))
            saveSnapshotPositions(snapshot_positions);

        for (const auto & [table_name, storage] : materialized_storages)
        {
            auto * materialized_storage = storage->as <StorageMaterializedPostgreSQL>();

            /// The server was terminated while this table was being loaded from a snapshot, so the cleanup after
            /// a failed snapshot did not run. Its nested table holds only a part of the rows: drop it, and take
            /// a new snapshot below, as for a table without a nested table.
            if (auto position = snapshot_positions.find(table_name);
                position != snapshot_positions.end() && !position->second.completed && materialized_storage->tryGetNested())
            {
                LOG_INFO(log, "The snapshot of table {}.{} did not complete in a previous run, its nested table will be "
                         "dropped and loaded again", postgres_database, table_name);
                dropNestedTable(materialized_storage);
            }

            if (!materialized_storage->tryGetNested())
            {
                if (!published)
                    published = fetchPublishedTablePairs(tx);
                if (published->contains(getNormalizedSchemaAndTableName(table_name)))
                {
                    tables_without_nested.emplace_back(table_name, materialized_storage);
                    continue;
                }

                /// Without `materialized_postgresql_tables_list` the tables are discovered from the replicated schemas
                /// (a configured tables list is validated against the publication in checkReplicationStateOnAttach).
                /// A table that is neither materialized nor published was removed from replication by
                /// `DETACH TABLE ... PERMANENTLY`, or was created in PostgreSQL later. It is not replicated.
                LOG_INFO(log, "Table {}.{} has no nested table and is not in publication {}, it is not replicated",
                         postgres_database, table_name, doubleQuoteString(publication_name));
                tables_skipped_on_attach.insert(table_name);
                continue;
            }

            try
            {
                auto table_structure = fetchTableStructure(tx, table_name);
                if (!table_structure->physical_columns)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "No columns");
                auto storage_info = StorageInfo(materialized_storage->getNested(), table_structure->physical_columns->attributes);
                if (auto position = snapshot_positions.find(table_name); position != snapshot_positions.end())
                {
                    LOG_INFO(log, "Table {}.{} was loaded from a snapshot at position {}, which replication slot {} has not "
                             "passed yet: its earlier changes will be skipped",
                             postgres_database, table_name, position->second.start_lsn, replication_slot);
                    tables_after_snapshot_position.emplace(table_name, std::make_pair(std::move(storage_info), position->second.start_lsn));
                }
                else
                    nested_storages.emplace(table_name, std::move(storage_info));
            }
            catch (Exception & e)
            {
                e.addMessage("while loading table {}.{}", postgres_database, table_name);
                tryLogCurrentException(log);

                if (throw_on_error)
                    throw;
            }
        }

        if (!tables_without_nested.empty())
        {
            postgres::Connection snapshot_replication_connection(connection_info, /* replication */true);
            auto snapshot_tx = std::make_shared<pqxx::nontransaction>(snapshot_replication_connection.getRef());

            String temporary_snapshot_name;
            String temporary_start_lsn;
            if (isReplicationSlotExist(*snapshot_tx, temporary_start_lsn, /* temporary */true))
                dropReplicationSlot(*snapshot_tx, /* temporary */true);

            TemporaryReplicationSlot temporary_slot(this, snapshot_tx, temporary_start_lsn, temporary_snapshot_name);

            /// Record the snapshot position before the snapshot is loaded, so that a restart at any moment after it
            /// cannot apply the changes before the snapshot position to the loaded table.
            if (is_materialized_postgresql_database)
            {
                for (const auto & [table_name, materialized_storage] : tables_without_nested)
                    snapshot_positions[table_name] = SnapshotPosition{
                        .start_lsn = temporary_start_lsn, .completed = false, .owns_publication_entry = false};
                saveSnapshotPositions(snapshot_positions);
            }
            else
            {
                /// The replication slot of the single-table engine streams only this table, so nothing needs
                /// the changes before the snapshot position.
                tx.exec(fmt::format(
                    "SELECT pg_replication_slot_advance({}, {})",
                    quoteStringPostgreSQL(replication_slot), quoteStringPostgreSQL(temporary_start_lsn)));
                LOG_DEBUG(log, "Advanced replication slot {} to the snapshot position {}", replication_slot, temporary_start_lsn);
            }

            for (const auto & [table_name, materialized_storage] : tables_without_nested)
            {
                LOG_INFO(log, "Table {}.{} has no nested table yet, loading it from a new snapshot", postgres_database, table_name);
                try
                {
                    auto storage_info = loadFromSnapshot(*tmp_connection, temporary_snapshot_name, table_name, materialized_storage);
                    markSnapshotCompleted(table_name);
                    tables_after_snapshot_position.emplace(table_name, std::make_pair(std::move(storage_info), temporary_start_lsn));
                }
                catch (...)
                {
                    /// Not only `DB::Exception`: a lost connection during the snapshot throws `pqxx::failure`.
                    tryLogCurrentException(log, fmt::format("while loading table `{}`.`{}`", postgres_database, table_name));

                    /// A partially loaded nested table must not stay. The next attach would also drop it, because
                    /// its snapshot is not marked completed, but dropping it now frees the space at once.
                    try
                    {
                        dropNestedTable(materialized_storage);
                    }
                    catch (...)
                    {
                        tryLogCurrentException(log, "Failed to drop the nested table of a table whose snapshot failed");
                    }

                    /// Like the initial synchronization, the database engine keeps replicating the other tables.
                    if (throw_on_error && !is_materialized_postgresql_database)
                        throw;
                }
            }

            fiu_do_on(FailPoints::materialized_postgresql_fail_after_snapshot_on_attach,
            {
                throw Exception(ErrorCodes::FAULT_INJECTED, "Failpoint materialized_postgresql_fail_after_snapshot_on_attach");
            });
        }

        LOG_DEBUG(log, "Loaded {} tables", nested_storages.size() + tables_after_snapshot_position.size());
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

    for (auto & [table_name, storage_info_and_position] : tables_after_snapshot_position)
    {
        try
        {
            consumer->addNested(table_name, std::move(storage_info_and_position.first), storage_info_and_position.second);
        }
        catch (const Exception & e)
        {
            /// The same as in the constructor of the consumer: the structure of a table restored from a saved snapshot
            /// position might have changed in PostgreSQL while the server was down. Skip only this table, otherwise
            /// every startup retry fails, the replication slot never passes the snapshot position, and the other
            /// tables stop replicating.
            if (e.code() != ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR)
                throw;

            tryLogCurrentException(
                log,
                fmt::format("Table {} is skipped from replication because its structure does not match "
                            "the structure of the nested ClickHouse table. "
                            "Please perform manual DETACH and ATTACH of the table to bring it back",
                            table_name));
        }
    }

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

    /// Not always PostgreSQL's own snapshot id from `CREATE_REPLICATION_SLOT`: with a user-managed
    /// slot it is `materialized_postgresql_snapshot` verbatim, so it has to be sent as data.
    std::string query_str = fmt::format("SET TRANSACTION SNAPSHOT {}", quoteStringPostgreSQL(snapshot_name));
    tx->exec(query_str);

    PostgreSQLTableStructurePtr table_structure;
    try
    {
        table_structure = fetchTableStructure(*tx, table_name);
    }
    catch (...)
    {
        tryLogCurrentException(log);
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
            allowed_columns.push_back(doubleQuoteStringPostgreSQL(column.name));

        query_str = fmt::format("SELECT {} FROM ONLY {}", boost::algorithm::join(allowed_columns, ","), quoted_name);
    }

    LOG_DEBUG(log, "Loading PostgreSQL table {}.{}", postgres_database, quoted_name);

    auto table_override = tryGetTableOverride(current_database_name, table_name);
    materialized_storage->createNestedIfNeeded(std::move(table_structure), table_override ? table_override->as<ASTTableOverride>() : nullptr);
    auto nested_storage = materialized_storage->getNested();

    auto insert = make_intrusive<ASTInsertQuery>();
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

    auto nested_metadata = nested_storage->getInMemoryMetadataPtr(insert_context, false);
    const StorageInMemoryMetadata & storage_metadata = *nested_metadata;
    auto sample_block = std::make_shared<const Block>(storage_metadata.getSampleBlockNonMaterialized());

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
    try
    {
        /// It is very important to make sure temporary replication slots are removed!
        /// So just in case every 30 minutes check if one still exists.
        postgres::Connection connection(connection_info);
        String last_committed_lsn;
        execWithRetryAndFaultInjection(connection, [&](pqxx::nontransaction & tx)
        {
            if (isReplicationSlotExist(tx, last_committed_lsn, /* temporary */true))
                dropReplicationSlot(tx, /* temporary */true);
        });
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }

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
        tryLogCurrentException(log);
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
    std::string query_str = fmt::format("SELECT exists (SELECT 1 FROM pg_publication WHERE pubname = {})", quoteStringPostgreSQL(publication_name));
    pqxx::result result{tx.exec(query_str)};
    chassert(!result.empty());
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
        else if (!is_materialized_postgresql_database)
        {
            /// Single `MaterializedPostgreSQL` storage: `tables_list` is the raw remote table name
            /// (see the `StorageMaterializedPostgreSQL` constructor) and is never passed through the
            /// quoting pass that `fetchRequiredTables` applies for the database engine. Quote it here,
            /// otherwise `CREATE PUBLICATION ... FOR TABLE ONLY <name>` folds an upper-case table name
            /// to lower case and fails with `relation "..." does not exist`.
            tables_list = doubleQuoteWithSchema(tables_list);
        }

        if (tables_list.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No table found to be replicated");

        /// 'ONLY' means just a table, without descendants.
        std::string query_str = fmt::format("CREATE PUBLICATION {} FOR TABLE ONLY {}", doubleQuoteStringPostgreSQL(publication_name), tables_list);
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

    String query_str = fmt::format("SELECT active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = {}", quoteStringPostgreSQL(slot_name));
    pqxx::result result{tx.exec(query_str)};

    /// Replication slot does not exist
    if (result.empty())
        return false;

    /// The LSN fields are NULL while the slot is still being created (PostgreSQL registers the slot
    /// in pg_replication_slots before assigning it a consistent snapshot point), and are never set for
    /// a physical slot of the same name. Converting the NULL would throw pqxx::conversion_error, which
    /// is a std::logic_error and must not escape this function. Such a slot exists but cannot be
    /// consumed from yet, so report a recoverable error: on the attach path the startup task retries,
    /// and a slot caught mid-creation becomes ready by the next attempt.
    if (result[0][1].is_null() || result[0][2].is_null())
        throw Exception(
            ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
            "Replication slot {} exists, but it is not ready: restart_lsn is {}, confirmed_flush_lsn is {}. "
            "It is either still being created, or it is not a logical replication slot",
            slot_name,
            result[0][1].is_null() ? "NULL" : result[0][1].as<std::string>(),
            result[0][2].is_null() ? "NULL" : result[0][2].as<std::string>());

    start_lsn = result[0][2].as<std::string>();

    LOG_DEBUG(log, "Replication slot {} already exists (active: {}). Restart lsn position: {}, confirmed flush lsn: {}",
            slot_name, result[0][0].as<bool>(), result[0][1].as<std::string>(), start_lsn);

    return true;
}


void PostgreSQLReplicationHandler::createReplicationSlot(
        pqxx::nontransaction & tx, String & start_lsn, String & snapshot_name, bool temporary)
{
    chassert(temporary || !user_managed_slot);

    String query_str;
    String slot_name;
    if (temporary)
        slot_name = tmp_replication_slot;
    else
        slot_name = replication_slot;

    query_str = fmt::format("CREATE_REPLICATION_SLOT {} LOGICAL pgoutput EXPORT_SNAPSHOT", doubleQuoteStringPostgreSQL(slot_name));

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
    chassert(temporary || !user_managed_slot);

    std::string slot_name;
    if (temporary)
        slot_name = tmp_replication_slot;
    else
        slot_name = replication_slot;

    std::string query_str = fmt::format("SELECT pg_drop_replication_slot({})", quoteStringPostgreSQL(slot_name));

    tx.exec(query_str);
    LOG_DEBUG(log, "Dropped replication slot: {}", slot_name);
}


void PostgreSQLReplicationHandler::dropPublication(pqxx::nontransaction & tx)
{
    std::string query_str = fmt::format("DROP PUBLICATION IF EXISTS {}", doubleQuoteStringPostgreSQL(publication_name));
    tx.exec(query_str);
    LOG_DEBUG(log, "Dropped publication: {}", doubleQuoteString(publication_name));
}


bool PostgreSQLReplicationHandler::addTableToPublication(pqxx::nontransaction & ntx, const String & table_name)
{
    const String definition_conflict = publicationDefinitionConflict(ntx);
    if (!definition_conflict.empty())
        throw Exception(
            ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
            "Cannot add table {} to MaterializedPostgreSQL replication: {}. Replicating the table through this publication "
            "would silently filter its replication stream",
            doubleQuoteWithSchema(table_name), definition_conflict);

    const auto published = fetchPublishedTablePairs(ntx);
    const auto table = getNormalizedSchemaAndTableName(table_name);

    const String colliding = collidingForeignPublishedTables({table}, published);
    if (!colliding.empty())
        throw Exception(
            ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
            "Cannot add table {} to MaterializedPostgreSQL replication: publication {} already publishes the following "
            "tables from other schemas with the same name: {}. Their changes would be applied to this table",
            doubleQuoteWithSchema(table_name), doubleQuoteString(publication_name), colliding);

    /// A publication cannot contain the same table twice.
    if (published.contains(table))
    {
        LOG_TRACE(log, "Table {} is already in publication `{}`", doubleQuoteWithSchema(table_name), publication_name);
        return false;
    }

    std::string query_str = fmt::format("ALTER PUBLICATION {} ADD TABLE ONLY {}", doubleQuoteStringPostgreSQL(publication_name), doubleQuoteWithSchema(table_name));
    ntx.exec(query_str);
    LOG_TRACE(log, "Added table {} to publication `{}`", doubleQuoteWithSchema(table_name), publication_name);
    return true;
}


void PostgreSQLReplicationHandler::removeTableFromPublication(pqxx::nontransaction & ntx, const String & table_name)
{
    try
    {
        std::string query_str = fmt::format("ALTER PUBLICATION {} DROP TABLE ONLY {}", doubleQuoteStringPostgreSQL(publication_name), doubleQuoteWithSchema(table_name));
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


/// `pos` points at the `"` that opens a quoted identifier; returns the position of the `"` closing it, or
/// the last position when it is unterminated. A `""` pair inside the identifier is one escaped quote.
static size_t skipQuotedIdentifier(std::string_view str, size_t pos)
{
    chassert(str[pos] == '"');
    for (++pos; pos < str.size(); ++pos)
    {
        if (str[pos] != '"')
            continue;
        if (pos + 1 < str.size() && str[pos + 1] == '"')
            ++pos;
        else
            return pos;
    }
    return str.size() - 1;
}


/// Allowed columns for table from materialized_postgresql_tables_list setting
Strings PostgreSQLReplicationHandler::getTableAllowedColumns(const std::string & table_name) const
{
    Strings result;
    if (tables_list.empty())
        return result;

    /// `fetchRequiredTables` wrote every element through `doubleQuoteWithSchema`, so the table is looked
    /// up by that spelling. A column may be spelled exactly like another element's relation, so the list
    /// is walked element by element and only the relation names are compared.
    const String quoted_table_name = doubleQuoteWithSchema(table_name);

    size_t scan_pos = 0;
    size_t after_name = std::string::npos;
    while (scan_pos < tables_list.size())
    {
        while (scan_pos < tables_list.size() && (tables_list[scan_pos] == ',' || tables_list[scan_pos] == ' '))
            ++scan_pos;

        const size_t name_start = scan_pos;
        while (scan_pos < tables_list.size() && tables_list[scan_pos] != '(' && tables_list[scan_pos] != ',')
        {
            if (tables_list[scan_pos] == '"')
                scan_pos = skipQuotedIdentifier(tables_list, scan_pos);
            ++scan_pos;
        }

        std::string_view element = std::string_view(tables_list).substr(name_start, scan_pos - name_start);
        element = trimWhitespace(element);
        if (element == quoted_table_name)
        {
            after_name = scan_pos;
            break;
        }

        if (scan_pos < tables_list.size() && tables_list[scan_pos] == '(')
        {
            for (; scan_pos < tables_list.size(); ++scan_pos)
            {
                if (tables_list[scan_pos] == '"')
                    scan_pos = skipQuotedIdentifier(tables_list, scan_pos);
                else if (tables_list[scan_pos] == ')')
                    break;
            }
            if (scan_pos < tables_list.size())
                ++scan_pos;
        }
    }

    if (after_name == std::string::npos)
        return result;

    std::string_view column_list = std::string_view(tables_list).substr(after_name);
    column_list = trimWhitespace(column_list);
    if (column_list.empty() || column_list[0] != '(')
        return result;

    /// `fetchRequiredTables` has already quoted this list (`"t"("id","a""b")`), so the identifier
    /// delimiters have to be honoured while scanning: a `)` or a `,` inside a quoted name belongs to the
    /// name rather than ending the list or the element.
    size_t end_bracket_pos = std::string::npos;
    for (size_t pos = 1; pos < column_list.size(); ++pos)
    {
        if (column_list[pos] == '"')
            pos = skipQuotedIdentifier(column_list, pos);
        else if (column_list[pos] == ')')
        {
            end_bracket_pos = pos;
            break;
        }
    }

    column_list = column_list.substr(1, end_bracket_pos - 1);
    if (column_list.empty())
        return result;

    size_t part_start = 0;
    for (size_t pos = 0; pos <= column_list.size(); ++pos)
    {
        if (pos < column_list.size())
        {
            if (column_list[pos] == '"')
                pos = skipQuotedIdentifier(column_list, pos);
            if (column_list[pos] != ',')
                continue;
        }

        String column(trimWhitespace(column_list.substr(part_start, pos - part_start)));
        part_start = pos + 1;

        /// A quoted element carries the name with its `"` doubled; an unquoted one is a legacy list
        /// element and stands for itself.
        if (column.size() > 1 && column.front() == '"' && column.back() == '"')
        {
            column = column.substr(1, column.size() - 2);
            boost::replace_all(column, "\"\"", "\"");
        }
        result.push_back(std::move(column));
    }

    return result;
}


void PostgreSQLReplicationHandler::shutdownFinal()
{
    try
    {
        shutdown();

        /// Do not use fault injection during cleanup: leaked replication slots
        /// can exhaust PostgreSQL's max_replication_slots and break subsequent
        /// MaterializedPostgreSQL databases.
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
    bool publication_exists_before_startup = false;

    {
        pqxx::nontransaction tx(connection.getRef());
        /// The database engine consults the publication before startSynchronization() runs, so a legacy
        /// deployment must switch to its legacy identity already here — otherwise the schema-aware
        /// publication name would (wrongly) look absent and the tables list would be refetched from the
        /// schema instead of the existing publication.
        adoptLegacyReplicationIdentityIfNeeded(tx);
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

    /// `schema1.table1, schema2.table2, ...` -> `"schema1"."table1", "schema2"."table2", ...`
    /// or
    /// `table1, table2, ...` + setting `schema` -> `"schema"."table1", "schema"."table2", ...`
    /// or
    /// `table1, table2(id,name), ...` + setting `schema` -> `"schema"."table1", "schema"."table2"("id","name"), ...`
    /// Every return below leaves `tables_list` in the spelling `getTableAllowedColumns` looks up by.
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
            /// `splitInto` keeps empty tokens, and the `part.back()` below needs a byte to read.
            if (part.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty element in tables list: {}", tables_list);

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
                /// A single-column subset carries both brackets in one element, so the closing one is
                /// here rather than in a later element.
                if (part.ends_with(')'))
                {
                    is_column = false;
                    part = part.substr(0, part.size() - 1);
                    boost::trim(part);
                    buf << doubleQuoteStringPostgreSQL(part);
                    buf << ')';
                }
                else
                {
                    buf << doubleQuoteStringPostgreSQL(part);
                }
            }
            else if (part.back() == ')')
            {
                is_column = false;
                part = part.substr(0, part.size() - 1);
                boost::trim(part);
                buf << doubleQuoteStringPostgreSQL(part);
                buf << ')';
            }
            else if (is_column)
            {
                buf << doubleQuoteStringPostgreSQL(part);
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

    /// Try to fetch tables list from publication if there is not tables list.
    /// If there is a tables list -- check that lists are consistent and if not -- remove publication, it will be recreated.
    if (publication_exists_before_startup)
    {
        if (!is_attach)
        {
            LOG_WARNING(log,
                        "Publication {} already exists, but it is a CREATE query, not ATTACH. Publication will be dropped",
                        doubleQuoteString(publication_name));

            execWithRetryAndFaultInjection(connection, [&](pqxx::nontransaction & tx_){ dropPublication(tx_); });
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

    return result_tables;
}


std::set<std::pair<String, String>> PostgreSQLReplicationHandler::fetchPublishedTablePairs(pqxx::nontransaction & tx) const
{
    pqxx::result result{tx.exec(fmt::format(
        "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = {}", quoteStringPostgreSQL(publication_name)))};
    std::set<std::pair<String, String>> tables;
    for (const auto & row : result)
    {
        const auto schema = row[0].as<std::string>();
        tables.emplace(isDefaultPostgreSQLSchema(schema) ? "public" : schema, row[1].as<std::string>());
    }
    return tables;
}


String PostgreSQLReplicationHandler::publicationDefinitionConflict(pqxx::nontransaction & tx) const
{
    const String name = doubleQuoteString(publication_name);
    const String quoted_name = quoteStringPostgreSQL(publication_name);

    pqxx::result flags{tx.exec(fmt::format(
        "SELECT pubinsert AND pubupdate AND pubdelete AND pubtruncate, puballtables FROM pg_publication WHERE pubname = {}",
        quoted_name))};
    if (flags.empty())
        return fmt::format("publication {} does not exist", name);
    if (!flags[0][0].as<bool>())
        return fmt::format(
            "publication {} does not publish all operation types (ClickHouse creates it with the default "
            "`publish = 'insert, update, delete, truncate'`)", name);
    if (flags[0][1].as<bool>())
        return fmt::format("publication {} publishes all tables (ClickHouse creates it with a list of tables)", name);

    /// Row filters, column lists and schema-level membership exist since PostgreSQL 15. Schema-level membership
    /// extends the publication with every table created in that schema later.
    if (tx.conn().server_version() < 150000)
        return {};

    pqxx::result filtered{tx.exec(fmt::format(
        "SELECT count(*) FROM pg_publication_rel r JOIN pg_publication p ON r.prpubid = p.oid "
        "WHERE p.pubname = {} AND r.prqual IS NOT NULL", quoted_name))};
    if (filtered[0][0].as<Int64>() > 0)
        return fmt::format("publication {} applies a row filter to a published table (ClickHouse creates it without one)", name);

    std::map<std::pair<String, String>, std::set<String>> published_column_lists;
    pqxx::result column_lists{tx.exec(fmt::format(
        "SELECT n.nspname, c.relname, a.attname FROM pg_publication_rel r "
        "JOIN pg_publication p ON r.prpubid = p.oid "
        "JOIN pg_class c ON c.oid = r.prrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_attribute a ON a.attrelid = r.prrelid AND a.attnum = ANY(r.prattrs) "
        "WHERE p.pubname = {} AND r.prattrs IS NOT NULL", quoted_name))};
    for (const auto & row : column_lists)
        published_column_lists[{row[0].as<String>(), row[1].as<String>()}].insert(row[2].as<String>());

    for (const auto & [table, columns] : published_column_lists)
    {
        auto it = configured_column_lists.find(table);
        if (it == configured_column_lists.end() || it->second != columns)
            return fmt::format(
                "publication {} applies a column list to the published table {}.{} that "
                "`materialized_postgresql_tables_list` does not request", name, table.first, table.second);
    }

    /// The reverse: a table restricted to a subset of columns is published with all columns. The consumer would
    /// skip the table after its first `Relation` message, which carries more columns than the nested table.
    const auto published_tables = fetchPublishedTablePairs(tx);
    for (const auto & [table, columns] : configured_column_lists)
    {
        if (published_tables.contains(table) && !published_column_lists.contains(table))
            return fmt::format(
                "publication {} publishes all columns of the table {}.{}, but `materialized_postgresql_tables_list` "
                "requests only a subset of them", name, table.first, table.second);
    }

    pqxx::result schema_membership{tx.exec(fmt::format(
        "SELECT count(*) FROM pg_publication_namespace n JOIN pg_publication p ON n.pnpubid = p.oid "
        "WHERE p.pubname = {}", quoted_name))};
    if (schema_membership[0][0].as<Int64>() > 0)
        return fmt::format("publication {} includes tables by schema (ClickHouse creates it with a list of tables)", name);

    return {};
}


std::set<String> PostgreSQLReplicationHandler::fetchTablesFromPublication(pqxx::work & tx)
{
    std::string query = fmt::format("SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = {}", quoteStringPostgreSQL(publication_name));
    std::set<String> tables;

    for (const auto & [schema, table] : tx.stream<std::string, std::string>(query))
        tables.insert(schema_as_a_part_of_table_name ? schema + '.' + table : table);

    return tables;
}


namespace
{
    /// Replace Date32 with Date and DateTime64 with DateTime (recursing into Nullable and Array),
    /// used when `materialized_postgresql_use_extended_date_and_time_types` is disabled.
    DataTypePtr narrowDateAndTimeType(const DataTypePtr & type)
    {
        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
            return std::make_shared<DataTypeNullable>(narrowDateAndTimeType(nullable->getNestedType()));
        if (const auto * array = typeid_cast<const DataTypeArray *>(type.get()))
            return std::make_shared<DataTypeArray>(narrowDateAndTimeType(array->getNestedType()));

        WhichDataType which(type);
        if (which.isDate32())
            return std::make_shared<DataTypeDate>();
        if (which.isDateTime64())
            return std::make_shared<DataTypeDateTime>();
        return type;
    }

    void narrowDateAndTimeTypes(const PostgreSQLTableStructure::ColumnsInfoPtr & columns_info)
    {
        if (!columns_info)
            return;

        NamesAndTypesList narrowed;
        for (const auto & name_and_type : columns_info->columns)
            narrowed.emplace_back(name_and_type.name, narrowDateAndTimeType(name_and_type.type));
        columns_info->columns = std::move(narrowed);
    }
}

template<typename T>
PostgreSQLTableStructurePtr PostgreSQLReplicationHandler::fetchTableStructure(
        T & tx, const std::string & table_name) const
{
    PostgreSQLTableStructure structure;
    auto [schema, table] = getSchemaAndTableName(table_name);
    structure = fetchPostgreSQLTableStructure(tx, table, schema, true, true, true, getTableAllowedColumns(table_name));

    /// PostgreSQL `date`/`timestamp` are mapped to `Date32`/`DateTime64` by default to cover their
    /// wider value range. The setting allows falling back to the narrower `Date`/`DateTime` types.
    if (!use_extended_date_and_time_types)
    {
        narrowDateAndTimeTypes(structure.physical_columns);
        narrowDateAndTimeTypes(structure.primary_key_columns);
        narrowDateAndTimeTypes(structure.replica_identity_columns);
    }

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

bool PostgreSQLReplicationHandler::addTableToReplication(StorageMaterializedPostgreSQL * materialized_storage, const String & postgres_table_name)
{
    assertInitialized();

    /// Note: we have to ensure that replication consumer task is stopped when we reload table, because otherwise
    /// it can read wal beyond start lsn position (from which this table is being loaded), which will result in losing data.
    bool publication_added = false;
    consumer_task->deactivate();
    try
    {
        LOG_TRACE(log, "Adding table `{}` to replication", postgres_table_name);
        postgres::Connection replication_connection(connection_info, /* replication */true);
        String snapshot_name;
        String start_lsn;
        StorageInfo nested_storage_info{ nullptr, {} };

        /// See `snapshot_positions_path`. The snapshot is marked incomplete before the table is published: otherwise, after
        /// a restart in between, a published table with an empty nested table would be resumed. The snapshot position is
        /// not known until the temporary replication slot is created. An entry left by a failure is dropped on the next
        /// attach if the table is not replicated then, together with its nested table and publication entry. Otherwise
        /// (it is discovered from `materialized_postgresql_schema_list`), the nested table is loaded again, because its
        /// snapshot is not marked completed.
        /// Whether this query creates the publication entry is persisted too, so that a restart in between keeps
        /// a publication entry that existed before, as the rollback below does. It is recorded before
        /// `ALTER PUBLICATION`, so that a restart right after it still removes the entry that it created.
        auto snapshot_positions = loadSnapshotPositions();
        {
            pqxx::nontransaction tx(replication_connection.getRef());
            const bool already_published = fetchPublishedTablePairs(tx).contains(getNormalizedSchemaAndTableName(postgres_table_name));
            snapshot_positions[postgres_table_name] = SnapshotPosition{.start_lsn = "", .completed = false, .owns_publication_entry = !already_published};
            saveSnapshotPositions(snapshot_positions);

            /// The table must be in the publication before the snapshot is taken: `pgoutput` decides publication membership
            /// by the catalog at the LSN of each change, so the changes between the snapshot and `ALTER PUBLICATION`
            /// would be neither in the snapshot nor in the replication stream.
            publication_added = addTableToPublication(tx, postgres_table_name);
        }

        {
            auto tx = std::make_shared<pqxx::nontransaction>(replication_connection.getRef());

            if (isReplicationSlotExist(*tx, start_lsn, /* temporary */true))
                dropReplicationSlot(*tx, /* temporary */true);

            TemporaryReplicationSlot temporary_slot(this, tx, start_lsn, snapshot_name);

            snapshot_positions[postgres_table_name] = SnapshotPosition{
                .start_lsn = start_lsn, .completed = false, .owns_publication_entry = publication_added};
            saveSnapshotPositions(snapshot_positions);

            /// Protect against deadlock.
            auto nested = DatabaseCatalog::instance().tryGetTable(materialized_storage->getNestedStorageID(), materialized_storage->getNestedTableContext());
            if (!nested)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Internal table was not created");

            FailPointInjection::pauseFailPoint(FailPoints::materialized_postgresql_pause_before_attach_table_snapshot);

            postgres::Connection tmp_connection(connection_info);
            nested_storage_info = loadFromSnapshot(tmp_connection, snapshot_name, postgres_table_name, materialized_storage);
            markSnapshotCompleted(postgres_table_name);
            materialized_storage->set(nested_storage_info.storage);
        }

        /// Pass storage to consumer and lsn position, from which to start receiving replication messages for this table.
        getConsumer()->addNested(postgres_table_name, nested_storage_info, start_lsn);
        LOG_TRACE(log, "Table `{}` successfully added to replication", postgres_table_name);
    }
    catch (...)
    {
        /// Do not leave the table published while nothing replicates it. A publication entry that existed
        /// before is kept: removing it would make the publication drift from the replicated tables.
        if (publication_added)
        {
            try
            {
                postgres::Connection replication_connection(connection_info, /* replication */true);
                pqxx::nontransaction tx(replication_connection.getRef());
                removeTableFromPublication(tx, postgres_table_name);
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to remove table from publication after a failed add to replication");
            }
        }

        consumer_task->activate();
        consumer_task->scheduleAfter(milliseconds_to_wait);

        auto error_message = getCurrentExceptionMessage(false);
        throw Exception(ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                        "Failed to add table `{}` to replication. Info: {}", postgres_table_name, error_message);
    }
    consumer_task->activateAndSchedule();
    return publication_added;
}


void PostgreSQLReplicationHandler::removeTableFromReplication(const String & postgres_table_name, bool remove_from_publication)
{
    assertInitialized();

    consumer_task->deactivate();
    try
    {
        /// `remove_from_publication` is false when undoing an `ATTACH TABLE` that reused an existing publication entry.
        if (remove_from_publication)
        {
            postgres::Connection replication_connection(connection_info, /* replication */true);
            pqxx::nontransaction tx(replication_connection.getRef());
            removeTableFromPublication(tx, postgres_table_name);
        }

        /// Pass storage to consumer and lsn position, from which to start receiving replication messages for this table.
        getConsumer()->removeNested(postgres_table_name);

        auto snapshot_positions = loadSnapshotPositions();
        if (snapshot_positions.erase(postgres_table_name))
            saveSnapshotPositions(snapshot_positions);
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

void PostgreSQLReplicationHandler::execWithRetryAndFaultInjection(postgres::Connection & connection, const std::function<void(pqxx::nontransaction &)> & exec) const
{
    if (fault_injection_probability > 0.f)
    {
        std::bernoulli_distribution fault(static_cast<double>(fault_injection_probability));
        if (fault(thread_local_rng))
            throw pqxx::broken_connection("Fault injected");
    }

    connection.execWithRetry(exec);
}

}
