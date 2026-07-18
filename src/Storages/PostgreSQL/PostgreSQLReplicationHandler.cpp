#include <random>
#include <base/sort.h>

#include <Core/Settings.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/ServerUUID.h>
#include <Core/UUID.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
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
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Databases/DatabaseOnDisk.h>

#include <boost/algorithm/string/join.hpp>
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

    String normalizeReplicationSlot(String name)
    {
        name = Poco::toLower(name);
        for (auto & c : name)
            if (c == '-')
                c = '_';
        return name;
    }

    /// When `materialized_postgresql_use_unique_replication_consumer_identifier` is enabled, the replication
    /// slot and the publication must be unique per ClickHouse server. The ClickHouse database/table UUID alone
    /// is not enough: a `CREATE ... ON CLUSTER` query assigns the same UUID to every replica, so all replicas
    /// would derive the same slot and publication names and fight over a single PostgreSQL replication slot and
    /// publication (see https://github.com/ClickHouse/ClickHouse/issues/58726). Mixing in the persistent
    /// per-server `ServerUUID` makes the identifier unique per (ClickHouse object, server). The combination is
    /// hashed back into a single UUID so the result always stays within PostgreSQL's identifier length limit.
    /// The ClickHouse object UUID already identifies the object regardless of the replicated PostgreSQL schema,
    /// so this identifier does not need to be schema-aware.
    String getUniqueReplicationIdentifier(const String & clickhouse_uuid)
    {
        const auto unique_uuid = UUIDHelpers::makeUUIDv4FromHash(fmt::format("{}_{}", clickhouse_uuid, toString(ServerUUID::get())));
        return normalizeReplicationSlot(toString(unique_uuid));
    }

    /// `salted_unique_identifier` selects between the current, `ServerUUID`-salted form of the unique
    /// replication consumer identifier and the pre-salt form generated by releases before the `ON CLUSTER`
    /// fix (the bare ClickHouse object UUID for the slot, and a name that ignored the setting for the
    /// publication). The pre-salt form is needed to recognize — and adopt on attach — objects created by
    /// those releases (see adoptLegacyReplicationIdentityIfNeeded()).
    String getPublicationName(
        const String & postgres_database,
        const String & postgres_schema,
        const String & postgres_table,
        const String & clickhouse_uuid,
        const MaterializedPostgreSQLSettings & replication_settings,
        bool salted_unique_identifier)
    {
        if (salted_unique_identifier
            && replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_use_unique_replication_consumer_identifier])
        {
            /// The publication must be unique per server too, otherwise several replicas created via
            /// `ON CLUSTER` would race to create (and drop, for a non-`ATTACH` query) the same publication.
            return fmt::format("{}_ch_publication", getUniqueReplicationIdentifier(clickhouse_uuid));
        }

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

    String getReplicationSlotName(
        const String & postgres_database,
        const String & postgres_schema,
        const String & postgres_table,
        const String & clickhouse_uuid,
        const MaterializedPostgreSQLSettings & replication_settings,
        bool salted_unique_identifier)
    {
        String slot_name = replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_replication_slot];
        if (slot_name.empty())
        {
            if (replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_use_unique_replication_consumer_identifier])
                slot_name = salted_unique_identifier ? getUniqueReplicationIdentifier(clickhouse_uuid) : clickhouse_uuid;
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

    /// The set of PostgreSQL schemas an engine with these settings replicates. A schema-blind legacy
    /// publication (whose name carries no schema) is adopted on attach only when every table it publishes
    /// belongs to one of these schemas — otherwise it may belong to a different engine replicating other
    /// schemas of the same PostgreSQL database. `pg_publication_tables.schemaname` reports the unqualified
    /// default schema as `"public"`, so the default schema is normalized to `"public"` here to match.
    /// Covers every configuration, not just the single-common-schema one: a `materialized_postgresql_schema`
    /// (a single non-default schema), a `materialized_postgresql_schema_list` (several schemas), and
    /// `materialized_postgresql_tables_list_with_schema` (schemas embedded per table in `tables_list`) all
    /// need their own schema set — the first two leave `postgres_schema` empty while still replicating
    /// non-default schemas.
    std::unordered_set<String> computeReplicatedSchemas(
        const String & postgres_schema,
        const String & schema_list,
        const String & tables_list,
        bool schema_as_a_part_of_table_name)
    {
        std::unordered_set<String> schemas;
        if (!schema_list.empty())
        {
            /// `materialized_postgresql_schema_list`: a comma-separated list of schemas whose tables are all replicated.
            Strings parts;
            splitInto<','>(parts, schema_list);
            for (auto & part : parts)
            {
                boost::trim(part);
                if (!part.empty())
                    schemas.insert(part);
            }
        }
        else if (schema_as_a_part_of_table_name && !tables_list.empty())
        {
            /// `materialized_postgresql_tables_list_with_schema`: each entry of `tables_list` is `schema.table`
            /// (an optional column list in parentheses may follow). Take the schema of each; an entry with no
            /// schema targets the default `"public"` schema (see getSchemaAndTableName()).
            String cleared = tables_list;
            while (true)
            {
                size_t open_bracket_pos = cleared.find('(');
                size_t close_bracket_pos = cleared.find(')');
                if (open_bracket_pos == std::string::npos || close_bracket_pos == std::string::npos)
                    break;
                cleared = cleared.substr(0, open_bracket_pos) + cleared.substr(close_bracket_pos + 1);
            }
            Strings parts;
            splitInto<','>(parts, cleared);
            for (auto & part : parts)
            {
                boost::trim(part);
                if (part.empty())
                    continue;
                if (auto pos = part.find('.'); pos != std::string::npos)
                    schemas.insert(part.substr(0, pos));
                else
                    schemas.insert("public");
            }
        }
        else if (!isDefaultPostgreSQLSchema(postgres_schema))
            /// A single non-default common schema (`materialized_postgresql_schema`).
            schemas.insert(postgres_schema);
        else
            /// The default PostgreSQL schema.
            schemas.insert("public");
        return schemas;
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
    , use_extended_date_and_time_types(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_use_extended_date_and_time_types])
    , user_managed_slot(!replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_replication_slot].value.empty())
    , user_provided_snapshot(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_snapshot])
    , use_unique_replication_consumer_identifier(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_use_unique_replication_consumer_identifier])
    , replication_slot(getReplicationSlotName(postgres_database_, postgres_schema, postgres_table_, clickhouse_uuid_, replication_settings, /* salted_unique_identifier */ true))
    , tmp_replication_slot(replication_slot + "_tmp")
    , publication_name(getPublicationName(postgres_database_, postgres_schema, postgres_table_, clickhouse_uuid_, replication_settings, /* salted_unique_identifier */ true))
    , legacy_replication_slot(getReplicationSlotName(postgres_database_, /* postgres_schema */ "", postgres_table_, clickhouse_uuid_, replication_settings, /* salted_unique_identifier */ false))
    , legacy_publication_name(getPublicationName(postgres_database_, /* postgres_schema */ "", postgres_table_, clickhouse_uuid_, replication_settings, /* salted_unique_identifier */ false))
    , replicated_schemas(computeReplicatedSchemas(postgres_schema, schema_list, tables_list, schema_as_a_part_of_table_name))
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

    /// A user-provided `materialized_postgresql_replication_slot` pins the replication slot to a single, fixed
    /// name, whereas `materialized_postgresql_use_unique_replication_consumer_identifier` exists to make the
    /// replication slot and publication unique per server so that an `ON CLUSTER` deployment works: every
    /// replica receives the same settings (hence the same user-managed slot name) and would otherwise fight
    /// over one logical PostgreSQL replication slot (see https://github.com/ClickHouse/ClickHouse/issues/58726).
    /// The generated publication can be made per-server, but a user-managed slot cannot, so the two settings
    /// contradict each other: all but one replica would remain unable to replicate. Reject the combination
    /// instead of silently leaving the deployment half-broken. Only on `CREATE`, not `ATTACH`, so that an
    /// already-created single-server deployment that happens to set both keeps starting up after an upgrade.
    if (!is_attach && user_managed_slot && use_unique_replication_consumer_identifier)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot use a user-managed replication slot (`materialized_postgresql_replication_slot`) together "
            "with `materialized_postgresql_use_unique_replication_consumer_identifier`: the latter makes the "
            "replication slot and publication unique per server so that `ON CLUSTER` replicas do not collide, "
            "but a user-managed slot has one fixed name shared by every replica and cannot be made per-server, "
            "so all but one replica would fail to replicate. Either remove "
            "`materialized_postgresql_replication_slot` to let each server derive its own unique slot, or "
            "disable `materialized_postgresql_use_unique_replication_consumer_identifier` if a single shared, "
            "user-managed slot is intended.");

    checkReplicationSlot(replication_slot);

    LOG_INFO(log, "Using replication slot {} and publication {}", replication_slot, doubleQuoteString(publication_name));

    startup_task = getContext()->getSchedulePool().createTask(StorageID::createEmpty(), "PostgreSQLReplicaStartup", [this]{ checkConnectionAndStart(); });
    consumer_task = getContext()->getSchedulePool().createTask(StorageID::createEmpty(), "PostgreSQLReplicaConsume", [this]{ consumerFunc(); });
    cleanup_task = getContext()->getSchedulePool().createTask(StorageID::createEmpty(), "PostgreSQLReplicaCleanup", [this]{ cleanupFunc(); });
}


void PostgreSQLReplicationHandler::addStorage(const std::string & table_name, StorageMaterializedPostgreSQL * storage)
{
    materialized_storages[table_name] = storage;
}


void PostgreSQLReplicationHandler::setTablesReplicatedByPreviousRun(std::set<String> tables)
{
    tables_replicated_by_previous_run = std::move(tables);
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
    /// In the multi-schema modes the WAL consumer keys relation messages by the qualified "schema.table"
    /// name, so a foreign-schema table can never shadow one of this engine's tables - extras are harmless.
    if (schema_as_a_part_of_table_name)
        return {};

    std::unordered_set<String> expected_bare_names;
    for (const auto & [schema, table] : expected)
        expected_bare_names.insert(table);

    String colliding;
    for (const auto & [schema, table] : published)
    {
        /// A pair this engine replicates itself is not an extra.
        if (expected.contains(std::make_pair(schema, table)))
            continue;
        /// An extra whose bare name does not collide with any replicated table is harmless: the consumer
        /// never maps it onto one of this engine's tables and its changes are simply ignored.
        if (!expected_bare_names.contains(table))
            continue;
        if (!colliding.empty())
            colliding += ", ";
        colliding += schema + '.' + table;
    }
    return colliding;
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


/// Deployments created before the generated names were last changed own the legacy objects on the
/// PostgreSQL side. On attach such a deployment must keep its legacy identity: looking for the renamed
/// slot instead would miss the existing slot, run an initial sync and reload a snapshot into the
/// already-existing nested tables, duplicating data. So, on attach, when the renamed objects do not
/// exist but the legacy ones do, switch to the legacy names. Two renames are covered:
///
/// 1. The unique replication consumer identifier became salted with the per-server `ServerUUID` (the
/// `ON CLUSTER` fix, https://github.com/ClickHouse/ClickHouse/issues/58726); it used to be the bare
/// ClickHouse object UUID for the slot, while the publication name ignored the setting and was always the
/// schema-blind form (see legacy_publication_name's construction). Here the pre-salt slot name embeds this
/// object's own ClickHouse UUID, so its existence is itself proof of ownership: nothing else generates that
/// name — except `ON CLUSTER` replicas, which shared the object UUID and therefore fought over that slot
/// already; adopting keeps such a deployment exactly as broken as it was (never worse), and recreating the
/// object moves it to the fixed, per-server identity. Once the pre-salt slot is adopted, its pre-salt
/// publication must be adopted too: replication streams through it, and creating a fresh publication under
/// the salted name would silently drop every change written to WAL before that publication existed, because
/// pgoutput resolves publication membership from a historic catalog snapshot at each change's LSN and skips
/// a not-yet-created publication. The pre-salt publication name carries no proof of its own — it is the same
/// schema-blind name another engine replicating other tables of the same PostgreSQL database could own — so
/// it is adopted only once its published table set is confirmed to be exactly this engine's set of replicated
/// tables (a schema-only check is not enough: a same-schema publication that lists a different set of tables
/// belongs to another engine). If it is missing or foreign, the attach fails closed with an exception (never
/// a fresh publication that would lose the WAL gap, never a hijacked one).
///
/// 2. The generated publication and default replication-slot names became schema-aware. Deployments
/// created before that own the legacy, schema-blind objects. The legacy names are schema-blind and
/// therefore shared with a
/// same-database deployment over the default schema (or another schema targeting the same bare table),
/// so the existence of the legacy slot alone does not prove the legacy objects belong to this engine —
/// only the legacy publication's table list carries the schema. The legacy identity is therefore only
/// adopted when the legacy publication exists and publishes EXACTLY this engine's set of replicated
/// tables. A schema check alone is not enough: a same-schema publication that lists a different set of
/// tables (this engine replicates `foo.a, foo.b` while the publication publishes only `foo.c`) belongs to
/// another engine, exactly as in the unique-identifier rename above. If the legacy publication is missing,
/// empty, publishes a table from another schema, or publishes a different table set, the legacy slot is
/// ambiguous or foreign, and adopting it (or returning to proceed under the schema-aware identity) would
/// either hijack another engine's slot/publication or, since the schema-aware slot is gone, run an initial
/// sync and reload a snapshot into the already-existing nested tables (duplicating data on disk). In that
/// case the attach fails closed with an exception instead of silently re-snapshotting a populated replica
/// or hijacking another engine's replication slot.
void PostgreSQLReplicationHandler::adoptLegacyReplicationIdentityIfNeeded(pqxx::nontransaction & tx)
{
    if (!is_attach)
        return;

    /// The generated names differ from the legacy ones only for a non-default schema (the schema-aware
    /// rename) or when a unique replication consumer identifier is used (the `ServerUUID`-salted rename);
    /// a user-managed slot always keeps its user-provided name. This check also keeps the slot part of
    /// the adoption idempotent: once adopted, the slot names compare equal.
    if (replication_slot == legacy_replication_slot && publication_name == legacy_publication_name)
        return;

    auto slot_exists = [&](const String & name)
    {
        pqxx::result result{tx.exec(fmt::format("SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}'", name))};
        return !result.empty();
    };
    auto publication_exists = [&](const String & name)
    {
        pqxx::result result{tx.exec(fmt::format("SELECT 1 FROM pg_publication WHERE pubname = '{}'", name))};
        return !result.empty();
    };
    /// The set of PostgreSQL tables this engine replicates, as `(schema, table)` pairs with the default
    /// schema reported as `"public"` to match `pg_publication_tables.schemaname`. In `tables_list` mode
    /// (including the single-table engine, whose `tables_list` is the one raw remote table name) the set is
    /// parsed from `tables_list`; otherwise the database engine provides the tables it already replicated
    /// in the previous run (their nested tables exist on disk). The live PostgreSQL schema must NOT be
    /// consulted here: a table created in PostgreSQL after `CREATE DATABASE` is not replicated without an
    /// explicit `ATTACH TABLE`, so after the source schema has grown, this engine's own publication
    /// legitimately publishes fewer tables than the schema contains, and comparing against the live schema
    /// would wrongly reject that publication as foreign. Adoption runs before `tables_list` is rewritten
    /// into its quoted form at the end of fetchRequiredTables() (and, for the single-table engine, on the
    /// raw name too), so the raw, comma-separated form is parsed here, resolving each entry's schema
    /// exactly as getSchemaAndTableName().
    auto expected_replicated_tables = [&]() -> std::set<std::pair<String, String>>
    {
        std::set<std::pair<String, String>> tables;
        auto add = [&](const String & schema, const String & table)
        {
            tables.emplace(isDefaultPostgreSQLSchema(schema) ? "public" : schema, table);
        };
        if (!tables_list.empty())
        {
            /// `schema.table, table2(col1,col2), ...` — drop the optional column lists, split on commas.
            String cleared = tables_list;
            while (true)
            {
                size_t open_bracket_pos = cleared.find('(');
                size_t close_bracket_pos = cleared.find(')');
                if (open_bracket_pos == std::string::npos || close_bracket_pos == std::string::npos)
                    break;
                cleared = cleared.substr(0, open_bracket_pos) + cleared.substr(close_bracket_pos + 1);
            }
            Strings parts;
            splitInto<','>(parts, cleared);
            for (auto & part : parts)
            {
                boost::trim(part);
                if (part.empty())
                    continue;
                auto [schema, table] = getSchemaAndTableName(part);
                add(schema, table);
            }
        }
        else
        {
            /// Whole-schema database engine: the set is the tables replicated by the previous run,
            /// which the database engine always provides before its attach reaches this point (see
            /// DatabaseMaterializedPostgreSQL::startSynchronization()); the single-table engine always
            /// has a non-empty `tables_list` and never gets here.
            if (!tables_replicated_by_previous_run)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "The set of tables replicated by the previous run was not provided on attach");
            for (const auto & name : *tables_replicated_by_previous_run)
            {
                auto [schema, table] = getSchemaAndTableName(name);
                add(schema, table);
            }
        }
        return tables;
    };

    /// `legacy_publication_name` is always the schema-blind name (see its construction in the constructor),
    /// while this engine may replicate one or more non-default schemas, so a schema-blind name alone does not
    /// prove the publication belongs to this engine: another engine replicating other tables of the same
    /// PostgreSQL database — a different schema, or a different table subset of the same schema — may already
    /// own an identically-named publication. Adopt it only when it publishes EXACTLY this engine's tables. A
    /// schema check alone is not enough: a same-schema publication that lists the wrong tables (e.g. this
    /// engine replicates `foo.a, foo.b` while the publication publishes only `foo.c`) would otherwise be
    /// adopted, and replication would then stream through it — silently never replicating this engine's own
    /// tables while consuming the wrong ones. Return a human-readable reason when ownership cannot be proven
    /// (an empty string means the publication is owned by this engine and may be adopted).
    auto legacy_publication_ownership_conflict = [&](const String & name) -> String
    {
        pqxx::result result{tx.exec(fmt::format(
            "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = '{}'", name))};
        if (result.empty())
            return fmt::format("the pre-salt publication {} publishes no tables", doubleQuoteString(name));
        std::set<std::pair<String, String>> published;
        for (const auto & row : result)
        {
            const auto schema = row[0].as<std::string>();
            if (!replicated_schemas.contains(schema))
                return fmt::format(
                    "the pre-salt publication {} publishes tables from a schema this engine does not replicate "
                    "('{}'), so it belongs to another engine",
                    doubleQuoteString(name), schema);
            published.emplace(schema, row[1].as<std::string>());
        }
        if (published != expected_replicated_tables())
            return fmt::format(
                "the pre-salt publication {} publishes a different set of tables than this engine replicates, so "
                "it belongs to another engine",
                doubleQuoteString(name));
        return {};
    };

    if (use_unique_replication_consumer_identifier)
    {
        /// Rename 1: the pre-salt slot name embeds this object's own ClickHouse UUID, so its existence is
        /// itself proof of ownership and no further evidence is needed (see the comment above the function).
        if (replication_slot != legacy_replication_slot)
        {
            /// The slot is the object whose loss triggers a destructive re-sync, so it carries the evidence:
            /// adopt only if the salted slot does not exist while the pre-salt one does.
            if (slot_exists(replication_slot) || !slot_exists(legacy_replication_slot))
                return;

            LOG_INFO(
                log,
                "Adopting the legacy replication identity of a deployment created before the unique replication "
                "consumer identifier became salted with the server UUID: replication slot {} (instead of {})",
                legacy_replication_slot, replication_slot);

            replication_slot = legacy_replication_slot;
            tmp_replication_slot = replication_slot + "_tmp";
        }
        else
        {
            /// A user-managed slot keeps its user-provided name; the publication is the only renamed object.
            /// If the salted publication already exists, this deployment is already on the salted identity
            /// and there is nothing to adopt.
            if (publication_exists(publication_name))
                return;
        }

        /// The pre-salt slot has been adopted (or the slot is user-managed): replication now streams through
        /// the pre-salt publication, so that publication must be reused. Creating a fresh publication under
        /// the salted name and streaming through it instead would silently drop every change committed to
        /// WAL before the fresh publication existed: pgoutput resolves publication membership from a historic
        /// catalog snapshot taken at each change's LSN, so a not-yet-created publication is skipped
        /// (`WARNING: skipped loading publication`) and its rows never reach the replica, which then falls
        /// permanently behind without any error. The pre-salt publication name is schema-blind, so it may
        /// instead belong to another engine; adopt it only when it exists and publishes exactly this engine's
        /// tables. Otherwise fail closed below: never hijack another engine's publication, and never fall back
        /// to a fresh publication that would lose the WAL gap.
        const String publication_conflict = publication_exists(legacy_publication_name)
            ? legacy_publication_ownership_conflict(legacy_publication_name)
            : fmt::format("the pre-salt publication {} does not exist", doubleQuoteString(legacy_publication_name));

        if (publication_conflict.empty())
        {
            LOG_INFO(
                log, "Adopting the legacy publication {} (instead of {})",
                doubleQuoteString(legacy_publication_name), doubleQuoteString(publication_name));
            publication_name = legacy_publication_name;
            return;
        }

        throw Exception(
            ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
            "Cannot start MaterializedPostgreSQL replication on attach: replication is bound to the pre-salt "
            "replication slot {}, but {}. Streaming through a freshly created publication instead would silently "
            "lose every change written to PostgreSQL before it is (re)created (pgoutput skips a publication that "
            "did not yet exist at the change's LSN), and adopting a publication that belongs to another engine "
            "would consume the wrong tables, so replication is refused. Recreate this engine's own publication "
            "{} on the PostgreSQL side (or drop the conflicting one), or recreate this table: startup keeps "
            "retrying and replication starts automatically once the conflict is resolved, without a server "
            "restart or a manual re-attach.",
            replication_slot, publication_conflict, doubleQuoteString(legacy_publication_name));
    }

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
            "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = '{}'", legacy_publication_name))};
        if (result.empty())
            ownership_conflict = fmt::format(
                "the legacy publication {} publishes no tables, so the schema-blind legacy replication slot "
                "cannot be proven to belong to this engine's schema '{}'",
                doubleQuoteString(legacy_publication_name), postgres_schema);
        std::set<std::pair<String, String>> published;
        for (const auto & row : result)
        {
            const auto schema = row[0].as<std::string>();
            if (schema != postgres_schema)
            {
                ownership_conflict = fmt::format(
                    "the legacy publication {} publishes a table from schema '{}', not this engine's schema "
                    "'{}', so it belongs to another engine",
                    doubleQuoteString(legacy_publication_name), schema, postgres_schema);
                break;
            }
            published.emplace(schema, row[1].as<std::string>());
        }
        /// A schema match alone is not enough: a same-schema legacy publication that lists a different set of
        /// tables (this engine replicates `foo.a, foo.b` while the publication publishes only `foo.c`) belongs
        /// to another engine. Adopting it would stream WAL through the foreign table set, so this replica would
        /// silently stop receiving changes for its own tables (and shutdownFinal() would later drop the foreign
        /// publication). Require the published set to be exactly this engine's tables — the same fail-closed
        /// proof legacy_publication_ownership_conflict() applies in the unique-identifier branch above.
        if (ownership_conflict.empty() && published != expected_replicated_tables())
            ownership_conflict = fmt::format(
                "the legacy publication {} publishes a different set of tables than this engine replicates in "
                "schema '{}', so it belongs to another engine",
                doubleQuoteString(legacy_publication_name), postgres_schema);
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


void PostgreSQLReplicationHandler::startSynchronization(bool throw_on_error)
{
    postgres::Connection replication_connection(connection_info, /* replication */true);
    pqxx::nontransaction tx(replication_connection.getRef());
    adoptLegacyReplicationIdentityIfNeeded(tx);

    /// On attach, an existing replication slot means replication resumes from the slot's
    /// confirmed_flush_lsn below. Two attach-time states must fail closed instead of silently corrupting an
    /// already-populated replica:
    ///
    ///  1. The slot survived but the publication is gone. Recreating the publication and streaming through
    ///     it would silently skip every change committed while the publication did not exist: pgoutput
    ///     resolves publication membership from a historic catalog snapshot taken at each change's LSN, so a
    ///     not-yet-created publication is skipped and the WAL gap between the publication's drop and its
    ///     re-creation never reaches the replica, which then falls permanently behind without any error (the
    ///     same rule adoptLegacyReplicationIdentityIfNeeded() enforces when switching to the legacy
    ///     identity).
    ///
    ///  2. The publication survived but the slot is gone (for example after a PostgreSQL major upgrade,
    ///     which does not preserve replication slots but keeps publications in the catalog, or after an
    ///     operator dropped the slot). There is no confirmed_flush_lsn left to resume from, so the code
    ///     below would fall through to initial_sync() and reload the current snapshot into the
    ///     already-populated nested tables. Snapshot rows are materialized with _sign = 1 and _version = 1
    ///     (StorageMaterializedPostgreSQL), so a re-snapshot cannot delete rows that disappeared from
    ///     PostgreSQL while the slot was gone (it produces no _sign = -1 tombstones) and cannot override
    ///     rows whose last replicated version is already greater than 1 (ReplacingMergeTree keeps the higher
    ///     version), silently leaving the replica stale while a fresh slot is created. A user-managed slot
    ///     is excluded: a missing user-managed slot is a configuration error reported below with its own
    ///     message, and that path never re-snapshots.
    ///
    ///  3. Both the slot and the publication are gone while this replica already holds data from a previous
    ///     run (a database engine's nested tables exist on disk, or - for the single-table engine - the
    ///     table exists in metadata, which it only does once its initial sync has succeeded). The slot being
    ///     gone too does not make the re-snapshot any less destructive: it is still the same in-place reload
    ///     into already-populated nested tables as case 2, with the same _sign = 1 / _version = 1 staleness,
    ///     so it fails closed for the same reason. The never-yet-synchronized state is exempted: a database
    ///     engine that has not created a single nested table yet (for example the server restarted before the
    ///     initial background synchronization created the slot and the publication) has nothing to be made
    ///     stale and must be allowed to run its initial snapshot.
    ///
    ///  4. The slot and the publication both survive, but the publication has drifted and no longer publishes
    ///     a table this engine replicates (for example an operator ran ALTER PUBLICATION ... DROP TABLE).
    ///     Resuming from the slot streams WAL filtered through the drifted publication, so the missing tables
    ///     silently stop receiving changes, and re-adding them to the publication now cannot recover the
    ///     changes committed while they were unpublished (the same historic-catalog-snapshot rule as case 1).
    ///     This is the current-identity counterpart of the exact-table-set ownership proof
    ///     adoptLegacyReplicationIdentityIfNeeded() enforces when adopting a legacy publication. (A whole-
    ///     schema database engine detects the same drift against its on-disk table set already in
    ///     fetchRequiredTables().)
    ///
    /// Recreate the object for a clean rebuild in the re-snapshot cases: dropping it removes any surviving
    /// slot or publication, and the fresh snapshot repopulates every row into empty nested tables, so nothing
    /// is lost.
    if (is_attach)
    {
        String slot_lsn;
        const bool slot_exists = isReplicationSlotExist(tx, slot_lsn, /* temporary */false);
        const bool publication_exists = isPublicationExist(tx);

        if (slot_exists && !publication_exists)
            throw Exception(
                ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                "Cannot start MaterializedPostgreSQL replication on attach: replication slot {} exists, but "
                "publication {} does not. Resuming from the existing slot through a freshly created "
                "publication would silently lose every change written to PostgreSQL while the publication "
                "did not exist (pgoutput skips a publication that did not yet exist at the change's LSN), so "
                "replication is refused. Recreate this object for a clean re-sync (dropping it removes the "
                "surviving replication slot, and the fresh snapshot contains every change, so nothing is "
                "lost), or recreate the publication with this engine's tables on the PostgreSQL side to "
                "resume from the slot, explicitly accepting that the changes written while the publication "
                "was absent are lost (newer PostgreSQL versions may refuse to stream past them at all): "
                "startup keeps retrying and replication starts automatically once the conflict is resolved, "
                "without a server restart or a manual re-attach.",
                replication_slot, doubleQuoteString(publication_name));

        if (!user_managed_slot && !slot_exists && publication_exists)
            throw Exception(
                ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                "Cannot start MaterializedPostgreSQL replication on attach: publication {} exists, but "
                "replication slot {} does not. The slot holds the position replication resumes from, so "
                "without it there is nothing to resume: re-snapshotting the current PostgreSQL state into "
                "the already-populated nested tables would silently leave the replica stale, because the "
                "snapshot rows are materialized with _sign = 1 and _version = 1 - they neither delete rows "
                "that disappeared from PostgreSQL while the slot was gone nor override rows whose last "
                "replicated version is already greater than 1. Replication is refused instead. Recreate this "
                "object for a clean rebuild: dropping it removes the leftover publication, and the fresh "
                "snapshot repopulates every row into empty nested tables, so nothing is lost. Startup keeps "
                "retrying and replication starts automatically once the conflict is resolved, without a "
                "server restart or a manual re-attach.",
                doubleQuoteString(publication_name), replication_slot);

        /// This replica already holds data from a previous run if the database engine kept nested tables on
        /// disk, or - for the single-table engine, which has no such set - if the table exists in metadata at
        /// all (a MaterializedPostgreSQL table is only left in metadata once its initial sync has succeeded,
        /// so an attach implies a completed previous run). A database engine that has not created a single
        /// nested table yet has nothing to be made stale and is allowed to run its initial snapshot.
        const bool has_previously_replicated_data
            = !is_materialized_postgresql_database
            || (tables_replicated_by_previous_run && !tables_replicated_by_previous_run->empty());

        if (!user_managed_slot && !slot_exists && !publication_exists && has_previously_replicated_data)
            throw Exception(
                ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                "Cannot start MaterializedPostgreSQL replication on attach: neither the replication slot {} "
                "nor the publication {} exists, but this replica already holds data from a previous run. "
                "Re-snapshotting the current PostgreSQL state into the already-populated nested tables would "
                "silently leave the replica stale, because the snapshot rows are materialized with _sign = 1 "
                "and _version = 1 - they neither delete rows that disappeared from PostgreSQL while "
                "replication was down nor override rows whose last replicated version is already greater than "
                "1. Replication is refused instead. Recreate this object for a clean rebuild: dropping it "
                "discards the stale nested data, and the fresh snapshot repopulates every row, so nothing is "
                "lost. Startup keeps retrying and replication starts automatically once the conflict is "
                "resolved, without a server restart or a manual re-attach.",
                replication_slot, doubleQuoteString(publication_name));

        /// The existing publication is reused as-is on attach (createPublicationIfNeeded() below is a no-op
        /// when it exists), and the slot resumes from its confirmed_flush_lsn with WAL filtered through
        /// whatever the publication currently publishes. If it has drifted and no longer publishes a table
        /// this engine replicates, that table silently stops receiving changes - fail closed instead.
        ///
        /// The comparison is by exact (schema, table) pair, not by bare table name: in the single-schema
        /// modes (a single `materialized_postgresql_schema`, the default `public` schema, or a whole-schema
        /// database over one common schema) the generated names, the publication table list and the WAL
        /// consumer all key tables by their bare name, so a publication rewritten from `foo.a` to `bar.a`
        /// while the server was down would otherwise pass this check unchanged and then replay WAL from
        /// `bar.a` into the ClickHouse table for `foo.a` (MaterializedPostgreSQLConsumer keys relation
        /// messages by the bare relation name in that mode). Deriving the expected schema from each
        /// replicated table via getNormalizedSchemaAndTableName() closes that hole.
        if (publication_exists)
        {
            const auto published = fetchPublishedTablePairs(tx);

            std::set<std::pair<String, String>> expected;
            for (const auto & entry : materialized_storages)
                expected.insert(getNormalizedSchemaAndTableName(entry.first));

            String missing;
            for (const auto & pair : expected)
            {
                if (published.contains(pair))
                    continue;
                if (!missing.empty())
                    missing += ", ";
                missing += pair.first + '.' + pair.second;
            }

            if (!missing.empty())
                throw Exception(
                    ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                    "Cannot start MaterializedPostgreSQL replication on attach: the existing publication {} no "
                    "longer publishes the following table(s) this engine replicates: {}. Resuming from the "
                    "existing replication slot through this drifted publication would silently stop streaming "
                    "changes for those tables, and re-adding them to the publication now would not recover the "
                    "changes committed while they were unpublished (pgoutput skips a table that was not "
                    "published at the change's LSN), so replication is refused. Recreate this object for a "
                    "clean rebuild, or add the missing tables back to the publication on the PostgreSQL side "
                    "and rebuild the affected tables: startup keeps retrying and replication starts "
                    "automatically once the conflict is resolved, without a server restart or a manual "
                    "re-attach.",
                    doubleQuoteString(publication_name), missing);

            /// Extra tables the publication publishes but this engine does not replicate are usually harmless
            /// (their changes are simply ignored). The exception is a foreign-schema table whose bare name
            /// collides with one of this engine's tables in the single-schema modes: the WAL consumer keys
            /// relation messages by the bare name there, so its changes would be replayed into this engine's
            /// ClickHouse table instead of being ignored. Fail closed on such a collision.
            const String colliding = collidingForeignPublishedTables(expected, published);
            if (!colliding.empty())
                throw Exception(
                    ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                    "Cannot start MaterializedPostgreSQL replication on attach: the existing publication {} "
                    "publishes the following foreign-schema table(s) whose bare name collides with a table this "
                    "engine replicates: {}. In the single-schema modes the replication consumer identifies "
                    "tables by their bare name, so resuming from the existing replication slot through this "
                    "publication would replay the foreign table's changes into this engine's ClickHouse table. "
                    "Replication is refused instead. Remove the colliding table(s) from the publication on the "
                    "PostgreSQL side, or recreate this object for a clean rebuild: startup keeps retrying and "
                    "replication starts automatically once the conflict is resolved, without a server restart "
                    "or a manual re-attach.",
                    doubleQuoteString(publication_name), colliding);
        }
    }

    createPublicationIfNeeded(tx);

    /// List of nested tables (table_name -> nested_storage), which is passed to replication consumer.
    std::unordered_map<String, StorageInfo> nested_storages;

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
                tryLogCurrentException(log);

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
            allowed_columns.push_back(doubleQuoteString(column.name));

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
    std::string query_str = fmt::format("SELECT exists (SELECT 1 FROM pg_publication WHERE pubname = '{}')", publication_name);
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
    chassert(temporary || !user_managed_slot);

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

                /// On attach the whole-schema database materializes the tables it already replicated in the
                /// previous run (their nested tables exist on disk), not whatever the publication currently
                /// publishes. The two sets can differ in either direction, and both differences must be
                /// tolerated instead of being turned into tables to materialize:
                ///  - A table created in PostgreSQL after `CREATE DATABASE` is not replicated without an
                ///    explicit `ATTACH TABLE`: it is not in the publication, no WAL is streamed for it, and it
                ///    has no nested table on disk.
                ///  - A table added to the publication while the server was down (for example an operator ran
                ///    `ALTER PUBLICATION ... ADD TABLE`) is likewise not something this database replicates: it
                ///    has no nested table on disk either.
                /// In both cases materializing the extra table would make startSynchronization() throw on its
                /// missing nested table on every attach retry, leaving the whole database unable to resume
                /// replication for the tables it does replicate. So the on-disk table set is authoritative and
                /// publication extras are ignored.
                if (tables_replicated_by_previous_run && !tables_replicated_by_previous_run->empty())
                {
                    result_tables = *tables_replicated_by_previous_run;

                    std::set<std::pair<String, String>> published;
                    {
                        pqxx::work tx(connection.getRef());
                        published = fetchPublishedTablePairs(tx);
                    }

                    /// The reverse of the extra-tables case above is a real drift: a table this database
                    /// already replicated in the previous run (its nested table exists on disk) is missing from
                    /// the publication - for example an operator ran ALTER PUBLICATION ... DROP TABLE. Resuming
                    /// from the slot then silently stops streaming changes for that table, and re-adding it to
                    /// the publication cannot recover the changes committed while it was unpublished (pgoutput
                    /// skips a table that was not published at the change's LSN). Fail closed instead - the
                    /// current-identity counterpart of the exact-table-set proof in
                    /// adoptLegacyReplicationIdentityIfNeeded() for the whole-schema database engine, whose
                    /// expected set is the on-disk table set rather than a `tables_list`. (The single-table and
                    /// `tables_list` engines are checked against their configured set in startSynchronization().)
                    /// The match is by exact (schema, table) pair, not by bare table name, so a publication
                    /// rewritten to a different schema with the same table names (`foo.a` -> `bar.a`) fails closed
                    /// here instead of resuming and replaying WAL from the wrong schema's table.
                    std::set<std::pair<String, String>> expected;
                    for (const auto & table_name : *tables_replicated_by_previous_run)
                        expected.insert(getNormalizedSchemaAndTableName(table_name));

                    String missing;
                    for (const auto & pair : expected)
                    {
                        if (published.contains(pair))
                            continue;
                        if (!missing.empty())
                            missing += ", ";
                        missing += pair.first + '.' + pair.second;
                    }

                    if (!missing.empty())
                        throw Exception(
                            ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                            "Cannot start MaterializedPostgreSQL replication on attach: the existing publication "
                            "{} no longer publishes the following table(s) this database already replicated: {}. "
                            "Resuming from the existing replication slot through this drifted publication would "
                            "silently stop streaming changes for those tables, and re-adding them to the "
                            "publication now would not recover the changes committed while they were unpublished "
                            "(pgoutput skips a table that was not published at the change's LSN), so replication "
                            "is refused. Recreate this database for a clean rebuild, or add the missing tables "
                            "back to the publication on the PostgreSQL side and rebuild the affected tables: "
                            "startup keeps retrying and replication starts automatically once the conflict is "
                            "resolved, without a server restart or a manual re-attach.",
                            doubleQuoteString(publication_name), missing);

                    /// The whole-schema counterpart of the collision check in startSynchronization(): a
                    /// foreign-schema table added to the publication (its nested table does not exist on disk,
                    /// so the extra is otherwise tolerated) whose bare name collides with a table this database
                    /// replicates would, in the single-schema modes, have its WAL replayed into the wrong
                    /// ClickHouse table by the consumer. Fail closed on it.
                    const String colliding = collidingForeignPublishedTables(expected, published);
                    if (!colliding.empty())
                        throw Exception(
                            ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                            "Cannot start MaterializedPostgreSQL replication on attach: the existing publication "
                            "{} publishes the following foreign-schema table(s) whose bare name collides with a "
                            "table this database replicates: {}. In the single-schema modes the replication "
                            "consumer identifies tables by their bare name, so resuming from the existing "
                            "replication slot through this publication would replay the foreign table's changes "
                            "into this database's ClickHouse table. Replication is refused instead. Remove the "
                            "colliding table(s) from the publication on the PostgreSQL side, or recreate this "
                            "database for a clean rebuild: startup keeps retrying and replication starts "
                            "automatically once the conflict is resolved, without a server restart or a manual "
                            "re-attach.",
                            doubleQuoteString(publication_name), colliding);
                }
                else
                {
                    /// No table has been replicated yet (the database was created but the initial
                    /// synchronization did not create a single nested table before the restart), so there is no
                    /// on-disk set to treat as authoritative. Fall back to the publication's tables to bootstrap
                    /// the initial synchronization.
                    pqxx::work tx(connection.getRef());
                    result_tables = fetchTablesFromPublication(tx);
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


template <typename T>
std::set<std::pair<String, String>> PostgreSQLReplicationHandler::fetchPublishedTablePairs(T & tx) const
{
    pqxx::result result{tx.exec(fmt::format(
        "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = '{}'", publication_name))};
    std::set<std::pair<String, String>> tables;
    for (const auto & row : result)
    {
        const auto schema = row[0].as<std::string>();
        tables.emplace(isDefaultPostgreSQLSchema(schema) ? "public" : schema, row[1].as<std::string>());
    }
    return tables;
}

template
std::set<std::pair<String, String>> PostgreSQLReplicationHandler::fetchPublishedTablePairs(pqxx::nontransaction & tx) const;

template
std::set<std::pair<String, String>> PostgreSQLReplicationHandler::fetchPublishedTablePairs(pqxx::work & tx) const;


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
        String snapshot_name;
        String start_lsn;
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
