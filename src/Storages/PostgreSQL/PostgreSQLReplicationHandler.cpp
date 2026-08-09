#include <random>
#include <base/sort.h>

#include <Core/Settings.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/UUID.h>
#include <Core/ServerUUID.h>
#include <Common/SipHash.h>
#include <Common/Macros.h>
#include <Common/escapeForFileName.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/FailPoint.h>
#include <Common/logger_useful.h>
#include <IO/ReadHelpers.h>
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
#include <Storages/StorageFactory.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/getTableOverride.h>
#include <Interpreters/InterpreterDropQuery.h>
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
    extern const MaterializedPostgreSQLSettingsString materialized_postgresql_table_engine;
    extern const MaterializedPostgreSQLSettingsString materialized_postgresql_keeper_path;
    extern const MaterializedPostgreSQLSettingsString materialized_postgresql_replica_name;
}

namespace Setting
{
    extern const SettingsFloat postgresql_fault_injection_probability;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int POSTGRESQL_REPLICATION_INTERNAL_ERROR;
    extern const int QUERY_NOT_ALLOWED;
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char materialized_postgresql_fail_teardown_after_shutdown[];
    extern const char materialized_postgresql_fail_load_from_snapshot[];
    extern const char materialized_postgresql_fail_add_table_to_replication[];
    extern const char materialized_postgresql_pause_before_register_replica[];
    extern const char materialized_postgresql_pause_before_marking_snapshot_completed[];
    extern const char materialized_postgresql_pause_before_redo_snapshot_truncate[];
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

    /// The canonical form of the identity through which a coordinated replica derives the ClickHouse names
    /// of the shared nested tables (and the names of the shared PostgreSQL slot and publication) from the
    /// shared publication. Stored at <keeper_path>/naming by the first replica and checked by every joining
    /// one (see `ensureCoordinatedNamingCompatible`): replicas that disagree on any of these would build
    /// disjoint replicated nested trees - or even separate slots/publications - on the same keeper path.
    /// The nested table engine is included because mixing Replicated and Shared nested engines on one
    /// shared tree is equally incoherent.
    ///
    /// Besides the ClickHouse-side settings, the fingerprint carries the remote source identity that
    /// getPublicationName()/getReplicationSlotName() derive the shared PostgreSQL objects from: the source
    /// database name and the source table name (empty for the database engine, so this also separates a
    /// single-table engine from a database engine). Without it, a coordinated single-table engine on
    /// `db.table` and a coordinated database engine with `materialized_postgresql_tables_list = 'table'`
    /// would pass the ClickHouse-side checks and the /table_set fence on one keeper path, yet still work
    /// against DIFFERENT PostgreSQL slots/publications (`db_table_*` vs `db_*`) - sharing /leader and
    /// /replicas bookkeeping without sharing the replicated object, so a drop of one setup would tear down
    /// or leak the other's PostgreSQL objects. The connection endpoint (host:port) is deliberately NOT
    /// part of the identity: replicas of one setup may legitimately reach the same PostgreSQL server
    /// through different addresses.
    String coordinatedNamingFingerprint(
        const MaterializedPostgreSQLSettings & settings, const String & postgres_database, const String & postgres_table)
    {
        const String schema = settings[MaterializedPostgreSQLSetting::materialized_postgresql_schema];
        const String schema_list = settings[MaterializedPostgreSQLSetting::materialized_postgresql_schema_list];
        const bool schema_as_a_part_of_table_name
            = !schema_list.empty() || settings[MaterializedPostgreSQLSetting::materialized_postgresql_tables_list_with_schema];
        return fmt::format(
            "table_engine: {}\nschema: {}\nschema_list: {}\nschema_as_a_part_of_table_name: {}\n"
            "postgres_database: {}\npostgres_table: {}\n",
            settings[MaterializedPostgreSQLSetting::materialized_postgresql_table_engine].value,
            schema,
            schema_list,
            schema_as_a_part_of_table_name,
            postgres_database,
            postgres_table);
    }

    /// The identity stored in this replica's <keeper_path>/replicas/<name> registration node. The registration
    /// is ownership-checked: `materialized_postgresql_replica_name` is documented to resolve to a distinct value
    /// on every replica, and without enforcement two replicas that resolve it to the same value would collapse
    /// onto one /replicas node - then one replica's unregistration (a failed join rollback, or a DROP) removes
    /// the other live replica's registration, and a later last-replica teardown removes the shared
    /// slot/publication/snapshot_completed marker around a replica that still holds data. The database (or
    /// single-table) UUID alone is not distinct enough - inside a Replicated database every replica of a
    /// single-table engine carries the same table UUID - and the server UUID alone would conflate two databases
    /// on one server, so use both. Both parts are stable across server restarts and DETACH/ATTACH.
    String coordinationReplicaOwnerId(const String & clickhouse_uuid)
    {
        return toString(ServerUUID::get()) + "|" + clickhouse_uuid;
    }

    /// The expanded `materialized_postgresql_replica_name` is used as a SINGLE Keeper node name under
    /// <keeper_path>/replicas (and as the replica name of the nested Replicated/SharedReplacingMergeTree
    /// tables), so it must be a valid single path component - the same requirement `DatabaseReplicated`
    /// enforces on its replica name.
    ///
    /// A value containing `/` (for example `'{shard}/{replica}'`) would silently turn the registration into a
    /// nested path `<keeper_path>/replicas/<shard>/<replica>`. `unregisterReplicaAndCheckLast` removes only the
    /// leaf node and then wins the last-replica fence by removing the now-empty `/replicas` parent, so with an
    /// intermediate level in between, `/replicas` would never become empty and the fence could never fire: the
    /// shared replication slot, publication and `snapshot_completed` marker would leak forever, even after the
    /// last replica is dropped. An empty name would make the registration collide with the `/replicas` node
    /// itself. Reject both before any Keeper path is formed.
    void assertValidCoordinationReplicaName(const String & expanded_replica_name)
    {
        if (expanded_replica_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "materialized_postgresql_replica_name must not be empty in coordinated mode: it names this "
                "replica's registration node under <keeper_path>/replicas");

        if (expanded_replica_name.contains('/'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Invalid materialized_postgresql_replica_name '{}': '/' is not allowed. The replica name is a "
                "single Keeper node name under <keeper_path>/replicas (and the replica name of the nested "
                "replicated tables), so a nested path would break the last-replica bookkeeping that decides "
                "when the shared replication slot and publication are removed",
                expanded_replica_name);
    }
}

void validateMaterializedPostgreSQLCoordinationSettings(
    const MaterializedPostgreSQLSettings & settings,
    ContextPtr context,
    const String & clickhouse_database_name,
    const UUID & clickhouse_uuid,
    const String & postgres_database,
    const String & postgres_table,
    bool allow_uuid_macro)
{
    const String engine = settings[MaterializedPostgreSQLSetting::materialized_postgresql_table_engine];
    const bool is_plain = engine == "ReplacingMergeTree";
    const bool is_replicated = engine == "ReplicatedReplacingMergeTree" || engine == "SharedReplacingMergeTree";

    if (!is_plain && !is_replicated)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unsupported value '{}' for setting materialized_postgresql_table_engine. Allowed values: "
            "ReplacingMergeTree, ReplicatedReplacingMergeTree, SharedReplacingMergeTree", engine);

    /// The nested tables are created with this engine, so it must actually be available in this build.
    /// `SharedReplacingMergeTree`, in particular, is a ClickHouse Cloud engine that is not registered in
    /// the open-source build: accepting it here would let `CREATE DATABASE` succeed and only fail much
    /// later, when `ensureNestedTablesExist` reaches `InterpreterCreateQuery`, leaving the database stuck
    /// in a background retry loop instead of rejecting the unsupported mode up front.
    if (!StorageFactory::instance().getAllStorages().contains(engine))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "materialized_postgresql_table_engine = '{}' is not available in this build. The nested tables "
            "are created with this engine, so it must be a registered table engine; otherwise the database "
            "would fail to create its nested tables and keep retrying forever instead of failing at CREATE time",
            engine);

    const bool coordination_enabled = !settings[MaterializedPostgreSQLSetting::materialized_postgresql_keeper_path].value.empty();

    if (is_replicated && !coordination_enabled)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "materialized_postgresql_table_engine = '{}' requires materialized_postgresql_keeper_path to be set. "
            "It enables single-active-worker coordination of the PostgreSQL replication slot across ClickHouse "
            "replicas, which is what makes a replicated/shared nested table engine safe to use", engine);

    if (coordination_enabled && !is_replicated)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "materialized_postgresql_keeper_path requires materialized_postgresql_table_engine to be "
            "ReplicatedReplacingMergeTree or SharedReplacingMergeTree. With a plain ReplacingMergeTree the standby "
            "replicas would hold no data (they receive it through ClickHouse replication of the nested tables), "
            "so a takeover would permanently lose every row replicated before the failover");

    /// Coordination needs Keeper/ZooKeeper for both the coordination nodes (leader election, replica
    /// registration, the snapshot-completion marker) and the nested Replicated/SharedReplacingMergeTree
    /// tables. The handler only reaches `getContext()->getZooKeeper()` in the background startup task, so
    /// without this up-front check a coordinated `CREATE` on a server with no Keeper configured would
    /// succeed and then sit in a permanent retry loop instead of failing synchronously. Reject it here.
    if (coordination_enabled && !context->hasZooKeeper())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "materialized_postgresql_keeper_path (replica coordination) requires Keeper/ZooKeeper to be "
            "configured on this server, but it is not. Coordination stores its leader-election, replica and "
            "snapshot-completion nodes in Keeper, and the nested tables are Replicated/SharedReplacingMergeTree, "
            "which also need it. Configure <zookeeper> (or <keeper_server>) or remove "
            "materialized_postgresql_keeper_path");

    /// The keeper path is both the coordination namespace (leader election, replica registration, snapshot
    /// marker) AND the root of the shared nested Replicated/SharedReplacingMergeTree tables. Every coordinated
    /// replica must therefore resolve it to the SAME path. A per-replica/per-server macro (such as {replica} or
    /// {server_uuid}) resolves to a different value on each replica/server, so they would end up in disjoint
    /// Keeper subtrees - each electing its own leader and creating its own nested tables - while still
    /// contending for the same shared PostgreSQL slot and publication. The advertised HA setup would then
    /// silently not share data, even though the loser never receives any. Reject such a path up front; the
    /// per-replica identity belongs in materialized_postgresql_replica_name, not in the shared path.
    ///
    /// Checking only the literal `{replica}` token is not enough: the path can reach a per-replica value through
    /// a config macro (e.g. `{coord_path}` where `<coord_path>` is `.../{replica}`), or use `{server_uuid}`. To
    /// catch every such case, expand the path twice with different injected values for the `replica`/`server_uuid`
    /// macros and reject if the two expansions differ. The injected map entries take precedence over both any
    /// config definition of those macros and the built-in special handling, and they propagate through any config
    /// macro that expands to them. The shared macros ({shard}, {database} and any other config macro that
    /// is identical on every replica) are held constant across both expansions, so they never trigger a false
    /// rejection.
    ///
    /// {uuid} is a separate case. It expands to the UUID of the database (or single table) being created, which
    /// every server generates independently unless the DDL carries the UUID with it - an ON CLUSTER / Replicated
    /// database query, or an explicit `UUID '...'` clause (`allow_uuid_macro`). Without that guarantee it behaves
    /// exactly like a per-server macro, so it gets the same probe and the same rejection. This matters even though
    /// such replicas never share /leader or the nested tree: the shared replication slot and publication names are
    /// derived from the PostgreSQL source, not from the keeper path, so the disjoint groups would still contend
    /// for the same slot, each believing it has its own active worker, and WAL could be lost.
    const String raw_keeper_path = settings[MaterializedPostgreSQLSetting::materialized_postgresql_keeper_path];
    String expanded_keeper_path;
    String expanded_replica_name;
    if (coordination_enabled)
    {
        const auto macros = context->getMacros();

        /// First expand both coordination settings strictly, exactly as the replication handler's constructor
        /// will (same macros, same database name and UUID, no table name): a misspelled or unsupported macro
        /// throws here, at CREATE time. Without this pass an invalid macro would only be caught later, in the
        /// background startup task that constructs the handler, leaving a mounted database stuck retrying
        /// instead of failing synchronously - exactly what this validator exists to prevent.
        {
            Macros::MacroExpansionInfo info;
            info.table_id.database_name = clickhouse_database_name;
            info.table_id.uuid = clickhouse_uuid;
            expanded_keeper_path = macros->expand(raw_keeper_path, info);
        }
        {
            const String raw_replica_name = settings[MaterializedPostgreSQLSetting::materialized_postgresql_replica_name];
            Macros::MacroExpansionInfo info;
            info.table_id.database_name = clickhouse_database_name;
            info.table_id.uuid = clickhouse_uuid;
            expanded_replica_name = macros->expand(raw_replica_name, info);
        }

        /// The expanded replica name must be a single Keeper path component before it is used to form
        /// <keeper_path>/replicas/<name> anywhere (see `assertValidCoordinationReplicaName`).
        assertValidCoordinationReplicaName(expanded_replica_name);

        auto expand_probe = [&](const String & per_replica_value, bool vary_uuid) -> String
        {
            Macros::MacroMap macro_map = macros->getMacroMap();
            macro_map["replica"] = per_replica_value;
            macro_map["server_uuid"] = per_replica_value;
            if (vary_uuid)
                macro_map["uuid"] = per_replica_value;
            Macros probing_macros(macro_map);

            /// The strict pass above has already rejected unknown macros, so ignoring them here only
            /// keeps the probe itself from ever throwing.
            Macros::MacroExpansionInfo info;
            info.ignore_unknown = true;
            info.table_id.database_name = clickhouse_database_name;
            info.table_id.uuid = clickhouse_uuid;
            return probing_macros.expand(raw_keeper_path, info);
        };

        if (expand_probe("__mpg_probe_a__", /* vary_uuid */ false) != expand_probe("__mpg_probe_b__", /* vary_uuid */ false))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "materialized_postgresql_keeper_path must resolve to the same path on every coordinated replica, "
                "so it cannot depend on a per-replica or per-server macro such as {{replica}} or {{server_uuid}} "
                "(directly, or through a config macro that expands to one): it would place each replica on a "
                "disjoint Keeper subtree, breaking data sharing (the loser never receives data through ClickHouse "
                "replication) while the replicas still contend for the same PostgreSQL slot and publication. Put "
                "the per-replica part in materialized_postgresql_replica_name instead");

        if (!allow_uuid_macro
            && expand_probe("__mpg_probe_a__", /* vary_uuid */ true) != expand_probe("__mpg_probe_b__", /* vary_uuid */ true))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "materialized_postgresql_keeper_path must resolve to the same path on every coordinated replica, "
                "so it cannot depend on the {{uuid}} macro here: this CREATE generates its own UUID, and every "
                "other replica would generate a different one, placing each of them on a disjoint Keeper subtree "
                "while they still contend for the same PostgreSQL replication slot and publication (their names "
                "are derived from the PostgreSQL source, not from the keeper path), so each replica would believe "
                "it is the only active worker and WAL could be lost. {{uuid}} is only accepted when the UUID is "
                "guaranteed to be identical on every replica: an ON CLUSTER query, a table inside a Replicated "
                "database, or an explicit UUID '...' clause in the CREATE query");
    }

    if (coordination_enabled && settings[MaterializedPostgreSQLSetting::materialized_postgresql_use_unique_replication_consumer_identifier])
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "materialized_postgresql_keeper_path (replica coordination) cannot be combined with "
            "materialized_postgresql_use_unique_replication_consumer_identifier: coordination requires a single "
            "shared replication slot, but the unique consumer identifier gives every replica its own slot");

    if (coordination_enabled && !settings[MaterializedPostgreSQLSetting::materialized_postgresql_replication_slot].value.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "materialized_postgresql_keeper_path (replica coordination) cannot be combined with a user-managed "
            "materialized_postgresql_replication_slot. Coordination owns the shared slot: if the active worker "
            "dies before the initial snapshot completes, the next leader must drop and recreate the slot to obtain "
            "a fresh exported snapshot, which is impossible for a slot it does not manage. Leave the slot unset so "
            "coordination can create and own it");

    if (coordination_enabled && !settings[MaterializedPostgreSQLSetting::materialized_postgresql_snapshot].value.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "materialized_postgresql_keeper_path (replica coordination) cannot be combined with a user-provided "
            "materialized_postgresql_snapshot. Coordination re-exports a fresh snapshot when it (re)creates the "
            "shared slot, so a fixed snapshot token would become stale and a mid-snapshot takeover could never "
            "recover. Leave the snapshot unset so coordination can manage it");

    /// In coordinated mode the shared publication's table set is authoritative and is adopted by every replica,
    /// but the per-table column projection (`table(col1, col2)`) is still taken from this replica's local
    /// `materialized_postgresql_tables_list`. All coordinated replicas share one set of nested
    /// Replicated/SharedReplacingMergeTree tables on the same Keeper path, so they must agree on the exact
    /// column set. If two replicas were created with different column filters (or one with a filter and one
    /// without) they would try to create diverging schemas on the same shared path, breaking the shared-state
    /// contract. Reject column-filtered lists so every replica builds the identical shared schema; a column
    /// projection is denoted by a `(` after a table name in the setting value (the same syntax parsed by
    /// `getTableAllowedColumns`).
    const String coordinated_tables_list = settings[MaterializedPostgreSQLSetting::materialized_postgresql_tables_list];
    if (coordination_enabled && coordinated_tables_list.find('(') != String::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "materialized_postgresql_keeper_path (replica coordination) cannot be combined with a column-filtered "
            "materialized_postgresql_tables_list (e.g. `table(col1, col2)`). Coordinated replicas share one set of "
            "nested tables on the same Keeper path, so they must agree on the exact column projection, but the "
            "per-table column list is taken from each replica's local setting rather than from the shared "
            "publication. List the tables without column filters so every replica builds the identical shared schema");

    /// If the coordinated setup already exists in Keeper, check this replica's naming-affecting settings
    /// against the ones it published (see `ensureCoordinatedNamingCompatible` for the startup-time
    /// enforcement and the reasoning), so a replica that would derive different ClickHouse table names
    /// from the shared publication is rejected synchronously at CREATE time rather than from the
    /// background startup task. Keeper errors propagate and fail the CREATE: coordination cannot work
    /// without Keeper anyway, and joining an existing setup on an unverified guess must not happen.
    if (coordination_enabled)
    {
        const String local_fingerprint = coordinatedNamingFingerprint(settings, postgres_database, postgres_table);
        String published_fingerprint;
        if (context->getZooKeeper()->tryGet(expanded_keeper_path + "/naming", published_fingerprint)
            && published_fingerprint != local_fingerprint)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "A coordinated MaterializedPostgreSQL setup already exists at Keeper path '{}', and its "
                "naming-affecting settings or source identity differ from the ones of this CREATE query. All "
                "replicas of one coordinated setup must agree on materialized_postgresql_table_engine, "
                "materialized_postgresql_schema, materialized_postgresql_schema_list and "
                "materialized_postgresql_tables_list_with_schema, and must replicate the same PostgreSQL "
                "source (the same source database and, for the single-table engine, the same source table): "
                "these determine how the ClickHouse names of the shared nested tables (and the names of the "
                "shared replication slot and publication) are derived, so a disagreeing replica would share "
                "the coordination bookkeeping without sharing the replicated data. Existing setup:\n{}\nThis "
                "query:\n{}\n(If the existing setup was dropped incompletely, remove the leftover Keeper "
                "path manually.)",
                expanded_keeper_path, published_fingerprint, local_fingerprint);

        /// Likewise reject a replica name that is already registered by another replica synchronously at
        /// CREATE time. The authoritative, ownership-checked enforcement lives in `registerReplicaInKeeper`
        /// (the registration node stores the owning replica's identity), but that runs in the background
        /// startup task, which would leave a mounted database stuck retrying; failing the CREATE up front is
        /// the better place for a misconfiguration that the user must fix anyway.
        String registered_owner;
        if (context->getZooKeeper()->tryGet(expanded_keeper_path + "/replicas/" + expanded_replica_name, registered_owner)
            && registered_owner != coordinationReplicaOwnerId(toString(clickhouse_uuid)))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "A replica named '{}' is already registered in the coordinated MaterializedPostgreSQL setup at "
                "Keeper path '{}'. materialized_postgresql_replica_name must resolve to a distinct value on "
                "every replica: replicas are tracked under <keeper_path>/replicas/<name>, so two replicas "
                "sharing one name would corrupt the shared bookkeeping that decides when the last replica "
                "removes the shared replication slot and publication. Use a distinct value (for example the "
                "{{replica}} macro). If this registration is a leftover of an incompletely dropped setup, "
                "remove the Keeper node manually",
                expanded_replica_name, expanded_keeper_path);

        /// Reject a CREATE while a last-replica drop is still tearing the setup on this keeper path down (its
        /// <keeper_path>/teardown ownership token is still in place): the pending teardown drops the shared
        /// PostgreSQL slot/publication by name, so a fresh setup built in that window would have its objects
        /// deleted from under it. The startup-time fence in `ensureCoordinatedNamingCompatible` enforces this
        /// authoritatively (and keeps retrying); failing the CREATE synchronously is the better UX for the
        /// common case, where the teardown finishes within moments and a retried CREATE succeeds.
        String teardown_owner;
        if (context->getZooKeeper()->tryGet(expanded_keeper_path + "/teardown", teardown_owner)
            && teardown_owner != coordinationReplicaOwnerId(toString(clickhouse_uuid)))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The coordinated MaterializedPostgreSQL setup at Keeper path '{}' is still being torn down by "
                "the drop of its last replica. Retry the CREATE once the teardown has finished. If the "
                "tearing-down server died before completing it, remove the leftover node '{}/teardown' "
                "manually after dropping the leftover replication slot and publication in PostgreSQL",
                expanded_keeper_path, expanded_keeper_path);
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

    LOG_INFO(log, "Using replication slot {} and publication {}", replication_slot, doubleQuoteString(publication_name));

    nested_engine_name = replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_table_engine];
    coordination_enabled = !replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_keeper_path].value.empty();

    if (coordination_enabled)
    {
        if (!getContext()->hasZooKeeper())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "materialized_postgresql_keeper_path is set, but no ZooKeeper/Keeper is configured for this server");

        /// Resolve the {uuid}/{shard}/{replica} macros in the coordination path and replica name once.
        /// {uuid} resolves to the ClickHouse database (or single-table) UUID passed to the handler.
        StorageID macro_table_id = StorageID::createEmpty();
        macro_table_id.database_name = current_database_name;
        macro_table_id.uuid = parse<UUID>(clickhouse_uuid_);

        const String raw_keeper_path = replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_keeper_path];
        const String raw_replica_name = replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_replica_name];
        const auto macros = getContext()->getMacros();
        /// A macro these settings expand through can disappear from the server configuration after the engine
        /// was created. Do not throw from the constructor in that case: `beforeDropDatabase` builds a handler
        /// purely to run the coordinated teardown, so a throwing constructor would make a misconfigured setup
        /// undroppable. Record the failure instead - the startup paths refuse to proceed with an unresolved
        /// identity, and the teardown recovers the real identity from the nested-table metadata.
        try
        {
            {
                Macros::MacroExpansionInfo info;
                info.table_id = macro_table_id;
                coordination_keeper_path = macros->expand(raw_keeper_path, info);
            }
            {
                Macros::MacroExpansionInfo info;
                info.table_id = macro_table_id;
                coordination_replica_name = macros->expand(raw_replica_name, info);
            }
        }
        catch (...)
        {
            coordination_identity_error = getCurrentExceptionMessage(/* with_stacktrace */ false);
            coordination_keeper_path.clear();
            coordination_replica_name.clear();
            LOG_ERROR(log, "Cannot resolve the coordination identity from the current configuration: {}",
                      coordination_identity_error);
        }

        coordination_naming_fingerprint = coordinatedNamingFingerprint(replication_settings, postgres_database_, postgres_table_);
        coordination_replica_owner = coordinationReplicaOwnerId(clickhouse_uuid_);

        LOG_INFO(log, "Replica coordination enabled: keeper path '{}', replica name '{}', nested table engine '{}'",
                 coordination_keeper_path, coordination_replica_name, nested_engine_name);
    }

    startup_task = getContext()->getSchedulePool()->createTask(StorageID::createEmpty(), "PostgreSQLReplicaStartup", [this]{ checkConnectionAndStart(); });
    consumer_task = getContext()->getSchedulePool()->createTask(StorageID::createEmpty(), "PostgreSQLReplicaConsume", [this]{ consumerFunc(); });
    cleanup_task = getContext()->getSchedulePool()->createTask(StorageID::createEmpty(), "PostgreSQLReplicaCleanup", [this]{ cleanupFunc(); });
    if (coordination_enabled)
        coordination_task = getContext()->getSchedulePool()->createTask(StorageID::createEmpty(), "PostgreSQLReplicaCoordination", [this]{ coordinationFunc(); });
}


PostgreSQLReplicationHandler::~PostgreSQLReplicationHandler()
{
    /// Stop the background tasks before the members they use are destroyed. The task holders are declared
    /// before those members, so their own destructors (which deactivate the tasks) would run too late: a task
    /// still executing at that point would read, for example, an already-destroyed `materialized_storages`.
    /// `deactivate` waits for an in-flight execution to finish and is idempotent, so this is safe for a handler
    /// that was already shut down.
    try
    {
        startup_task->deactivate();
        consumer_task->deactivate();
        cleanup_task->deactivate();
        if (coordination_task)
            coordination_task->deactivate();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}


void PostgreSQLReplicationHandler::addStorage(const std::string & table_name, StorageMaterializedPostgreSQL * storage)
{
    materialized_storages[table_name] = storage;
}


void PostgreSQLReplicationHandler::startup(bool delayed)
{
    if (coordination_enabled)
    {
        /// Every replica creates the nested tables (as replicas of the shared replicated tree) so reads
        /// work everywhere and a standby can take over without reloading. Only the elected active worker
        /// consumes the slot and loads the initial snapshot; that is driven by `coordination_task`.
        if (delayed)
        {
            startup_task->activateAndSchedule();
        }
        else
        {
            registerReplicaThenEnsureNestedTables();
            coordination_task->activateAndSchedule();
        }
        return;
    }

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
        if (coordination_enabled)
        {
            /// This path retries on error (see below), and every step is idempotent.
            registerReplicaThenEnsureNestedTables();
            coordination_task->activateAndSchedule();
        }
        else
        {
            startSynchronization(is_attach);
        }
    }
    catch (const pqxx::broken_connection & pqxx_error)
    {
        tryLogCurrentException(log);

        if (!is_attach && !retry_startup_on_error)
            throw;

        LOG_ERROR(log, "Unable to set up connection. Reconnection attempt will continue. Error message: {}", pqxx_error.what());
        startup_task->scheduleAfter(milliseconds_to_wait);
    }
    catch (const Exception & e)
    {
        tryLogCurrentException(log);

        if (!is_attach && !retry_startup_on_error)
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

        if (!is_attach && !retry_startup_on_error)
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
    /// Releasing `leader_node` below issues a Keeper remove request from this thread.
    auto component_guard = Coordination::setCurrentComponent("PostgreSQLReplicationHandler::shutdown");

    stop_synchronization.store(true);

    LOG_TRACE(log, "Deactivating startup task");
    startup_task->deactivate();

    /// Deactivate coordination before touching the leader node, so `coordinationFunc` is not mid-mutation.
    if (coordination_task)
    {
        LOG_TRACE(log, "Deactivating coordination task");
        coordination_task->deactivate();
    }

    LOG_TRACE(log, "Deactivating consumer task");
    consumer_task->deactivate();

    LOG_TRACE(log, "Deactivating cleanup task");
    cleanup_task->deactivate();

    LOG_TRACE(log, "Resetting consumer");
    {
        std::lock_guard lock(consumer_ptr_mutex);
        consumer.reset(); /// Clear shared pointers to inner storages.
    }

    /// Release the ephemeral leader node so a peer can take over promptly (rather than waiting for the
    /// Keeper session to expire). Reset the node before its backing session.
    is_active_worker.store(false);
    leader_node.reset();
    coordination_zookeeper.reset();
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
        pqxx::result result{tx.exec(fmt::format("SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}'", name))};
        return !result.empty();
    };
    auto publication_exists = [&](const String & name)
    {
        pqxx::result result{tx.exec(fmt::format("SELECT 1 FROM pg_publication WHERE pubname = '{}'", name))};
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
            "SELECT DISTINCT schemaname FROM pg_publication_tables WHERE pubname = '{}'", legacy_publication_name))};
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


void PostgreSQLReplicationHandler::startSynchronization(bool throw_on_error)
{
    /// In coordinated mode this function reads and writes the snapshot-completion marker in Keeper, and
    /// loading the snapshot inserts into Replicated tables, which also issues Keeper requests.
    auto component_guard = Coordination::setCurrentComponent("PostgreSQLReplicationHandler::startSynchronization");

    postgres::Connection replication_connection(connection_info, /* replication */true);
    pqxx::nontransaction tx(replication_connection.getRef());
    adoptLegacyReplicationIdentityIfNeeded(tx);
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
            /// The slot is shared state: a deposed worker recreating it here would race the successor
            /// that may already have created its own replacement slot (see the redo-the-snapshot branch
            /// below, which drops the slot under the same fence).
            assertReplicationLeadershipIsLive();
            createReplicationSlot(tx, start_lsn, snapshot_name);
        }

        for (const auto & [table_name, storage] : materialized_storages)
        {
            /// Abort as soon as leadership is lost: once the Keeper session backing /leader expires,
            /// another replica may already have won the election, truncated the shared nested tables and
            /// started a replacement snapshot, and this worker's inserts would interleave with it.
            assertReplicationLeadershipIsLive();

            try
            {
                nested_storages.emplace(table_name, loadFromSnapshot(*tmp_connection, snapshot_name, table_name, storage->as<StorageMaterializedPostgreSQL>()));
            }
            catch (Exception & e)
            {
                e.addMessage("while loading table `{}`.`{}`", postgres_database, table_name);
                tryLogCurrentException(log);

                /// Without coordination the database engine tolerates a per-table failure: the remaining
                /// tables still get a consumer and the failed one can be repaired later with `ATTACH TABLE`.
                /// The single-table engine has exactly one table, so when its snapshot failed there is
                /// nothing useful a consumer could do, and `throw_on_error` decides between failing the
                /// query and the startup-task retry.
                ///
                /// In coordinated mode any snapshot failure must abort the whole attempt for both engines:
                /// proceeding would construct a consumer that marks the missing tables as skipped while
                /// still advancing the shared slot's `confirmed_flush_lsn` on every commit, silently
                /// discarding their WAL - and `ATTACH TABLE`, the usual repair path, is rejected in
                /// coordinated mode, so nothing could heal the subset short of an unrelated failover. The
                /// abort lets `coordinationFunc` (which drives the retry, with `throw_on_error` false)
                /// release the leadership, so a healthy peer can redo the full snapshot before any WAL is
                /// advanced past unapplied rows.
                if (coordination_enabled || (throw_on_error && !is_materialized_postgresql_database))
                    throw;
            }
        }

        /// Only after every table's snapshot data is durably inserted may a future active worker resume
        /// from the slot's confirmed LSN instead of redoing the snapshot (see isInitialSnapshotCompleted).
        if (coordination_enabled)
        {
            /// Holds the worker between loading the snapshot and publishing the marker, so a test can
            /// expire its Keeper session here and verify that a worker that lost its leadership
            /// mid-snapshot cannot publish a stale marker over a successor's replacement snapshot.
            fiu_do_on(FailPoints::materialized_postgresql_pause_before_marking_snapshot_completed,
            {
                LOG_INFO(log, "Pausing before marking the initial snapshot as completed until failpoint "
                         "materialized_postgresql_pause_before_marking_snapshot_completed is disabled");
                FailPointInjection::pauseFailPoint(FailPoints::materialized_postgresql_pause_before_marking_snapshot_completed);
            });

            /// Reaching this point means every table's snapshot loaded: in coordinated mode any per-table
            /// failure above aborts the whole attempt.
            markInitialSnapshotCompleted(start_lsn);
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
    /// When coordination is enabled the slot is shared across replicas and an existing slot means another
    /// replica (or this one before a restart/handover) already created it; a new active worker must resume
    /// from `confirmed_flush_lsn` instead of dropping the slot and reloading - but only when the durable
    /// snapshot-completion marker confirms the initial snapshot actually finished (see below).
    else if (!is_attach && !coordination_enabled)
    {
        if (!user_managed_slot)
            dropReplicationSlot(tx);

        initial_sync();
        LOG_DEBUG(log, "Loaded {} tables", nested_storages.size());
    }
    /// The slot exists, but no active worker ever finished the initial snapshot: the previous one died
    /// mid-snapshot. WAL replay from the slot only covers changes after the slot-creation LSN, so resuming
    /// would permanently lose the rows that were never copied. Redo the snapshot from scratch.
    ///
    /// The nested tables must be cleared first. Rows the dead worker already copied are still there, and
    /// re-inserting the fresh snapshot on top does not repair every case: a row that the dead worker copied
    /// and that PostgreSQL has since DELETEd has no counterpart in the new snapshot, so nothing overrides the
    /// stale copy (a ReplacingMergeTree collapses duplicate keys by `_version` but never turns a now-absent
    /// row into a tombstone). Truncating makes the redo start from an empty table so the reloaded snapshot is
    /// the exact current PostgreSQL state. The nested tables are Replicated/SharedReplacingMergeTree, so the
    /// truncate propagates to every replica.
    else if (coordination_enabled && !isInitialSnapshotCompleted())
    {
        LOG_WARNING(log,
            "Replication slot {} exists, but the initial snapshot is not marked as completed. "
            "Assuming the previous active worker died before finishing it; clearing the nested tables and "
            "reloading all of them from a new snapshot",
            replication_slot);

        /// Holds the worker between entering the mid-snapshot recovery branch and mutating any shared
        /// state, so a test can expire its Keeper session here and verify that a deposed worker aborts
        /// at the leadership fence below instead of truncating the tables its successor is reloading
        /// and dropping the slot the successor just created.
        fiu_do_on(FailPoints::materialized_postgresql_pause_before_redo_snapshot_truncate,
        {
            LOG_INFO(log, "Pausing before redoing the initial snapshot until failpoint "
                     "materialized_postgresql_pause_before_redo_snapshot_truncate is disabled");
            FailPointInjection::pauseFailPoint(FailPoints::materialized_postgresql_pause_before_redo_snapshot_truncate);
        });

        /// This whole recovery branch mutates shared state - the replicated nested tables and the shared
        /// slot - so it is fenced on the live leadership session exactly like the snapshot load and the
        /// marker write. If this worker's Keeper session expired after it entered this branch, a successor
        /// may already have won /leader and be running the replacement snapshot: truncating now would wipe
        /// the tables the successor has already reloaded, and dropping the slot would discard the slot it
        /// just created. `truncateNestedTables` re-checks the fence per table, and it is re-checked here
        /// once more before the slot is touched (the truncate can take a while on large tables).
        assertReplicationLeadershipIsLive();

        truncateNestedTables();

        assertReplicationLeadershipIsLive();

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

    /// A worker that lost its leadership session while setting up - most importantly while loading the
    /// initial snapshot - must not start a consumer: the new active worker owns the shared slot now, and a
    /// stale consumer could grab it first and starve the legitimate one. PostgreSQL's single-active-session
    /// rule on the slot is only a backstop against concurrent consumption, not against the wrong consumer.
    assertReplicationLeadershipIsLive();

    /// Pass current connection to consumer. It is not std::moved implicitly, but a shared_ptr is passed.
    /// Consumer and replication handler are always executed one after another (not concurrently) and share the same connection.
    /// (Apart from the case, when shutdownFinal is called).
    /// Handler uses it only for loadFromSnapshot and shutdown methods.
    {
        std::lock_guard lock(consumer_ptr_mutex);
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
    }

    replication_handler_initialized = true;

    consumer_task->activateAndSchedule();
    cleanup_task->activateAndSchedule();

    /// Do not rely anymore on saved storage pointers.
    /// Exception: when coordination is enabled this handler may be re-elected as the active worker after a
    /// handover and has to rebuild the consumer from these pointers, so keep them.
    if (!coordination_enabled)
        materialized_storages.clear();
}


ASTPtr PostgreSQLReplicationHandler::getCreateNestedTableQuery(StorageMaterializedPostgreSQL * storage, const String & table_name)
{
    postgres::Connection connection(connection_info);
    pqxx::nontransaction tx(connection.getRef());

    auto table_structure = fetchTableStructure(tx, table_name);
    auto table_override = tryGetTableOverride(current_database_name, table_name);
    return storage->getCreateNestedTableQuery(makeNestedEngineSpec(table_name), std::move(table_structure), table_override ? table_override->as<ASTTableOverride>() : nullptr);
}


StorageInfo PostgreSQLReplicationHandler::loadFromSnapshot(postgres::Connection & connection, String & snapshot_name, const String & table_name,
                                                          StorageMaterializedPostgreSQL * materialized_storage)
{
    fiu_do_on(FailPoints::materialized_postgresql_fail_load_from_snapshot,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED,
            "Injected failure while loading table `{}` from the initial snapshot", table_name);
    });

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
    materialized_storage->createNestedIfNeeded(makeNestedEngineSpec(table_name), std::move(table_structure), table_override ? table_override->as<ASTTableOverride>() : nullptr);
    auto nested_storage = materialized_storage->getNested();

    auto insert = make_intrusive<ASTInsertQuery>();
    insert->table_id = nested_storage->getStorageID();

    auto insert_context = Context::createCopy(materialized_storage->getNestedTableContext());
    /// The snapshot is a bulk reload, not a user INSERT: it must never be silently thrown away by the
    /// Replicated insert deduplication. The nested replicated table may carry block-deduplication hashes
    /// from a previous load of the same data - a previous incarnation of the shared table whose Keeper tree
    /// is still being removed by a background drop, or the pre-truncate parts of the mid-snapshot failover
    /// recovery (TRUNCATE removes the parts, but the block hashes in Keeper are cleaned up lazily). With
    /// deduplication on, re-inserting identical blocks would be ignored, leaving the nested table silently
    /// empty (or partial) while the snapshot is marked as completed.
    insert_context->setSetting("insert_deduplicate", false);

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
    /// In coordinated mode consuming inserts into Replicated nested tables, which issues Keeper requests.
    auto component_guard = Coordination::setCurrentComponent("PostgreSQLReplicationHandler::consumerFunc");

    assertInitialized();

    /// When coordination is enabled, only the active worker may consume (peek/advance) the shared slot.
    /// If we are no longer the active worker, go dormant without rescheduling; `coordination_task` owns
    /// tearing down the consumer and will re-arm this task if we become the active worker again. This
    /// must happen before consume() so a demoted worker never advances the slot's confirmed LSN.
    if (coordination_enabled && !isLeader())
    {
        LOG_DEBUG(log, "Not the active worker anymore, pausing consumption");
        return;
    }

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


bool PostgreSQLReplicationHandler::isLeader() const
{
    return is_active_worker.load();
}


NestedTableEngineSpec PostgreSQLReplicationHandler::makeNestedEngineSpec(const String & table_name) const
{
    NestedTableEngineSpec spec;
    if (!coordination_enabled)
        return spec; /// default: plain ReplacingMergeTree

    spec.engine_name = nested_engine_name;
    spec.replicated = nested_engine_name != "ReplacingMergeTree";
    spec.replica_name = coordination_replica_name;
    /// A deterministic, node-identical per-table path, so each replica's nested table joins the same
    /// replicated tree. Table names may contain a schema-qualifying dot, so escape them for the path.
    spec.zookeeper_path = coordination_keeper_path + "/tables/" + escapeForFileName(table_name);
    return spec;
}


void PostgreSQLReplicationHandler::ensureNestedTablesExist()
{
    /// Create the nested tables on this replica without loading a snapshot. When coordination is enabled
    /// they are replicas of a shared replicated tree, so the data (including the initial snapshot that the
    /// active worker loads) propagates to this replica through ClickHouse replication.
    /// Creating a Replicated table issues Keeper requests.
    auto component_guard = Coordination::setCurrentComponent("PostgreSQLReplicationHandler::ensureNestedTablesExist");

    postgres::Connection connection(connection_info);
    pqxx::nontransaction tx(connection.getRef());

    for (const auto & [table_name, materialized_storage] : materialized_storages)
    {
        /// A refused coordinated DROP leaves this replica's local nested table shut down but still in the
        /// catalog: `InterpreterDropQuery` shuts the nested (Replicated/Shared)ReplacingMergeTree down before
        /// it removes it, so if the removal is then refused (e.g. the last-replica teardown already ran and the
        /// nested-table drop itself failed) the shut-down replica survives. A shut-down ReplicatedMergeTree
        /// cannot be restarted in place - it stays permanently read-only - so the retrying startup that recovers
        /// such a drop must drop the dead local copy and recreate it below, or it would loop forever (e.g.
        /// `truncateNestedTables` during a redo-snapshot fails with TABLE_IS_READ_ONLY). The data on disk and
        /// the shared Keeper tree are preserved; recreating rejoins the shared tree. This is gated on
        /// `isShutdownCalled` (not merely read-only) so a nested table that is only transiently read-only while
        /// starting up is never dropped. If Keeper is unreachable the drop throws and the startup task retries.
        bool nested_is_shut_down = false;
        {
            /// The StoragePtr must not outlive this scope: the synchronous drop below waits until the dropped
            /// table is not referenced anywhere, so holding the reference across the drop would deadlock it.
            auto nested = materialized_storage->tryGetNested();
            const auto * replicated = nested ? nested->as<StorageReplicatedMergeTree>() : nullptr;
            nested_is_shut_down = replicated && replicated->isShutdownCalled();
        }

        if (nested_is_shut_down)
        {
            LOG_WARNING(log,
                "The local nested table for `{}`.`{}` was shut down (most likely by a refused coordinated "
                "drop) and cannot be restarted in place; dropping and recreating it so this replica rejoins "
                "the shared replicated tree",
                postgres_database, table_name);

            /// Use the wrapper's internal nested-table context: for the database engine the nested table
            /// lives inside the coordinated DatabaseMaterializedPostgreSQL, whose `dropTable` refuses a
            /// non-internal (user) DROP; an internal context bypasses that guard exactly as the DROP DATABASE
            /// nested-drop loop does (`executeDropQuery` propagates the internal-query flag).
            InterpreterDropQuery::executeDropQuery(
                ASTDropQuery::Kind::Drop, getContext(), materialized_storage->getNestedTableContext(),
                materialized_storage->getNestedStorageID(), /* sync */ true, /* ignore_sync_setting */ true);
            materialized_storage->resetNested();
        }

        if (!materialized_storage->tryGetNested())
        {
            /// The single-table engine derives the nested structure from its own declared metadata, so it
            /// does not need the PostgreSQL structure here; the database engine does.
            PostgreSQLTableStructurePtr table_structure;
            if (is_materialized_postgresql_database)
                table_structure = fetchTableStructure(tx, table_name);

            auto table_override = tryGetTableOverride(current_database_name, table_name);
            const auto engine_spec = makeNestedEngineSpec(table_name);

            /// In coordinated mode all replicas share one set of nested Replicated/SharedReplacingMergeTree
            /// tables on the same Keeper path. The schema of that shared tree is established by whichever
            /// replica created it first and is authoritative: ReplicatedMergeTree compares any joining
            /// replica's declared structure against the metadata already stored in Keeper and refuses the join
            /// on a mismatch. This replica derives its structure from the *current* PostgreSQL schema, so if
            /// the PostgreSQL table was altered or renamed after the shared tree was created (MaterializedPostgreSQL
            /// continues by column position and does not track PostgreSQL DDL), the locally derived structure
            /// would differ and the join would fail. Detect up front that this is a join into an already
            /// existing shared tree, so the failure can be reported as an actionable schema-drift error instead
            /// of a cryptic metadata mismatch that the startup task then retries indefinitely.
            /// Not applicable when this replica has just dropped its own shut-down copy above: the shared
            /// tree's Keeper path may then still exist only because its removal (triggered by dropping the
            /// last replica) is in progress, and the create fails with a transient "dropped right now"
            /// (ALL_REPLICAS_LOST) error that the startup task resolves by retrying - reporting it as schema
            /// drift would be wrong and misleading.
            bool joining_existing_shared_tree = false;
            if (coordination_enabled && engine_spec.replicated && !nested_is_shut_down)
            {
                try
                {
                    joining_existing_shared_tree = getContext()->getZooKeeper()->exists(engine_spec.zookeeper_path);
                }
                catch (...)
                {
                    /// A transient Keeper error here just means we fall back to the plain create path (which
                    /// needs Keeper too and will surface the real error and retry on its own).
                    LOG_TRACE(log, "Could not check whether the shared nested tree already exists: {}",
                              getCurrentExceptionMessage(false));
                }
            }

            try
            {
                materialized_storage->createNestedIfNeeded(
                    engine_spec,
                    std::move(table_structure),
                    table_override ? table_override->as<ASTTableOverride>() : nullptr);
            }
            catch (Exception & e)
            {
                if (joining_existing_shared_tree)
                    e.addMessage(
                        "This coordinated MaterializedPostgreSQL replica could not join the shared nested table '{}' on "
                        "Keeper path '{}'. The shared nested-table schema is authoritative, but this replica's structure, "
                        "derived from the current PostgreSQL table, does not match it - the PostgreSQL table was most "
                        "likely altered or renamed after the coordinated database was first created (MaterializedPostgreSQL "
                        "continues by column position and does not track PostgreSQL DDL). Reconcile the PostgreSQL schema "
                        "with the shared one, or drop and recreate the coordinated database on a fresh keeper path",
                        table_name, engine_spec.zookeeper_path);
                throw;
            }
        }

        /// Mark the nested table as available so the wrapper becomes queryable on this replica (the
        /// database engine only exposes a table once its nested table exists). Data arrives through
        /// ClickHouse replication of the shared replicated tree, even on replicas that never consume.
        if (auto nested = materialized_storage->tryGetNested(); nested && !materialized_storage->hasNested())
            materialized_storage->set(nested);
    }
}


void PostgreSQLReplicationHandler::truncateNestedTables()
{
    /// Only reached in coordinated mode, from the mid-snapshot recovery branch of `startSynchronization`.
    /// The nested tables were created by `ensureNestedTablesExist` and are Replicated/SharedReplacingMergeTree,
    /// so truncating them here clears the shared tree on every replica. Only the single active worker may run
    /// this, which the per-table fence below enforces.
    auto component_guard = Coordination::setCurrentComponent("PostgreSQLReplicationHandler::truncateNestedTables");

    for (const auto & [table_name, materialized_storage] : materialized_storages)
    {
        /// Abort as soon as leadership is lost, mirroring the per-table fence of the snapshot load: a
        /// deposed worker must not keep truncating tables its successor may already be reloading.
        assertReplicationLeadershipIsLive();

        auto nested = materialized_storage->tryGetNested();
        if (!nested)
            continue;

        /// A dedicated internal query context, exactly like the snapshot INSERT and the consumer use for
        /// operations against the nested tables.
        auto truncate_context = Context::createCopy(getContext());
        truncate_context->makeQueryContext();
        truncate_context->setInternalQuery(true);

        /// Empty lock: the nested tables are (Replicated/Shared)ReplacingMergeTree, i.e. MergeTreeData, which
        /// InterpreterDropQuery also truncates without an exclusive table lock.
        TableExclusiveLockHolder table_lock;
        auto metadata_snapshot = nested->getInMemoryMetadataPtr(truncate_context, false);
        nested->truncate(nullptr, metadata_snapshot, truncate_context, table_lock);

        LOG_DEBUG(log, "Truncated nested table for `{}`.`{}` before redoing the initial snapshot",
                  postgres_database, table_name);
    }
}


void PostgreSQLReplicationHandler::coordinationFunc()
{
    /// This task talks to Keeper directly (leader election, session management).
    auto component_guard = Coordination::setCurrentComponent("PostgreSQLReplicationHandler::coordinationFunc");

    if (stop_synchronization)
        return;

    static constexpr UInt64 healthy_poll_ms = 10000;
    static constexpr UInt64 retry_poll_ms = 5000;
    UInt64 reschedule_ms = healthy_poll_ms;

    try
    {
        /// If the Keeper session backing our leadership expired, we are no longer the active worker. Stop
        /// consuming and drop the (dead) session before trying to re-acquire leadership. The consumer must
        /// be stopped so its PostgreSQL replication connection is closed, letting the new active worker
        /// connect to the shared slot.
        if (coordination_zookeeper && coordination_zookeeper->expired())
        {
            LOG_WARNING(log, "Keeper session expired, releasing replication leadership");
            is_active_worker.store(false);
            consumer_task->deactivate(); /// blocks until the in-flight consume() iteration finishes
            {
                std::lock_guard lock(consumer_ptr_mutex);
                consumer.reset();
            }
            leader_node.reset();
            coordination_zookeeper.reset();
        }

        if (leader_node)
        {
            /// Still the active worker. Make sure the consumer is running (retry if a previous
            /// startSynchronization attempt failed).
            if (!consumer)
                startSynchronization(/* throw_on_error */ false);
        }
        else
        {
            /// Try to become the active worker by creating the ephemeral leader node. Only its holder
            /// consumes the shared slot; peers stay on standby and take over when it disappears.
            auto zookeeper = getContext()->getZooKeeper();
            const String leader_path = coordination_keeper_path + "/leader";
            zookeeper->createAncestors(leader_path);
            leader_node = zkutil::EphemeralNodeHolder::tryCreate(leader_path, *zookeeper, coordination_replica_name);

            if (leader_node)
            {
                /// Keep the session that the ephemeral node references alive for as long as the node.
                coordination_zookeeper = zookeeper;
                is_active_worker.store(true);
                LOG_INFO(log, "Acquired replication leadership as '{}' at {}", coordination_replica_name, leader_path);
                startSynchronization(/* throw_on_error */ false);
            }
            else
            {
                LOG_TRACE(log, "Another replica is the active worker, staying on standby");
                reschedule_ms = retry_poll_ms;
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
        releaseLeadershipAfterFailedStartup();
        reschedule_ms = retry_poll_ms;
    }

    if (!stop_synchronization)
        coordination_task->scheduleAfter(reschedule_ms);
}


void PostgreSQLReplicationHandler::releaseLeadershipAfterFailedStartup()
{
    /// A leader whose startup failed before a consumer got running must not camp on the leadership: the
    /// failure may be local to this replica (for example, only its snapshot load keeps failing), and while
    /// it holds /leader every healthy peer stays on standby. Release the leadership so the peers can
    /// compete; this replica re-enters the election on its next iteration and may win again once its
    /// problem clears. A consumer that is already running is left alone: post-startup failures are handled
    /// by the consumer/session-expiry paths.
    if (!leader_node || !coordination_zookeeper || consumer)
        return;

    try
    {
        /// Remove the node through the leadership session and only forget it when the removal is
        /// confirmed. If the removal fails the node may still exist, and dropping the reference to it
        /// would leave a leader node nobody tracks, blocking every peer until the whole Keeper session
        /// expires - keep the leadership state instead, so the regular retained-leadership retry and
        /// session-expiry handling stay responsible for it.
        coordination_zookeeper->remove(coordination_keeper_path + "/leader");
        leader_node->setAlreadyRemoved();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to release the replication leadership after a failed startup attempt, keeping it");
        return;
    }

    is_active_worker.store(false);
    leader_node.reset();
    coordination_zookeeper.reset();
    LOG_WARNING(log, "Released replication leadership after a failed startup attempt");
}


void PostgreSQLReplicationHandler::registerReplicaThenEnsureNestedTables()
{
    /// Order matters and must be register-first: the nested tables are replicas of a shared
    /// Replicated/SharedReplacingMergeTree tree, so once `ensureNestedTablesExist` returns this replica holds
    /// a durable, replicated copy of the shared data. `shutdownFinal` decides "last replica" purely from
    /// <keeper_path>/replicas, so the /replicas/<name> node has to exist *before* this replica owns any nested
    /// data - otherwise a Keeper blip between the two steps could leave a data-bearing but unregistered
    /// replica, and a last-replica drop on a peer would then remove the shared slot/publication/marker while
    /// this copy still exists (a later resnapshot would resume into it without truncation, duplicating rows or
    /// preserving stale deletes).
    ///
    /// The flip side of registering first is a ghost participant if nested-table creation fails outright before
    /// this replica owns any shared data (the database engine is never created, so no shutdownFinal runs). Undo
    /// the registration in that case to close that hole. Both steps are idempotent, so the startup task can
    /// safely retry.
    ///
    /// But `ensureNestedTablesExist` is not all-or-nothing: it creates the nested tables one by one, and as soon
    /// as one is created it is a live replica of the shared Replicated/SharedReplacingMergeTree tree and starts
    /// receiving replicated data. If a later table in the loop then throws, this replica already owns a copy of
    /// the shared data - so the registration must NOT be undone, or a peer's last-replica teardown (which decides
    /// solely from <keeper_path>/replicas) could delete the shared slot/publication/marker while this partial
    /// copy still exists. In that case keep the replica registered and let the idempotent startup-task retry
    /// finish creating the remaining tables (it skips the ones that already exist).
    /// Re-check the expanded replica name here as well, not only in the CREATE-time validator: the macros it
    /// expands through live in the server configuration and can change after the database was created, and a
    /// name that is not a single Keeper path component would leak the shared PostgreSQL objects (see
    /// `assertValidCoordinationReplicaName`). This runs before any Keeper path is formed from it - both the
    /// registration node and the nested replicated tables come later - and refusing here (rather than in the
    /// handler's constructor) keeps `DROP DATABASE` of a misconfigured setup possible.
    assertCoordinationIdentityResolved();
    assertValidCoordinationReplicaName(coordination_replica_name);

    /// For the same reason, refuse to proceed when the expanded coordination identity no longer matches the
    /// one persisted in the nested tables this replica already owns. Runs before any Keeper node is created
    /// under the (possibly new) path.
    assertCoordinationIdentityMatchesNestedTables();

    ensureCoordinatedNamingCompatible();
    ensureCoordinatedTableSetCompatible();

    /// Holds the startup inside the window between the advisory teardown-token check (in
    /// ensureCoordinatedNamingCompatible above) and the registration, so a test can win the teardown fence
    /// on a peer in between and verify that the registration's own atomic token probe refuses to join.
    fiu_do_on(FailPoints::materialized_postgresql_pause_before_register_replica,
    {
        LOG_INFO(log, "Pausing before registering the replica until failpoint "
                 "materialized_postgresql_pause_before_register_replica is disabled");
        FailPointInjection::pauseFailPoint(FailPoints::materialized_postgresql_pause_before_register_replica);
    });

    registerReplicaInKeeper();
    try
    {
        ensureNestedTablesExist();
    }
    catch (...)
    {
        if (hasAnyNestedTable())
        {
            LOG_WARNING(log,
                "Nested-table creation failed partway, but this replica already owns a copy of the shared "
                "nested data; keeping its registration at {}/replicas/{} so a peer cannot decide it is the last "
                "replica and tear down the shared slot/publication. Retrying.",
                coordination_keeper_path, coordination_replica_name);
        }
        else
        {
            try
            {
                unregisterReplica();
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to unregister replica after nested-table creation failed");
            }
        }
        throw;
    }
}


void PostgreSQLReplicationHandler::assertCoordinationIdentityResolved() const
{
    /// The constructor tolerates an unexpandable coordination path / replica name so that a misconfigured
    /// setup stays droppable (see there). Everything that forms a Keeper path from the identity must refuse
    /// to run with an unresolved one - the retrying startup task recovers by itself once the configuration
    /// is restored.
    if (!coordination_identity_error.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot resolve materialized_postgresql_keeper_path / materialized_postgresql_replica_name from the "
            "current server configuration: {}", coordination_identity_error);
}


void PostgreSQLReplicationHandler::assertCoordinationIdentityMatchesNestedTables() const
{
    /// `coordination_keeper_path` and `coordination_replica_name` are expanded from the *current* server
    /// configuration every time the handler is constructed, but the nested Replicated/SharedReplacingMergeTree
    /// tables of this replica were created with the expansion that was in effect back then, and they keep it
    /// verbatim in their persisted engine arguments (the shared tree's Keeper path and this replica's name in
    /// it). The <keeper_path>/replicas/<name> registration is persistent as well. So a configuration-only
    /// change of a macro these settings go through (or of an intermediate config macro they expand through)
    /// would make this handler elect a leader, register and tear down under one identity while the shared
    /// data it already owns lives under another: the old /replicas subtree could never drain (leaking the
    /// shared replication slot, publication and snapshot marker forever), and leader election could even be
    /// split from the shared nested-table path.
    ///
    /// The expanded identity is therefore treated as immutable for a setup that already exists, and the
    /// nested-table metadata is its authoritative record. Recovering the old identity from it and silently
    /// continuing under it is deliberately NOT done: the settings are what the user asked for, and quietly
    /// ignoring them would hide the misconfiguration. Refuse fail-close instead, so the setup keeps working
    /// as soon as the configuration is restored. Expanding the settings at CREATE time and persisting the
    /// literals is not an option either: `materialized_postgresql_replica_name` must stay per-replica, and a
    /// coordinated single-table engine may be created through a `Replicated` database, which replicates the
    /// CREATE query verbatim to every replica.
    for (const auto & [table_name, persisted] : readPersistedNestedIdentities())
    {
        const String & persisted_zookeeper_path = persisted.zookeeper_path;
        const String & persisted_replica_name = persisted.replica_name;

        const auto expected_spec = makeNestedEngineSpec(table_name);
        if (persisted_zookeeper_path == expected_spec.zookeeper_path && persisted_replica_name == expected_spec.replica_name)
            continue;

        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The coordination identity of this MaterializedPostgreSQL replica changed after it was created: "
            "materialized_postgresql_keeper_path and materialized_postgresql_replica_name now expand to '{}' "
            "and '{}', while the existing nested table for `{}` was created as a replica named '{}' of the "
            "shared replicated tree at '{}'. The expanded coordination identity must stay the same for the "
            "lifetime of a coordinated setup - the registration under <keeper_path>/replicas and the shared "
            "nested tables are persistent, so continuing would leak the shared replication slot, publication "
            "and snapshot marker, and could split leader election from the shared data. Restore the "
            "configuration (most likely the macros these settings expand through) to the values this replica "
            "was created with, or drop this replica's coordinated MaterializedPostgreSQL engine and recreate "
            "it on the new coordination path",
            coordination_keeper_path, coordination_replica_name, table_name,
            persisted_replica_name, persisted_zookeeper_path);
    }
}


std::map<String, PostgreSQLReplicationHandler::PersistedNestedIdentity> PostgreSQLReplicationHandler::readPersistedNestedIdentities() const
{
    /// Collect the (table name -> nested table) pairs whose persisted metadata can hold a coordination
    /// identity. The storages this handler knows about are the authoritative source, but a handler that
    /// `DatabaseMaterializedPostgreSQL::beforeDropDatabase` built purely to run the teardown (the attach /
    /// restart window, in which the background startup task has not built the real handler yet) has none
    /// attached - the nested tables of the database engine then live in that database under the PostgreSQL
    /// table names, so read them from the database directly.
    std::map<String, StorageID> nested_tables;
    for (const auto & [table_name, materialized_storage] : materialized_storages)
        nested_tables.emplace(table_name, materialized_storage->getNestedStorageID());

    if (nested_tables.empty() && is_materialized_postgresql_database)
    {
        if (auto database = DatabaseCatalog::instance().tryGetDatabase(current_database_name))
        {
            for (auto it = database->getTablesIterator(getContext()->getGlobalContext()); it->isValid(); it->next())
                nested_tables.emplace(it->name(), StorageID(current_database_name, it->name()));
        }
    }

    std::map<String, PersistedNestedIdentity> result;
    for (const auto & [table_name, nested_id] : nested_tables)
    {
        auto database = DatabaseCatalog::instance().tryGetDatabase(nested_id.database_name);
        if (!database)
            continue;

        /// The persisted definition is what matters here, so ask for it with a context that has no query
        /// context: `DatabaseMaterializedPostgreSQL::getCreateTableQueryImpl` otherwise regenerates the
        /// definition from the live settings, which is exactly the value that must not be trusted here.
        ASTPtr create_ast = database->tryGetCreateTableQuery(nested_id.table_name, getContext()->getGlobalContext());
        const auto * create_query = create_ast ? create_ast->as<ASTCreateQuery>() : nullptr;
        if (!create_query || !create_query->storage || !create_query->storage->engine)
            continue;

        /// Both replicated nested engines take the fully expanded Keeper path and replica name as their first
        /// two arguments (see `getCreateNestedTableQuery`). Anything else is not a coordinated nested table.
        const auto & engine = *create_query->storage->engine;
        if (!engine.arguments || engine.arguments->children.size() < 2)
            continue;

        const auto * path_literal = engine.arguments->children[0]->as<ASTLiteral>();
        const auto * replica_name_literal = engine.arguments->children[1]->as<ASTLiteral>();
        if (!path_literal || !replica_name_literal
            || path_literal->value.getType() != Field::Types::String
            || replica_name_literal->value.getType() != Field::Types::String)
            continue;

        PersistedNestedIdentity identity;
        identity.zookeeper_path = path_literal->value.safeGet<String>();
        identity.replica_name = replica_name_literal->value.safeGet<String>();
        result.emplace(table_name, std::move(identity));
    }

    return result;
}


void PostgreSQLReplicationHandler::adoptPersistedCoordinationIdentityForTeardown()
{
    /// The teardown of a coordinated setup - unregistering this replica, the race-free last-replica decision
    /// and, for the last replica, the removal of the shared coordination nodes - must operate on the identity
    /// the shared data this replica owns actually lives under, NOT on whatever the current server
    /// configuration happens to expand the coordination settings to. Otherwise a DROP after a
    /// configuration-only macro change (which the startup refuses, see
    /// `assertCoordinationIdentityMatchesNestedTables`, but which leaves the database mounted and droppable)
    /// would unregister and do last-replica accounting under the new identity, orphaning the original
    /// /replicas subtree together with the shared replication slot, publication and snapshot marker.
    ///
    /// Unlike the startup, adopting the persisted identity here is the right thing to do: this path only
    /// deletes state, it never resumes replication under it, and the persisted nested-table metadata is the
    /// authoritative record of the identity the shared data was created with.
    const auto persisted_identities = readPersistedNestedIdentities();
    for (const auto & [table_name, persisted] : persisted_identities)
    {
        /// Derive the coordination path from the nested table's Keeper path, which `makeNestedEngineSpec`
        /// formed as <keeper_path>/tables/<escaped table name>.
        const String suffix = "/tables/" + escapeForFileName(table_name);
        if (!persisted.zookeeper_path.ends_with(suffix))
            continue;

        const String persisted_keeper_path = persisted.zookeeper_path.substr(0, persisted.zookeeper_path.size() - suffix.size());
        if (persisted_keeper_path == coordination_keeper_path && persisted.replica_name == coordination_replica_name)
            return;

        LOG_WARNING(log,
            "The coordination settings of this MaterializedPostgreSQL replica now expand to keeper path '{}' and "
            "replica name '{}', while the nested tables it owns were created under keeper path '{}' and replica "
            "name '{}'. Tearing down the setup that actually exists, so the shared coordination state cannot be "
            "orphaned; the configuration this replica was created with should be restored if it is still in use "
            "by other replicas.",
            coordination_keeper_path, coordination_replica_name, persisted_keeper_path, persisted.replica_name);

        coordination_keeper_path = persisted_keeper_path;
        coordination_replica_name = persisted.replica_name;
        return;
    }

    /// No nested table to learn the identity from. If the settings could not be expanded at all (a macro they
    /// go through was removed from the configuration), there is nothing to tear down under, so refuse instead
    /// of touching an arbitrary Keeper path.
    if (!coordination_identity_error.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot resolve the coordination identity of this MaterializedPostgreSQL replica, and it owns no "
            "nested table to recover it from: {}", coordination_identity_error);
}


void PostgreSQLReplicationHandler::ensureCoordinatedNamingCompatible()
{
    /// The first replica of a coordinated setup publishes its naming-affecting settings at
    /// <keeper_path>/naming; every replica that joins later must carry the identical settings, because the
    /// ClickHouse names of the shared nested tables (which feed the shared Keeper paths of their replicated
    /// trees), as well as the names of the shared PostgreSQL slot and publication, are derived from them.
    /// A replica that disagrees would adopt the same publication yet build a disjoint replicated tree: it
    /// would never receive the other replicas' data through ClickHouse replication, but on failover it
    /// would still resume the shared slot from confirmed_flush_lsn, silently losing all pre-failover rows.
    /// Runs BEFORE this replica registers itself, so the check cannot be satisfied by its own state.
    ///
    /// This is enforced fail-close: on a mismatch the replica refuses to join (the same check also runs
    /// synchronously at CREATE time in validateMaterializedPostgreSQLCoordinationSettings when the setup
    /// already exists). A leftover /naming node from an incompletely dropped setup keeps rejecting
    /// replicas with different settings; overwriting it automatically would race a concurrent fresh
    /// CREATE on the same path, so it deliberately requires manual removal instead.
    auto component_guard = Coordination::setCurrentComponent("PostgreSQLReplicationHandler::ensureCoordinatedNamingCompatible");

    auto zookeeper = getContext()->getZooKeeper();

    /// While a last-replica drop is tearing this setup down, its <keeper_path>/teardown ownership token is
    /// still in place: the dropper has removed the coordination nodes but has not yet dropped the shared
    /// PostgreSQL slot/publication (that happens after its local data is gone). A fresh setup must not be
    /// built in that window - the pending teardown would delete the new setup's slot/publication by name.
    /// Refuse and let the startup task retry until the teardown finishes and removes the token. The token of
    /// this replica's own earlier refused drop (the teardown ran, but the later local nested-table drop
    /// failed) is reclaimed instead: this replica still owns the path, and the retrying startup is exactly
    /// the recovery of that refused drop.
    const String teardown_path = coordination_keeper_path + "/teardown";
    String teardown_owner;
    if (zookeeper->tryGet(teardown_path, teardown_owner))
    {
        if (teardown_owner == coordination_replica_owner)
        {
            LOG_INFO(log, "Reclaiming the teardown token of this replica's own earlier refused drop at {}", teardown_path);
            zookeeper->tryRemove(teardown_path);
        }
        else
        {
            throw Exception(ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                "The coordinated MaterializedPostgreSQL setup at Keeper path '{}' is still being torn down by "
                "the drop of its last replica (teardown token owner: {}). Will retry once the teardown has "
                "finished. If the tearing-down server died before completing it, remove the leftover node '{}' "
                "manually after dropping the leftover replication slot and publication in PostgreSQL",
                coordination_keeper_path, teardown_owner, teardown_path);
        }
    }

    const String naming_path = coordination_keeper_path + "/naming";
    zookeeper->createAncestors(naming_path);
    auto code = zookeeper->tryCreate(naming_path, coordination_naming_fingerprint, zkutil::CreateMode::Persistent);
    if (code == Coordination::Error::ZOK)
        return;
    if (code != Coordination::Error::ZNODEEXISTS)
        throw zkutil::KeeperException::fromPath(code, naming_path);

    const String published_fingerprint = zookeeper->get(naming_path);
    if (published_fingerprint != coordination_naming_fingerprint)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The coordinated MaterializedPostgreSQL setup at Keeper path '{}' was created with "
            "naming-affecting settings or a source identity that differ from this replica's. All replicas "
            "of one coordinated setup must agree on materialized_postgresql_table_engine, "
            "materialized_postgresql_schema, materialized_postgresql_schema_list and "
            "materialized_postgresql_tables_list_with_schema, and must replicate the same PostgreSQL source "
            "(the same source database and, for the single-table engine, the same source table): these "
            "determine how the ClickHouse names of the shared nested tables (and the names of the shared "
            "replication slot and publication) are derived, so a disagreeing replica would share the "
            "coordination bookkeeping without sharing the replicated data. "
            "Existing setup:\n{}\nThis replica:\n{}\n(If the existing setup was dropped incompletely, "
            "remove the leftover Keeper path manually.)",
            coordination_keeper_path, published_fingerprint, coordination_naming_fingerprint);
}


void PostgreSQLReplicationHandler::ensureCoordinatedTableSetCompatible()
{
    /// The authoritative shared table set must be fenced BEFORE this replica builds any nested table.
    /// `fetchRequiredTables` derives the set locally (from materialized_postgresql_tables_list, from the
    /// shared publication once it exists, or from the source schema), but the shared publication is only
    /// created later, by the elected active worker - so two fresh replicas starting concurrently could both
    /// pass that derivation with different sets and silently build diverging nested tables on one keeper
    /// path: whichever set the publication is then created from, the other replica keeps extra
    /// never-replicated (forever empty) tables or misses published ones, breaking failover for them.
    ///
    /// The first replica therefore publishes its derived set at <keeper_path>/table_set, and every replica
    /// must match it before it may register or create nested tables. This is enforced fail-close (mismatch
    /// refuses the join and the startup task retries), and it converges on its own: once the shared
    /// publication exists, a joining replica derives its set from the publication (which the fenced set
    /// created), and a mismatching explicit materialized_postgresql_tables_list is overridden by the
    /// publication's set with a warning (see fetchRequiredTables). Only a genuine divergence - e.g. the
    /// publication was altered externally, or a pre-publication CREATE raced with a different table set -
    /// keeps being refused and requires the operator to reconcile the table lists.
    auto component_guard = Coordination::setCurrentComponent("PostgreSQLReplicationHandler::ensureCoordinatedTableSetCompatible");

    std::vector<String> table_names;
    table_names.reserve(materialized_storages.size());
    for (const auto & [table_name, _] : materialized_storages)
        table_names.push_back(table_name);
    ::sort(table_names.begin(), table_names.end());
    const String local_table_set = fmt::format("{}\n", fmt::join(table_names, "\n"));

    auto zookeeper = getContext()->getZooKeeper();
    const String table_set_path = coordination_keeper_path + "/table_set";
    zookeeper->createAncestors(table_set_path);
    auto code = zookeeper->tryCreate(table_set_path, local_table_set, zkutil::CreateMode::Persistent);
    if (code == Coordination::Error::ZOK)
        return;
    if (code != Coordination::Error::ZNODEEXISTS)
        throw zkutil::KeeperException::fromPath(code, table_set_path);

    const String published_table_set = zookeeper->get(table_set_path);
    if (published_table_set != local_table_set)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The table set this replica derived differs from the table set of the coordinated "
            "MaterializedPostgreSQL setup at Keeper path '{}'. All replicas of one coordinated setup replicate "
            "the same shared set of tables through one shared publication; a replica that disagrees would "
            "build nested tables the publication never feeds, or miss tables it does feed. Make "
            "materialized_postgresql_tables_list match the existing setup (or recreate the setup with the new "
            "list). Existing setup:\n{}This replica:\n{}(If the existing setup was dropped incompletely, "
            "remove the leftover Keeper path manually.)",
            coordination_keeper_path, published_table_set, local_table_set);
}


std::optional<std::set<String>> PostgreSQLReplicationHandler::readCoordinatedTableSetFromKeeper()
{
    auto component_guard = Coordination::setCurrentComponent("PostgreSQLReplicationHandler::readCoordinatedTableSetFromKeeper");

    auto zookeeper = getContext()->getZooKeeper();
    String published_table_set;
    if (!zookeeper->tryGet(coordination_keeper_path + "/table_set", published_table_set))
        return {};

    std::set<String> table_names;
    Strings lines;
    splitInto<'\n'>(lines, published_table_set);
    for (const auto & line : lines)
    {
        if (!line.empty())
            table_names.insert(line);
    }
    return table_names;
}


void PostgreSQLReplicationHandler::registerReplicaInKeeper()
{
    auto component_guard = Coordination::setCurrentComponent("PostgreSQLReplicationHandler::registerReplicaInKeeper");

    /// The registration node stores this replica's identity, and an existing node owned by another replica
    /// rejects the registration: `materialized_postgresql_replica_name` must resolve to a distinct value on
    /// every replica, because the /replicas children are the shared bookkeeping behind the last-replica
    /// decision. With a blind createIfNotExists two same-named replicas would collapse onto one node - then a
    /// failed join rollback (or a DROP) on one of them removes the other live replica's registration, and a
    /// later last-replica teardown removes the shared slot/publication/snapshot_completed marker around a
    /// replica that still holds data. Re-registering an own node (server restart, ATTACH, teardown re-register)
    /// stays idempotent because the owner identity is stable.
    ///
    /// The registration is also where the <keeper_path>/teardown ownership token becomes an authoritative
    /// fence. The token check in ensureCoordinatedNamingCompatible alone is only advisory: between that check
    /// and this registration, the last replica of the previous setup can still win the teardown fence in
    /// unregisterReplicaAndCheckLast (winning requires /replicas to be empty, and this replica has not
    /// registered yet) - and would then still be entitled to drop the shared PostgreSQL slot/publication by
    /// name while this replica goes on to create nested tables and, once elected, fresh shared objects for
    /// the new setup. So the token's absence is asserted ATOMICALLY with the registration, in one Keeper
    /// multi-request: a create+remove pair on the token path (which fails with ZNODEEXISTS exactly when the
    /// token exists, and nets to nothing when it does not) together with the creation of the registration
    /// node. Once the registration node exists the teardown fence can no longer be won (removing the
    /// non-empty /replicas parent fails with ZNOTEMPTY), closing the race completely.
    auto zookeeper = getContext()->getZooKeeper();
    const String replica_path = coordination_keeper_path + "/replicas/" + coordination_replica_name;
    const String teardown_path = coordination_keeper_path + "/teardown";

    while (true)
    {
        zookeeper->createAncestors(replica_path);

        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(teardown_path, "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeRemoveRequest(teardown_path, -1));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path, coordination_replica_owner, zkutil::CreateMode::Persistent));
        Coordination::Responses responses;
        const auto code = zookeeper->tryMulti(ops, responses);

        if (code == Coordination::Error::ZOK)
        {
            LOG_DEBUG(log, "Registered replica '{}' at {}", coordination_replica_name, replica_path);
            return;
        }

        const size_t failed_op = zkutil::getFailedOpIndex(code, responses);

        if (failed_op == 0 && code == Coordination::Error::ZNODEEXISTS)
        {
            /// A teardown token appeared after ensureCoordinatedNamingCompatible checked for it: the last
            /// replica of the previous setup won the teardown fence concurrently. Refuse to join; the startup
            /// task retries and re-enters through ensureCoordinatedNamingCompatible, which keeps refusing a
            /// foreign token until the teardown finishes (and reclaims this replica's own token).
            throw Exception(ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                "The coordinated MaterializedPostgreSQL setup at Keeper path '{}' has concurrently begun being "
                "torn down by the drop of its last replica. Will retry once the teardown has finished. If the "
                "tearing-down server died before completing it, remove the leftover node '{}' manually after "
                "dropping the leftover replication slot and publication in PostgreSQL",
                coordination_keeper_path, teardown_path);
        }

        if (failed_op == 2 && code == Coordination::Error::ZNODEEXISTS)
        {
            const String registered_owner = zookeeper->get(replica_path);
            if (registered_owner != coordination_replica_owner)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "A replica named '{}' is already registered in the coordinated MaterializedPostgreSQL setup at "
                    "Keeper path '{}'. materialized_postgresql_replica_name must resolve to a distinct value on "
                    "every replica; use a distinct value (for example the {{replica}} macro). If this registration "
                    "is a leftover of an incompletely dropped setup, remove the Keeper node manually",
                    coordination_replica_name, coordination_keeper_path);
            /// This replica's own node already exists (server restart, ATTACH, startup retry) - it is
            /// registered, and while its node exists the teardown fence cannot be won, so no token re-check
            /// is needed.
            LOG_DEBUG(log, "Replica '{}' is already registered at {}", coordination_replica_name, replica_path);
            return;
        }

        if (failed_op == 2 && code == Coordination::Error::ZNONODE)
        {
            /// The /replicas parent vanished between createAncestors and the multi-request: a concurrent
            /// last-replica teardown just won the fence and removed it. Retry - the next iteration fails on
            /// the token probe above (or succeeds if the teardown has already finished).
            continue;
        }

        throw zkutil::KeeperMultiException(code, ops, responses);
    }
}


void PostgreSQLReplicationHandler::unregisterReplica()
{
    /// Best-effort removal of this replica's registration node, used to undo `registerReplicaInKeeper` when a
    /// later startup step fails. Unlike `unregisterReplicaAndCheckLast` this makes no last-replica decision and
    /// never removes any shared state, so it is safe to call from an error path.
    auto component_guard = Coordination::setCurrentComponent("PostgreSQLReplicationHandler::unregisterReplica");

    tryRemoveOwnReplicaRegistration(getContext()->getZooKeeper());
}


bool PostgreSQLReplicationHandler::tryRemoveOwnReplicaRegistration(const zkutil::ZooKeeperPtr & zookeeper)
{
    /// Removes this replica's <keeper_path>/replicas/<name> node only if this replica owns it. When the name is
    /// held by another replica (this replica's own registration was rejected for the duplicate name, but its
    /// database still exists and is now being dropped or rolled back), removing the node would delete that live
    /// peer's registration and let a later drop tear down the shared slot/publication around it. Returns false
    /// in exactly that case; an already-absent node counts as removed. Keeper errors propagate (the callers on
    /// the drop path are fail-close, and the rollback caller catches).
    const String replica_path = coordination_keeper_path + "/replicas/" + coordination_replica_name;

    String registered_owner;
    if (!zookeeper->tryGet(replica_path, registered_owner))
        return true;

    if (registered_owner != coordination_replica_owner)
    {
        LOG_WARNING(log,
            "Not removing the replica registration node {}: it belongs to another replica that resolved "
            "materialized_postgresql_replica_name to the same value '{}'",
            replica_path, coordination_replica_name);
        return false;
    }

    if (auto code = zookeeper->tryRemove(replica_path);
        code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
        throw zkutil::KeeperException::fromPath(code, replica_path);

    return true;
}


bool PostgreSQLReplicationHandler::hasAnyNestedTable() const
{
    /// A nested table exists in the catalog once `createNestedIfNeeded` created it (regardless of whether the
    /// wrapper has been marked queryable yet), which is exactly when this replica starts holding a copy of the
    /// shared replicated data. Note this also sees nested tables created by an earlier startup attempt.
    for (const auto & [table_name, materialized_storage] : materialized_storages)
        if (materialized_storage->tryGetNested())
            return true;
    return false;
}


void PostgreSQLReplicationHandler::coordinatedTeardownBeforeDataDrop()
{
    /// Runs (in coordinated mode) from the DROP path BEFORE the caller deletes this replica's local nested
    /// tables, so the last-replica decision - and, for the last replica, the removal of the shared
    /// coordination state - happens while this replica still holds the data. This closes the data-loss window
    /// that existed when the last-replica teardown ran only in shutdownFinal, AFTER the nested tables had
    /// already been dropped: a Keeper outage in between could then delete the last copy of the data while the
    /// shared slot, publication and snapshot_completed marker survived, and a later recreate on the same
    /// keeper path would resume from confirmed_flush_lsn into empty tables.
    auto component_guard = Coordination::setCurrentComponent("PostgreSQLReplicationHandler::coordinatedTeardownBeforeDataDrop");

    /// The retrying startup task must not run concurrently with the teardown. It could otherwise observe the
    /// <keeper_path>/teardown ownership token that unregisterReplicaAndCheckLast creates below and misread it
    /// as the leftover of this replica's own earlier refused drop - reclaiming (removing) the token and
    /// re-registering the replica mid-drop (see ensureCoordinatedNamingCompatible), after which shutdownFinal
    /// no longer owns the teardown and skips the shared slot/publication removal, leaking them. It would also
    /// race the identity adoption below, which mutates the members the startup path reads. Deactivation waits
    /// for an in-flight execution to finish; if the teardown is refused (throws) before the handler was
    /// stopped, reactivate the task so a replica whose startup was still retrying keeps retrying.
    startup_task->deactivate();
    try
    {
        /// Tear down the identity the shared data actually lives under, not the one the current configuration
        /// expands to (they differ after a configuration-only macro change, which the startup refuses but which
        /// must not make the drop orphan the original coordination state). Everything below, including the
        /// post-data teardown in `shutdownFinal`, then works on the adopted identity.
        adoptPersistedCoordinationIdentityForTeardown();

        /// Make the fail-close last-replica decision first: unregisterReplicaAndCheckLast throws if Keeper is
        /// unreachable, which aborts the drop before any nested table is removed (and, importantly, before the
        /// consumer below is stopped, so a transient Keeper outage does not disturb an otherwise-healthy replica).
        const bool is_last = unregisterReplicaAndCheckLast();

        /// Record the decision so shutdownFinal (the post-data teardown) does not re-run the race-free check: if this
        /// replica was the last one it has already removed the shared /replicas node here, and re-checking there
        /// would read its absence as "another replica was last" and skip the shared PostgreSQL cleanup (a leak).
        coordinated_teardown_was_last = is_last;

        if (!is_last)
        {
            /// Not the last replica: the shared state must stay. We had to remove /replicas/<name> to make the
            /// last-replica decision race-free, so re-register it - this replica keeps counting as a live data
            /// holder until its nested tables have actually been dropped. The authoritative removal then happens
            /// in shutdownFinal, AFTER the caller has dropped the nested tables, so a failure while dropping them
            /// never leaves this replica unregistered while it still holds a copy of the shared data. Re-register
            /// BEFORE stopping the consumer below: this is the teardown's last refusable Keeper write, so a
            /// refused (thrown) drop leaves the live replication handler untouched.
            ///
            /// Do not create ancestors here: a missing /replicas parent means a concurrent dropper on a peer has
            /// won the last-replica fence in the meantime and removed the shared coordination state. Re-creating
            /// the node would resurrect a tree that peer is tearing down, and throwing would refuse this
            /// otherwise-valid drop; proceed unregistered instead - the re-check in shutdownFinal then reads the
            /// parent's absence as "another replica was last" and correctly skips the shared cleanup.
            auto zookeeper = getContext()->getZooKeeper();
            const String replica_path = coordination_keeper_path + "/replicas/" + coordination_replica_name;
            const auto code = zookeeper->tryCreate(replica_path, coordination_replica_owner, zkutil::CreateMode::Persistent);
            if (code == Coordination::Error::ZNONODE)
                LOG_INFO(log,
                    "Not re-registering replica '{}': the shared /replicas node was removed by a concurrent last-replica teardown",
                    coordination_replica_name);
            else if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
                throw zkutil::KeeperException::fromPath(code, replica_path);
        }

        shutdown();

        /// Simulates a Keeper failure in the only teardown step that runs after the handler has been stopped
        /// (the last replica's removeCoordinationNodes below), to test the callers' recovery from a drop
        /// refused after that point.
        fiu_do_on(FailPoints::materialized_postgresql_fail_teardown_after_shutdown,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED,
                "Injected failure in coordinated teardown after the replication handler was shut down");
        });

        if (is_last)
        {
            /// Last replica: remove the shared coordination nodes (including the snapshot_completed marker) now,
            /// before the caller drops the last local copy. The shared PostgreSQL slot/publication are cleaned up
            /// authoritatively in shutdownFinal (a leaked slot/publication is recoverable, and with the marker
            /// already gone a recreate correctly redoes the snapshot rather than resuming into empty tables).
            removeCoordinationNodes();
        }
    }
    catch (...)
    {
        /// A teardown refused before shutdown() ran (a Keeper error in the steps above) leaves the handler
        /// alive; the callers' recovery only re-arms a STOPPED handler, so undo the deactivation here.
        /// The startup task matters exactly when replication has not started yet (it keeps retrying, e.g.
        /// while the configured coordination identity mismatches the persisted one); once replication runs,
        /// rescheduling it is a harmless idempotent no-op.
        ///
        /// Only restore it in the modes in which it is the retrying recovery path (`is_attach` /
        /// `retry_startup_on_error`), i.e. in which `checkConnectionAndStart` catches every error and
        /// reschedules itself. Arming it outside of them would let that function rethrow out of the
        /// background task, which aborts the server ("Tasks in BackgroundSchedulePool cannot throw") - and
        /// would leave a task running on a handler the caller is about to discard. The database engine, which
        /// starts synchronously and never arms this task, recovers a refused drop through its own startup task
        /// instead (DatabaseMaterializedPostgreSQL::recoverAfterRefusedDrop), and the single-table engine
        /// through `restartCoordinatedReplicationAfterFailedTeardown`, which enables the retrying mode first.
        if (!stop_synchronization && (is_attach || retry_startup_on_error))
            startup_task->activateAndSchedule();
        throw;
    }
}


void PostgreSQLReplicationHandler::restartCoordinatedReplicationAfterFailedTeardown()
{
    chassert(coordination_enabled);

    /// `shutdown` (run by the failed teardown) deactivated the background tasks and destroyed the consumer,
    /// all of which the retrying startup path rebuilds. Re-registration and nested-table creation are
    /// idempotent, and the shared snapshot_completed marker decides (as always) whether the elected worker
    /// resumes from confirmed_flush_lsn or redoes the initial snapshot.
    ///
    /// The failed teardown had already made its last-replica decision and re-registered this replica (or, for the
    /// last replica, removed the shared coordination nodes, which the rebuilt startup then recreates). Clear that
    /// stale decision so a later drop re-runs the race-free last-replica check from scratch instead of reusing
    /// `coordinated_teardown_was_last` (which a retried drop that skips the teardown would otherwise trust).
    coordinated_teardown_was_last = false;
    stop_synchronization.store(false);
    retry_startup_on_error.store(true);
    startup_task->activateAndSchedule();
}


void PostgreSQLReplicationHandler::restartReplicationAfterFailedDrop()
{
    chassert(!coordination_enabled);

    /// The plain engine's refused drop: `shutdown` (run by `flushAndShutdown` on the DROP path) deactivated
    /// the background tasks and destroyed the consumer, but the nested table still holds the data and the
    /// slot/publication still exist in PostgreSQL. Resume from the slot's confirmed_flush_lsn exactly like an
    /// ATTACH does - without `is_attach`, `startSynchronization` would drop the existing slot and redo the
    /// initial snapshot, needlessly reloading everything into the surviving nested table.
    is_attach = true;
    stop_synchronization.store(false);
    retry_startup_on_error.store(true);
    startup_task->activateAndSchedule();
}


bool PostgreSQLReplicationHandler::unregisterReplicaAndCheckLast()
{
    auto component_guard = Coordination::setCurrentComponent("PostgreSQLReplicationHandler::unregisterReplicaAndCheckLast");

    auto zookeeper = getContext()->getZooKeeper();
    const String replicas_path = coordination_keeper_path + "/replicas";

    /// The last-replica decision must be race-free across replicas dropping concurrently. A plain "remove my
    /// node, then list the parent" lets two droppers both delete their node before either lists it, so both
    /// observe an empty list and both conclude they are the last replica - and then both tear down the shared
    /// slot/publication/snapshot_completed marker, potentially around a replica whose nested-table drop later
    /// fails and still holds a copy of the data. Instead, fence the transition on the /replicas parent node:
    /// remove this replica's node, then try to remove the (now possibly empty) parent. Removing an empty node
    /// succeeds for exactly one caller - the last one out - and every other concurrent dropper then gets ZNONODE
    /// (the parent was already removed by that last replica) or ZNOTEMPTY (other replicas are still registered),
    /// so at most one replica can ever observe itself as the last one.
    ///
    /// This is also fail-close: the caller (DROP DATABASE / DROP TABLE) is about to remove this replica's local
    /// nested tables, so any Keeper error other than the expected ZNONODE/ZNOTEMPTY codes propagates rather than
    /// defaulting to "this was the last replica". Otherwise a Keeper blip during the drop could delete the last
    /// copy of the data while leaving the shared slot/publication/marker behind.
    ///
    /// The removal is ownership-checked: when this replica's name is held by another replica (this replica's
    /// own registration was rejected for the duplicate materialized_postgresql_replica_name), it is not
    /// registered at all - it must not remove the peer's node and cannot be the last replica.
    if (!tryRemoveOwnReplicaRegistration(zookeeper))
        return false;

    /// Removing the parent succeeds only when this replica's node was its last child, which atomically
    /// designates this caller - and only this caller - as the last replica. The <keeper_path>/teardown
    /// ownership token is created in the same multi-request, so winning the last-replica fence and fencing
    /// the keeper path against fresh CREATEs is one atomic step: from this moment until the token is removed
    /// (after the shared PostgreSQL slot/publication have been dropped, see shutdownFinal), no fresh setup
    /// can be built on this path, so the pending by-name drops can never delete a new setup's objects.
    const String teardown_path = coordination_keeper_path + "/teardown";
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeRemoveRequest(replicas_path, -1));
    ops.emplace_back(zkutil::makeCreateRequest(teardown_path, coordination_replica_owner, zkutil::CreateMode::Persistent));
    Coordination::Responses responses;
    const auto multi_code = zookeeper->tryMulti(ops, responses);

    if (multi_code == Coordination::Error::ZOK)
    {
        LOG_INFO(log, "Unregistered the last replica '{}'", coordination_replica_name);
        return true;
    }

    if (responses.empty() || (responses[0]->error != Coordination::Error::ZNOTEMPTY && responses[0]->error != Coordination::Error::ZNONODE))
        throw zkutil::KeeperMultiException(multi_code, ops, responses);

    const auto parent_code = responses[0]->error;

    if (parent_code == Coordination::Error::ZNOTEMPTY)
    {
        LOG_INFO(log,
            "Unregistered replica '{}'; other replica(s) still registered, keeping the shared replication slot and publication",
            coordination_replica_name);
        return false;
    }

    /// The /replicas parent is already gone: either a concurrent dropper removed it as the last replica
    /// (and is tearing down the shared state, so this replica must not repeat that teardown), or this
    /// replica's OWN earlier refused drop did - its pre-data teardown won the fence and left its teardown
    /// token behind, and this call is the retried drop resuming it. The token's owner distinguishes the
    /// two; resuming matters, because reading "not last" here would skip the shared PostgreSQL cleanup and
    /// leak the slot/publication while the token keeps rejecting recreates.
    String teardown_owner;
    if (zookeeper->tryGet(teardown_path, teardown_owner) && teardown_owner == coordination_replica_owner)
    {
        LOG_INFO(log,
            "Replica '{}' resumes the last-replica teardown of its own earlier refused drop",
            coordination_replica_name);
        return true;
    }

    LOG_INFO(log,
        "Unregistered replica '{}'; the shared /replicas node was already removed by a concurrent last-replica teardown",
        coordination_replica_name);
    return false;
}


void PostgreSQLReplicationHandler::removeCoordinationNodes()
{
    auto component_guard = Coordination::setCurrentComponent("PostgreSQLReplicationHandler::removeCoordinationNodes");

    auto zookeeper = getContext()->getZooKeeper();

    /// Fail closed: `tryRemove` / `tryRemoveRecursive` return recoverable Keeper failures (connection
    /// loss, operation timeout, ...) as codes instead of throwing, and silently ignoring them here would
    /// break the invariant this teardown exists for. If the `snapshot_completed` marker survived this
    /// step while the PostgreSQL cleanup later leaks the shared slot/publication, a recreate on the same
    /// keeper path would see a live marker and resume from the slot's `confirmed_flush_lsn` into empty
    /// tables instead of redoing the snapshot. Throwing aborts the teardown before any of that - and,
    /// on the drop paths, before the local nested tables are deleted - so the caller retries.
    auto remove_checked = [&](const String & path, bool recursive = false, bool tolerate_not_empty = false)
    {
        auto code = recursive ? zookeeper->tryRemoveRecursive(path) : zookeeper->tryRemove(path);
        if (code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE
            || (tolerate_not_empty && code == Coordination::Error::ZNOTEMPTY))
            return;
        throw zkutil::KeeperException::fromPath(code, path);
    };

    remove_checked(coordination_keeper_path + "/leader");
    remove_checked(coordination_keeper_path + "/replicas", /* recursive */ true);
    remove_checked(coordination_keeper_path + "/snapshot_completed");
    /// The naming fingerprint and the table set go AFTER the marker and the replicas: if this teardown dies
    /// partway, the surviving state must keep them, or a replica with different naming settings or a
    /// different table set could adopt the leftover publication/marker unchecked. The safe failure direction
    /// is a leftover /naming or /table_set node, which merely rejects a recreate with different settings
    /// until removed manually.
    remove_checked(coordination_keeper_path + "/naming");
    remove_checked(coordination_keeper_path + "/table_set");
    /// The nested Replicated tables remove their own trees under <keeper_path>/tables when they are
    /// dropped; only clean up the (then empty) parents. For the single-table engine the nested table is
    /// dropped after this handler shuts down, so this removal may legitimately fail as not-empty,
    /// leaving empty nodes behind - correctness over tidiness.
    remove_checked(coordination_keeper_path + "/tables", /* recursive */ false, /* tolerate_not_empty */ true);
    /// The <keeper_path>/teardown ownership token (created atomically with winning the last-replica fence in
    /// unregisterReplicaAndCheckLast) and the keeper path root itself are deliberately NOT removed here: the
    /// token must survive until the shared PostgreSQL slot/publication have actually been dropped, fencing
    /// the path against fresh CREATEs whose new objects those pending by-name drops would otherwise delete.
    /// Both are removed at the very end of the teardown, in shutdownFinal.
}


bool PostgreSQLReplicationHandler::hasSurvivingCoordinationState()
{
    /// Whether any coordination state of this setup survives in Keeper: the `snapshot_completed` marker or
    /// at least one registered replica. Used to distinguish a live shared publication (to adopt) from one
    /// leaked by a failed final teardown (to drop). Keeper errors propagate: adopting or dropping a shared
    /// publication on an unverified guess must not happen (fail-close), the caller retries.
    auto component_guard = Coordination::setCurrentComponent("PostgreSQLReplicationHandler::hasSurvivingCoordinationState");

    auto zookeeper = getContext()->getZooKeeper();
    if (zookeeper->exists(coordination_keeper_path + "/snapshot_completed"))
        return true;

    Strings replicas;
    auto code = zookeeper->tryGetChildren(coordination_keeper_path + "/replicas", replicas);
    if (code == Coordination::Error::ZNONODE)
        return false;
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException::fromPath(code, coordination_keeper_path + "/replicas");
    return !replicas.empty();
}


bool PostgreSQLReplicationHandler::isInitialSnapshotCompleted()
{
    auto component_guard = Coordination::setCurrentComponent("PostgreSQLReplicationHandler::isInitialSnapshotCompleted");

    auto zookeeper = getContext()->getZooKeeper();
    return zookeeper->exists(coordination_keeper_path + "/snapshot_completed");
}


void PostgreSQLReplicationHandler::assertReplicationLeadershipIsLive() const
{
    if (!coordination_enabled)
        return;

    /// Called only from the coordinated `startSynchronization` path, which runs inside `coordinationFunc`
    /// after this worker won /leader, so `leader_node` and `coordination_zookeeper` are set by then and are
    /// not mutated concurrently (`shutdown` deactivates the coordination task before touching them).
    if (!leader_node || !coordination_zookeeper || coordination_zookeeper->expired())
        throw Exception(
            ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
            "The Keeper session backing this replica's replication leadership is no longer alive, "
            "so another replica may already have taken over as the active worker. Aborting this attempt; "
            "coordination retries and re-elects a leader");
}


void PostgreSQLReplicationHandler::markInitialSnapshotCompleted(const String & lsn)
{
    auto component_guard = Coordination::setCurrentComponent("PostgreSQLReplicationHandler::markInitialSnapshotCompleted");

    /// The marker must be published only by a worker that still holds the live leadership. A worker whose
    /// Keeper session expired mid-snapshot has already been replaced: the new leader saw no marker,
    /// truncated the shared nested tables and started a replacement snapshot, and a marker published now
    /// would describe a snapshot that no longer exists in the tables - a later failover would then skip
    /// `initial_sync` and permanently lose the rows the replacement snapshot never finished copying.
    assertReplicationLeadershipIsLive();

    const String marker_path = coordination_keeper_path + "/snapshot_completed";

    /// A stale marker can be left when the shared slot disappeared without a full teardown (it is exactly
    /// what put us on the redo-the-snapshot path); replace it so the recorded LSN matches the new slot.
    /// Both requests go through the session that backs /leader, never through a fresh one: an expired
    /// session fails them, which is the authoritative fence - only a live leader can publish the marker.
    coordination_zookeeper->tryRemove(marker_path);

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCheckRequest(coordination_keeper_path + "/leader", -1));
    ops.emplace_back(zkutil::makeCreateRequest(marker_path, lsn, zkutil::CreateMode::Persistent));
    coordination_zookeeper->multi(ops);

    LOG_INFO(log, "Marked the initial snapshot as completed (lsn: {})", lsn);
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

    /// When coordination is enabled the publication is shared state, exactly like the replication slot: it
    /// was created by another replica of the same coordinated setup (or by this one before a handover), and
    /// dropping it here would break the replica that is consuming through it. Adopt it instead.
    if (!is_attach && publication_exists && !coordination_enabled)
    {
        /// This is a case for single Materialized storage. In case of database engine this check is done in advance.
        LOG_WARNING(log,
                    "Publication {} already exists, but it is a CREATE query, not ATTACH. Publication will be dropped",
                    doubleQuoteString(publication_name));

        dropPublication(tx);
    }

    if ((!is_attach && !coordination_enabled) || !publication_exists)
    {
        /// This branch is re-entered on the same handler whenever the publication has to be recreated (for
        /// example after it was dropped externally, or after retrying a failure before it existed), so the
        /// SQL-quoted table list is built in a local variable: `tables_list` keeps the raw form the retries
        /// and re-elections started from, and is never turned into an already-quoted value that a second
        /// pass would quote again.
        String publication_tables_list = tables_list;
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
            publication_tables_list = buf.str();
            publication_tables_list.resize(publication_tables_list.size() - 1);
        }
        else if (!is_materialized_postgresql_database)
        {
            /// Single `MaterializedPostgreSQL` storage: `tables_list` is the raw remote table name
            /// (see the `StorageMaterializedPostgreSQL` constructor) and is never passed through the
            /// quoting pass that `fetchRequiredTables` applies for the database engine. Quote it here,
            /// otherwise `CREATE PUBLICATION ... FOR TABLE ONLY <name>` folds an upper-case table name
            /// to lower case and fails with `relation "..." does not exist`.
            publication_tables_list = doubleQuoteWithSchema(tables_list);
        }

        if (publication_tables_list.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No table found to be replicated");

        /// 'ONLY' means just a table, without descendants.
        std::string query_str = fmt::format("CREATE PUBLICATION {} FOR TABLE ONLY {}", doubleQuoteString(publication_name), publication_tables_list);
        try
        {
            tx.exec(query_str);
            LOG_DEBUG(log, "Created publication {} with tables list: {}", doubleQuoteString(publication_name), publication_tables_list);
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
    /// The consumer may not exist: a coordinated standby builds the handler at startup but creates the
    /// consumer only after winning the leader election - and a former leader that lost the election has had
    /// its consumer destroyed again by coordinationFunc - while a plain database keeps an uninitialized
    /// handler while the background startup task is retrying. Store the new value in the handler first, so
    /// the consumer created later (e.g. on the next takeover) starts with it instead of the value captured
    /// at construction time.
    if (setting.name == "materialized_postgresql_max_block_size")
        max_block_size = setting.value.safeGet<UInt64>();

    /// Push the change into the live consumer only if one exists right now. `replication_handler_initialized`
    /// is not a sufficient proxy for that: it stays set when a demoted leader's consumer is destroyed on
    /// leader loss, so gating on it alone would throw from getConsumer on every former-leader standby.
    /// Deactivating the consumer task first serializes with an in-flight consume() iteration; the task is
    /// re-armed only when a consumer exists (on a standby it belongs to the active worker and must stay
    /// dormant until the coordination task re-arms it on election).
    consumer_task->deactivate();
    ConsumerPtr current_consumer;
    {
        std::lock_guard lock(consumer_ptr_mutex);
        current_consumer = consumer;
    }
    if (current_consumer)
    {
        current_consumer->setSetting(setting);
        consumer_task->activateAndSchedule();
    }
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
    /// In coordinated mode the race-free, fail-close last-replica decision was already made in
    /// coordinatedTeardownBeforeDataDrop, BEFORE the caller removed this replica's local nested tables (that is
    /// what closes the data-loss window: a Keeper outage while the last copy is being deleted can never leave the
    /// shared slot/publication/snapshot_completed marker behind). shutdownFinal is the authoritative post-data
    /// step that finalizes the teardown once the nested tables are gone.
    if (coordination_enabled)
    {
        shutdown();

        if (coordinated_teardown_was_last)
        {
            /// This replica already claimed the last-replica role (race-free) in the pre-data teardown and
            /// removed the shared /replicas node itself, so it must not re-run the check here: the parent's
            /// absence would be read as "another replica was last" and wrongly skip the shared PostgreSQL
            /// cleanup below. Remove the coordination nodes again (idempotent) and fall through to drop the
            /// shared slot/publication.
            removeCoordinationNodes();
        }
        else
        {
            /// This replica re-registered /replicas/<name> in the pre-data teardown (it was not the last one
            /// then; if a concurrent last-replica teardown had removed the /replicas parent by that point, the
            /// re-registration was skipped and the re-check below reads the parent's absence as "another
            /// replica was last") and has since dropped its local nested tables. Remove that registration now
            /// and re-run the race-free check: a peer that dropped concurrently may have left this the actual last replica. If
            /// it is still not the last one, another replica holds the shared data, so keep the shared
            /// slot/publication and coordination nodes for the peers and let the caller drop only this replica's
            /// local nested tables.
            if (!unregisterReplicaAndCheckLast())
                return;

            /// Now the last replica: remove the coordination nodes (including the snapshot_completed marker)
            /// before dropping the shared PostgreSQL slot/publication below, so that even if that cleanup fails a
            /// later recreate on the same keeper path finds no marker and correctly redoes the initial snapshot
            /// instead of resuming into empty tables. A leaked PostgreSQL slot/publication is recoverable manually.
            removeCoordinationNodes();
        }

        /// The shared PostgreSQL slot/publication below are dropped by NAME, so the drop must still own the
        /// setup incarnation it decided to tear down: if the <keeper_path>/teardown ownership token (created
        /// atomically with winning the last-replica fence) is gone or owned by someone else, a fresh setup may
        /// already live on this path - or the cleanup already ran - and dropping by name would delete objects
        /// that are not this teardown's to remove. While this replica DOES own the token, no fresh setup can be
        /// built (every CREATE and every joining startup refuses while the token exists), which is exactly what
        /// makes the by-name drops safe.
        auto zookeeper = getContext()->getZooKeeper();
        const String teardown_path = coordination_keeper_path + "/teardown";
        String teardown_owner;
        if (!zookeeper->tryGet(teardown_path, teardown_owner) || teardown_owner != coordination_replica_owner)
        {
            LOG_INFO(log,
                "Skipping the removal of the shared replication slot and publication: this replica no longer "
                "owns the teardown of the coordinated setup at {} (the cleanup has already run, or the path "
                "has been reused)",
                coordination_keeper_path);
            return;
        }
    }

    try
    {
        if (!coordination_enabled)
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

        if (!user_managed_slot)
        {
            connection.execWithRetry([&](pqxx::nontransaction & tx)
            {
                if (isReplicationSlotExist(tx, last_committed_lsn, /* temporary */false))
                    dropReplicationSlot(tx, /* temporary */false);
            });
        }
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to drop replication slot: {}. It must be dropped manually. Error: {}", replication_slot, getCurrentExceptionMessage(true));
    }

    if (coordination_enabled)
    {
        /// The teardown is complete (the shared PostgreSQL objects were dropped, or their failure was reported
        /// as requiring manual cleanup - which a fresh setup also recovers from by itself: a leaked publication
        /// or slot without coordination state is dropped and recreated). Release the keeper path for reuse by
        /// removing the teardown ownership token, and clean up the (now empty) root. This is the point of no
        /// return, so a Keeper failure here is only reported: the leftover token keeps rejecting recreates on
        /// this path until it is removed manually (the safe direction).
        try
        {
            auto zookeeper = getContext()->getZooKeeper();
            /// `tryRemove` reports recoverable Keeper failures (connection loss, operation timeout)
            /// through its return code, so it must be checked explicitly: a silently leftover token
            /// would keep rejecting recreates on this keeper path.
            auto code = zookeeper->tryRemove(coordination_keeper_path + "/teardown");
            if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
                throw zkutil::KeeperException::fromPath(code, coordination_keeper_path + "/teardown");

            /// Removing the root is pure tidiness: a leftover empty node does not block a recreate.
            /// The nested tables remove their own Keeper subtrees asynchronously, so the root may
            /// legitimately still be non-empty here.
            code = zookeeper->tryRemove(coordination_keeper_path);
            if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE && code != Coordination::Error::ZNOTEMPTY)
                LOG_WARNING(log, "Failed to remove the coordination keeper path {}: {}. It is harmless and can be removed manually.",
                    coordination_keeper_path, code);
        }
        catch (...)
        {
            LOG_ERROR(log,
                "Failed to remove the teardown ownership token at {}/teardown; recreating a coordinated setup "
                "on this keeper path will be rejected until it is removed manually. Error: {}",
                coordination_keeper_path, getCurrentExceptionMessage(true));
        }
    }
}


/// Used by MaterializedPostgreSQL database engine.
std::set<String> PostgreSQLReplicationHandler::fetchRequiredTables()
{
    /// Runs before `startup` on the database-engine path and consults the coordination state below, so it
    /// needs a resolved coordination identity just as much (the startup task retries once it is resolvable).
    if (coordination_enabled)
        assertCoordinationIdentityResolved();

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

    /// A publication that outlived its coordinated setup must not be adopted as live shared state. The
    /// last-replica teardown removes the Keeper coordination nodes first and only then drops the shared
    /// slot/publication in PostgreSQL, so if that final step failed the publication leaks - with the table
    /// set of the OLD setup. A fresh coordinated CREATE on the same keeper path would otherwise silently
    /// adopt that stale table set instead of the requested `materialized_postgresql_tables_list` / current
    /// schema. A live publication always has surviving coordination state (a replica registers itself
    /// before it creates the publication, and the `snapshot_completed` marker persists after that), so if
    /// neither the marker nor any registered replica exists, nothing can be consuming through the
    /// publication anymore: drop it and start fresh. Keeper errors propagate (fail-close) - the background
    /// startup task retries. On ATTACH the local nested tables mirror the publication's old table set, so
    /// it is kept and the slot-without-marker recovery redoes the snapshot as needed.
    if (publication_exists_before_startup && coordination_enabled && !is_attach && !hasSurvivingCoordinationState())
    {
        LOG_WARNING(log,
                    "Publication {} exists, but there is no coordination state under {} (no snapshot marker and no "
                    "registered replicas): it was left behind by an incompletely dropped coordinated setup and its "
                    "table set is stale. Dropping it; it will be recreated",
                    doubleQuoteString(publication_name), coordination_keeper_path);

        execWithRetryAndFaultInjection(connection, [&](pqxx::nontransaction & tx_){ dropPublication(tx_); });
        publication_exists_before_startup = false;
    }

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
        /// When coordination is enabled the publication is shared with the other replicas of the same
        /// coordinated setup: a second CREATE must adopt it (like an ATTACH does), not drop it from under
        /// the replica that is consuming through it.
        if (!is_attach && !coordination_enabled)
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
                if (coordination_enabled)
                {
                    /// A coordinated CREATE adopts the shared publication rather than dropping it. Its table
                    /// set is authoritative, so derive the tables from the publication itself instead of
                    /// re-scanning the live PostgreSQL schema. The schema may have drifted (tables added or
                    /// dropped) since the first replica created the publication; scanning it here would make
                    /// this replica build a different set of nested tables than the leader replicates through
                    /// the publication, so the extra tables would stay empty forever after a failover.
                    LOG_WARNING(log,
                                "Coordinated setup: deriving tables from the existing shared publication {}.",
                                doubleQuoteString(publication_name));

                    pqxx::work tx(connection.getRef());
                    result_tables = fetchTablesFromPublication(tx);
                }
                else
                {
                    LOG_WARNING(log,
                                "Publication {} already exists and tables list is empty. Assuming publication is correct.",
                                doubleQuoteString(publication_name));

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

                    /// In coordinated mode the shared publication is authoritative and is adopted (not
                    /// recreated), so the table set must be derived from the publication. Honoring a
                    /// mismatching explicit `materialized_postgresql_tables_list` here would make this
                    /// replica build nested tables that PostgreSQL never publishes into (or skip tables
                    /// the publication does publish), so replicas would silently diverge on which tables
                    /// actually replicate. Use the publication's table set (already in `result_tables`)
                    /// and warn that the setting is overridden. To honor an explicit list, the user must
                    /// make it match the publication (e.g. via ALTER PUBLICATION) or recreate the setup.
                    if (coordination_enabled)
                    {
                        LOG_WARNING(log,
                            "Coordinated setup: the specified `materialized_postgresql_tables_list` ({}) does not match "
                            "the shared publication {} ({}); differing tables: {}. The publication is authoritative in "
                            "coordinated mode, so its table set is used instead of the setting.",
                            listed_tables, doubleQuoteString(publication_name), publication_tables, diff_tables);

                        /// The adopted set must also drive a later publication recreation. When the publication
                        /// goes missing, `createPublicationIfNeeded` rebuilds `CREATE PUBLICATION` from
                        /// `tables_list` whenever it is not empty; keeping the stale local setting there would
                        /// make this replica recreate the shared publication with the extra (or without the
                        /// missing) tables of its own list, silently changing the authoritative table set under
                        /// the other replicas. Rewrite it to the adopted set - the quoting pass below then
                        /// treats it exactly like a matching user-provided list. (Coordinated mode rejects
                        /// column-filtered lists at construction, so no `table(col1, col2)` entry can be lost.)
                        tables_list = fmt::format("{}", fmt::join(result_tables, ", "));
                    }
                    else
                    {
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
    }

    /// The shared publication is absent, but this is not a fresh setup: coordination state survives in
    /// Keeper, so this replica is rejoining an existing coordinated setup whose publication has to be
    /// recreated (it was dropped externally, or the active worker died between the teardown of a previous
    /// generation and the recreation). The authoritative table set of that setup is the one fenced at
    /// <keeper_path>/table_set, not this replica's local `materialized_postgresql_tables_list`: a replica
    /// that once adopted a smaller publication set over a mismatching explicit setting is rebuilt from the
    /// persisted (stale) setting after a restart, so honoring the setting here would recreate the shared
    /// publication with a different table set - and `ensureCoordinatedTableSetCompatible` would refuse the
    /// startup against the already fenced set, wedging the replica instead of repairing the publication.
    /// Only a setup with surviving coordination state is trusted, for the same reason the publication-leak
    /// check above requires it: a leftover /table_set node of an incompletely dropped setup must not become
    /// authoritative for a fresh CREATE. Keeper errors propagate (fail-close), the startup task retries.
    if (coordination_enabled && !publication_exists_before_startup && result_tables.empty() && hasSurvivingCoordinationState())
    {
        if (auto fenced_tables = readCoordinatedTableSetFromKeeper(); fenced_tables && !fenced_tables->empty())
        {
            const std::set<String> local_tables(expected_tables.begin(), expected_tables.end());
            if (!tables_list.empty() && local_tables != *fenced_tables)
            {
                LOG_WARNING(log,
                    "Coordinated setup: the shared publication {} does not exist and the specified "
                    "`materialized_postgresql_tables_list` ({}) does not match the table set fenced at {}/table_set "
                    "({}). The fenced set is authoritative in coordinated mode, so it is used instead of the setting "
                    "and the publication is recreated from it.",
                    doubleQuoteString(publication_name), fmt::join(local_tables, ", "), coordination_keeper_path,
                    fmt::join(*fenced_tables, ", "));
            }
            else
            {
                LOG_DEBUG(log, "Coordinated setup: the shared publication {} does not exist, deriving tables from {}/table_set ({})",
                    doubleQuoteString(publication_name), coordination_keeper_path, fmt::join(*fenced_tables, ", "));
            }

            result_tables = *fenced_tables;
            /// `createPublicationIfNeeded` rebuilds the publication from `tables_list` whenever it is not
            /// empty, so it must carry the adopted set too (see the mismatch branch above).
            tables_list = fmt::format("{}", fmt::join(result_tables, ", "));
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
    /// Adding a table mutates the shared publication and reloads data through a temporary slot, and it
    /// only takes effect on the replica executing it: the other replicas would neither create the nested
    /// table nor learn the updated tables list, and after a failover the new active worker would consume a
    /// publication that no longer matches its configuration. Until these operations are routed through the
    /// coordination (and applied on every replica), refuse them instead of corrupting the shared state.
    if (coordination_enabled)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "ATTACH TABLE is not supported for a coordinated MaterializedPostgreSQL setup "
            "(materialized_postgresql_keeper_path is set). "
            "Recreate the database with an updated materialized_postgresql_tables_list instead");

    assertInitialized();

    /// Note: we have to ensure that replication consumer task is stopped when we reload table, because otherwise
    /// it can read wal beyond start lsn position (from which this table is being loaded), which will result in losing data.
    consumer_task->deactivate();
    try
    {
        LOG_TRACE(log, "Adding table `{}` to replication", postgres_table_name);

        fiu_do_on(FailPoints::materialized_postgresql_fail_add_table_to_replication,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED,
                "Injected failure while adding table `{}` to replication", postgres_table_name);
        });

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
    /// See the explanation in addTableToReplication.
    if (coordination_enabled)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "DETACH TABLE PERMANENTLY is not supported for a coordinated MaterializedPostgreSQL setup "
            "(materialized_postgresql_keeper_path is set). "
            "Recreate the database with an updated materialized_postgresql_tables_list instead");

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
