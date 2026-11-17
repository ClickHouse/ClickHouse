#include <Storages/MergeTree/SelectiveReplication/MigrationCoordinator.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/SelectiveReplication/KeeperAssignment.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <Common/FailPoint.h>
#include <Core/UUID.h>
#include <IO/WriteHelpers.h>
#include <base/find_symbols.h>
#include <base/scope_guard.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <algorithm>
#include <sstream>
#include <fmt/ranges.h>

namespace ProfileEvents
{
    extern const Event SelectiveReplicationMigrationStarted;
    extern const Event SelectiveReplicationMigrationCompleted;
    extern const Event SelectiveReplicationMigrationFailed;
    extern const Event SelectiveReplicationMigrationRolledBack;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNFINISHED;
    extern const int NO_ZOOKEEPER;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 replication_factor;
    extern const MergeTreeSettingsUInt64 max_concurrent_partition_migrations;
    extern const MergeTreeSettingsUInt64 migration_timeout_seconds;
    extern const MergeTreeSettingsUInt64 migration_coordinator_timeout;
}

namespace FailPoints
{
    extern const char selective_replication_takeover_cas_fail[];
    extern const char selective_replication_rollback_cas_fail[];
}

/// ---- MigrationMetadata ----

String MigrationMetadata::serialize() const
{
    Poco::JSON::Object obj;
    obj.set("version", FORMAT_VERSION);
    obj.set("state", state);
    obj.set("partition_id", partition_id);
    obj.set("source_replica", source_replica);
    obj.set("target_replica", target_replica);
    obj.set("coordinator", coordinator);
    obj.set("created_at", created_at);
    obj.set("source_parts_snapshot", source_parts_snapshot.empty()
        ? String("")
        : fmt::format("{}", fmt::join(source_parts_snapshot, "\n")));
    obj.set("log_pointer_at_start", log_pointer_at_start);

    std::ostringstream buf; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(obj, buf);
    return buf.str();
}

bool MigrationMetadata::deserialize(const String & data, MigrationMetadata & out)
{
    if (data.empty())
        return false;

    try
    {
        Poco::JSON::Parser parser;
        auto result = parser.parse(data);
        const auto & obj = result.extract<Poco::JSON::Object::Ptr>();

        auto get_string = [&](const String & key) -> String
        {
            if (obj->has(key))
                return obj->getValue<String>(key);
            return {};
        };

        out.state = get_string("state");
        out.partition_id = get_string("partition_id");
        out.source_replica = get_string("source_replica");
        out.target_replica = get_string("target_replica");
        out.coordinator = get_string("coordinator");
        out.created_at = get_string("created_at");
        out.log_pointer_at_start = get_string("log_pointer_at_start");

        String snapshot = get_string("source_parts_snapshot");
        out.source_parts_snapshot.clear();
        if (!snapshot.empty())
            splitInto<'\n'>(out.source_parts_snapshot, snapshot, true);

        return !out.state.empty();
    }
    catch (const Poco::Exception &)
    {
        /// Corrupt or incompatible JSON format - not an error for caller.
        return false;
    }
}

bool MigrationMetadata::read(const zkutil::ZooKeeperPtr & zk, const String & migration_path,
                             MigrationMetadata & out, Coordination::Stat * stat_out)
{
    String data;
    Coordination::Stat stat;
    if (!zk->tryGet(migration_path, data, &stat))
        return false;

    if (stat_out)
        *stat_out = stat;

    if (data.empty())
        return false;

    return deserialize(data, out);
}

bool MigrationMetadata::updateState(
    const zkutil::ZooKeeperPtr & zk,
    const String & migration_path,
    const String & new_state)
{
    String data;
    Coordination::Stat stat;
    if (!zk->tryGet(migration_path, data, &stat))
        return false;

    MigrationMetadata meta;
    if (!deserialize(data, meta))
        return false;

    meta.state = new_state;
    String new_data = meta.serialize();

    auto rc = zk->trySet(migration_path, new_data, stat.version);
    return rc == Coordination::Error::ZOK;
}

std::unordered_set<String> PartitionMigrationCoordinator::getActiveMigrationPartitions(
    const zkutil::ZooKeeperPtr & zk, const String & migrations_path)
{
    std::unordered_set<String> result;
    Strings migration_ids;
    if (zk->tryGetChildren(migrations_path, migration_ids) != Coordination::Error::ZOK)
        return result;

    for (const auto & mid : migration_ids)
    {
        MigrationMetadata meta;
        if (!MigrationMetadata::read(zk, migrations_path + "/" + mid, meta))
            continue;
        if (meta.state == SelectiveReplication::MIGRATION_STATE_CLONE
            || meta.state == SelectiveReplication::MIGRATION_STATE_SWITCH)
        {
            result.insert(meta.partition_id);
        }
    }
    return result;
}

/// ---- Path helpers ----

PartitionMigrationCoordinator::PartitionMigrationCoordinator(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , log(getLogger("PartitionMigrationCoordinator"))
{
}

String PartitionMigrationCoordinator::migrationsPath() const
{
    return fs::path(storage.getZooKeeperPath()) / "selective" / SelectiveReplication::MIGRATIONS_SUBPATH;
}

String PartitionMigrationCoordinator::migrationPath(const String & migration_id) const
{
    return fs::path(migrationsPath()) / migration_id;
}

String PartitionMigrationCoordinator::assignmentPath(const String & partition_id) const
{
    return fs::path(storage.getZooKeeperPath()) / "selective" / "assignments" / partition_id;
}

String PartitionMigrationCoordinator::countsPath() const
{
    return fs::path(storage.getZooKeeperPath()) / "selective" / "counts";
}

/// ---- CAS helper ----

bool PartitionMigrationCoordinator::casUpdateAssignment(
    const zkutil::ZooKeeperPtr & zk,
    const String & partition_id,
    std::function<std::optional<Strings>(Strings)> modifier)
{
    String path = assignmentPath(partition_id);
    bool success = false;

    /// initial_backoff_ms=100, max_backoff_ms=5000 — matches the original exponential backoff.
    /// max_retries is one less than MAX_MIGRATION_CAS_RETRIES because ZooKeeperRetriesControl
    /// always executes the first attempt unconditionally.
    ZooKeeperRetriesInfo retries_info{
        static_cast<UInt64>(SelectiveReplication::MAX_MIGRATION_CAS_RETRIES - 1),
        /*initial_backoff_ms=*/ 100,
        /*max_backoff_ms=*/ 5000,
        /*query_status=*/ nullptr};
    ZooKeeperRetriesControl retries("casUpdateAssignment", log, retries_info);

    retries.retryLoop([&]
    {
        /// Early abort if ZK session has expired — no point retrying.
        if (zk->expired())
            throw Exception(ErrorCodes::NO_ZOOKEEPER,
                "ZK session expired during CAS retry for partition {}", partition_id);

        String data;
        Coordination::Stat stat;
        if (!zk->tryGet(path, data, &stat))
        {
            success = false;
            return;
        }

        Strings replicas = KeeperReplicaAssignment::parseAssignment(data);
        auto new_replicas = modifier(std::move(replicas));
        if (!new_replicas)
        {
            success = false;
            return;
        }

        std::sort(new_replicas->begin(), new_replicas->end());
        String new_data = KeeperReplicaAssignment::serializeAssignment(*new_replicas);

        auto rc = zk->trySet(path, new_data, stat.version);
        if (rc == Coordination::Error::ZOK)
        {
            success = true;
            return;
        }
        if (rc == Coordination::Error::ZBADVERSION)
        {
            LOG_DEBUG(log, "Assignment CAS failed for partition {} (attempt {}/{}), retrying",
                partition_id, retries.isRetry() ? 2 : 1, SelectiveReplication::MAX_MIGRATION_CAS_RETRIES);
            retries.setUserError(Exception(ErrorCodes::UNFINISHED,
                "CAS version conflict for assignment of partition {}", partition_id));
            return;
        }
        throw Coordination::Exception::fromPath(rc, path);
    });

    /// If retries were exhausted due to ZBADVERSION, ZooKeeperRetriesControl already
    /// re-threw the UNFINISHED exception above. If we reach here, either we succeeded
    /// or the node/modifier indicated a non-retryable "false" result.
    return success;
}

/// ---- Part helpers ----

Strings PartitionMigrationCoordinator::readActivePartsForPartition(
    const zkutil::ZooKeeperPtr & zk,
    const String & replica_name,
    const String & partition_id)
{
    String parts_path = fs::path(storage.getZooKeeperPath()) / "replicas" / replica_name / "parts";
    Strings all_parts;
    auto rc = zk->tryGetChildren(parts_path, all_parts);
    if (rc != Coordination::Error::ZOK)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot read parts of replica '{}': {}", replica_name, Coordination::errorMessage(rc));

    LOG_TRACE(log, "readActivePartsForPartition: path={}, replica={}, partition={}, all_parts_count={}",
        parts_path, replica_name, partition_id, all_parts.size());

    ActiveDataPartSet active_set(storage.format_version);
    for (const auto & part_name : all_parts)
    {
        auto info = MergeTreePartInfo::tryParsePartName(part_name, storage.format_version);
        if (info && info->getPartitionId() == partition_id)
        {
            active_set.add(part_name);
        }
    }

    auto result = active_set.getParts();
    LOG_TRACE(log, "readActivePartsForPartition: result_count={} for partition '{}'", result.size(), partition_id);
    return result;
}

/// ---- Main operations ----

String PartitionMigrationCoordinator::startClone(
    const zkutil::ZooKeeperPtr & zk,
    const String & partition_id,
    const String & source_replica,
    const String & target_replica)
{
    /// 1. Generate migration_id.
    String migration_id = toString(UUIDHelpers::generateV4());

    /// 2. Read source parts snapshot and filter by partition.
    Strings snapshot_parts = readActivePartsForPartition(zk, source_replica, partition_id);

    if (snapshot_parts.empty())
    {
        LOG_INFO(log, "startClone: partition {} has no active parts on {}, skipping migration",
            partition_id, source_replica);
        return {};
    }

    /// 3. Create ZK migration node with JSON metadata.
    String mig_path = migrationPath(migration_id);
    /// NOTE: wall clock used for ZK-stored value; NTP drift is negligible
    /// compared to migration_timeout_seconds (minutes-scale).
    String now_str = toString(time(nullptr));

    /// Ensure ancestors (selective/migrations/) exist.
    zk->createAncestors(mig_path);

    MigrationMetadata meta;
    meta.state = SelectiveReplication::MIGRATION_STATE_CLONE;
    meta.partition_id = partition_id;
    meta.source_replica = source_replica;
    meta.target_replica = target_replica;
    meta.coordinator = storage.getReplicaName();
    meta.created_at = now_str;
    meta.source_parts_snapshot = std::move(snapshot_parts);

    /// Capture current log pointer so isMigrationFullyInitialized can detect
    /// partial init (crash before GET_PART entries are written).
    String shared_log_dir = fs::path(storage.getZooKeeperPath()) / "log";
    Strings log_entries;
    if (zk->tryGetChildren(shared_log_dir, log_entries) == Coordination::Error::ZOK && !log_entries.empty())
    {
        std::sort(log_entries.begin(), log_entries.end());
        meta.log_pointer_at_start = log_entries.back();
    }

    String serialized_data = meta.serialize();

    /// 3+4. Atomically create migration node and CAS-append target:cloning to assignment.
    /// Both operations are combined in a single ZK multi-op to eliminate the partial-init
    /// window that existed when they were separate steps.
    String a_path = assignmentPath(partition_id);
    String cloning_name = String(target_replica) + SelectiveReplication::CLONING_SUFFIX;

    bool initialized = false;
    bool already_exists = false;

    ZooKeeperRetriesInfo retries_info{
        static_cast<UInt64>(SelectiveReplication::MAX_MIGRATION_CAS_RETRIES - 1),
        /*initial_backoff_ms=*/ 100,
        /*max_backoff_ms=*/ 5000,
        /*query_status=*/ nullptr};
    ZooKeeperRetriesControl retries("startClone", log, retries_info);

    retries.retryLoop([&]
    {
        if (zk->expired())
            throw Exception(ErrorCodes::NO_ZOOKEEPER,
                "ZK session expired during startClone CAS for partition {}", partition_id);

        String asgn_data;
        Coordination::Stat asgn_stat;
        if (!zk->tryGet(a_path, asgn_data, &asgn_stat))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Assignment node missing for partition {} during startClone", partition_id);

        Strings replicas = KeeperReplicaAssignment::parseAssignment(asgn_data);
        replicas.push_back(cloning_name);
        std::sort(replicas.begin(), replicas.end());
        String new_asgn_data = KeeperReplicaAssignment::serializeAssignment(replicas);

        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(mig_path, serialized_data, zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeSetRequest(a_path, new_asgn_data, asgn_stat.version));

        Coordination::Responses responses;
        auto rc = zk->tryMulti(ops, responses);

        if (rc == Coordination::Error::ZOK)
        {
            initialized = true;
            return;
        }

        /// If migration node already exists, another coordinator created it — skip init.
        if (!responses.empty() && responses[0]->error == Coordination::Error::ZNODEEXISTS)
        {
            LOG_INFO(log, "startClone: migration node {} already exists, "
                "skipping creation", mig_path);
            initialized = true;
            already_exists = true;
            retries.stopRetries();
            return;
        }

        /// Assignment version changed — retry.
        if (rc == Coordination::Error::ZBADVERSION
            || (!responses.empty() && responses[1]->error == Coordination::Error::ZBADVERSION))
        {
            LOG_DEBUG(log, "startClone: assignment CAS failed for partition {} "
                "(attempt {}/{}), retrying",
                partition_id, retries.isRetry() ? 2 : 1, SelectiveReplication::MAX_MIGRATION_CAS_RETRIES);
            retries.setUserError(Exception(ErrorCodes::UNFINISHED,
                "CAS version conflict during startClone for partition {}", partition_id));
            return;
        }

        throw Coordination::Exception::fromPath(rc, mig_path);
    });

    if (!initialized)
        throw Exception(ErrorCodes::UNFINISHED,
            "startClone: failed to atomically create migration node and update assignment "
            "for partition {} after {} CAS retries",
            partition_id, SelectiveReplication::MAX_MIGRATION_CAS_RETRIES);

    /// Another coordinator already created this migration — nothing more to do.
    if (already_exists)
        return migration_id;

    /// 5. Write GET_PART entries to the shared replication log.
    /// This ensures all replicas will see the entries and pull them via pullLogsToQueue.
    /// Non-target replicas will skip them via shouldSkipForSelectiveReplication.
    /// NOTE: We only create sequential log entries here. No CAS on the shared log
    /// pointer is needed — PersistentSequential is atomic, and each replica advances
    /// its own log_pointer in pullLogsToQueue. Using CAS would conflict with
    /// concurrent INSERTs or migrations from other replicas.

    String shared_log_path = fs::path(storage.getZooKeeperPath()) / "log/log-";

    Coordination::Requests ops;
    for (const auto & part_name : meta.source_parts_snapshot)
    {
        ReplicatedMergeTreeLogEntryData entry;
        entry.type = ReplicatedMergeTreeLogEntryData::GET_PART;
        entry.source_replica = source_replica;
        entry.new_part_name = part_name;
        entry.create_time = time(nullptr);

        ops.emplace_back(zkutil::makeCreateRequest(
            shared_log_path, entry.toString(), zkutil::CreateMode::PersistentSequential));

        if (ops.size() >= zkutil::MULTI_BATCH_SIZE)
        {
            zk->multi(ops);
            ops.clear();
        }
    }
    if (!ops.empty())
        zk->multi(ops);

    LOG_INFO(log, "CLONE phase started for migration {}: {} GET_PART entries written to shared log, fetching from {} to {}",
        migration_id, meta.source_parts_snapshot.size(), source_replica, target_replica);

    ProfileEvents::increment(ProfileEvents::SelectiveReplicationMigrationStarted);
    return migration_id;
}

void PartitionMigrationCoordinator::driveStateMachine(
    const zkutil::ZooKeeperPtr & zk, const String & migration_id)
{
    String mig_path = migrationPath(migration_id);

    MigrationMetadata meta;
    if (!MigrationMetadata::read(zk, mig_path, meta))
    {
        LOG_DEBUG(log, "Migration {} node not found, skipping", migration_id);
        return;
    }

    if (meta.state == SelectiveReplication::MIGRATION_STATE_CLONE)
    {
        /// Defense-in-depth: guard against partially initialized migrations
        /// (crash between startClone steps 3 and 4). tryTakeoverMigration()
        /// will eventually clean these up, but we skip them here to avoid
        /// wasted work or incorrect timeout calculations.
        if (!isMigrationFullyInitialized(zk, meta))
        {
            LOG_DEBUG(log, "Migration {} is not fully initialized, skipping until recovered", migration_id);
            return;
        }

        /// Check timeout against metadata created_at field.
        if (!meta.created_at.empty())
        {
            time_t created_at = parse<time_t>(meta.created_at);
            const auto settings = storage.getSettings();
            time_t timeout = static_cast<time_t>((*settings)[MergeTreeSetting::migration_timeout_seconds]);
            if (time(nullptr) > created_at + timeout)
            {
                LOG_INFO(log, "Migration {} timed out (created_at={}, timeout={}s), rolling back",
                    migration_id, created_at, timeout);
                ProfileEvents::increment(ProfileEvents::SelectiveReplicationMigrationRolledBack);
                rollback(zk, migration_id);
                return;
            }
        }

        if (isCloneComplete(zk, migration_id))
        {
            LOG_INFO(log, "Migration {} CLONE phase complete, advancing to SWITCH", migration_id);
            if (!MigrationMetadata::updateState(zk, mig_path, SelectiveReplication::MIGRATION_STATE_SWITCH))
            {
                LOG_WARNING(log, "Migration {} failed to update state to SWITCH (CAS conflict), retrying next cycle", migration_id);
                return;
            }
            meta.state = SelectiveReplication::MIGRATION_STATE_SWITCH;
        }
        else
        {
            return; /// Still in progress.
        }
    }

    if (meta.state == SelectiveReplication::MIGRATION_STATE_SWITCH)
    {
        /// Read assignment.
        String a_path = assignmentPath(meta.partition_id);
        String asgn_data;
        Coordination::Stat asgn_stat;
        if (!zk->tryGet(a_path, asgn_data, &asgn_stat))
        {
            LOG_WARNING(log, "Migration {} SWITCH failed: assignment node missing for partition {}, rolling back",
                migration_id, meta.partition_id);
            rollback(zk, migration_id);
            return;
        }

        Strings replicas = KeeperReplicaAssignment::parseAssignment(asgn_data);

        /// Guard: if source is already absent, this SWITCH was already applied
        /// (crash between assignment update and state transition). Skip to DONE.
        bool source_present = false;
        Strings new_replicas;
        for (const auto & r : replicas)
        {
            String clean = KeeperReplicaAssignment::stripCloningSuffix(r);
            if (clean == meta.source_replica)
            {
                source_present = true;
                continue; /// Remove source.
            }
            if (clean == meta.target_replica)
                new_replicas.push_back(clean); /// Strip :cloning.
            else
                new_replicas.push_back(r); /// Keep as-is.
        }

        if (!source_present)
        {
            /// Already switched in a previous run — skip straight to DONE.
            LOG_INFO(log, "Migration {} SWITCH already applied (source absent), advancing to DONE", migration_id);
        }
        else
        {
            std::sort(new_replicas.begin(), new_replicas.end());
            String new_asgn_data = KeeperReplicaAssignment::serializeAssignment(new_replicas);

            /// Build multi-op: assignment set + counts set.
            Coordination::Requests ops;
            ops.emplace_back(zkutil::makeSetRequest(a_path, new_asgn_data, asgn_stat.version));

            /// Read counts for atomic update.
            String c_path = countsPath();
            String counts_data;
            Coordination::Stat counts_stat;
            bool has_counts = zk->tryGet(c_path, counts_data, &counts_stat);

            if (!has_counts)
            {
                /// Counts node missing (e.g. upgrade). Initialize it.
                auto keeper_assign = storage.getReplicaAssignment();
                if (keeper_assign)
                {
                    keeper_assign->initializeCounts(zk);
                    has_counts = zk->tryGet(c_path, counts_data, &counts_stat);
                }
            }

            if (has_counts)
            {
                auto counts = KeeperReplicaAssignment::parseCounts(counts_data);
                String source_clean = KeeperReplicaAssignment::stripCloningSuffix(meta.source_replica);
                String target_clean = KeeperReplicaAssignment::stripCloningSuffix(meta.target_replica);
                if (counts.contains(source_clean) && counts[source_clean] > 0)
                    counts[source_clean]--;
                counts[target_clean]++;
                String new_counts_data = KeeperReplicaAssignment::serializeCounts(counts);
                ops.emplace_back(zkutil::makeSetRequest(c_path, new_counts_data, counts_stat.version));
            }

            Coordination::Responses responses;
            auto rc = zk->tryMulti(ops, responses);

            if (rc == Coordination::Error::ZBADVERSION)
            {
                LOG_WARNING(log, "Migration {} SWITCH CAS conflict, will retry next cycle", migration_id);
                return;
            }
            if (rc != Coordination::Error::ZOK)
            {
                LOG_WARNING(log, "Migration {} SWITCH multi-op failed ({}), rolling back",
                    migration_id, Coordination::errorMessage(rc));
                rollback(zk, migration_id);
                return;
            }

            LOG_INFO(log, "Migration {} SWITCH complete for partition {}", migration_id, meta.partition_id);
            ProfileEvents::increment(ProfileEvents::SelectiveReplicationMigrationCompleted);
        }

        /// Clean up clone_complete child and transition to DONE.
        zk->tryRemove(mig_path + "/clone_complete");
        if (!MigrationMetadata::updateState(zk, mig_path, SelectiveReplication::MIGRATION_STATE_DONE))
        {
            LOG_WARNING(log, "Migration {} failed to transition to DONE (CAS conflict), will retry", migration_id);
            return;
        }
        LOG_INFO(log, "Migration {} completed (DONE)", migration_id);
    }

}

void PartitionMigrationCoordinator::rollback(
    const zkutil::ZooKeeperPtr & zk, const String & migration_id)
{
    String mig_path = migrationPath(migration_id);

    MigrationMetadata meta;
    if (!MigrationMetadata::read(zk, mig_path, meta))
    {
        LOG_WARNING(log, "Rollback: cannot read migration {} metadata, "
            "cleaning up subtree only", migration_id);
        cleanupMigrationSubtree(zk, mig_path);
        ProfileEvents::increment(ProfileEvents::SelectiveReplicationMigrationFailed);
        return;
    }

    bool assignment_cleaned = false;
    if (!meta.partition_id.empty() && !meta.target_replica.empty())
    {
        String cloning_name = meta.target_replica + SelectiveReplication::CLONING_SUFFIX;
        fiu_do_on(FailPoints::selective_replication_rollback_cas_fail,
            throw Coordination::Exception(
                Coordination::Error::ZOPERATIONTIMEOUT,
                "Failpoint: rollback CAS failed"));
        try
        {
            casUpdateAssignment(zk, meta.partition_id, [&](Strings replicas) -> std::optional<Strings>
            {
                std::erase_if(replicas, [&](const String & r) { return r == cloning_name; });
                return replicas;
            });
            assignment_cleaned = true;
        }
        catch (const Coordination::Exception & e)
        {
            LOG_WARNING(log, "Rollback: failed to update assignment for partition {} ({}), "
                "migration node will be preserved for retry in next recovery cycle",
                meta.partition_id, e.message());
        }
    }

    MigrationMetadata::updateState(zk, mig_path, SelectiveReplication::MIGRATION_STATE_FAILED);
    zk->tryRemove(mig_path + "/clone_complete");

    if (assignment_cleaned)
    {
        cleanupMigrationSubtree(zk, mig_path);
        LOG_WARNING(log, "Migration {} rolled back to FAILED and cleaned up", migration_id);
    }
    else
    {
        LOG_WARNING(log, "Migration {} marked FAILED but migration node preserved "
            "(assignment cleanup pending)", migration_id);
    }
    ProfileEvents::increment(ProfileEvents::SelectiveReplicationMigrationFailed);
}

bool PartitionMigrationCoordinator::isCloneComplete(
    const zkutil::ZooKeeperPtr & zk, const String & migration_id)
{
    return zk->exists(migrationPath(migration_id) + "/clone_complete");
}

void PartitionMigrationCoordinator::checkAndSignalCloneCompleteInternal(const zkutil::ZooKeeperPtr & zk)
{
    String self_name = storage.getReplicaName();
    String m_path = migrationsPath();

    Strings migration_ids;
    if (zk->tryGetChildren(m_path, migration_ids) != Coordination::Error::ZOK)
        return;

    /// Cache our own parts list across all migrations to avoid re-reading per migration.
    String self_parts_path = fs::path(storage.getZooKeeperPath()) / "replicas" / self_name / "parts";
    Strings our_parts;
    bool our_parts_loaded = false;

    for (const auto & mid : migration_ids)
    {
        String mig_path = m_path + "/" + mid;

        MigrationMetadata meta;
        if (!MigrationMetadata::read(zk, mig_path, meta))
            continue;

        if (meta.state != SelectiveReplication::MIGRATION_STATE_CLONE || meta.target_replica != self_name)
            continue;

        if (zk->exists(mig_path + "/clone_complete"))
            continue;

        if (meta.source_parts_snapshot.empty())
            continue;

        const Strings & snapshot_parts = meta.source_parts_snapshot;

        /// Lazily load our parts (once for all migrations).
        if (!our_parts_loaded)
        {
            if (zk->tryGetChildren(self_parts_path, our_parts) != Coordination::Error::ZOK)
                return;
            our_parts_loaded = true;
        }

        /// Build active set for this partition.
        ActiveDataPartSet our_set(storage.format_version);
        for (const auto & p : our_parts)
        {
            auto info = MergeTreePartInfo::tryParsePartName(p, storage.format_version);
            if (info && info->getPartitionId() == meta.partition_id)
                our_set.add(p);
        }

        /// Check if all snapshot parts are covered.
        bool all_covered = true;
        for (const auto & sp : snapshot_parts)
        {
            if (our_set.getContainingPart(sp).empty())
            {
                all_covered = false;
                break;
            }
        }

        if (all_covered)
        {
            auto code = zk->tryCreate(mig_path + "/clone_complete", "", zkutil::CreateMode::Persistent);
            if (code == Coordination::Error::ZOK)
                LOG_INFO(log, "Signaled clone_complete for migration {} (target: {})", mid, self_name);
            else if (code != Coordination::Error::ZNODEEXISTS)
                LOG_WARNING(log, "Failed to create clone_complete for migration {}: {}", mid, Coordination::errorMessage(code));
        }
    }
}

void PartitionMigrationCoordinator::cleanupMigrationSubtree(
    const zkutil::ZooKeeperPtr & zk, const String & migration_path)
{
    auto code = zk->tryRemoveRecursive(migration_path);
    if (code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE)
        LOG_INFO(log, "Cleaned up migration subtree {}", migration_path);
    else
        LOG_WARNING(log, "Failed to clean up migration subtree {}: {}", migration_path, Coordination::errorMessage(code));
}

void PartitionMigrationCoordinator::checkAndSignalCloneComplete()
{
    auto zk = storage.getZooKeeper();
    checkAndSignalCloneCompleteInternal(zk);
}

bool PartitionMigrationCoordinator::isMigrationFullyInitialized(
    const zkutil::ZooKeeperPtr & zk,
    const MigrationMetadata & meta)
{
    /// Check 1: assignment must contain target:cloning.
    String a_path = assignmentPath(meta.partition_id);
    String assignment_data;
    if (!zk->tryGet(a_path, assignment_data))
        return false;

    String cloning_name = meta.target_replica + SelectiveReplication::CLONING_SUFFIX;
    Strings replicas = KeeperReplicaAssignment::parseAssignment(assignment_data);
    if (std::find(replicas.begin(), replicas.end(), cloning_name) == replicas.end())
        return false;

    /// Check 2: snapshot should be non-empty.
    if (meta.source_parts_snapshot.empty())
        return false;

    /// Check 3: verify GET_PART entries exist in the shared log.
    /// Skip for pre-upgrade migrations that lack log_pointer_at_start.
    if (meta.log_pointer_at_start.empty())
        return true;

    String shared_log_dir = fs::path(storage.getZooKeeperPath()) / "log";
    Strings log_entries;
    if (zk->tryGetChildren(shared_log_dir, log_entries) != Coordination::Error::ZOK)
        return false;

    /// Only scan entries after the recorded pointer.
    std::sort(log_entries.begin(), log_entries.end());
    bool found_any = false;
    for (const auto & entry_name : log_entries)
    {
        if (entry_name <= meta.log_pointer_at_start)
            continue;

        String entry_path = shared_log_dir + "/" + entry_name;
        String entry_data;
        Coordination::Stat entry_stat;
        if (!zk->tryGet(entry_path, entry_data, &entry_stat))
            continue;

        auto entry = ReplicatedMergeTreeLogEntry::parse(entry_data, entry_stat, storage.format_version);
        if (entry->type == ReplicatedMergeTreeLogEntryData::GET_PART
            && entry->source_replica == meta.source_replica)
        {
            found_any = true;
            break;
        }
    }

    return found_any;
}

bool PartitionMigrationCoordinator::tryTakeoverMigration(
    const zkutil::ZooKeeperPtr & zk,
    const String & migration_id,
    const MigrationMetadata & meta)
{
    String mig_path = migrationPath(migration_id);

    /// CAS update coordinator field in migration JSON.
    String data;
    Coordination::Stat stat;
    if (!zk->tryGet(mig_path, data, &stat))
        return false;

    /// Failpoint: simulate CAS failure.
    fiu_do_on(FailPoints::selective_replication_takeover_cas_fail, stat.version += 1);

    MigrationMetadata takeover_meta;
    if (!MigrationMetadata::deserialize(data, takeover_meta))
        return false;

    if (takeover_meta.coordinator == storage.getReplicaName())
        return true;  /// Already the coordinator

    takeover_meta.coordinator = storage.getReplicaName();
    String new_data = takeover_meta.serialize();

    auto set_rc = zk->trySet(mig_path, new_data, stat.version);
    if (set_rc != Coordination::Error::ZOK)
    {
        LOG_DEBUG(log, "Failed to take over migration {} coordinator (CAS failed: {})",
            migration_id, Coordination::errorMessage(set_rc));
        return false;
    }

    LOG_DEBUG(log, "Took over migration {} coordinator from '{}'", migration_id, meta.coordinator);

    /// Detect partially-initialized migrations (crash between `startClone` steps).
    /// Use takeover_meta (freshly read from ZK) rather than the caller-supplied meta,
    /// which may be stale — another coordinator could have advanced the state since
    /// the caller's read.
    if (takeover_meta.state == SelectiveReplication::MIGRATION_STATE_CLONE
        && !takeover_meta.partition_id.empty() && !takeover_meta.target_replica.empty()
        && !isMigrationFullyInitialized(zk, takeover_meta))
    {
        LOG_WARNING(log, "Migration {} is partially initialized (CLONE state but assignment "
            "not updated or GET_PART entries missing). Rolling back.", migration_id);

        /// Clean up orphaned :cloning suffix in the assignment.
        String cloning_name = takeover_meta.target_replica + SelectiveReplication::CLONING_SUFFIX;
        try
        {
            casUpdateAssignment(zk, takeover_meta.partition_id, [&](Strings replicas) -> std::optional<Strings>
            {
                auto it = std::find(replicas.begin(), replicas.end(), cloning_name);
                if (it == replicas.end())
                    return std::nullopt; /// No :cloning entry — nothing to clean.
                replicas.erase(it);
                return replicas;
            });
        }
        catch (const DB::Exception &)
        {
            LOG_WARNING(log, "Failed to clean up :cloning entry for partition {}",
                takeover_meta.partition_id);
        }

        cleanupMigrationSubtree(zk, mig_path);
        return false;
    }

    return true;
}

void PartitionMigrationCoordinator::driveMigrations(bool own_only)
{
    auto zk = storage.getZooKeeper();
    String m_path = migrationsPath();

    Strings migration_ids;
    if (zk->tryGetChildren(m_path, migration_ids) != Coordination::Error::ZOK)
        return;

    String self_name = own_only ? storage.getReplicaName() : String{};

    for (const auto & mid : migration_ids)
    {
        MigrationMetadata meta;
        if (!MigrationMetadata::read(zk, m_path + "/" + mid, meta))
            continue;

        if (own_only && meta.coordinator != self_name)
            continue;

        if (meta.state != SelectiveReplication::MIGRATION_STATE_CLONE
            && meta.state != SelectiveReplication::MIGRATION_STATE_SWITCH)
            continue;

        try
        {
            driveStateMachine(zk, mid);
        }
        catch (const DB::Exception &)
        {
            tryLogCurrentException(log, fmt::format("Failed to drive migration {}", mid));
        }
    }
}

void PartitionMigrationCoordinator::cleanupTerminatedMigrations(const zkutil::ZooKeeperPtr & zk)
{
    String m_path = migrationsPath();
    Strings migration_ids;
    if (zk->tryGetChildren(m_path, migration_ids) != Coordination::Error::ZOK)
        return;

    for (const auto & mid : migration_ids)
    {
        MigrationMetadata meta;
        if (!MigrationMetadata::read(zk, m_path + "/" + mid, meta))
            continue;

        if (meta.state == SelectiveReplication::MIGRATION_STATE_FAILED)
        {
            LOG_INFO(log, "Cleanup: detected FAILED migration {} for partition {}, "
                "cleaning up :cloning suffix in assignment", mid, meta.partition_id);

            bool cleaned = false;
            try
            {
                casUpdateAssignment(zk, meta.partition_id,
                    [&](Strings replicas) -> std::optional<Strings>
                    {
                        String cloning_name = meta.target_replica + SelectiveReplication::CLONING_SUFFIX;
                        auto it = std::find(replicas.begin(), replicas.end(), cloning_name);
                        if (it == replicas.end())
                            return std::nullopt; /// Already cleaned — no-op.
                        replicas.erase(it);
                        return replicas;
                    });
                /// casUpdateAssignment returns false if lambda returns nullopt
                /// (already cleaned). Either way, assignment is clean now.
                cleaned = true;
            }
            catch (const Coordination::Exception & e)
            {
                LOG_WARNING(log, "Cleanup: failed to clean :cloning from assignment for partition {} ({}), "
                    "migration node preserved for retry in next cycle.",
                    meta.partition_id, e.message());
            }

            if (cleaned)
                cleanupMigrationSubtree(zk, migrationPath(mid));
            continue;
        }

        /// Clean up completed (DONE) migrations.
        if (meta.state == SelectiveReplication::MIGRATION_STATE_DONE)
        {
            cleanupMigrationSubtree(zk, migrationPath(mid));
            continue;
        }
    }
}

void PartitionMigrationCoordinator::recoverOrphanedMigrations()
{
    auto zk = storage.getZooKeeper();
    String zk_path = storage.getZooKeeperPath();

    checkAndSignalCloneCompleteInternal(zk);
    cleanupTerminatedMigrations(zk);

    String m_path = migrationsPath();
    Strings migration_ids;
    if (zk->tryGetChildren(m_path, migration_ids) != Coordination::Error::ZOK)
        return;

    const auto settings = storage.getSettings();
    time_t coordinator_timeout = static_cast<time_t>((*settings)[MergeTreeSetting::migration_coordinator_timeout]);

    for (const auto & mid : migration_ids)
    {
        Coordination::Stat node_stat;
        MigrationMetadata meta;
        if (!MigrationMetadata::read(zk, m_path + "/" + mid, meta, &node_stat))
            continue;

        /// Only recover active migrations.
        if (meta.state != SelectiveReplication::MIGRATION_STATE_CLONE
            && meta.state != SelectiveReplication::MIGRATION_STATE_SWITCH)
            continue;

        /// Check if coordinator is still active.
        if (zk->exists(fs::path(zk_path) / "replicas" / meta.coordinator / "is_active"))
            continue;

        /// Coordinator is offline. Use the ZK node's mtime (last modification time)
        /// to determine when the coordinator last made progress. This is more accurate
        /// than created_at because mtime updates on every state transition.
        /// ZK mtime is in milliseconds.
        time_t last_activity = node_stat.mtime / 1000;
        if (time(nullptr) < last_activity + coordinator_timeout)
            continue;

        LOG_INFO(log, "Taking over orphaned migration {} (coordinator '{}' is offline, "
            "last activity {}s ago)", mid, meta.coordinator,
            time(nullptr) - last_activity);

        if (!tryTakeoverMigration(zk, mid, meta))
            continue;

        try
        {
            driveStateMachine(zk, mid);
        }
        catch (const DB::Exception &)
        {
            tryLogCurrentException(log, fmt::format("Failed to drive state machine for orphaned migration {}", mid));
        }
    }
}

void PartitionMigrationCoordinator::checkAndStartAutoRebalance()
{
    auto zk = storage.getZooKeeper();

    /// Try to acquire rebalance lock (ephemeral). Silently return if held by another replica.
    String lock_path = fs::path(storage.getZooKeeperPath()) / "selective" / SelectiveReplication::REBALANCE_LOCK_SUBPATH;
    zk->createAncestors(lock_path);
    auto lock_code = zk->tryCreate(lock_path, storage.getReplicaName(), zkutil::CreateMode::Ephemeral);
    if (lock_code != Coordination::Error::ZOK)
    {
        LOG_TRACE(log, "Auto-rebalance: cannot acquire lock ({}), skipping", Coordination::errorMessage(lock_code));
        return;
    }

    /// Read stat for CAS delete in SCOPE_EXIT.
    /// If session expires and another replica creates a new lock,
    /// tryRemove will harmlessly return ZBADVERSION.
    Coordination::Stat lock_stat;
    String lock_data;
    if (!zk->tryGet(lock_path, lock_data, &lock_stat))
    {
        /// Lock vanished after creation (session expired?).
        LOG_DEBUG(log, "Auto-rebalance: lock disappeared immediately after creation, skipping");
        return;
    }

    SCOPE_EXIT({
        try
        {
            zk->tryRemove(lock_path, lock_stat.version);
        }
        catch (const Coordination::Exception & e)
        {
            LOG_WARNING(log, "Auto-rebalance: failed to remove lock node: {}", e.message());
        }
    });

    auto plans = computeRebalancePlan(zk);

    if (plans.empty())
    {
        LOG_TRACE(log, "Auto-rebalance: table is balanced, no migrations needed");
        return;
    }

    LOG_INFO(log, "Auto-rebalance: computed {} migration plan(s)", plans.size());

    const auto settings = storage.getSettings();
    UInt64 max_concurrent = (*settings)[MergeTreeSetting::max_concurrent_partition_migrations];

    for (size_t i = 0; i < plans.size() && i < max_concurrent; ++i)
    {
        try
        {
            String mid = startClone(zk, plans[i].partition_id, plans[i].source_replica, plans[i].target_replica);
            if (mid.empty())
                continue;
            LOG_INFO(log, "Auto-rebalance: started migration {} ({} -> {} for partition {})",
                mid, plans[i].source_replica, plans[i].target_replica, plans[i].partition_id);
        }
        catch (const DB::Exception &)
        {
            tryLogCurrentException(log, fmt::format("Auto-rebalance: failed to start migration for partition {}",
                plans[i].partition_id));
        }
    }
}

std::vector<PartitionMigrationCoordinator::MigrationPlan>
PartitionMigrationCoordinator::computeRebalancePlan(const zkutil::ZooKeeperPtr & zk)
{
    String zk_path = storage.getZooKeeperPath();

    auto keeper_assign = storage.getReplicaAssignment();
    if (!keeper_assign)
        return {};

    auto assignment_map = keeper_assign->getAssignments(zk, {}, /*force_refresh=*/true);
    if (assignment_map.empty())
        return {};

    /// Get active replicas.
    String replicas_path = fs::path(zk_path) / "replicas";
    Strings all_replicas;
    zk->tryGetChildren(replicas_path, all_replicas);

    Strings active_replicas;
    for (const auto & r : all_replicas)
    {
        if (zk->exists(replicas_path + "/" + r + "/is_active"))
            active_replicas.push_back(r);
    }

    if (active_replicas.empty())
    {
        LOG_TRACE(log, "Auto-rebalance: no active replicas found (all_replicas={})", all_replicas);
        return {};
    }

    /// Count partitions per replica (strip :cloning suffix).
    std::unordered_map<String, size_t> load;
    for (const auto & r : active_replicas)
        load[r] = 0;

    for (const auto & [pid, entry] : assignment_map)
    {
        for (const auto & r : entry.replicas)
        {
            String clean = KeeperReplicaAssignment::stripCloningSuffix(r);
            if (load.contains(clean))
                load[clean]++;
        }
    }

    /// Compute ideal load.
    const auto settings = storage.getSettings();
    UInt64 rf = (*settings)[MergeTreeSetting::replication_factor];
    size_t effective_rf = std::min(rf, static_cast<UInt64>(active_replicas.size()));

    size_t total_assignments = 0;
    for (const auto & [_, count] : load)
        total_assignments += count;

    size_t ideal_floor = total_assignments / active_replicas.size();
    size_t ideal_ceil = ideal_floor + (total_assignments % active_replicas.size() != 0 ? 1 : 0);

    LOG_TRACE(log, "Auto-rebalance: active_replicas={}, assignments={}, ideal_floor={}, ideal_ceil={}, rf={}, effective_rf={}",
        fmt::join(active_replicas, ","), total_assignments,
        ideal_floor, ideal_ceil, rf, effective_rf);

    /// Find partitions with active migrations to skip.
    std::unordered_set<String> migrating_partitions = getActiveMigrationPartitions(zk, migrationsPath());

    /// Greedy migration generation.
    std::vector<MigrationPlan> plans;

    /// Phase 1: Fix under-replicated partitions (assigned replica count < effective_rf).
    /// For each under-replicated partition, pick an existing assigned replica as source
    /// and the least-loaded unassigned active replica as target.
    for (const auto & [pid, entry] : assignment_map)
    {
        if (migrating_partitions.contains(pid))
            continue;

        /// Count current assigned replicas (strip :cloning suffix, only count active ones).
        std::unordered_set<String> assigned_set;
        for (const auto & r : entry.replicas)
            assigned_set.insert(KeeperReplicaAssignment::stripCloningSuffix(r));

        size_t current_replication = 0;
        for (const auto & r : assigned_set)
        {
            if (load.contains(r))
                current_replication++;
        }

        if (current_replication >= effective_rf)
            continue;

        /// This partition is under-replicated. Pick source from assigned replicas.
        String source;
        size_t max_src_load = 0;
        for (const auto & r : assigned_set)
        {
            if (load.contains(r) && load[r] > max_src_load)
            {
                max_src_load = load[r];
                source = r;
            }
        }

        if (source.empty())
            continue;

        /// Pick target: least-loaded active replica not already assigned.
        String target;
        size_t min_tgt_load = std::numeric_limits<size_t>::max();
        for (const auto & r : active_replicas)
        {
            if (!assigned_set.contains(r) && load[r] < min_tgt_load)
            {
                min_tgt_load = load[r];
                target = r;
            }
        }

        if (target.empty())
            continue;

        plans.push_back({pid, source, target});
        load[target]++;

        LOG_DEBUG(log, "Auto-rebalance: under-replicated partition {} (assigned={}, effective_rf={}), adding replica {} -> {}",
            pid, current_replication, effective_rf, source, target);
    }

    /// Phase 2: Load balance — move partitions from overloaded to underloaded replicas.
    /// Recompute ideal_floor/ideal_ceil after Phase 1 may have changed the totals.
    total_assignments = 0;
    for (const auto & [_, count] : load)
        total_assignments += count;
    ideal_floor = total_assignments / active_replicas.size();
    ideal_ceil = ideal_floor + (total_assignments % active_replicas.size() != 0 ? 1 : 0);

    std::vector<std::pair<String, Strings>> partition_list;
    for (const auto & [pid, entry] : assignment_map)
    {
        if (migrating_partitions.contains(pid))
            continue;
        partition_list.emplace_back(pid, entry.replicas);
    }

    /// Sort: partitions with highest minimum load among assigned replicas first.
    std::sort(partition_list.begin(), partition_list.end(),
        [&](const auto & a, const auto & b)
        {
            auto min_load_a = std::numeric_limits<size_t>::max();
            for (const auto & r : a.second)
            {
                String clean = KeeperReplicaAssignment::stripCloningSuffix(r);
                if (load.contains(clean))
                    min_load_a = std::min(min_load_a, load[clean]);
            }
            auto min_load_b = std::numeric_limits<size_t>::max();
            for (const auto & r : b.second)
            {
                String clean = KeeperReplicaAssignment::stripCloningSuffix(r);
                if (load.contains(clean))
                    min_load_b = std::min(min_load_b, load[clean]);
            }
            return min_load_a > min_load_b;
        });

    for (const auto & [pid, replicas] : partition_list)
    {
        String source;
        size_t max_src_load = 0;
        for (const auto & r : replicas)
        {
            String clean = KeeperReplicaAssignment::stripCloningSuffix(r);
            if (load.contains(clean) && load[clean] > ideal_ceil)
            {
                if (load[clean] > max_src_load)
                {
                    max_src_load = load[clean];
                    source = clean;
                }
            }
        }

        if (source.empty())
            continue;

        std::unordered_set<String> assigned_set;
        for (const auto & r : replicas)
            assigned_set.insert(KeeperReplicaAssignment::stripCloningSuffix(r));

        String target;
        size_t min_tgt_load = std::numeric_limits<size_t>::max();
        for (const auto & r : active_replicas)
        {
            if (!assigned_set.contains(r) && load[r] < ideal_ceil && load[r] < min_tgt_load)
            {
                min_tgt_load = load[r];
                target = r;
            }
        }

        if (target.empty())
            continue;

        plans.push_back({pid, source, target});

        load[source]--;
        load[target]++;

        /// Check if we're balanced now.
        bool balanced = true;
        for (const auto & [_, count] : load)
        {
            if (count > ideal_ceil || count < ideal_floor)
            {
                balanced = false;
                break;
            }
        }
        if (balanced)
            break;
    }

    LOG_INFO(log, "Rebalance plan: {} migrations needed (rf={}, effective_rf={}, {} active replicas, {} partitions)",
        plans.size(), rf, effective_rf, active_replicas.size(), assignment_map.size());

    return plans;
}

}
