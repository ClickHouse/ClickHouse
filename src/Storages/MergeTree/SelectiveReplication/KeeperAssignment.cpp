#include <Storages/MergeTree/SelectiveReplication/KeeperAssignment.h>
#include <Storages/MergeTree/SelectiveReplication/Constants.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/FailPoint.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <base/find_symbols.h>
#include <base/scope_guard.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <filesystem>
#include <unordered_set>

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event SelectiveReplicationAssignmentZKError;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNFINISHED;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char selective_replication_after_batch_ensure[];
    extern const char selective_replication_read_partitions_zk_error[];
}

KeeperReplicaAssignment::KeeperReplicaAssignment(
    const String & zookeeper_path_, UInt64 cache_ttl_seconds)
    : cache_ttl(cache_ttl_seconds)
    , zookeeper_path(zookeeper_path_)
{
}

std::unordered_map<String, KeeperReplicaAssignment::CachedEntry>
KeeperReplicaAssignment::readAllAssignmentsFromZK(const zkutil::ZooKeeperPtr & zk) const
{
    String assignments_path = fs::path(zookeeper_path) / "selective" / "assignments";

    Strings children;
    auto code = zk->tryGetChildren(assignments_path, children);
    if (code == Coordination::Error::ZNONODE)
        return {};
    if (code != Coordination::Error::ZOK)
    {
        ProfileEvents::increment(ProfileEvents::SelectiveReplicationAssignmentZKError);
        throw Coordination::Exception::fromPath(code, assignments_path);
    }

    std::unordered_map<String, CachedEntry> result;
    if (!children.empty())
    {
        Strings get_paths;
        get_paths.reserve(children.size());
        for (const auto & child : children)
            get_paths.push_back(fs::path(assignments_path) / child);

        auto results = zk->tryGet(get_paths);
        for (size_t i = 0; i < children.size(); ++i)
        {
            if (results[i].error == Coordination::Error::ZNONODE)
                continue;
            if (results[i].error != Coordination::Error::ZOK)
            {
                ProfileEvents::increment(ProfileEvents::SelectiveReplicationAssignmentZKError);
                throw Coordination::Exception::fromPath(results[i].error,
                    fs::path(assignments_path) / children[i]);
            }
            Strings assigned = parseAssignment(results[i].data);
            if (!assigned.empty())
                result[children[i]] = CachedEntry{std::move(assigned), results[i].stat.version};
        }
    }

    return result;
}

std::unordered_map<String, KeeperReplicaAssignment::CachedEntry>
KeeperReplicaAssignment::readPartitionsFromZK(
    const zkutil::ZooKeeperPtr & zk, const std::vector<String> & partition_ids) const
{
    String assignments_dir = fs::path(zookeeper_path) / "selective" / "assignments";

    Strings paths;
    paths.reserve(partition_ids.size());
    for (const auto & pid : partition_ids)
        paths.push_back(fs::path(assignments_dir) / pid);

    auto results = zk->tryGet(paths);

    std::unordered_map<String, CachedEntry> result;
    for (size_t i = 0; i < partition_ids.size(); ++i)
    {
        if (results[i].error == Coordination::Error::ZOK)
        {
            Strings assigned = parseAssignment(results[i].data);
            result[partition_ids[i]] = CachedEntry{std::move(assigned), results[i].stat.version};
        }
        else if (results[i].error == Coordination::Error::ZNONODE)
        {
            /// Record empty entry so callers can distinguish "not in ZK" from "not queried".
            result[partition_ids[i]] = CachedEntry{{}, -1};
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::SelectiveReplicationAssignmentZKError);
            throw Coordination::Exception::fromPath(results[i].error,
                fs::path(assignments_dir) / partition_ids[i]);
        }
    }

    return result;
}

std::unordered_map<String, KeeperReplicaAssignment::CachedEntry>
KeeperReplicaAssignment::getAssignments(
    const zkutil::ZooKeeperPtr & zk,
    const std::vector<String> & partition_ids,
    bool force_refresh)
{
    if (force_refresh && !zk)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "KeeperReplicaAssignment::getAssignments: force_refresh=true requires a valid ZooKeeper connection");

    if (force_refresh)
    {
        if (partition_ids.empty())
        {
            /// Full refresh from ZK.
            auto new_data = readAllAssignmentsFromZK(zk);

            /// Merge into cache instead of full replace — a concurrent allocatePartitions
            /// call may have added new entries between our ZK read and this lock acquisition.
            /// Full replace would silently discard those entries.
            std::lock_guard lock(cache_mutex);
            for (auto & [pid, entry] : new_data)
                cached.partitions[pid] = entry;
            cached.last_refresh = std::chrono::steady_clock::now();

            return new_data;
        }
        else
        {
            /// Per-partition forced ZK read.
            auto zk_data = readPartitionsFromZK(zk, partition_ids);

            /// Write back individual entries to cache.
            std::lock_guard lock(cache_mutex);
            for (const auto & [pid, entry] : zk_data)
                cached.partitions[pid] = entry;
            if (!partition_ids.empty())
                cached.last_refresh = std::chrono::steady_clock::now();

            return zk_data;
        }
    }

    /// force_refresh=false: try cache first.

    if (partition_ids.empty())
    {
        /// All partitions requested — check if cache is fresh.
        {
            std::lock_guard lock(cache_mutex);
            if (!cached.empty())
            {
                auto age = std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::steady_clock::now() - cached.last_refresh);
                if (age < cache_ttl)
                {
                    /// Cache fresh — return full snapshot.
                    std::unordered_map<String, CachedEntry> result;
                    result.reserve(cached.partitions.size());
                    for (const auto & [pid, entry] : cached.partitions)
                        result[pid] = entry;
                    return result;
                }
            }
        }

        /// Cache expired or cold.
        if (!zk)
            return {}; /// No ZK available — return empty.

        /// Thundering herd protection: only one thread refreshes.
        bool expected = false;
        if (cache_refreshing.compare_exchange_strong(expected, true))
        {
            SCOPE_EXIT({ cache_refreshing.store(false); });

            auto new_data = readAllAssignmentsFromZK(zk);

            std::lock_guard lock(cache_mutex);
            for (auto & [pid, entry] : new_data)
                cached.partitions[pid] = entry;
            cached.last_refresh = std::chrono::steady_clock::now();

            return new_data;
        }
        else
        {
            /// Another thread is refreshing — return stale cache (or empty).
            std::lock_guard lock(cache_mutex);
            std::unordered_map<String, CachedEntry> result;
            result.reserve(cached.partitions.size());
            for (const auto & [pid, entry] : cached.partitions)
                result[pid] = entry;
            return result;
        }
    }
    else
    {
        /// Specific partitions: check cache for each, collect misses.
        std::unordered_map<String, CachedEntry> hits;
        std::vector<String> misses;

        {
            std::lock_guard lock(cache_mutex);
            for (const auto & pid : partition_ids)
            {
                auto it = cached.partitions.find(pid);
                if (it != cached.partitions.end())
                    hits[pid] = it->second;
                else
                    misses.push_back(pid);
            }
        }

        if (misses.empty() || !zk)
            return hits;

        /// Batch-get misses from ZK.
        auto zk_data = readPartitionsFromZK(zk, misses);

        /// Write back to cache and merge into hits.
        {
            std::lock_guard lock(cache_mutex);
            for (const auto & [pid, entry] : zk_data)
                cached.partitions[pid] = entry;
        }

        for (auto & [pid, entry] : zk_data)
            hits[pid] = std::move(entry);

        return hits;
    }
}

KeeperReplicaAssignment::ComputedAssignment KeeperReplicaAssignment::computeAssignment(
    const String & partition_id,
    const Strings & all_replicas,
    const std::unordered_map<String, size_t> & partition_counts,
    const std::unordered_map<String, size_t> & replica_positions,
    UInt64 rf)
{
    chassert(!all_replicas.empty());
    chassert(rf <= all_replicas.size());

    Strings sorted_replicas = all_replicas;
    size_t rotation = std::hash<String>{}(partition_id) % sorted_replicas.size();

    std::sort(sorted_replicas.begin(), sorted_replicas.end(),
        [&](const String & a, const String & b)
        {
            auto ca = partition_counts.at(a);
            auto cb = partition_counts.at(b);
            if (ca != cb)
                return ca < cb;
            auto rotated_a = (replica_positions.at(a) + rotation) % all_replicas.size();
            auto rotated_b = (replica_positions.at(b) + rotation) % all_replicas.size();
            return rotated_a < rotated_b;
        });

    Strings assigned(sorted_replicas.begin(), sorted_replicas.begin() + rf);
    std::sort(assigned.begin(), assigned.end());

    return ComputedAssignment{
        .replicas = assigned,
        .serialized = serializeAssignment(assigned),
    };
}

Strings KeeperReplicaAssignment::allocateSinglePartition(
    const zkutil::ZooKeeperPtr & zk,
    const String & partition_id,
    const Strings & all_replicas,
    UInt64 replication_factor)
{
    if (all_replicas.empty())
        return {};

    String assignment_path = fs::path(zookeeper_path) / "selective" / "assignments" / partition_id;
    String counts_path = fs::path(zookeeper_path) / "selective" / "counts";

    /// Fast-path: read existing assignment.
    String existing_data;
    Coordination::Stat stat;
    Coordination::Error get_code;
    bool got = zk->tryGet(assignment_path, existing_data, &stat, nullptr, &get_code);
    if (got)
    {
        Strings assigned = parseAssignment(existing_data);
        if (!assigned.empty())
            return assigned;
    }
    else if (get_code != Coordination::Error::ZNONODE)
    {
        throw Coordination::Exception::fromPath(get_code, assignment_path);
    }

    /// Read counts node (a few KB) instead of scanning all assignments.
    String counts_data;
    Coordination::Stat counts_stat;
    Coordination::Error counts_code;
    bool counts_ok = zk->tryGet(counts_path, counts_data, &counts_stat, nullptr, &counts_code);

    std::unordered_map<String, size_t> partition_counts;
    if (counts_ok)
    {
        partition_counts = parseCounts(counts_data);
    }
    else if (counts_code == Coordination::Error::ZNONODE)
    {
        /// Counts node not yet created — fall back to full scan.
        /// This happens only during the very first allocation before initializeCounts runs.
        partition_counts = readReplicaPartitionCounts(zk);
    }
    else
    {
        throw Coordination::Exception::fromPath(counts_code, counts_path);
    }

    for (const auto & replica : all_replicas)
    {
        if (!partition_counts.contains(replica))
            partition_counts[replica] = 0;
    }

    /// Pick top `replication_factor` replicas; degrade if fewer are available.
    UInt64 rf = std::min(replication_factor, static_cast<UInt64>(all_replicas.size()));
    if (replication_factor > static_cast<UInt64>(all_replicas.size()))
        LOG_INFO(getLogger("KeeperReplicaAssignment"),
            "replication_factor={} exceeds available replica count {}, degrading to {} for partition {}",
            replication_factor, all_replicas.size(), rf, partition_id);

    std::unordered_map<String, size_t> replica_position;
    for (size_t i = 0; i < all_replicas.size(); ++i)
        replica_position[all_replicas[i]] = i;

    auto computed = computeAssignment(partition_id, all_replicas, partition_counts, replica_position, rf);
    Strings assigned = computed.replicas;

    /// Atomic multi-op: CAS-create assignment + CAS-update counts.
    ZooKeeperRetriesInfo retries_info{
        /*max_retries=*/ 2,  /// ZooKeeperRetriesControl executes first attempt unconditionally, so 2 retries = 3 total attempts
        /*initial_backoff_ms=*/ 100,
        /*max_backoff_ms=*/ 5000,
        /*query_status=*/ nullptr};
    ZooKeeperRetriesControl retries("allocateSinglePartition", getLogger("KeeperReplicaAssignment"), retries_info);

    Strings result_assigned;
    retries.retryLoop([&]
    {
        String assignment_data = serializeAssignment(assigned);

        if (counts_ok)
        {
            /// Update counts locally for this allocation.
            auto new_counts = partition_counts;
            for (const auto & r : assigned)
                new_counts[stripCloningSuffix(r)]++;
            String new_counts_data = serializeCounts(new_counts);

            Coordination::Requests ops;
            ops.emplace_back(zkutil::makeCreateRequest(assignment_path, assignment_data, zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeSetRequest(counts_path, new_counts_data, counts_stat.version));

            Coordination::Responses responses;
            auto rc = zk->tryMulti(ops, responses);

            if (rc == Coordination::Error::ZOK)
            {
                result_assigned = assigned;
                return;
            }

            /// Assignment already exists — another replica won.
            if (!responses.empty() && responses[0]->error == Coordination::Error::ZNODEEXISTS)
            {
                String winner_data;
                Coordination::Stat winner_stat;
                if (zk->tryGet(assignment_path, winner_data, &winner_stat) && !winner_data.empty())
                {
                    Strings winner_assigned = parseAssignment(winner_data);
                    if (!winner_assigned.empty())
                    {
                        result_assigned = winner_assigned;
                        return;
                    }
                }
                result_assigned = assigned;
                return;
            }

            /// Counts version conflict — re-read counts and retry.
            if (responses.size() > 1 && responses[1]->error == Coordination::Error::ZBADVERSION)
            {
                counts_ok = zk->tryGet(counts_path, counts_data, &counts_stat, nullptr, &counts_code);
                if (counts_ok)
                    partition_counts = parseCounts(counts_data);
                retries.setUserError(Exception(ErrorCodes::UNFINISHED,
                    "Counts CAS failed for partition {}", partition_id));
                return;
            }

            throw Coordination::Exception::fromPath(rc, assignment_path);
        }
        else
        {
            /// Fallback: counts node does not exist, use simple tryCreate without counts update.
            auto code = zk->tryCreate(assignment_path, assignment_data, zkutil::CreateMode::Persistent);

            if (code == Coordination::Error::ZOK)
            {
                result_assigned = assigned;
                return;
            }

            if (code != Coordination::Error::ZNODEEXISTS)
                throw Coordination::Exception::fromPath(code, assignment_path);

            /// Another replica won the CAS — read its assignment.
            String winner_data;
            Coordination::Stat winner_stat;
            if (zk->tryGet(assignment_path, winner_data, &winner_stat) && !winner_data.empty())
            {
                Strings winner_assigned = parseAssignment(winner_data);
                if (!winner_assigned.empty())
                {
                    result_assigned = winner_assigned;
                    return;
                }

                LOG_WARNING(getLogger("KeeperReplicaAssignment"),
                    "Assignment node {} has empty/corrupt data after ZNODEEXISTS, using locally computed assignment",
                    assignment_path);
                result_assigned = assigned;
                return;
            }

            /// Node was deleted between tryCreate and tryGet — retry.
            retries.setUserError(Exception(ErrorCodes::UNFINISHED,
                "Assignment node {} disappeared after ZNODEEXISTS", assignment_path));
        }
    });

    if (!result_assigned.empty())
        return result_assigned;

    throw Exception(ErrorCodes::UNFINISHED,
        "Assignment node for partition {}: exhausted retries. Retry the INSERT.",
        partition_id);
}

KeeperReplicaAssignment::BatchAllocationResult KeeperReplicaAssignment::allocatePartitions(
    const zkutil::ZooKeeperPtr & zk,
    const std::vector<String> & partition_ids,
    const Strings & all_replicas,
    UInt64 replication_factor)
{
    BatchAllocationResult result;

    if (partition_ids.empty() || all_replicas.empty())
        return result;

    /// 1. Batch-check which partitions already have assignments.
    String assignments_dir = fs::path(zookeeper_path) / "selective" / "assignments";
    Strings check_paths;
    check_paths.reserve(partition_ids.size());
    for (const auto & pid : partition_ids)
        check_paths.push_back(fs::path(assignments_dir) / pid);

    auto check_results = zk->tryGet(check_paths);

    std::vector<String> new_partitions;
    for (size_t i = 0; i < partition_ids.size(); ++i)
    {
        if (check_results[i].error == Coordination::Error::ZOK)
        {
            Strings assigned = parseAssignment(check_results[i].data);
            if (!assigned.empty())
            {
                result.existing[partition_ids[i]] = std::move(assigned);
                continue;
            }
        }
        new_partitions.push_back(partition_ids[i]);
    }

    if (new_partitions.empty())
        return result;

    /// 2. Read counts node.
    String counts_path = fs::path(zookeeper_path) / "selective" / "counts";
    String counts_data;
    Coordination::Stat counts_stat;
    Coordination::Error counts_code;
    bool counts_ok = zk->tryGet(counts_path, counts_data, &counts_stat, nullptr, &counts_code);

    std::unordered_map<String, size_t> partition_counts;
    if (counts_ok)
        partition_counts = parseCounts(counts_data);
    else if (counts_code == Coordination::Error::ZNONODE)
        partition_counts = readReplicaPartitionCounts(zk);
    else
        throw Coordination::Exception::fromPath(counts_code, counts_path);

    for (const auto & replica : all_replicas)
    {
        if (!partition_counts.contains(replica))
            partition_counts[replica] = 0;
    }

    /// 3. Compute assignments locally for all new partitions.
    /// Pre-compute replica positions for tie-breaking.
    std::unordered_map<String, size_t> replica_position;
    for (size_t i = 0; i < all_replicas.size(); ++i)
        replica_position[all_replicas[i]] = i;

    UInt64 rf = std::min(replication_factor, static_cast<UInt64>(all_replicas.size()));
    if (replication_factor > static_cast<UInt64>(all_replicas.size()))
        LOG_INFO(log, "replication_factor={} exceeds available replica count {}, degrading to {}",
            replication_factor, all_replicas.size(), rf);

    struct PendingAssignment
    {
        String partition_id;
        Strings replicas;
        String serialized;
    };
    std::vector<PendingAssignment> pending;
    pending.reserve(new_partitions.size());

    for (const auto & pid : new_partitions)
    {
        auto computed = computeAssignment(pid, all_replicas, partition_counts, replica_position, rf);

        /// Update local counts for subsequent partitions.
        for (const auto & r : computed.replicas)
            partition_counts[stripCloningSuffix(r)]++;

        pending.push_back({pid, std::move(computed.replicas), std::move(computed.serialized)});
    }

    /// 4. Batch commit in chunks of (MULTI_BATCH_SIZE - 1) create ops + 1 counts set op.
    /// Reserve 1 slot in each multi-op for the counts update.
    constexpr size_t max_creates_per_batch = zkutil::MULTI_BATCH_SIZE - 1;
    size_t idx = 0;

    /// Fallback: allocate remaining partitions one by one.
    auto fallback_to_per_partition = [&]()
    {
        for (size_t i = idx; i < pending.size(); ++i)
        {
            if (result.assignments.contains(pending[i].partition_id)
                || result.existing.contains(pending[i].partition_id))
                continue;
            Strings assigned = allocateSinglePartition(zk, pending[i].partition_id, all_replicas, replication_factor);
            result.assignments[pending[i].partition_id] = std::move(assigned);
        }
    };

    bool fallback_triggered = false;

    while (idx < pending.size())
    {
        size_t batch_end = std::min(idx + max_creates_per_batch, pending.size());

        ZooKeeperRetriesInfo batch_retries_info{
            /*max_retries=*/ 2,  /// 2 retries = 3 total attempts
            /*initial_backoff_ms=*/ 100,
            /*max_backoff_ms=*/ 5000,
            /*query_status=*/ nullptr};
        ZooKeeperRetriesControl batch_retries("allocatePartitionsBatch", log, batch_retries_info);

        bool batch_committed = false;
        batch_retries.retryLoop([&]
        {
            Coordination::Requests ops;
            std::vector<size_t> batch_indices;

            for (size_t i = idx; i < batch_end; ++i)
            {
                /// Skip partitions already resolved (from conflict in earlier attempt).
                if (result.assignments.contains(pending[i].partition_id)
                    || result.existing.contains(pending[i].partition_id))
                    continue;

                ops.emplace_back(zkutil::makeCreateRequest(
                    fs::path(assignments_dir) / pending[i].partition_id,
                    pending[i].serialized,
                    zkutil::CreateMode::Persistent));
                batch_indices.push_back(i);
            }

            if (ops.empty())
            {
                batch_committed = true;
                return;
            }

            /// Append counts update as the last op.
            if (counts_ok)
            {
                String new_counts_data = serializeCounts(partition_counts);
                ops.emplace_back(zkutil::makeSetRequest(counts_path, new_counts_data, counts_stat.version));
            }

            Coordination::Responses responses;
            auto rc = zk->tryMulti(ops, responses);

            if (rc == Coordination::Error::ZOK)
            {
                /// All succeeded — record assignments.
                for (size_t bi : batch_indices)
                    result.assignments[pending[bi].partition_id] = pending[bi].replicas;

                /// Update counts_stat version for next batch.
                if (counts_ok)
                {
                    auto set_response = std::dynamic_pointer_cast<Coordination::SetResponse>(responses.back());
                    if (set_response)
                        counts_stat = set_response->stat;
                }
                batch_committed = true;
                return;
            }

            /// Analyze per-op errors.
            bool has_node_exists = false;
            bool has_bad_version = false;

            for (size_t ri = 0; ri < batch_indices.size(); ++ri)
            {
                if (responses[ri]->error == Coordination::Error::ZNODEEXISTS)
                {
                    has_node_exists = true;
                    /// Read winner's assignment.
                    size_t pi = batch_indices[ri];
                    String winner_data;
                    if (zk->tryGet(fs::path(assignments_dir) / pending[pi].partition_id, winner_data))
                    {
                        Strings winner = parseAssignment(winner_data);
                        if (!winner.empty())
                            result.existing[pending[pi].partition_id] = std::move(winner);
                    }
                }
            }

            /// Check counts op (last in responses).
            if (counts_ok && responses.size() > batch_indices.size()
                && responses.back()->error == Coordination::Error::ZBADVERSION)
            {
                has_bad_version = true;
            }

            if (has_bad_version)
            {
                /// Re-read counts and retry the batch.
                counts_ok = zk->tryGet(counts_path, counts_data, &counts_stat, nullptr, &counts_code);
                if (counts_ok)
                {
                    partition_counts = parseCounts(counts_data);
                    for (const auto & replica : all_replicas)
                        if (!partition_counts.contains(replica))
                            partition_counts[replica] = 0;
                    for (const auto & [_, replicas] : result.assignments)
                        for (const auto & r : replicas)
                            partition_counts[stripCloningSuffix(r)]++;
                    for (const auto & [_, replicas] : result.existing)
                        for (const auto & r : replicas)
                            partition_counts[stripCloningSuffix(r)]++;
                }
                batch_retries.setUserError(Exception(ErrorCodes::UNFINISHED,
                    "allocatePartitions: counts CAS failed, re-read counts and retrying"));
                return;
            }

            if (has_node_exists && !has_bad_version)
            {
                /// Some partitions already existed. The multi-op is atomic so
                /// non-conflicting partitions were also rolled back.
                /// Fall back to per-partition for the remaining.
                fallback_to_per_partition();
                fallback_triggered = true;
                batch_committed = true;  /// Treat as handled — no retry needed.
                return;
            }

            /// Unknown error — fall back to per-partition.
            LOG_WARNING(log, "allocatePartitions: multi-op failed with {}, falling back to per-partition for remaining {} partitions",
                Coordination::errorMessage(rc), pending.size() - idx);
            fallback_to_per_partition();
            fallback_triggered = true;
            batch_committed = true;  /// Treat as handled — no retry needed.
        });

        if (fallback_triggered)
            return result;

        idx = batch_end;
    }

    return result;
}

void KeeperReplicaAssignment::removePartition(const zkutil::ZooKeeperPtr & zk, const String & partition_id)
{
    String assignment_path = fs::path(zookeeper_path) / "selective" / "assignments" / partition_id;
    String counts_path = fs::path(zookeeper_path) / "selective" / "counts";

    /// Read current assignment to know which replicas to decrement.
    String assignment_data;
    Coordination::Stat assignment_stat;
    bool has_assignment = zk->tryGet(assignment_path, assignment_data, &assignment_stat);

    Strings assigned_replicas;
    if (has_assignment && !assignment_data.empty())
        assigned_replicas = parseAssignment(assignment_data);

    /// Try to atomically remove assignment + update counts.
    String counts_data;
    Coordination::Stat counts_stat;
    bool has_counts = zk->tryGet(counts_path, counts_data, &counts_stat);

    if (has_counts && !assigned_replicas.empty())
    {
        auto counts = parseCounts(counts_data);
        for (const auto & r : assigned_replicas)
        {
            String clean = stripCloningSuffix(r);
            if (counts.contains(clean) && counts[clean] > 0)
                counts[clean]--;
        }
        String new_counts_data = serializeCounts(counts);

        /// Best-effort atomic: remove assignment + update counts.
        /// If multi-op fails, fall through to simple remove.
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeRemoveRequest(assignment_path, assignment_stat.version));
        ops.emplace_back(zkutil::makeSetRequest(counts_path, new_counts_data, counts_stat.version));

        Coordination::Responses responses;
        auto rc = zk->tryMulti(ops, responses);
        if (rc == Coordination::Error::ZOK)
        {
            LOG_INFO(log, "Removed assignment node and decremented counts for dropped partition {}", partition_id);
            {
                std::lock_guard lock(cache_mutex);
                cached.partitions.erase(partition_id);
            }
            return;
        }

        LOG_DEBUG(log, "Multi-op remove+counts failed for partition {} ({}), falling back to simple remove",
            partition_id, Coordination::errorMessage(rc));
    }

    /// Fallback: just remove the assignment node (counts may drift slightly, this is acceptable).
    auto code = zk->tryRemove(assignment_path);
    if (code == Coordination::Error::ZOK)
        LOG_INFO(log, "Removed assignment node {} for dropped partition", assignment_path);
    else if (code == Coordination::Error::ZNONODE)
        LOG_DEBUG(log, "Assignment node {} already absent for dropped partition", assignment_path);
    else
        LOG_WARNING(log, "Failed to remove assignment node {}: {}", assignment_path, Coordination::errorMessage(code));

    /// Synchronize cache: remove the partition entry so stale data is not served.
    {
        std::lock_guard lock(cache_mutex);
        cached.partitions.erase(partition_id);
    }
}

std::unordered_map<String, size_t> KeeperReplicaAssignment::readReplicaPartitionCounts(
    const zkutil::ZooKeeperPtr & zk) const
{
    std::unordered_map<String, size_t> counts;

    auto assignment_map = readAllAssignmentsFromZK(zk);
    for (const auto & [_, entry] : assignment_map)
        for (const auto & replica : entry.replicas)
            counts[stripCloningSuffix(replica)]++;

    return counts;
}

Strings KeeperReplicaAssignment::parseAssignment(const String & data)
{
    if (data.empty())
        return {};

    ReadBufferFromString in(data);

    assertString("format version: ", in);
    UInt64 version;
    readText(version, in);
    assertChar('\n', in);

    if (version > static_cast<UInt64>(ASSIGNMENT_FORMAT_VERSION))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot parse assignment with unknown format version {}. "
            "This likely means a newer ClickHouse version wrote this assignment. "
            "Please upgrade this replica.",
            version);

    String payload;
    readStringUntilEOF(payload, in);

    Strings result;
    splitInto<','>(result, payload, true);
    return result;
}

String KeeperReplicaAssignment::serializeAssignment(const Strings & replicas)
{
    return "format version: " + std::to_string(ASSIGNMENT_FORMAT_VERSION) + "\n"
        + fmt::format("{}", fmt::join(replicas, ","));
}

std::unordered_map<String, size_t> KeeperReplicaAssignment::parseCounts(const String & data)
{
    std::unordered_map<String, size_t> counts;
    if (data.empty())
        return counts;

    ReadBufferFromString in(data);

    assertString("format version: ", in);
    UInt64 version;
    readText(version, in);
    assertChar('\n', in);

    if (version > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot parse counts with unknown format version {}. "
            "This likely means a newer ClickHouse version wrote this node. "
            "Please upgrade this replica.",
            version);

    String payload;
    readStringUntilEOF(payload, in);

    /// Parse "r1:10,r2:20,r3:30"
    Strings tokens;
    splitInto<','>(tokens, payload, true);
    for (const auto & token : tokens)
    {
        auto colon_pos = token.rfind(':');
        if (colon_pos == String::npos || colon_pos == 0 || colon_pos == token.size() - 1)
            continue;
        String replica = token.substr(0, colon_pos);
        size_t count = parse<size_t>(token.substr(colon_pos + 1));
        counts[replica] = count;
    }
    return counts;
}

String KeeperReplicaAssignment::serializeCounts(const std::unordered_map<String, size_t> & counts)
{
    /// Sort by replica name for deterministic output (important for ZK CAS — same logical
    /// state must produce the same bytes so that two replicas computing the same counts
    /// don't create unnecessary version conflicts).
    std::vector<std::pair<String, size_t>> sorted(counts.begin(), counts.end());
    std::sort(sorted.begin(), sorted.end());

    String payload;
    for (size_t i = 0; i < sorted.size(); ++i)
    {
        if (i > 0)
            payload += ',';
        payload += sorted[i].first + ':' + toString(sorted[i].second);
    }
    return "format version: 1\n" + payload;
}

std::unordered_map<String, size_t> KeeperReplicaAssignment::readCounts(const zkutil::ZooKeeperPtr & zk) const
{
    String counts_path = fs::path(zookeeper_path) / "selective" / "counts";
    String data;
    Coordination::Stat stat;
    Coordination::Error code;
    bool ok = zk->tryGet(counts_path, data, &stat, nullptr, &code);
    if (ok)
        return parseCounts(data);
    if (code == Coordination::Error::ZNONODE)
        return {};
    throw Coordination::Exception::fromPath(code, counts_path);
}

void KeeperReplicaAssignment::initializeCounts(const zkutil::ZooKeeperPtr & zk)
{
    String counts_path = fs::path(zookeeper_path) / "selective" / "counts";

    /// Fast path: node already exists.
    if (zk->exists(counts_path))
        return;

    /// Compute counts from all current assignments.
    auto assignment_map = readAllAssignmentsFromZK(zk);
    std::unordered_map<String, size_t> counts;
    for (const auto & [_, entry] : assignment_map)
        for (const auto & replica : entry.replicas)
            counts[stripCloningSuffix(replica)]++;

    String data = serializeCounts(counts);
    auto code = zk->tryCreate(counts_path, data, zkutil::CreateMode::Persistent);
    if (code == Coordination::Error::ZOK)
        LOG_INFO(log, "Initialized /selective/counts with {} replicas from {} partitions",
            counts.size(), assignment_map.size());
    else if (code == Coordination::Error::ZNODEEXISTS)
        LOG_DEBUG(log, "/selective/counts already exists, skipping initialization");
    else
        throw Coordination::Exception::fromPath(code, counts_path);
}

void KeeperReplicaAssignment::appendCreateSelectiveOps(
    Coordination::Requests & ops,
    const String & zookeeper_path,
    UInt64 replication_factor)
{
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/selective", "",
        zkutil::CreateMode::Persistent));
    String config_json = SelectiveReplication::serializeSelectiveConfig(replication_factor);
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/selective/config", config_json,
        zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/selective/assignments", "",
        zkutil::CreateMode::Persistent));
    String empty_counts = serializeCounts({});
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/selective/counts", empty_counts,
        zkutil::CreateMode::Persistent));
}

void KeeperReplicaAssignment::asyncCreateSelectiveNodes(
    const zkutil::ZooKeeperPtr & zookeeper,
    const String & zookeeper_path,
    UInt64 replication_factor,
    std::vector<zkutil::ZooKeeper::FutureCreate> & futures)
{
    futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/selective", String(), zkutil::CreateMode::Persistent));
    String config_json = SelectiveReplication::serializeSelectiveConfig(replication_factor);
    futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/selective/config", config_json, zkutil::CreateMode::Persistent));
    futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/selective/assignments", String(), zkutil::CreateMode::Persistent));
    String empty_counts = serializeCounts({});
    futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/selective/counts", empty_counts, zkutil::CreateMode::Persistent));
}

String KeeperReplicaAssignment::stripCloningSuffix(const String & replica_name)
{
    constexpr std::string_view suffix{SelectiveReplication::CLONING_SUFFIX};
    if (replica_name.ends_with(suffix))
        return replica_name.substr(0, replica_name.size() - suffix.size());
    return replica_name;
}

bool KeeperReplicaAssignment::hasCloningSuffix(const String & replica_name)
{
    return replica_name.ends_with(SelectiveReplication::CLONING_SUFFIX);
}

bool KeeperReplicaAssignment::isReplicaAssigned(const Strings & assigned_replicas, const String & replica_name)
{
    return std::any_of(assigned_replicas.begin(), assigned_replicas.end(),
        [&](const String & r) { return stripCloningSuffix(r) == replica_name; });
}

Strings KeeperReplicaAssignment::getTargetReplicasForMerge(
    const std::shared_ptr<KeeperReplicaAssignment> & assignment,
    MergeTreeDataFormatVersion format_version,
    const ReplicatedMergeTreeLogEntryData & entry,
    UInt64 replication_factor,
    LoggerPtr log)
{
    if (!assignment || replication_factor == 0)
        return {};

    String partition_id = entry.getPartitionId(format_version);
    if (partition_id.empty())
        return {};

    if (isPatchPartitionId(partition_id))
        partition_id = getOriginalPartitionIdOfPatch(partition_id);

    try
    {
        auto assign_map = assignment->getAssignments(nullptr, {partition_id});
        auto assign_it = assign_map.find(partition_id);
        return assign_it != assign_map.end() ? assign_it->second.replicas : Strings{};
    }
    catch (const DB::Exception &)
    {
        tryLogCurrentException(log, "Failed to read selective assignment for merge strategy, falling back to unfiltered");
        return {};
    }
}

bool KeeperReplicaAssignment::shouldSkipEntry(
    const std::shared_ptr<KeeperReplicaAssignment> & assignment,
    const ReplicatedMergeTreeLogEntryData & entry,
    MergeTreeDataFormatVersion format_version,
    const String & replica_name,
    const zkutil::ZooKeeperPtr & zk,
    LoggerPtr log)
{
    /// Partition-scoped entry types are filtered by assignment.
    /// DROP_RANGE and global types (ALTER_METADATA, CLEAR_COLUMN, etc.) always pass through.
    using LogEntry = ReplicatedMergeTreeLogEntryData;
    switch (entry.type)
    {
        case LogEntry::GET_PART:
        case LogEntry::ATTACH_PART:
        case LogEntry::MERGE_PARTS:
        case LogEntry::REPLACE_RANGE:
        case LogEntry::MUTATE_PART:
            break;
        default:
            return false;
    }

    String partition_id = entry.getPartitionId(format_version);
    if (partition_id.empty())
        return false;

    if (isPatchPartitionId(partition_id))
        partition_id = getOriginalPartitionIdOfPatch(partition_id);

    /// Use in-memory snapshot first (no TTL check, no ZK call on cache hit).
    auto assigned_map = assignment->getAssignments(nullptr, {partition_id});
    auto assigned_it = assigned_map.find(partition_id);
    Strings assigned = assigned_it != assigned_map.end() ? assigned_it->second.replicas : Strings{};

    bool is_assigned = !assigned.empty() && isReplicaAssigned(assigned, replica_name);

    if (!is_assigned)
    {
        /// Cache says we are not assigned -- verify via ZK because the cache may be stale.
        fiu_do_on(FailPoints::selective_replication_read_partitions_zk_error,
            throw Coordination::Exception(
                Coordination::Error::ZOPERATIONTIMEOUT,
                "Failpoint: ZK error during shouldSkipEntry Layer 2"));
        auto refreshed_map = assignment->getAssignments(zk, {partition_id}, /*force_refresh=*/true);
        auto refreshed_it = refreshed_map.find(partition_id);
        assigned = refreshed_it != refreshed_map.end() ? refreshed_it->second.replicas : Strings{};
        is_assigned = !assigned.empty() && isReplicaAssigned(assigned, replica_name);
    }

    if (assigned.empty())
        return false;  /// No assignment node for this partition -- do not skip.

    LOG_DEBUG(log, "shouldSkipEntry: entry_type={} partition_id={} replica_name={} assigned=[{}] result={}",
        entry.type, partition_id, replica_name,
        fmt::join(assigned, ","),
        is_assigned ? "NOT_SKIP(assigned)" : "SKIP");

    return !is_assigned;
}

void KeeperReplicaAssignment::ensureCacheForEntries(
    const std::shared_ptr<KeeperReplicaAssignment> & assignment,
    const std::vector<ReplicatedMergeTreeLogEntryPtr> & entries,
    MergeTreeDataFormatVersion format_version,
    const zkutil::ZooKeeperPtr & zk)
{
    std::unordered_set<String> partition_ids;
    for (const auto & entry : entries)
    {
        String pid = entry->getPartitionId(format_version);
        if (!pid.empty())
        {
            if (isPatchPartitionId(pid))
                pid = getOriginalPartitionIdOfPatch(pid);
            partition_ids.insert(pid);
        }
    }
    if (!partition_ids.empty())
    {
        FailPointInjection::pauseFailPoint(FailPoints::selective_replication_after_batch_ensure);
        assignment->getAssignments(zk, {partition_ids.begin(), partition_ids.end()});
    }
}

Strings KeeperReplicaAssignment::filterPartsForClone(
    const std::unordered_map<String, CachedEntry> & selective_assignments,
    const Strings & source_replica_parts,
    MergeTreeDataFormatVersion format_version,
    const String & replica_name,
    LoggerPtr log)
{
    if (selective_assignments.empty())
        return source_replica_parts;

    Strings result;
    for (const auto & active_part : source_replica_parts)
    {
        auto part_info_opt = MergeTreePartInfo::tryParsePartName(active_part, format_version);
        if (part_info_opt)
        {
            const String & partition_id = part_info_opt->getPartitionId();
            String lookup_id = part_info_opt->isPatch() ? part_info_opt->getOriginalPartitionId() : partition_id;
            auto it = selective_assignments.find(lookup_id);
            if (it != selective_assignments.end())
            {
                const Strings & assigned = it->second.replicas;
                bool is_assigned = isReplicaAssigned(assigned, replica_name);
                if (!is_assigned)
                {
                    LOG_DEBUG(log, "cloneReplica: skipping part {} (partition {}) -- not assigned to this replica ({}), assigned to [{}]",
                        active_part, partition_id, replica_name, fmt::join(assigned, ","));
                    continue;
                }
            }
            /// If partition not in assignments map, allow cloning to avoid data loss.
        }
        result.push_back(active_part);
    }
    return result;
}

bool KeeperReplicaAssignment::ensurePartitionAssignment(
    const zkutil::ZooKeeperPtr & zk,
    const String & partition_id,
    const Strings & all_replicas,
    UInt64 replication_factor,
    LoggerPtr logger,
    const String & operation_name)
{
    auto alloc_result = allocatePartitions(zk, {partition_id}, all_replicas, replication_factor);
    Strings assigned;
    if (auto it = alloc_result.assignments.find(partition_id); it != alloc_result.assignments.end())
        assigned = std::move(it->second);
    else if (auto it2 = alloc_result.existing.find(partition_id); it2 != alloc_result.existing.end())
        assigned = std::move(it2->second);
    if (assigned.empty())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Selective replication: allocatePartitions failed for partition {} during {}. "
            "Cannot determine which replicas should receive data.",
            partition_id, operation_name);
    }
    LOG_DEBUG(logger, "Selective replication: ensured assignment for partition {} in {}: [{}]",
        partition_id, operation_name, fmt::join(assigned, ","));
    return true;
}

void KeeperReplicaAssignment::ensurePartitionAssignments(
    const zkutil::ZooKeeperPtr & zk,
    const std::vector<String> & partition_ids,
    const Strings & all_replicas,
    UInt64 replication_factor,
    LoggerPtr logger,
    const String & operation_name)
{
    for (const auto & partition_id : partition_ids)
        ensurePartitionAssignment(zk, partition_id, all_replicas, replication_factor, logger, operation_name);
}

void KeeperReplicaAssignment::validateReplicationFactorConsistency(
    const zkutil::ZooKeeperPtr & zk,
    const String & zookeeper_path,
    UInt64 local_rf,
    int32_t num_existing_replicas)
{
    if (num_existing_replicas > 0)
    {
        String zk_config;
        if (zk->tryGet(zookeeper_path + "/selective/config", zk_config))
        {
            UInt64 zk_rf = SelectiveReplication::parseSelectiveConfig(zk_config);

            if (local_rf != zk_rf)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "replication_factor mismatch: table in ZooKeeper has replication_factor={}, "
                    "but this replica has replication_factor={}. All replicas must use the same value",
                    zk_rf, local_rf);
        }
        else if (local_rf > 0)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "First replica was created without selective replication (replication_factor=0), "
                "but this replica has replication_factor={}. All replicas must use the same value",
                local_rf);
        }
    }
}

void KeeperReplicaAssignment::validateReplicationFactorOnStartup(
    const zkutil::ZooKeeperPtr & zk,
    const String & zookeeper_path,
    UInt64 local_rf)
{
    if (local_rf == 0)
        return;

    String zk_config;
    if (zk && zk->tryGet(zookeeper_path + "/selective/config", zk_config))
    {
        UInt64 zk_rf = SelectiveReplication::parseSelectiveConfig(zk_config);
        if (local_rf != zk_rf)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "replication_factor mismatch on startup: ZooKeeper has replication_factor={}, "
                "but this replica has replication_factor={}. All replicas must use the same value",
                zk_rf, local_rf);
    }
}

}
