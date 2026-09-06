#pragma once

#include <Storages/MergeTree/RequestResponse.h>

#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>
#include <unordered_set>

namespace DB
{
struct Progress;
using ProgressCallback = std::function<void(const Progress & progress)>;
using ReadCompletedCallback = std::function<void(const std::set<size_t> & used_replicas)>;

/// The main class to spread mark ranges across replicas dynamically
class ParallelReplicasReadingCoordinator
{
public:
    class ImplInterface;

    explicit ParallelReplicasReadingCoordinator(size_t replicas_count_);
    ~ParallelReplicasReadingCoordinator();

    InitialAllRangesAnnouncementResponse handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement);
    ParallelReadResponse handleRequest(ParallelReadRequest request);

    /// Called when some replica is unavailable and we skipped it.
    /// This is needed to "finalize" reading state e.g. spread all the marks using
    /// consistent hashing, because otherwise coordinator will continue working in
    /// "pending" state waiting for the unavailable replica to send the announcement.
    void markReplicaAsUnavailable(size_t replica_number);

    /// needed to report total rows to read
    void setProgressCallback(ProgressCallback callback);

    std::optional<size_t> getSnapshotReplicaNum() const { return snapshot_replica_num; }

    /// The fixed number of replicas the coordinator was sized for. Replica numbers in announcements
    /// must stay below it, so a reused coordinator must keep being fed the same set of replicas.
    size_t getReplicasCount() const { return replicas_count; }

    /// Pin the snapshot replica to a specific replica_num before any announcement arrives.
    /// Called by the initiator-local replica during pipeline build (synchronously, before any
    /// follower announcement can reach the coordinator).
    void setSnapshotReplicaNum(size_t replica_num);

    void setReadCompletedCallback(ReadCompletedCallback callback);

    /// Registers the authoritative part-name identity class of the table named `table_name` (a full
    /// table name, i.e. the `stream_id` of its parallel-replicas streams with the `#split_{i}` suffix
    /// stripped), derived on the initiator from its OWN `MergeTreeData` via `partNameIdentityOf`.
    ///
    /// Announcements carry `part_name_identity` only since
    /// `DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_PART_FINGERPRINT`, so every announcement from an
    /// older replica reports `PartNameIdentity::Unknown`. The initiator reads the same table as the
    /// announcing replicas, so its own classification of that table is authoritative and is applied
    /// to those announcements: without it, a mixed-version cluster reading a non-replicated
    /// `MergeTree` would see only `Unknown` announcements and fall back to the weaker mark-count
    /// check, which merges divergent same-named parts whose mark counts happen to coincide.
    ///
    /// Must be called before any announcement for the table can reach the coordinator (i.e. during
    /// pipeline build, right after the coordinator is created).
    void setAuthoritativePartNameIdentity(const String & table_name, RangesInDataPartDescription::PartNameIdentity identity);

private:
    bool isReadingCompleted() const;
    std::shared_ptr<ImplInterface> getCoordinator(const String & stream_id) const;
    std::shared_ptr<ImplInterface> getOrCreateCoordinator(const String & stream_id, CoordinationMode mode);

    std::mutex mutex;
    const size_t replicas_count{0};
    ProgressCallback progress_callback; // store the callback only to bypass it to coordinator implementation
    std::set<size_t> replicas_used;
    std::optional<size_t> snapshot_replica_num;
    std::optional<ReadCompletedCallback> read_completed_callback;
    std::atomic_bool is_reading_completed{false};

    /// `markReplicaAsUnavailable` might be called before any coordinator is created.
    /// In this case we remember the unavailable replicas and apply when coordinators are created.
    std::unordered_set<size_t> unavailable_replicas;

    /// Per-table coordinators. Each table gets its own ImplInterface instance.
    std::unordered_map<String, std::shared_ptr<ImplInterface>> stream_to_coordinator;

    /// Authoritative parts for each stream, captured from the snapshot replica's announcement.
    std::unordered_map<String, RangesInDataPartsDescription> stream_to_registered_parts;

    /// Per-table part-name identity classes derived on the initiator from its own `MergeTreeData`,
    /// keyed by full table name. See `setAuthoritativePartNameIdentity`.
    std::unordered_map<String, RangesInDataPartDescription::PartNameIdentity> table_to_part_name_identity;
};

using ParallelReplicasReadingCoordinatorPtr = std::shared_ptr<ParallelReplicasReadingCoordinator>;

}
