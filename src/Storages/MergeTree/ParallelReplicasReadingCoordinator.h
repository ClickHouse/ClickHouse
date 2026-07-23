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

    void handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement);
    ParallelReadResponse handleRequest(ParallelReadRequest request);

    /// Called when some replica is unavailable and we skipped it.
    /// This is needed to "finalize" reading state e.g. spread all the marks using
    /// consistent hashing, because otherwise coordinator will continue working in
    /// "pending" state waiting for the unavailable replica to send the announcement.
    void markReplicaAsUnavailable(size_t replica_number);

    /// needed to report total rows to read
    void setProgressCallback(ProgressCallback callback);

    /// snapshot replica - first replica the coordinator got InitialAllRangesAnnouncement from
    std::optional<size_t> getSnapshotReplicaNum() const { return snapshot_replica_num; }

    void setReadCompletedCallback(ReadCompletedCallback callback);

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
};

using ParallelReplicasReadingCoordinatorPtr = std::shared_ptr<ParallelReplicasReadingCoordinator>;

}
