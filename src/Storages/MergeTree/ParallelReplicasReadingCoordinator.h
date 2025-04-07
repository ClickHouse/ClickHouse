#pragma once

#include <memory>
#include <Storages/MergeTree/RequestResponse.h>


namespace DB
{
struct Progress;
using ProgressCallback = std::function<void(const Progress & progress)>;

/// The main class to spread mark ranges across replicas dynamically
class ParallelReplicasReadingCoordinator
{
public:
    class ImplInterface;

    explicit ParallelReplicasReadingCoordinator(size_t replicas_count_);
    ~ParallelReplicasReadingCoordinator();

    void handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement);
    ParallelReadResponse handleRequest(ParallelReadRequest request);

    /// Called when some replica is unavailable and we skipped it.
    /// This is needed to "finalize" reading state e.g. spread all the marks using
    /// consistent hashing, because otherwise coordinator will continue working in
    /// "pending" state waiting for the unavailable replica to send the announcement.
    void markReplicaAsUnavailable(size_t replica_number);

    /// needed to report total rows to read
    void setProgressCallback(ProgressCallback callback);

private:
    void initialize(CoordinationMode mode);

    std::mutex mutex;
    const size_t replicas_count{0};
    std::unique_ptr<ImplInterface> pimpl;
    ProgressCallback progress_callback; // store the callback only to bypass it to coordinator implementation
    std::set<size_t> replicas_used;

    /// To initialize `pimpl` we need to know the coordinator mode. We can know it only from initial announcement or regular request.
    /// The problem is `markReplicaAsUnavailable` might be called before any of these requests happened.
    /// In this case we will remember the numbers of unavailable replicas and apply this knowledge later on initialization.
    std::vector<size_t> unavailable_nodes_registered_before_initialization;
};

using ParallelReplicasReadingCoordinatorPtr = std::shared_ptr<ParallelReplicasReadingCoordinator>;

}
