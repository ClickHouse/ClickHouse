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
    void initialize();

    std::mutex mutex;
    size_t replicas_count{0};
    CoordinationMode mode{CoordinationMode::Default};
    std::atomic<bool> initialized{false};
    std::unique_ptr<ImplInterface> pimpl;
    ProgressCallback progress_callback; // store the callback only to bypass it to coordinator implementation
};

using ParallelReplicasReadingCoordinatorPtr = std::shared_ptr<ParallelReplicasReadingCoordinator>;

}
