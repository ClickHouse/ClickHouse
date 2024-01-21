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

    explicit ParallelReplicasReadingCoordinator(size_t replicas_count_, size_t mark_segment_size_ = 0);
    ~ParallelReplicasReadingCoordinator();

    void handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement);
    ParallelReadResponse handleRequest(ParallelReadRequest request);

    /// needed to report total rows to read
    void setProgressCallback(ProgressCallback callback);

private:
    void initialize();

    std::mutex mutex;
    size_t replicas_count{0};
    size_t mark_segment_size{0};
    CoordinationMode mode{CoordinationMode::Default};
    std::unique_ptr<ImplInterface> pimpl;
    ProgressCallback progress_callback; // store the callback only to bypass it to coordinator implementation
    std::set<size_t> replicas_used;
};

using ParallelReplicasReadingCoordinatorPtr = std::shared_ptr<ParallelReplicasReadingCoordinator>;

}
