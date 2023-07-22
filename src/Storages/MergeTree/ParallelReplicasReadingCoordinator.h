#pragma once

#include <memory>
#include <Storages/MergeTree/RequestResponse.h>


namespace DB
{

/// The main class to spread mark ranges across replicas dynamically
/// The reason why it uses pimpl - this header file is included in
/// multiple other files like Context or RemoteQueryExecutor
class ParallelReplicasReadingCoordinator
{
public:
    class ImplInterface;

    explicit ParallelReplicasReadingCoordinator(size_t replicas_count_);
    ~ParallelReplicasReadingCoordinator();

    void setMode(CoordinationMode mode);
    void handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement);
    ParallelReadResponse handleRequest(ParallelReadRequest request);

private:
    void initialize();

    CoordinationMode mode{CoordinationMode::Default};
    size_t replicas_count{0};
    std::atomic<bool> initialized{false};
    std::unique_ptr<ImplInterface> pimpl;
};

using ParallelReplicasReadingCoordinatorPtr = std::shared_ptr<ParallelReplicasReadingCoordinator>;

}
