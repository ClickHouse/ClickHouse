#pragma once

#include <memory>
#include <Storages/MergeTree/RequestResponse.h>

namespace DB
{

class ParallelReplicasReadingCoordinator
{
public:
    ParallelReplicasReadingCoordinator();
    ~ParallelReplicasReadingCoordinator();
    PartitionReadResponse handleRequest(PartitionReadRequest request);
private:
    class Impl;
    std::unique_ptr<Impl> pimpl;
};

using ParallelReplicasReadingCoordinatorPtr = std::shared_ptr<ParallelReplicasReadingCoordinator>;

}
