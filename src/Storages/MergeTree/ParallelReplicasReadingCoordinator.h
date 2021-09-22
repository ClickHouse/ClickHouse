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
    PartitionReadResponce handleRequest(PartitionReadRequest request);
private:
    class Impl;
    std::unique_ptr<Impl> pimpl;
};

}
