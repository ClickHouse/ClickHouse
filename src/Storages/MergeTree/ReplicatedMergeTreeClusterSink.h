#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Interpreters/Context_fwd.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class StorageReplicatedMergeTree;
struct ReplicatedMergeTreeClusterReplica;

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class ReplicatedMergeTreeClusterSink : public SinkToStorage
{
public:
    ReplicatedMergeTreeClusterSink(
        StorageReplicatedMergeTree & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        ContextPtr context_);
    ~ReplicatedMergeTreeClusterSink() override;

    String getName() const override { return "ReplicatedMergeTreeClusterSink"; }

    void consume(Chunk chunk) override;

private:
    StorageReplicatedMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    Poco::Logger * log;

    void sendBlockToReplica(const ReplicatedMergeTreeClusterReplica & replica, const Block & block);
};

}
