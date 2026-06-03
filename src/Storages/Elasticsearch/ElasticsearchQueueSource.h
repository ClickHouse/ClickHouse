#pragma once

#include <Processors/ISource.h>
#include <Storages/Elasticsearch/StorageElasticsearchQueue.h>

namespace DB
{

class ElasticsearchQueueSource final : public ISource
{
public:
    ElasticsearchQueueSource(
        StorageElasticsearchQueue & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        const Names & columns,
        size_t max_block_size_,
        bool commit_in_suffix_);

    ~ElasticsearchQueueSource() override;

    String getName() const override { return storage.getName(); }

    Chunk generate() override;

    void commit();

    bool isStalled() const { return stalled; }

private:
    void releaseKeeperLock();

    StorageElasticsearchQueue & storage;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    Names column_names;
    UInt64 max_block_size;
    bool commit_in_suffix;

    bool is_finished = false;
    bool stalled = false;
    bool committed = false;
    String checkpoint;
    zkutil::ZooKeeperPtr checkpoint_zookeeper;
    zkutil::EphemeralNodeHolder::Ptr checkpoint_lock;
};

}
