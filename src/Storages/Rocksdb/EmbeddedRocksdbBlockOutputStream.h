#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Interpreters/Context.h>
#include <Storages/Rocksdb/StorageEmbeddedRocksdb.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class EmbeddedRocksdbBlockOutputStream : public IBlockOutputStream
{
public:
    explicit EmbeddedRocksdbBlockOutputStream(
        StorageEmbeddedRocksdb & storage_,
        const StorageMetadataPtr & metadata_snapshot_)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
    {}

    Block getHeader() const override;
    void write(const Block & block) override;

private:
    StorageEmbeddedRocksdb & storage;
    StorageMetadataPtr metadata_snapshot;
};

}
