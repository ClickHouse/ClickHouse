#pragma once

#include <DataStreams/IBlockInputStream.h>
#include "StoragePostgreSQLReplica.h"
#include "PostgreSQLReplicaConsumerBuffer.h"
#include "buffer_fwd.h"


namespace DB
{

class PostgreSQLReplicaBlockInputStream : public IBlockInputStream
{

public:
    PostgreSQLReplicaBlockInputStream(
            StoragePostgreSQLReplica & storage_,
            ConsumerBufferPtr buffer_,
            const StorageMetadataPtr & metadata_snapshot_,
            std::shared_ptr<Context> context_,
            const Names & columns,
            size_t max_block_size_);

    ~PostgreSQLReplicaBlockInputStream() override;

    String getName() const override { return storage.getName(); }
    Block getHeader() const override { return sample_block; }

    void readPrefixImpl() override;
    Block readImpl() override;
    void readSuffixImpl() override;

private:
    StoragePostgreSQLReplica & storage;
    ConsumerBufferPtr buffer;
    StorageMetadataPtr metadata_snapshot;
    std::shared_ptr<Context> context;
    Names column_names;
    const size_t max_block_size;

    bool finished = false;
    const Block non_virtual_header;
    Block sample_block;
    const Block virtual_header;
};

}
