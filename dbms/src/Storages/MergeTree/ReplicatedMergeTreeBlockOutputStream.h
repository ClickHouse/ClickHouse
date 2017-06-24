#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Core/Types.h>

namespace Poco { class Logger; }

namespace DB
{

class StorageReplicatedMergeTree;


class ReplicatedMergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
    ReplicatedMergeTreeBlockOutputStream(StorageReplicatedMergeTree & storage_,
        size_t quorum_, size_t quorum_timeout_ms_);

    void write(const Block & block) override;

private:
    StorageReplicatedMergeTree & storage;
    size_t quorum;
    size_t quorum_timeout_ms;

    using Logger = Poco::Logger;
    Logger * log;
};

}
