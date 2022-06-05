#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/ExternalResultDescription.h>
#include "SelectQueryInfo.h"

namespace DB
{
class ZooKeeperSource final : public SourceWithProgress
{
public:
    ZooKeeperSource(
        zkutil::ZooKeeperPtr connection_,
        ContextPtr context_,
        const SelectQueryInfo & query_info_,
        const Block & sample_block_,
        const std::string & path_);
        // const std::string & path_);

    String getName() const override { return "ZooKeeper"; }

private:
    Chunk generate() override;

    zkutil::ZooKeeperPtr connection;
    ContextPtr context;
    const SelectQueryInfo query_info;
    int count = 0;

    std::string p1; 

    // const std::string path;

    ExternalResultDescription description;
};

}
