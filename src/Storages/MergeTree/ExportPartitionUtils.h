#pragma once

#include <vector>
#include <string>
#include <Common/Logger.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include "Storages/IStorage.h"

namespace DB
{

struct ExportReplicatedMergeTreePartitionManifest;

namespace ExportPartitionUtils
{
    std::vector<std::string> getExportedPaths(const LoggerPtr & log, const zkutil::ZooKeeperPtr & zk, const std::string & export_path);

    void commit(
        const ExportReplicatedMergeTreePartitionManifest & manifest,
        const StoragePtr & destination_storage,
        const zkutil::ZooKeeperPtr & zk,
        const LoggerPtr & log,
        const std::string & entry_path,
        const ContextPtr & context
    );
}

}
