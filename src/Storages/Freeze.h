#pragma once

#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

namespace ErrorCodes {
    extern const int SUPPORT_IS_DISABLED;
}

/// Special metadata used during freeze table. Required for zero-copy
/// replication.
struct FreezeMetaData
{
public:
    void fill(const StorageReplicatedMergeTree & storage);

    void save(DiskPtr data_disk, const String & path) const;

    bool load(DiskPtr data_disk, const String & path);

    static void clean(DiskPtr data_disk, const String & path);

private:
    static String getFileName(const String & path);

public:
    int version = 2;
    String replica_name;
    String zookeeper_name;
    String table_shared_id;
};

class Unfreezer
{
public:
    Unfreezer(ContextPtr context) : local_context(context), zookeeper() { 
        const auto & config = local_context->getConfigRef();
        static constexpr auto config_key = "enable_system_unfreeze";
        if (!config.getBool(config_key, false)) {
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Support for SYSTEM UNFREEZE query is disabled. You can enable it via '{}' server setting", config_key);
        }
        zookeeper = this->local_context->getZooKeeper(); 
    }
    PartitionCommandsResultInfo unfreezePartitionsFromTableDirectory(MergeTreeData::MatcherFn matcher, const String & backup_name, const Disks & disks, const fs::path & table_directory);
    BlockIO unfreeze(const String & backup_name);
private:
    ContextPtr local_context;
    zkutil::ZooKeeperPtr zookeeper;
    Poco::Logger * log = &Poco::Logger::get("Unfreezer");
    static constexpr std::string_view backup_directory_prefix = "shadow";
    static bool removeFreezedPart(DiskPtr disk, const String & path, const String & part_name, ContextPtr local_context,  zkutil::ZooKeeperPtr zookeeper);
};

}
