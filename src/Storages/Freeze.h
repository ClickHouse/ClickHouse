#pragma once

#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

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
    int version = 1;
    bool is_replicated{false};
    bool is_remote{false};
    String replica_name;
    String zookeeper_name;
    String table_shared_id;
};

class Unfreezer
{
public:
    PartitionCommandsResultInfo unfreezePartitionsFromTableDirectory(MergeTreeData::MatcherFn matcher, const String & backup_name, const Disks & disks, const fs::path & table_directory, ContextPtr local_context);
    BlockIO unfreeze(const String & backup_name, ContextPtr local_context);
private:
    Poco::Logger * log = &Poco::Logger::get("Unfreezer");
    static constexpr std::string_view backup_directory_prefix = "shadow";
    static bool removeFreezedPart(DiskPtr disk, const String & path, const String & part_name, ContextPtr local_context);
};

}
