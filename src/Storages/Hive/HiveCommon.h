#pragma once

#include <Common/config.h>

#if USE_HIVE

#include <mutex>
#include <string>
#include <base/types.h>
#include <ThriftHiveMetastore.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Common/LRUCache.h>


namespace DB
{
class HiveMetastoreClient : public WithContext
{
public:
    struct FileInfo
    {
        std::string path;
        UInt64 last_modify_time; // in ms
        size_t size;

        FileInfo() = default;
        FileInfo(const FileInfo &) = default;
        FileInfo(const std::string & path_, UInt64 last_modify_time_, size_t size_) : path(path_), last_modify_time(last_modify_time_), size(size_) {}
    };

    struct PartitionInfo
    {
        Apache::Hadoop::Hive::Partition partition;
        std::vector<FileInfo> files;

        bool equal(const Apache::Hadoop::Hive::Partition & other);
    };

    // use for speeding up query metadata
    struct HiveTableMeta : public WithContext
    {
    public:
        HiveTableMeta(
            const std::string & db_name_,
            const std::string & table_name_,
            std::shared_ptr<Apache::Hadoop::Hive::Table> table_,
            std::map<std::string, PartitionInfo> && partition_infos_,
            ContextPtr context_)
            : WithContext(context_)
            , db_name(db_name_)
            , table_name(table_name_)
            , table(table_)
            , partition_infos(partition_infos_)
            , empty_partition_keys(table->partitionKeys.empty())
        {
        }
        std::vector<Apache::Hadoop::Hive::Partition> getPartitions();
        inline std::map<std::string, PartitionInfo> getPartitionInfos()
        {
            std::lock_guard lock{mutex};
            return partition_infos;
        }
        std::vector<FileInfo> getLocationFiles(const std::string & location);
        std::vector<FileInfo> getLocationFiles(const HDFSFSPtr & fs, const std::string & location);
        inline std::shared_ptr<Apache::Hadoop::Hive::Table> getTable() { return table; }

    private:
        std::string db_name;
        std::string table_name;

        std::mutex mutex;
        std::shared_ptr<Apache::Hadoop::Hive::Table> table;
        std::map<std::string, PartitionInfo> partition_infos;
        const bool empty_partition_keys;
    };

    explicit HiveMetastoreClient(std::shared_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient> client_, ContextPtr context_)
        : WithContext(context_), client(client_), table_meta_cache(1000)
    {
    }

    std::shared_ptr<HiveTableMeta> getTableMeta(const std::string & db_name, const std::string & table_name);
    void clearTableMeta(const std::string & db_name, const std::string & table_name);
    void setClient(std::shared_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient> client_);
    inline bool isExpired() const { return expired; }
    inline void setExpired() { expired = true; }
    inline void clearExpired() { expired = false; }

private:
    std::shared_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient> client;
    LRUCache<std::string, HiveTableMeta> table_meta_cache;
    mutable std::mutex mutex;
    std::atomic<bool> expired{false};

    Poco::Logger * log = &Poco::Logger::get("HMSClient");
};
}

#endif
