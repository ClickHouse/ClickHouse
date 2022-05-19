#pragma once

#include <memory>
#include <Common/config.h>

#if USE_HIVE

#include <mutex>
#include <string>
#include <ThriftHiveMetastore.h>

#include <base/types.h>
#include <Common/LRUCache.h>
#include <Common/PoolBase.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/Hive/HiveFile.h>


namespace DB
{

using ThriftHiveMetastoreClientBuilder = std::function<std::shared_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient>()>;

class ThriftHiveMetastoreClientPool : public PoolBase<Apache::Hadoop::Hive::ThriftHiveMetastoreClient>
{
public:
    using Object = Apache::Hadoop::Hive::ThriftHiveMetastoreClient;
    using ObjectPtr = std::shared_ptr<Object>;
    using Entry = PoolBase<Apache::Hadoop::Hive::ThriftHiveMetastoreClient>::Entry;
    explicit ThriftHiveMetastoreClientPool(ThriftHiveMetastoreClientBuilder builder_);

protected:
    ObjectPtr allocObject() override
    {
        return builder();
    }

private:
    ThriftHiveMetastoreClientBuilder builder;
};
class HiveMetastoreClient
{
public:
    struct FileInfo
    {
        String path;
        UInt64 last_modify_time; /// In ms
        size_t size;

        explicit FileInfo() = default;
        FileInfo & operator = (const FileInfo &) = default;
        FileInfo(const FileInfo &) = default;
        FileInfo(const String & path_, UInt64 last_modify_time_, size_t size_)
            : path(path_), last_modify_time(last_modify_time_), size(size_)
        {
        }
    };

    struct PartitionInfo
    {
        Apache::Hadoop::Hive::Partition partition;
        std::vector<FileInfo> files;
        bool initialized = false; /// If true, files are initialized.

        explicit PartitionInfo(const Apache::Hadoop::Hive::Partition & partition_): partition(partition_) {}
        PartitionInfo(PartitionInfo &&) = default;

        bool haveSameParameters(const Apache::Hadoop::Hive::Partition & other) const;
    };

    class HiveTableMetadata;
    using HiveTableMetadataPtr = std::shared_ptr<HiveTableMetadata>;

    /// Used for speeding up metadata query process.
    class HiveTableMetadata : boost::noncopyable
    {
    public:
        HiveTableMetadata(
            const String & db_name_,
            const String & table_name_,
            std::shared_ptr<Apache::Hadoop::Hive::Table> table_,
            const std::vector<Apache::Hadoop::Hive::Partition> & partitions_)
            : db_name(db_name_)
            , table_name(table_name_)
            , table(std::move(table_))
            , empty_partition_keys(table->partitionKeys.empty())
            , hive_files_cache(std::make_shared<HiveFilesCache>(10000))
        {
            std::lock_guard lock(mutex);
            for (const auto & partition : partitions_)
                partition_infos.emplace(partition.sd.location, PartitionInfo(partition));
        }

        std::shared_ptr<Apache::Hadoop::Hive::Table> getTable() const { return table; }

        std::vector<Apache::Hadoop::Hive::Partition> getPartitions() const;

        std::vector<FileInfo> getFilesByLocation(const HDFSFSPtr & fs, const String & location);

        HiveFilesCachePtr getHiveFilesCache() const;

        void updateIfNeeded(const std::vector<Apache::Hadoop::Hive::Partition> & partitions);

    private:
        bool shouldUpdate(const std::vector<Apache::Hadoop::Hive::Partition> & partitions);

        const String db_name;
        const String table_name;
        const std::shared_ptr<Apache::Hadoop::Hive::Table> table;

        /// Mutex to protect partition_infos.
        mutable std::mutex mutex;
        std::map<String, PartitionInfo> partition_infos;

        const bool empty_partition_keys;
        const HiveFilesCachePtr hive_files_cache;

        Poco::Logger * log = &Poco::Logger::get("HiveMetastoreClient");
    };


    explicit HiveMetastoreClient(ThriftHiveMetastoreClientBuilder builder_)
        : table_metadata_cache(1000)
        , client_pool(builder_)
    {
    }

    HiveTableMetadataPtr getTableMetadata(const String & db_name, const String & table_name);
    // Access hive table information by hive client
    std::shared_ptr<Apache::Hadoop::Hive::Table> getHiveTable(const String & db_name, const String & table_name);
    void clearTableMetadata(const String & db_name, const String & table_name);

private:
    static String getCacheKey(const String & db_name, const String & table_name)  { return db_name + "." + table_name; }

    void tryCallHiveClient(std::function<void(ThriftHiveMetastoreClientPool::Entry &)> func);

    LRUCache<String, HiveTableMetadata> table_metadata_cache;
    ThriftHiveMetastoreClientPool client_pool;

    Poco::Logger * log = &Poco::Logger::get("HiveMetastoreClient");
};

using HiveMetastoreClientPtr = std::shared_ptr<HiveMetastoreClient>;
class HiveMetastoreClientFactory final : private boost::noncopyable
{
public:
    static HiveMetastoreClientFactory & instance();

    HiveMetastoreClientPtr getOrCreate(const String & name);

private:
    static std::shared_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient> createThriftHiveMetastoreClient(const String & name);

    std::mutex mutex;
    std::map<String, HiveMetastoreClientPtr> clients;
};

}


#endif
