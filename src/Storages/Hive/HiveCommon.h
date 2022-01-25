#pragma once

#include <Common/config.h>

#if USE_HIVE

#include <mutex>
#include <string>
#include <ThriftHiveMetastore.h>

#include <base/types.h>
#include <Common/LRUCache.h>
#include <IO/Marshallable.h>
#include <Storages/HDFS/HDFSCommon.h>


namespace DB
{

class HiveMetastoreClient : public WithContext
{
public:

    struct FileInfo : public Marshallable
    {
        String path;
        UInt64 last_modify_time; /// In ms
        size_t size;

        explicit FileInfo() = default;
        FileInfo & operator = (const FileInfo &) = default;
        FileInfo(const FileInfo &) = default;
        ~FileInfo() override = default;
        FileInfo(const String & path_, UInt64 last_modify_time_, size_t size_)
            : path(path_), last_modify_time(last_modify_time_), size(size_)
        {
        }

        void marshal(MarshallablePack & p) const override
        {
            p << path << last_modify_time << size;
        }

        void unmarshal(MarshallableUnPack & p) override
        {
            p >> path >> last_modify_time >> size;
        }

        std::ostream & trace(std::ostream & os) const override
        {
            os << "path=" << path << ","
                << "last_modify_time=" << last_modify_time << ","
                << "size=" << size;
            return os;
        }
    };

    struct PartitionInfo
    {
        Apache::Hadoop::Hive::Partition partition;
        std::vector<FileInfo> files;
        bool initialized = false; /// If true, files are initialized.

        explicit PartitionInfo(const Apache::Hadoop::Hive::Partition & partition_): partition(partition_) {}
        bool haveSameParameters(const Apache::Hadoop::Hive::Partition & other) const;
    };


    /// Used for speeding up metadata query process.
    struct HiveTableMetadata : public WithContext
    {
    public:
        HiveTableMetadata(
            const String & db_name_,
            const String & table_name_,
            std::shared_ptr<Apache::Hadoop::Hive::Table> table_,
            const std::map<String, PartitionInfo> & partition_infos_,
            ContextPtr context_)
            : WithContext(context_)
            , db_name(db_name_)
            , table_name(table_name_)
            , table(table_)
            , partition_infos(partition_infos_)
            , empty_partition_keys(table->partitionKeys.empty())
        {
        }


        std::map<String, PartitionInfo> & getPartitionInfos()
        {
            std::lock_guard lock{mutex};
            return partition_infos;
        }

        std::shared_ptr<Apache::Hadoop::Hive::Table> getTable() const
        {
            std::lock_guard lock{mutex};
            return table;
        }

        std::vector<Apache::Hadoop::Hive::Partition> getPartitions() const;

        std::vector<FileInfo> getFilesByLocation(const HDFSFSPtr & fs, const String & location);

    private:
        String db_name;
        String table_name;

        mutable std::mutex mutex;
        std::shared_ptr<Apache::Hadoop::Hive::Table> table;
        std::map<String, PartitionInfo> partition_infos;
        const bool empty_partition_keys;

        Poco::Logger * log = &Poco::Logger::get("HiveMetastoreClient");
    };

    using HiveTableMetadataPtr = std::shared_ptr<HiveMetastoreClient::HiveTableMetadata>;

    explicit HiveMetastoreClient(std::shared_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient> client_, ContextPtr context_)
        : WithContext(context_), client(client_), table_metadata_cache(1000)
    {
    }

    HiveTableMetadataPtr getTableMetadata(const String & db_name, const String & table_name);
    void clearTableMetadata(const String & db_name, const String & table_name);
    void setClient(std::shared_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient> client_);
    bool isExpired() const { return expired; }
    void setExpired() { expired = true; }
    void clearExpired() { expired = false; }

private:
    static String getCacheKey(const String & db_name, const String & table_name)  { return db_name + "." + table_name; }

    bool shouldUpdateTableMetadata(
        const String & db_name, const String & table_name, const std::vector<Apache::Hadoop::Hive::Partition> & partitions);

    std::shared_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient> client;
    LRUCache<String, HiveTableMetadata> table_metadata_cache;
    mutable std::mutex mutex;
    std::atomic<bool> expired{false};

    Poco::Logger * log = &Poco::Logger::get("HiveMetastoreClient");
};

using HiveMetastoreClientPtr = std::shared_ptr<HiveMetastoreClient>;
class HiveMetastoreClientFactory final : private boost::noncopyable
{
public:
    static HiveMetastoreClientFactory & instance();

    HiveMetastoreClientPtr getOrCreate(const String & name, ContextPtr context);

private:
    std::mutex mutex;
    std::map<String, HiveMetastoreClientPtr> clients;

    const int conn_timeout_ms = 10000;
    const int recv_timeout_ms = 10000;
    const int send_timeout_ms = 10000;
};

}


#endif
