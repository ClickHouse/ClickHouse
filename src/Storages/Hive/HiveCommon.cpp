#include <Storages/Hive/HiveCommon.h>


#if USE_HDFS

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_HIVEMETASTORE;
    extern const int BAD_ARGUMENTS;
}

std::shared_ptr<HMSClient::HiveTableMeta> HMSClient::getTableMeta(const std::string & db_name, const std::string & table_name)
{
    LOG_TRACE(log, "get table meta:" + db_name + ":" + table_name);
    std::lock_guard lock{mutex};

    auto table = std::make_shared<Apache::Hadoop::Hive::Table>();
    std::vector<Apache::Hadoop::Hive::Partition> partitions;
    try
    {
        client->get_table(*table, db_name, table_name);

        /**
          * query the lastest partition info to check new change
          */
        client->get_partitions(partitions, db_name, table_name, -1);
    }
    catch (apache::thrift::transport::TTransportException & e)
    {
        setExpired();
        throw Exception("Hive Metastore expired because " + String(e.what()), ErrorCodes::NO_HIVEMETASTORE);
    }

    std::string cache_key = db_name + "." + table_name;
    std::shared_ptr<HMSClient::HiveTableMeta> result = table_meta_cache.get(cache_key);
    bool update_cache = false;
    std::map<std::string, PartitionInfo> old_partition_infos;
    std::map<std::string, PartitionInfo> partition_infos;
    if (result)
    {
        old_partition_infos = result->getPartitionInfos();
        if (old_partition_infos.size() != partitions.size())
            update_cache = true;
    }
    else
    {
        update_cache = true;
    }

    for (const auto & partition : partitions)
    {
        auto & pinfo = partition_infos[partition.sd.location];
        pinfo.partition = partition;

        // query files under the patition by hdfs api is costly, we reuse the files in case the partion has no change
        if (result)
        {
            auto it = old_partition_infos.find(partition.sd.location);
            if (it != old_partition_infos.end() && it->second.equal(partition))
                pinfo.files = it->second.files;
            else
                update_cache = true;
        }
    }

    /**
     * FIXME: force to update.
     * we have found some cases under which the partition's meta don't update if the table changed
     */
    //invalid_meta = true;

    if (update_cache)
    {
        LOG_INFO(log, "reload hive partition meta info:" + db_name + ":" + table_name);
        result = std::make_shared<HMSClient::HiveTableMeta>(db_name, table_name, table, std::move(partition_infos), getContext());
        table_meta_cache.set(cache_key, result);
    }
    return result;
}

void HMSClient::clearTableMeta(const std::string & db_name, const std::string & table_name)
{
    std::lock_guard lock{mutex};
    std::string cache_key = db_name + "." + table_name;
    std::shared_ptr<HMSClient::HiveTableMeta> meta = table_meta_cache.get(cache_key);
    if (meta)
        table_meta_cache.set(cache_key, nullptr);
}

void HMSClient::setClient(std::shared_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient> c)
{
    std::lock_guard lock{mutex};
    client = c;
    clearExpired();
}

bool HMSClient::PartitionInfo::equal(const Apache::Hadoop::Hive::Partition & other)
{
    // parameters include keys:numRows,numFiles,rawDataSize,totalSize,transient_lastDdlTime
    auto it1 = partition.parameters.begin();
    auto it2 = other.parameters.begin();
    for (; it1 != partition.parameters.end() && it2 != other.parameters.end(); ++it1, ++it2)
    {
        if (it1->first != it2->first || it1->second != it2->second)
            return false;
    }
    return (it1 == partition.parameters.end() && it2 == other.parameters.end());
}

std::vector<Apache::Hadoop::Hive::Partition> HMSClient::HiveTableMeta::getPartitions()
{
    std::vector<Apache::Hadoop::Hive::Partition> result;

    std::lock_guard lock{mutex};
    for (const auto & partition : partition_infos)
        result.emplace_back(partition.second.partition);
    return result;
}

std::vector<HMSClient::FileInfo> HMSClient::HiveTableMeta::getLocationFiles(const std::string & location)
{
    std::map<std::string, PartitionInfo>::const_iterator it;
    if (!empty_partition_keys)
    {
        std::lock_guard lock{mutex};
        it = partition_infos.find(location);
        if (it == partition_infos.end())
            throw Exception("invalid location " + location, ErrorCodes::BAD_ARGUMENTS);
        if (it->second.files != nullptr)
            return *(it->second.files);
    }

    auto fs_builder = createHDFSBuilder(getNameNodeUrl(table->sd.location), getContext()->getGlobalContext()->getConfigRef());
    auto fs = createHDFSFS(fs_builder.get());
    Poco::URI uri(location);
    HDFSFileInfo ls;
    ls.file_info = hdfsListDirectory(fs.get(), uri.getPath().c_str(), &ls.length);
    auto result = std::make_shared<std::vector<FileInfo>>();
    for (int i = 0; i < ls.length; ++i)
    {
        auto & finfo = ls.file_info[i];
        if (finfo.mKind != 'D' && finfo.mSize > 0)
            result->emplace_back(String(finfo.mName), finfo.mLastMod, finfo.mSize);
    }

    if (!empty_partition_keys)
    {
        std::lock_guard lock{mutex};
        partition_infos[location].files = result;
    }
    return *result;
}

std::vector<HMSClient::FileInfo> HMSClient::HiveTableMeta::getLocationFiles(const HDFSFSPtr & fs, const std::string & location)
{
    std::map<std::string, PartitionInfo>::const_iterator it;
    if (!empty_partition_keys)
    {
        std::lock_guard lock{mutex};
        it = partition_infos.find(location);
        if (it == partition_infos.end())
            throw Exception("invalid location " + location, ErrorCodes::BAD_ARGUMENTS);
        if (it->second.files != nullptr)
            return *(it->second.files);
    }

    Poco::URI location_uri(location);
    HDFSFileInfo ls;
    ls.file_info = hdfsListDirectory(fs.get(), location_uri.getPath().c_str(), &ls.length);
    auto result = std::make_shared<std::vector<FileInfo>>();
    for (int i = 0; i < ls.length; ++i)
    {
        auto & finfo = ls.file_info[i];
        if (finfo.mKind != 'D' && finfo.mSize > 0)
            result->emplace_back(String(finfo.mName), finfo.mLastMod, finfo.mSize);
    }

    if (!empty_partition_keys)
    {
        std::lock_guard lock{mutex};
        partition_infos[location].files = result;
    }
    return *result;
}


}
#endif
