#include "config.h"

#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSettings.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueIFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueOrderedFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueUnorderedFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueTableMetadata.h>
#include <Storages/StorageSnapshot.h>
#include <base/sleep.h>
#include <Common/CurrentThread.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/getRandomASCIIString.h>
#include <Common/randomSeed.h>
#include <numeric>


namespace ProfileEvents
{
    extern const Event ObjectStorageQueueCleanupMaxSetSizeOrTTLMicroseconds;
    extern const Event ObjectStorageQueueLockLocalFileStatusesMicroseconds;
};

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int REPLICA_ALREADY_EXISTS;
}

namespace
{
    UInt64 getCurrentTime()
    {
        return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    }

    size_t generateRescheduleInterval(size_t min, size_t max)
    {
        /// Use more or less random interval for unordered mode cleanup task.
        /// So that distributed processing cleanup tasks would not schedule cleanup at the same time.
        pcg64 rng(randomSeed());
        return min + rng() % (max - min + 1);
    }

    zkutil::ZooKeeperPtr getZooKeeper()
    {
        return Context::getGlobalContextInstance()->getZooKeeper();
    }
}

class ObjectStorageQueueMetadata::LocalFileStatuses
{
public:
    LocalFileStatuses() = default;

    FileStatuses getAll() const
    {
        auto lk = lock();
        return file_statuses;
    }

    FileStatusPtr get(const std::string & filename, bool create)
    {
        auto lk = lock();
        auto it = file_statuses.find(filename);
        if (it == file_statuses.end())
        {
            if (create)
                it = file_statuses.emplace(filename, std::make_shared<FileStatus>()).first;
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "File status for {} doesn't exist", filename);
        }
        return it->second;
    }

    bool remove(const std::string & filename, bool if_exists)
    {
        auto lk = lock();
        auto it = file_statuses.find(filename);
        if (it == file_statuses.end())
        {
            if (if_exists)
                return false;
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "File status for {} doesn't exist", filename);
        }
        file_statuses.erase(it);
        return true;
    }

private:
    FileStatuses file_statuses;
    mutable std::mutex mutex;

    std::unique_lock<std::mutex> lock() const
    {
        auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::ObjectStorageQueueLockLocalFileStatusesMicroseconds);
        return std::unique_lock(mutex);
    }
};

ObjectStorageQueueMetadata::ObjectStorageQueueMetadata(
    const fs::path & zookeeper_path_,
    const ObjectStorageQueueTableMetadata & table_metadata_,
    size_t cleanup_interval_min_ms_,
    size_t cleanup_interval_max_ms_)
    : table_metadata(table_metadata_)
    , mode(table_metadata.getMode())
    , zookeeper_path(zookeeper_path_)
    , buckets_num(getBucketsNum(table_metadata_))
    , cleanup_interval_min_ms(cleanup_interval_min_ms_)
    , cleanup_interval_max_ms(cleanup_interval_max_ms_)
    , log(getLogger("StorageObjectStorageQueue(" + zookeeper_path_.string() + ")"))
    , local_file_statuses(std::make_shared<LocalFileStatuses>())
{
    if (mode == ObjectStorageQueueMode::UNORDERED
        && (table_metadata.tracked_files_limit || table_metadata.tracked_files_ttl_sec))
    {
        task = Context::getGlobalContextInstance()->getSchedulePool().createTask(
            "ObjectStorageQueueCleanupFunc",
            [this] { cleanupThreadFunc(); });

        task->activate();
        task->scheduleAfter(
            generateRescheduleInterval(
                cleanup_interval_min_ms, cleanup_interval_max_ms));
    }
    LOG_TRACE(log, "Mode: {}, buckets: {}, processing threads: {}, result buckets num: {}",
              table_metadata.mode, table_metadata.buckets, table_metadata.processing_threads_num, buckets_num);

}

ObjectStorageQueueMetadata::~ObjectStorageQueueMetadata()
{
    shutdown();
}

void ObjectStorageQueueMetadata::shutdown()
{
    shutdown_called = true;
    if (task)
        task->deactivate();
}

ObjectStorageQueueMetadata::FileStatusPtr ObjectStorageQueueMetadata::getFileStatus(const std::string & path)
{
    return local_file_statuses->get(path, /* create */false);
}

ObjectStorageQueueMetadata::FileStatuses ObjectStorageQueueMetadata::getFileStatuses() const
{
    return local_file_statuses->getAll();
}

ObjectStorageQueueMetadata::FileMetadataPtr ObjectStorageQueueMetadata::getFileMetadata(
    const std::string & path,
    ObjectStorageQueueOrderedFileMetadata::BucketInfoPtr bucket_info)
{
    auto file_status = local_file_statuses->get(path, /* create */true);
    switch (mode)
    {
        case ObjectStorageQueueMode::ORDERED:
            return std::make_shared<ObjectStorageQueueOrderedFileMetadata>(
                zookeeper_path,
                path,
                file_status,
                bucket_info,
                buckets_num,
                table_metadata.loading_retries,
                log);
        case ObjectStorageQueueMode::UNORDERED:
            return std::make_shared<ObjectStorageQueueUnorderedFileMetadata>(
                zookeeper_path,
                path,
                file_status,
                table_metadata.loading_retries,
                log);
    }
}

size_t ObjectStorageQueueMetadata::getBucketsNum(const ObjectStorageQueueTableMetadata & metadata)
{
    if (metadata.buckets)
        return metadata.buckets;
    return metadata.processing_threads_num;
}

bool ObjectStorageQueueMetadata::useBucketsForProcessing() const
{
    return mode == ObjectStorageQueueMode::ORDERED && (buckets_num > 1);
}

ObjectStorageQueueMetadata::Bucket ObjectStorageQueueMetadata::getBucketForPath(const std::string & path) const
{
    return ObjectStorageQueueOrderedFileMetadata::getBucketForPath(path, buckets_num);
}

ObjectStorageQueueOrderedFileMetadata::BucketHolderPtr
ObjectStorageQueueMetadata::tryAcquireBucket(const Bucket & bucket, const Processor & processor)
{
    return ObjectStorageQueueOrderedFileMetadata::tryAcquireBucket(zookeeper_path, bucket, processor, log);
}

ObjectStorageQueueTableMetadata ObjectStorageQueueMetadata::syncWithKeeper(
    const fs::path & zookeeper_path,
    const ObjectStorageQueueSettings & settings,
    const ColumnsDescription & columns,
    const std::string & format,
    LoggerPtr log)
{
    ObjectStorageQueueTableMetadata table_metadata(settings, columns, format);

    std::vector<std::string> metadata_paths;
    size_t buckets_num = 0;
    if (settings.mode == ObjectStorageQueueMode::ORDERED)
    {
        buckets_num = getBucketsNum(table_metadata);
        if (buckets_num == 0)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot have zero values of `processing_threads_num` and `buckets`");

        LOG_TRACE(log, "Local buckets num: {}", buckets_num);

        metadata_paths = ObjectStorageQueueOrderedFileMetadata::getMetadataPaths(buckets_num);
    }
    else
    {
        metadata_paths = ObjectStorageQueueUnorderedFileMetadata::getMetadataPaths();
    }

    const auto table_metadata_path = zookeeper_path / "metadata";
    auto zookeeper = getZooKeeper();
    zookeeper->createAncestors(zookeeper_path);

    for (size_t i = 0; i < 1000; ++i)
    {
        if (zookeeper->exists(table_metadata_path))
        {
            const auto metadata_str = zookeeper->get(fs::path(zookeeper_path) / "metadata");
            const auto metadata_from_zk = ObjectStorageQueueTableMetadata::parse(metadata_str);

            LOG_TRACE(log, "Metadata in keeper: {}", metadata_str);

            table_metadata.checkEquals(metadata_from_zk);
            return table_metadata;
        }

        Coordination::Requests requests;
        requests.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "", zkutil::CreateMode::Persistent));
        requests.emplace_back(zkutil::makeCreateRequest(
                                  table_metadata_path, table_metadata.toString(), zkutil::CreateMode::Persistent));

        for (const auto & path : metadata_paths)
        {
            const auto zk_path = zookeeper_path / path;
            requests.emplace_back(zkutil::makeCreateRequest(zk_path, "", zkutil::CreateMode::Persistent));
        }

        if (!table_metadata.last_processed_path.empty())
        {
            ObjectStorageQueueOrderedFileMetadata(
                zookeeper_path,
                table_metadata.last_processed_path,
                std::make_shared<FileStatus>(),
                /* bucket_info */nullptr,
                buckets_num,
                table_metadata.loading_retries,
                log).setProcessedAtStartRequests(requests, zookeeper);
        }

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(requests, responses);
        if (code == Coordination::Error::ZNODEEXISTS)
        {
            auto exception = zkutil::KeeperMultiException(code, requests, responses);

            LOG_INFO(log, "Got code `{}` for path: {}. "
                     "It looks like the table {} was created by another server at the same moment, "
                     "will retry",
                     code, exception.getPathForFirstFailedOp(), zookeeper_path.string());
            continue;
        }
        else if (code != Coordination::Error::ZOK)
            zkutil::KeeperMultiException::check(code, requests, responses);

        return table_metadata;
    }

    throw Exception(
        ErrorCodes::REPLICA_ALREADY_EXISTS,
        "Cannot create table, because it is created concurrently every time or because "
        "of wrong zookeeper path or because of logical error");
}

void ObjectStorageQueueMetadata::cleanupThreadFunc()
{
    /// A background task is responsible for maintaining
    /// table_metadata.tracked_files_limit and max_set_age settings for `unordered` processing mode.

    if (shutdown_called)
        return;

    try
    {
        cleanupThreadFuncImpl();
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to cleanup nodes in zookeeper: {}", getCurrentExceptionMessage(true));
    }

    if (shutdown_called)
        return;

    task->scheduleAfter(
        generateRescheduleInterval(
            cleanup_interval_min_ms, cleanup_interval_max_ms));
}

void ObjectStorageQueueMetadata::cleanupThreadFuncImpl()
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::ObjectStorageQueueCleanupMaxSetSizeOrTTLMicroseconds);
    const auto zk_client = getZooKeeper();
    const fs::path zookeeper_processed_path = zookeeper_path / "processed";
    const fs::path zookeeper_failed_path = zookeeper_path / "failed";
    const fs::path zookeeper_cleanup_lock_path = zookeeper_path / "cleanup_lock";

    Strings processed_nodes;
    auto code = zk_client->tryGetChildren(zookeeper_processed_path, processed_nodes);
    if (code != Coordination::Error::ZOK)
    {
        if (code == Coordination::Error::ZNONODE)
        {
            LOG_TEST(log, "Path {} does not exist", zookeeper_processed_path.string());
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected error: {}", magic_enum::enum_name(code));
    }

    Strings failed_nodes;
    code = zk_client->tryGetChildren(zookeeper_failed_path, failed_nodes);
    if (code != Coordination::Error::ZOK)
    {
        if (code == Coordination::Error::ZNONODE)
        {
            LOG_TEST(log, "Path {} does not exist", zookeeper_failed_path.string());
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected error: {}", magic_enum::enum_name(code));
    }

    const size_t nodes_num = processed_nodes.size() + failed_nodes.size();
    if (!nodes_num)
    {
        LOG_TEST(log, "There are neither processed nor failed nodes (in {} and in {})",
                 zookeeper_processed_path.string(), zookeeper_failed_path.string());
        return;
    }

    chassert(table_metadata.tracked_files_limit || table_metadata.tracked_files_ttl_sec);
    const bool check_nodes_limit = table_metadata.tracked_files_limit > 0;
    const bool check_nodes_ttl = table_metadata.tracked_files_ttl_sec > 0;

    const bool nodes_limit_exceeded = nodes_num > table_metadata.tracked_files_limit;
    if ((!nodes_limit_exceeded || !check_nodes_limit) && !check_nodes_ttl)
    {
        LOG_TEST(log, "No limit exceeded");
        return;
    }

    LOG_TRACE(log, "Will check limits for {} nodes", nodes_num);

    /// Create a lock so that with distributed processing
    /// multiple nodes do not execute cleanup in parallel.
    auto ephemeral_node = zkutil::EphemeralNodeHolder::tryCreate(zookeeper_cleanup_lock_path, *zk_client, toString(getCurrentTime()));
    if (!ephemeral_node)
    {
        LOG_TEST(log, "Cleanup is already being executed by another node");
        return;
    }
    /// TODO because of this lock we might not update local file statuses on time on one of the nodes.

    struct Node
    {
        std::string zk_path;
        ObjectStorageQueueIFileMetadata::NodeMetadata metadata;
    };
    auto node_cmp = [](const Node & a, const Node & b)
    {
        return std::tie(a.metadata.last_processed_timestamp, a.metadata.file_path)
            < std::tie(b.metadata.last_processed_timestamp, b.metadata.file_path);
    };

    /// Ordered in ascending order of timestamps.
    std::set<Node, decltype(node_cmp)> sorted_nodes(node_cmp);

    auto fetch_nodes = [&](const Strings & nodes, const fs::path & base_path)
    {
        for (const auto & node : nodes)
        {
            const std::string path = base_path / node;
            try
            {
                std::string metadata_str;
                if (zk_client->tryGet(path, metadata_str))
                {
                    sorted_nodes.emplace(path, ObjectStorageQueueIFileMetadata::NodeMetadata::fromString(metadata_str));
                    LOG_TEST(log, "Fetched metadata for node {}", path);
                }
                else
                    LOG_ERROR(log, "Failed to fetch node metadata {}", path);
            }
            catch (const zkutil::KeeperException & e)
            {
                if (!Coordination::isHardwareError(e.code))
                {
                    LOG_WARNING(log, "Unexpected exception: {}", getCurrentExceptionMessage(true));
                    chassert(false);
                }

                /// Will retry with a new zk connection.
                throw;
            }
        }
    };

    fetch_nodes(processed_nodes, zookeeper_processed_path);
    fetch_nodes(failed_nodes, zookeeper_failed_path);

    auto get_nodes_str = [&]()
    {
        WriteBufferFromOwnString wb;
        for (const auto & [node, metadata] : sorted_nodes)
            wb << fmt::format("Node: {}, path: {}, timestamp: {};\n", node, metadata.file_path, metadata.last_processed_timestamp);
        return wb.str();
    };

    LOG_TEST(log, "Checking node limits (max size: {}, max age: {}) for {}",
             table_metadata.tracked_files_limit, table_metadata.tracked_files_ttl_sec, get_nodes_str());

    size_t nodes_to_remove = check_nodes_limit && nodes_limit_exceeded ? nodes_num - table_metadata.tracked_files_limit : 0;
    for (const auto & node : sorted_nodes)
    {
        if (nodes_to_remove)
        {
            LOG_TRACE(log, "Removing node at path {} ({}) because max files limit is reached",
                     node.metadata.file_path, node.zk_path);

            local_file_statuses->remove(node.metadata.file_path, /* if_exists */true);

            code = zk_client->tryRemove(node.zk_path);
            if (code == Coordination::Error::ZOK)
                --nodes_to_remove;
            else
                LOG_ERROR(log, "Failed to remove a node `{}` (code: {})", node.zk_path, code);
        }
        else if (check_nodes_ttl)
        {
            UInt64 node_age = getCurrentTime() - node.metadata.last_processed_timestamp;
            if (node_age >= table_metadata.tracked_files_ttl_sec)
            {
                LOG_TRACE(log, "Removing node at path {} ({}) because file ttl is reached",
                        node.metadata.file_path, node.zk_path);

                local_file_statuses->remove(node.metadata.file_path, /* if_exists */true);

                code = zk_client->tryRemove(node.zk_path);
                if (code != Coordination::Error::ZOK)
                    LOG_ERROR(log, "Failed to remove a node `{}` (code: {})", node.zk_path, code);
            }
            else if (!nodes_to_remove)
            {
                /// Nodes limit satisfied.
                /// Nodes ttl satisfied as well as if current node is under tll, then all remaining as well
                /// (because we are iterating in timestamp ascending order).
                break;
            }
        }
        else
        {
            /// Nodes limit and ttl are satisfied.
            break;
        }
    }

    LOG_TRACE(log, "Node limits check finished");
}

}
