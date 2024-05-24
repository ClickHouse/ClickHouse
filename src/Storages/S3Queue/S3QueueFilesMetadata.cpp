#include "config.h"

#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/S3Queue/S3QueueFilesMetadata.h>
#include <Storages/S3Queue/S3QueueSettings.h>
#include <Storages/S3Queue/S3QueueIFileMetadata.h>
#include <Storages/S3Queue/S3QueueOrderedFileMetadata.h>
#include <Storages/S3Queue/S3QueueUnorderedFileMetadata.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/StorageSnapshot.h>
#include <base/sleep.h>
#include <Common/CurrentThread.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/getRandomASCIIString.h>
#include <Common/randomSeed.h>
#include <Common/DNSResolver.h>

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>


namespace ProfileEvents
{
    extern const Event S3QueueSetFileProcessingMicroseconds;
    extern const Event S3QueueSetFileProcessedMicroseconds;
    extern const Event S3QueueSetFileFailedMicroseconds;
    extern const Event S3QueueFailedFiles;
    extern const Event S3QueueProcessedFiles;
    extern const Event S3QueueCleanupMaxSetSizeOrTTLMicroseconds;
    extern const Event S3QueueLockLocalFileStatusesMicroseconds;
};

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
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

    size_t getBucketsNum(const S3QueueSettings & settings)
    {
        if (settings.s3queue_buckets)
            return settings.s3queue_buckets;
        if (settings.s3queue_processing_threads_num)
            return settings.s3queue_processing_threads_num;
        return 1;
    }

    zkutil::ZooKeeperPtr getZooKeeper()
    {
        return Context::getGlobalContextInstance()->getZooKeeper();
    }
}

std::unique_lock<std::mutex> S3QueueFilesMetadata::LocalFileStatuses::lock() const
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3QueueLockLocalFileStatusesMicroseconds);
    return std::unique_lock(mutex);
}

S3QueueFilesMetadata::FileStatuses S3QueueFilesMetadata::LocalFileStatuses::getAll() const
{
    auto lk = lock();
    return file_statuses;
}

S3QueueFilesMetadata::FileStatusPtr S3QueueFilesMetadata::LocalFileStatuses::get(const std::string & filename, bool create)
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

bool S3QueueFilesMetadata::LocalFileStatuses::remove(const std::string & filename, bool if_exists)
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

S3QueueFilesMetadata::S3QueueFilesMetadata(const fs::path & zookeeper_path_, const S3QueueSettings & settings_)
    : mode(settings_.mode)
    , max_set_size(settings_.s3queue_tracked_files_limit.value)
    , max_set_age_sec(settings_.s3queue_tracked_file_ttl_sec.value)
    , max_loading_retries(settings_.s3queue_loading_retries.value)
    , min_cleanup_interval_ms(settings_.s3queue_cleanup_interval_min_ms.value)
    , max_cleanup_interval_ms(settings_.s3queue_cleanup_interval_max_ms.value)
    , buckets_num(getBucketsNum(settings_))
    , zookeeper_path(zookeeper_path_)
    , log(getLogger("StorageS3Queue(" + zookeeper_path_.string() + ")"))
{
    if (mode == S3QueueMode::UNORDERED && (max_set_size || max_set_age_sec))
    {
        task = Context::getGlobalContextInstance()->getSchedulePool().createTask("S3QueueCleanupFunc", [this] { cleanupThreadFunc(); });
        task->activate();
        task->scheduleAfter(generateRescheduleInterval(min_cleanup_interval_ms, max_cleanup_interval_ms));
    }
    else if (mode == S3QueueMode::ORDERED && buckets_num > 1)
        LOG_TEST(log, "Using {} buckets", buckets_num);
}

S3QueueFilesMetadata::~S3QueueFilesMetadata()
{
    shutdown();
}

void S3QueueFilesMetadata::shutdown()
{
    shutdown_called = true;
    if (task)
        task->deactivate();
}

S3QueueFilesMetadata::FileMetadataPtr S3QueueFilesMetadata::getFileMetadata(const std::string & path)
{
    auto file_status = local_file_statuses.get(path, /* create */true);
    switch (mode)
    {
        case S3QueueMode::ORDERED:
            return std::make_shared<OrderedFileMetadata>(zookeeper_path, path, file_status, buckets_num, max_loading_retries, log);
        case S3QueueMode::UNORDERED:
            return std::make_shared<UnorderedFileMetadata>(zookeeper_path, path, file_status, max_loading_retries, log);
    }
}

S3QueueFilesMetadata::FileStatusPtr S3QueueFilesMetadata::getFileStatus(const std::string & path)
{
    /// Return a locally cached file status.
    return local_file_statuses.get(path, /* create */false);
}

bool S3QueueFilesMetadata::useBucketsForProcessing() const
{
    return mode == S3QueueMode::ORDERED && (buckets_num > 1);
}

S3QueueFilesMetadata::Bucket S3QueueFilesMetadata::getBucketForPath(const std::string & path) const
{
    return OrderedFileMetadata::getBucketForPath(path, buckets_num);
}

OrderedFileMetadata::BucketHolderPtr S3QueueFilesMetadata::tryAcquireBucket(const Bucket & bucket, const Processor & processor)
{
    return OrderedFileMetadata::tryAcquireBucket(zookeeper_path, bucket, processor);
}

void S3QueueFilesMetadata::cleanupThreadFunc()
{
    /// A background task is responsible for maintaining
    /// max_set_size and max_set_age settings for `unordered` processing mode.

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

    task->scheduleAfter(generateRescheduleInterval(min_cleanup_interval_ms, max_cleanup_interval_ms));
}

void S3QueueFilesMetadata::cleanupThreadFuncImpl()
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3QueueCleanupMaxSetSizeOrTTLMicroseconds);
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

    chassert(max_set_size || max_set_age_sec);
    const bool check_nodes_limit = max_set_size > 0;
    const bool check_nodes_ttl = max_set_age_sec > 0;

    const bool nodes_limit_exceeded = nodes_num > max_set_size;
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
        IFileMetadata::NodeMetadata metadata;
    };
    auto node_cmp = [](const Node & a, const Node & b)
    {
        return std::tie(a.metadata.last_processed_timestamp, a.metadata.file_path)
            < std::tie(b.metadata.last_processed_timestamp, b.metadata.file_path);
    };

    /// Ordered in ascending order of timestamps.
    std::set<Node, decltype(node_cmp)> sorted_nodes(node_cmp);

    for (const auto & node : processed_nodes)
    {
        const std::string path = zookeeper_processed_path / node;
        try
        {
            std::string metadata_str;
            if (zk_client->tryGet(path, metadata_str))
            {
                sorted_nodes.emplace(path, IFileMetadata::NodeMetadata::fromString(metadata_str));
                LOG_TEST(log, "Fetched metadata for node {}", path);
            }
            else
                LOG_ERROR(log, "Failed to fetch node metadata {}", path);
        }
        catch (const zkutil::KeeperException & e)
        {
            if (e.code != Coordination::Error::ZCONNECTIONLOSS)
            {
                LOG_WARNING(log, "Unexpected exception: {}", getCurrentExceptionMessage(true));
                chassert(false);
            }

            /// Will retry with a new zk connection.
            throw;
        }
    }

    for (const auto & node : failed_nodes)
    {
        const std::string path = zookeeper_failed_path / node;
        try
        {
            std::string metadata_str;
            if (zk_client->tryGet(path, metadata_str))
            {
                sorted_nodes.emplace(path, IFileMetadata::NodeMetadata::fromString(metadata_str));
                LOG_TEST(log, "Fetched metadata for node {}", path);
            }
            else
                LOG_ERROR(log, "Failed to fetch node metadata {}", path);
        }
        catch (const zkutil::KeeperException & e)
        {
            if (e.code != Coordination::Error::ZCONNECTIONLOSS)
            {
                LOG_WARNING(log, "Unexpected exception: {}", getCurrentExceptionMessage(true));
                chassert(false);
            }

            /// Will retry with a new zk connection.
            throw;
        }
    }

    auto get_nodes_str = [&]()
    {
        WriteBufferFromOwnString wb;
        for (const auto & [node, metadata] : sorted_nodes)
            wb << fmt::format("Node: {}, path: {}, timestamp: {};\n", node, metadata.file_path, metadata.last_processed_timestamp);
        return wb.str();
    };
    LOG_TEST(log, "Checking node limits (max size: {}, max age: {}) for {}", max_set_size, max_set_age_sec, get_nodes_str());

    size_t nodes_to_remove = check_nodes_limit && nodes_limit_exceeded ? nodes_num - max_set_size : 0;
    for (const auto & node : sorted_nodes)
    {
        if (nodes_to_remove)
        {
            LOG_TRACE(log, "Removing node at path {} ({}) because max files limit is reached",
                     node.metadata.file_path, node.zk_path);

            local_file_statuses.remove(node.metadata.file_path, /* if_exists */true);

            code = zk_client->tryRemove(node.zk_path);
            if (code == Coordination::Error::ZOK)
                --nodes_to_remove;
            else
                LOG_ERROR(log, "Failed to remove a node `{}` (code: {})", node.zk_path, code);
        }
        else if (check_nodes_ttl)
        {
            UInt64 node_age = getCurrentTime() - node.metadata.last_processed_timestamp;
            if (node_age >= max_set_age_sec)
            {
                LOG_TRACE(log, "Removing node at path {} ({}) because file is reached",
                        node.metadata.file_path, node.zk_path);

                local_file_statuses.remove(node.metadata.file_path, /* if_exists */true);

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

bool S3QueueFilesMetadata::checkSettings(const S3QueueSettings & settings) const
{
    return mode == settings.mode
        && max_set_size == settings.s3queue_tracked_files_limit.value
        && max_set_age_sec == settings.s3queue_tracked_file_ttl_sec.value
        && max_loading_retries == settings.s3queue_loading_retries.value
        && min_cleanup_interval_ms == settings.s3queue_cleanup_interval_min_ms.value
        && max_cleanup_interval_ms == settings.s3queue_cleanup_interval_max_ms.value;
}

}
