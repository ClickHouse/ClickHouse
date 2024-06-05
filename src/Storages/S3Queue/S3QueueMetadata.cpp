#include "config.h"

#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/S3Queue/S3QueueMetadata.h>
#include <Storages/S3Queue/S3QueueSettings.h>
#include <Storages/S3Queue/S3QueueIFileMetadata.h>
#include <Storages/S3Queue/S3QueueOrderedFileMetadata.h>
#include <Storages/S3Queue/S3QueueUnorderedFileMetadata.h>
#include <Storages/S3Queue/S3QueueTableMetadata.h>
#include <IO/S3Settings.h>
#include <Storages/StorageSnapshot.h>
#include <base/sleep.h>
#include <Common/CurrentThread.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/getRandomASCIIString.h>
#include <Common/randomSeed.h>
#include <numeric>


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
    extern const int REPLICA_ALREADY_EXISTS;
    extern const int INCOMPATIBLE_COLUMNS;
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

class S3QueueMetadata::LocalFileStatuses
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
        auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3QueueLockLocalFileStatusesMicroseconds);
        return std::unique_lock(mutex);
    }
};

S3QueueMetadata::S3QueueMetadata(const fs::path & zookeeper_path_, const S3QueueSettings & settings_)
    : settings(settings_)
    , zookeeper_path(zookeeper_path_)
    , buckets_num(getBucketsNum(settings_))
    , log(getLogger("StorageS3Queue(" + zookeeper_path_.string() + ")"))
    , local_file_statuses(std::make_shared<LocalFileStatuses>())
{
    if (settings.mode == S3QueueMode::UNORDERED
        && (settings.s3queue_tracked_files_limit || settings.s3queue_tracked_file_ttl_sec))
    {
        task = Context::getGlobalContextInstance()->getSchedulePool().createTask(
            "S3QueueCleanupFunc",
            [this] { cleanupThreadFunc(); });

        task->activate();
        task->scheduleAfter(
            generateRescheduleInterval(
                settings.s3queue_cleanup_interval_min_ms, settings.s3queue_cleanup_interval_max_ms));
    }
}

S3QueueMetadata::~S3QueueMetadata()
{
    shutdown();
}

void S3QueueMetadata::shutdown()
{
    shutdown_called = true;
    if (task)
        task->deactivate();
}

void S3QueueMetadata::checkSettings(const S3QueueSettings & settings_) const
{
    S3QueueTableMetadata::checkEquals(settings, settings_);
}

S3QueueMetadata::FileStatusPtr S3QueueMetadata::getFileStatus(const std::string & path)
{
    return local_file_statuses->get(path, /* create */false);
}

S3QueueMetadata::FileStatuses S3QueueMetadata::getFileStatuses() const
{
    return local_file_statuses->getAll();
}

S3QueueMetadata::FileMetadataPtr S3QueueMetadata::getFileMetadata(
    const std::string & path,
    S3QueueOrderedFileMetadata::BucketInfoPtr bucket_info)
{
    auto file_status = local_file_statuses->get(path, /* create */true);
    switch (settings.mode.value)
    {
        case S3QueueMode::ORDERED:
            return std::make_shared<S3QueueOrderedFileMetadata>(
                zookeeper_path,
                path,
                file_status,
                bucket_info,
                buckets_num,
                settings.s3queue_loading_retries,
                log);
        case S3QueueMode::UNORDERED:
            return std::make_shared<S3QueueUnorderedFileMetadata>(
                zookeeper_path,
                path,
                file_status,
                settings.s3queue_loading_retries,
                log);
    }
}

size_t S3QueueMetadata::getBucketsNum(const S3QueueSettings & settings)
{
    if (settings.s3queue_buckets)
        return settings.s3queue_buckets;
    if (settings.s3queue_processing_threads_num)
        return settings.s3queue_processing_threads_num;
    return 0;
}

size_t S3QueueMetadata::getBucketsNum(const S3QueueTableMetadata & settings)
{
    if (settings.buckets)
        return settings.buckets;
    if (settings.processing_threads_num)
        return settings.processing_threads_num;
    return 0;
}

bool S3QueueMetadata::useBucketsForProcessing() const
{
    return settings.mode == S3QueueMode::ORDERED && (buckets_num > 1);
}

S3QueueMetadata::Bucket S3QueueMetadata::getBucketForPath(const std::string & path) const
{
    return S3QueueOrderedFileMetadata::getBucketForPath(path, buckets_num);
}

S3QueueOrderedFileMetadata::BucketHolderPtr
S3QueueMetadata::tryAcquireBucket(const Bucket & bucket, const Processor & processor)
{
    return S3QueueOrderedFileMetadata::tryAcquireBucket(zookeeper_path, bucket, processor);
}

void S3QueueMetadata::initialize(
    const ConfigurationPtr & configuration,
    const StorageInMemoryMetadata & storage_metadata)
{
    const auto metadata_from_table = S3QueueTableMetadata(*configuration, settings, storage_metadata);
    const auto & columns_from_table = storage_metadata.getColumns();
    const auto table_metadata_path = zookeeper_path / "metadata";
    const auto metadata_paths = settings.mode == S3QueueMode::ORDERED
        ? S3QueueOrderedFileMetadata::getMetadataPaths(buckets_num)
        : S3QueueUnorderedFileMetadata::getMetadataPaths();

    auto zookeeper = getZooKeeper();
    zookeeper->createAncestors(zookeeper_path);

    for (size_t i = 0; i < 1000; ++i)
    {
        if (zookeeper->exists(table_metadata_path))
        {
            const auto metadata_from_zk = S3QueueTableMetadata::parse(zookeeper->get(fs::path(zookeeper_path) / "metadata"));
            const auto columns_from_zk = ColumnsDescription::parse(metadata_from_zk.columns);

            metadata_from_table.checkEquals(metadata_from_zk);
            if (columns_from_zk != columns_from_table)
            {
                throw Exception(
                    ErrorCodes::INCOMPATIBLE_COLUMNS,
                    "Table columns structure in ZooKeeper is different from local table structure. "
                    "Local columns:\n{}\nZookeeper columns:\n{}",
                    columns_from_table.toString(), columns_from_zk.toString());
            }
            return;
        }

        Coordination::Requests requests;
        requests.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "", zkutil::CreateMode::Persistent));
        requests.emplace_back(zkutil::makeCreateRequest(table_metadata_path, metadata_from_table.toString(), zkutil::CreateMode::Persistent));

        for (const auto & path : metadata_paths)
        {
            const auto zk_path = zookeeper_path / path;
            requests.emplace_back(zkutil::makeCreateRequest(zk_path, "", zkutil::CreateMode::Persistent));
        }

        if (!settings.s3queue_last_processed_path.value.empty())
            getFileMetadata(settings.s3queue_last_processed_path)->setProcessedAtStartRequests(requests, zookeeper);

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(requests, responses);
        if (code == Coordination::Error::ZNODEEXISTS)
        {
            auto exception = zkutil::KeeperMultiException(code, requests, responses);
            LOG_INFO(log, "Got code `{}` for path: {}. "
                     "It looks like the table {} was created by another server at the same moment, "
                     "will retry", code, exception.getPathForFirstFailedOp(), zookeeper_path.string());
            continue;
        }
        else if (code != Coordination::Error::ZOK)
            zkutil::KeeperMultiException::check(code, requests, responses);

        return;
    }

    throw Exception(
        ErrorCodes::REPLICA_ALREADY_EXISTS,
        "Cannot create table, because it is created concurrently every time or because "
        "of wrong zookeeper path or because of logical error");
}

void S3QueueMetadata::cleanupThreadFunc()
{
    /// A background task is responsible for maintaining
    /// settings.s3queue_tracked_files_limit and max_set_age settings for `unordered` processing mode.

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
            settings.s3queue_cleanup_interval_min_ms, settings.s3queue_cleanup_interval_max_ms));
}

void S3QueueMetadata::cleanupThreadFuncImpl()
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

    chassert(settings.s3queue_tracked_files_limit || settings.s3queue_tracked_file_ttl_sec);
    const bool check_nodes_limit = settings.s3queue_tracked_files_limit > 0;
    const bool check_nodes_ttl = settings.s3queue_tracked_file_ttl_sec > 0;

    const bool nodes_limit_exceeded = nodes_num > settings.s3queue_tracked_files_limit;
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
        S3QueueIFileMetadata::NodeMetadata metadata;
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
                    sorted_nodes.emplace(path, S3QueueIFileMetadata::NodeMetadata::fromString(metadata_str));
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
    LOG_TEST(log, "Checking node limits (max size: {}, max age: {}) for {}", settings.s3queue_tracked_files_limit, settings.s3queue_tracked_file_ttl_sec, get_nodes_str());

    size_t nodes_to_remove = check_nodes_limit && nodes_limit_exceeded ? nodes_num - settings.s3queue_tracked_files_limit : 0;
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
            if (node_age >= settings.s3queue_tracked_file_ttl_sec)
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
