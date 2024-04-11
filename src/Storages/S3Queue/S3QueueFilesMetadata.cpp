#include "config.h"

#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/S3Queue/S3QueueFilesMetadata.h>
#include <Storages/S3Queue/S3QueueSettings.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/StorageSnapshot.h>
#include <base/sleep.h>
#include <Common/CurrentThread.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/getRandomASCIIString.h>
#include <Common/randomSeed.h>

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>


namespace ProfileEvents
{
    extern const Event S3QueueSetFileProcessingMicroseconds;
    extern const Event S3QueueSetFileProcessedMicroseconds;
    extern const Event S3QueueSetFileFailedMicroseconds;
    extern const Event S3QueueCleanupMaxSetSizeOrTTLMicroseconds;
    extern const Event S3QueueLockLocalFileStatusesMicroseconds;
    extern const Event CannotRemoveEphemeralNode;
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

std::string S3QueueFilesMetadata::NodeMetadata::toString() const
{
    Poco::JSON::Object json;
    json.set("file_path", file_path);
    json.set("last_processed_timestamp", getCurrentTime());
    json.set("last_exception", last_exception);
    json.set("retries", retries);
    json.set("processing_id", processing_id);

    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);
    return oss.str();
}

S3QueueFilesMetadata::NodeMetadata S3QueueFilesMetadata::NodeMetadata::fromString(const std::string & metadata_str)
{
    Poco::JSON::Parser parser;
    auto json = parser.parse(metadata_str).extract<Poco::JSON::Object::Ptr>();

    NodeMetadata metadata;
    metadata.file_path = json->getValue<String>("file_path");
    metadata.last_processed_timestamp = json->getValue<UInt64>("last_processed_timestamp");
    metadata.last_exception = json->getValue<String>("last_exception");
    metadata.retries = json->getValue<UInt64>("retries");
    metadata.processing_id = json->getValue<String>("processing_id");
    return metadata;
}

S3QueueFilesMetadata::S3QueueFilesMetadata(const fs::path & zookeeper_path_, const S3QueueSettings & settings_)
    : mode(settings_.mode)
    , max_set_size(settings_.s3queue_tracked_files_limit.value)
    , max_set_age_sec(settings_.s3queue_tracked_file_ttl_sec.value)
    , max_loading_retries(settings_.s3queue_loading_retries.value)
    , min_cleanup_interval_ms(settings_.s3queue_cleanup_interval_min_ms.value)
    , max_cleanup_interval_ms(settings_.s3queue_cleanup_interval_max_ms.value)
    , shards_num(settings_.s3queue_total_shards_num)
    , threads_per_shard(settings_.s3queue_processing_threads_num)
    , zookeeper_processing_path(zookeeper_path_ / "processing")
    , zookeeper_processed_path(zookeeper_path_ / "processed")
    , zookeeper_failed_path(zookeeper_path_ / "failed")
    , zookeeper_shards_path(zookeeper_path_ / "shards")
    , zookeeper_cleanup_lock_path(zookeeper_path_ / "cleanup_lock")
    , log(getLogger("S3QueueFilesMetadata"))
{
    if (mode == S3QueueMode::UNORDERED && (max_set_size || max_set_age_sec))
    {
        task = Context::getGlobalContextInstance()->getSchedulePool().createTask("S3QueueCleanupFunc", [this] { cleanupThreadFunc(); });
        task->activate();
        task->scheduleAfter(generateRescheduleInterval(min_cleanup_interval_ms, max_cleanup_interval_ms));
    }
}

S3QueueFilesMetadata::~S3QueueFilesMetadata()
{
    deactivateCleanupTask();
}

void S3QueueFilesMetadata::deactivateCleanupTask()
{
    shutdown = true;
    if (task)
        task->deactivate();
}

zkutil::ZooKeeperPtr S3QueueFilesMetadata::getZooKeeper() const
{
    return Context::getGlobalContextInstance()->getZooKeeper();
}

S3QueueFilesMetadata::FileStatusPtr S3QueueFilesMetadata::getFileStatus(const std::string & path)
{
    /// Return a locally cached file status.
    return local_file_statuses.get(path, /* create */false);
}

std::string S3QueueFilesMetadata::getNodeName(const std::string & path)
{
    /// Since with are dealing with paths in s3 which can have "/",
    /// we cannot create a zookeeper node with the name equal to path.
    /// Therefore we use a hash of the path as a node name.

    SipHash path_hash;
    path_hash.update(path);
    return toString(path_hash.get64());
}

S3QueueFilesMetadata::NodeMetadata S3QueueFilesMetadata::createNodeMetadata(
    const std::string & path,
    const std::string & exception,
    size_t retries)
{
    /// Create a metadata which will be stored in a node named as getNodeName(path).

    /// Since node name is just a hash we want to know to which file it corresponds,
    /// so we keep "file_path" in nodes data.
    /// "last_processed_timestamp" is needed for TTL metadata nodes enabled by s3queue_tracked_file_ttl_sec.
    /// "last_exception" is kept for introspection, should also be visible in system.s3queue_log if it is enabled.
    /// "retries" is kept for retrying the processing enabled by s3queue_loading_retries.
    NodeMetadata metadata;
    metadata.file_path = path;
    metadata.last_processed_timestamp = getCurrentTime();
    metadata.last_exception = exception;
    metadata.retries = retries;
    return metadata;
}

bool S3QueueFilesMetadata::isShardedProcessing() const
{
    return getProcessingIdsNum() > 1 && mode == S3QueueMode::ORDERED;
}

size_t S3QueueFilesMetadata::registerNewShard()
{
    if (!isShardedProcessing())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot register a new shard, because processing is not sharded");
    }

    const auto zk_client = getZooKeeper();
    zk_client->createIfNotExists(zookeeper_shards_path, "");

    std::string shard_node_path;
    size_t shard_id = 0;
    for (size_t i = 0; i < shards_num; ++i)
    {
        const auto node_path = getZooKeeperPathForShard(i);
        auto err = zk_client->tryCreate(node_path, "", zkutil::CreateMode::Persistent);
        if (err == Coordination::Error::ZOK)
        {
            shard_node_path = node_path;
            shard_id = i;
            break;
        }
        else if (err == Coordination::Error::ZNODEEXISTS)
            continue;
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Unexpected error: {}", magic_enum::enum_name(err));
    }

    if (shard_node_path.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to register a new shard");

    LOG_TRACE(log, "Using shard {} (zk node: {})", shard_id, shard_node_path);
    return shard_id;
}

std::string S3QueueFilesMetadata::getZooKeeperPathForShard(size_t shard_id) const
{
    return zookeeper_shards_path / ("shard" + toString(shard_id));
}

void S3QueueFilesMetadata::registerNewShard(size_t shard_id)
{
    if (!isShardedProcessing())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot register a new shard, because processing is not sharded");
    }

    const auto zk_client = getZooKeeper();
    const auto node_path = getZooKeeperPathForShard(shard_id);
    zk_client->createAncestors(node_path);

    auto err = zk_client->tryCreate(node_path, "", zkutil::CreateMode::Persistent);
    if (err != Coordination::Error::ZOK)
    {
        if (err == Coordination::Error::ZNODEEXISTS)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot register shard {}: already exists", shard_id);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Unexpected error: {}", magic_enum::enum_name(err));
    }
}

bool S3QueueFilesMetadata::isShardRegistered(size_t shard_id)
{
    const auto zk_client = getZooKeeper();
    const auto node_path = getZooKeeperPathForShard(shard_id);
    return zk_client->exists(node_path);
}

void S3QueueFilesMetadata::unregisterShard(size_t shard_id)
{
    if (!isShardedProcessing())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot unregister a shard, because processing is not sharded");
    }

    const auto zk_client = getZooKeeper();
    const auto node_path = getZooKeeperPathForShard(shard_id);
    auto error_code = zk_client->tryRemove(node_path);
    if (error_code != Coordination::Error::ZOK
        && error_code != Coordination::Error::ZNONODE)
        throw zkutil::KeeperException::fromPath(error_code, node_path);
}

size_t S3QueueFilesMetadata::getProcessingIdsNum() const
{
    return shards_num * threads_per_shard;
}

std::vector<size_t> S3QueueFilesMetadata::getProcessingIdsForShard(size_t shard_id) const
{
    std::vector<size_t> res(threads_per_shard);
    std::iota(res.begin(), res.end(), shard_id * threads_per_shard);
    return res;
}

bool S3QueueFilesMetadata::isProcessingIdBelongsToShard(size_t id, size_t shard_id) const
{
    return shard_id * threads_per_shard <= id && id < (shard_id + 1) * threads_per_shard;
}

size_t S3QueueFilesMetadata::getIdForProcessingThread(size_t thread_id, size_t shard_id) const
{
    return shard_id * threads_per_shard + thread_id;
}

size_t S3QueueFilesMetadata::getProcessingIdForPath(const std::string & path) const
{
    return sipHash64(path) % getProcessingIdsNum();
}

S3QueueFilesMetadata::ProcessingNodeHolderPtr S3QueueFilesMetadata::trySetFileAsProcessing(const std::string & path)
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3QueueSetFileProcessingMicroseconds);
    auto file_status = local_file_statuses.get(path, /* create */true);

    /// Check locally cached file status.
    ///     Processed or Failed state is always cached.
    ///     Processing state is cached only if processing is being done by current clickhouse server
    ///     (because If another server is doing the processing,
    ///     we cannot know if state changes without checking with zookeeper so there is no point in cache here).

    {
        std::lock_guard lock(file_status->metadata_lock);
        switch (file_status->state)
        {
            case FileStatus::State::Processing:
            {
                LOG_TEST(log, "File {} is already processing", path);
                return {};
            }
            case FileStatus::State::Processed:
            {
                LOG_TEST(log, "File {} is already processed", path);
                return {};
            }
            case FileStatus::State::Failed:
            {
                /// If max_loading_retries == 0, file is not retriable.
                if (max_loading_retries == 0)
                {
                    LOG_TEST(log, "File {} is failed and processing retries are disabled", path);
                    return {};
                }

                /// Otherwise file_status->retries is also cached.
                /// In case file_status->retries >= max_loading_retries we can fully rely that it is true
                /// and will not attempt processing it.
                /// But in case file_status->retries < max_loading_retries we cannot be sure
                /// (another server could have done a try after we cached retries value),
                /// so check with zookeeper here.
                if (file_status->retries >= max_loading_retries)
                {
                    LOG_TEST(log, "File {} is failed and processing retries are exceeeded", path);
                    return {};
                }

                break;
            }
            case FileStatus::State::None:
            {
                /// The file was not processed by current server and file status was not cached,
                /// check metadata in zookeeper.
                break;
            }
        }
    }

    /// Another thread could already be trying to set file as processing.
    /// So there is no need to attempt the same, better to continue with the next file.
    std::unique_lock processing_lock(file_status->processing_lock, std::defer_lock);
    if (!processing_lock.try_lock())
    {
        return {};
    }

    /// Let's go and check metadata in zookeeper and try to create a /processing ephemeral node.
    /// If successful, return result with processing node holder.
    SetFileProcessingResult result;
    ProcessingNodeHolderPtr processing_node_holder;

    switch (mode)
    {
        case S3QueueMode::ORDERED:
        {
            std::tie(result, processing_node_holder) = trySetFileAsProcessingForOrderedMode(path, file_status);
            break;
        }
        case S3QueueMode::UNORDERED:
        {
            std::tie(result, processing_node_holder) = trySetFileAsProcessingForUnorderedMode(path, file_status);
            break;
        }
    }

    /// Cache file status, save some statistics.
    switch (result)
    {
        case SetFileProcessingResult::Success:
        {
            std::lock_guard lock(file_status->metadata_lock);
            file_status->state = FileStatus::State::Processing;

            file_status->profile_counters.increment(ProfileEvents::S3QueueSetFileProcessingMicroseconds, timer.get());
            timer.cancel();

            if (!file_status->processing_start_time)
                file_status->processing_start_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

            return processing_node_holder;
        }
        case SetFileProcessingResult::AlreadyProcessed:
        {
            std::lock_guard lock(file_status->metadata_lock);
            file_status->state = FileStatus::State::Processed;
            return {};
        }
        case SetFileProcessingResult::AlreadyFailed:
        {
            std::lock_guard lock(file_status->metadata_lock);
            file_status->state = FileStatus::State::Failed;
            return {};
        }
        case SetFileProcessingResult::ProcessingByOtherNode:
        {
            /// We cannot save any local state here, see comment above.
            return {};
        }
    }
}

std::pair<S3QueueFilesMetadata::SetFileProcessingResult,
          S3QueueFilesMetadata::ProcessingNodeHolderPtr>
S3QueueFilesMetadata::trySetFileAsProcessingForUnorderedMode(const std::string & path, const FileStatusPtr & file_status)
{
    /// In one zookeeper transaction do the following:
    /// 1. check that corresponding persistent nodes do not exist in processed/ and failed/;
    /// 2. create an ephemenral node in /processing if it does not exist;
    /// Return corresponding status if any of the step failed.

    const auto node_name = getNodeName(path);
    const auto zk_client = getZooKeeper();
    auto node_metadata = createNodeMetadata(path);
    node_metadata.processing_id = getRandomASCIIString(10);

    Coordination::Requests requests;

    requests.push_back(zkutil::makeCreateRequest(zookeeper_processed_path / node_name, "", zkutil::CreateMode::Persistent));
    requests.push_back(zkutil::makeRemoveRequest(zookeeper_processed_path / node_name, -1));

    requests.push_back(zkutil::makeCreateRequest(zookeeper_failed_path / node_name, "", zkutil::CreateMode::Persistent));
    requests.push_back(zkutil::makeRemoveRequest(zookeeper_failed_path / node_name, -1));

    requests.push_back(zkutil::makeCreateRequest(zookeeper_processing_path / node_name, node_metadata.toString(), zkutil::CreateMode::Ephemeral));

    Coordination::Responses responses;
    auto code = zk_client->tryMulti(requests, responses);

    if (code == Coordination::Error::ZOK)
    {
        auto holder = std::make_unique<ProcessingNodeHolder>(
            node_metadata.processing_id, path, zookeeper_processing_path / node_name, file_status, zk_client);
        return std::pair{SetFileProcessingResult::Success, std::move(holder)};
    }

    if (responses[0]->error != Coordination::Error::ZOK)
    {
        return std::pair{SetFileProcessingResult::AlreadyProcessed, nullptr};
    }
    else if (responses[2]->error != Coordination::Error::ZOK)
    {
        return std::pair{SetFileProcessingResult::AlreadyFailed, nullptr};
    }
    else if (responses[4]->error != Coordination::Error::ZOK)
    {
        return std::pair{SetFileProcessingResult::ProcessingByOtherNode, nullptr};
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state of zookeeper transaction: {}", magic_enum::enum_name(code));
    }
}

std::pair<S3QueueFilesMetadata::SetFileProcessingResult,
          S3QueueFilesMetadata::ProcessingNodeHolderPtr>
S3QueueFilesMetadata::trySetFileAsProcessingForOrderedMode(const std::string & path, const FileStatusPtr & file_status)
{
    /// Same as for Unordered mode.
    /// The only difference is the check if the file is already processed.
    /// For Ordered mode we do not keep a separate /processed/hash_node for each file
    /// but instead we only keep a maximum processed file
    /// (since all files are ordered and new files have a lexically bigger name, it makes sense).

    const auto node_name = getNodeName(path);
    const auto zk_client = getZooKeeper();
    auto node_metadata = createNodeMetadata(path);
    node_metadata.processing_id = getRandomASCIIString(10);

    while (true)
    {
        /// Get a /processed node content - max_processed path.
        /// Compare our path to it.
        /// If file is not yet processed, check corresponding /failed node and try create /processing node
        /// and in the same zookeeper transaction also check that /processed node did not change
        /// in between, e.g. that stat.version remained the same.
        /// If the version did change - retry (since we cannot do Get and Create requests
        /// in the same zookeeper transaction, so we use a while loop with tries).

        auto processed_node = isShardedProcessing()
            ? zookeeper_processed_path / toString(getProcessingIdForPath(path))
            : zookeeper_processed_path;

        NodeMetadata processed_node_metadata;
        Coordination::Stat processed_node_stat;
        std::string data;
        auto processed_node_exists = zk_client->tryGet(processed_node, data, &processed_node_stat);
        if (processed_node_exists && !data.empty())
            processed_node_metadata = NodeMetadata::fromString(data);

        auto max_processed_file_path = processed_node_metadata.file_path;
        if (!max_processed_file_path.empty() && path <= max_processed_file_path)
        {
            LOG_TEST(log, "File {} is already processed, max processed file: {}", path, max_processed_file_path);
            return std::pair{SetFileProcessingResult::AlreadyProcessed, nullptr};
        }

        Coordination::Requests requests;
        requests.push_back(zkutil::makeCreateRequest(zookeeper_failed_path / node_name, "", zkutil::CreateMode::Persistent));
        requests.push_back(zkutil::makeRemoveRequest(zookeeper_failed_path / node_name, -1));

        requests.push_back(zkutil::makeCreateRequest(zookeeper_processing_path / node_name, node_metadata.toString(), zkutil::CreateMode::Ephemeral));

        if (processed_node_exists)
        {
            requests.push_back(zkutil::makeCheckRequest(processed_node, processed_node_stat.version));
        }
        else
        {
            requests.push_back(zkutil::makeCreateRequest(processed_node, "", zkutil::CreateMode::Persistent));
            requests.push_back(zkutil::makeRemoveRequest(processed_node, -1));
        }

        Coordination::Responses responses;
        auto code = zk_client->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
        {
            auto holder = std::make_unique<ProcessingNodeHolder>(
                node_metadata.processing_id, path, zookeeper_processing_path / node_name, file_status, zk_client);

            LOG_TEST(log, "File {} is ready to be processed", path);
            return std::pair{SetFileProcessingResult::Success, std::move(holder)};
        }

        if (responses[0]->error != Coordination::Error::ZOK)
        {
            LOG_TEST(log, "Skipping file `{}`: failed", path);
            return std::pair{SetFileProcessingResult::AlreadyFailed, nullptr};
        }
        else if (responses[2]->error != Coordination::Error::ZOK)
        {
            LOG_TEST(log, "Skipping file `{}`: already processing", path);
            return std::pair{SetFileProcessingResult::ProcessingByOtherNode, nullptr};
        }
        else
        {
            LOG_TEST(log, "Version of max processed file changed. Retrying the check for file `{}`", path);
        }
    }
}

void S3QueueFilesMetadata::setFileProcessed(ProcessingNodeHolderPtr holder)
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3QueueSetFileProcessedMicroseconds);
    auto file_status = holder->getFileStatus();
    {
        std::lock_guard lock(file_status->metadata_lock);
        file_status->state = FileStatus::State::Processed;
        file_status->processing_end_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    }

    SCOPE_EXIT({
        file_status->profile_counters.increment(ProfileEvents::S3QueueSetFileProcessedMicroseconds, timer.get());
        timer.cancel();
    });

    switch (mode)
    {
        case S3QueueMode::ORDERED:
        {
            return setFileProcessedForOrderedMode(holder);
        }
        case S3QueueMode::UNORDERED:
        {
            return setFileProcessedForUnorderedMode(holder);
        }
    }
}

void S3QueueFilesMetadata::setFileProcessedForUnorderedMode(ProcessingNodeHolderPtr holder)
{
    /// Create a persistent node in /processed and remove ephemeral node from /processing.

    const auto & path = holder->path;
    const auto node_name = getNodeName(path);
    const auto node_metadata = createNodeMetadata(path).toString();
    const auto zk_client = getZooKeeper();

    Coordination::Requests requests;
    requests.push_back(zkutil::makeCreateRequest(zookeeper_processed_path / node_name, node_metadata, zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    if (holder->remove(&requests, &responses))
    {
        LOG_TEST(log, "Moved file `{}` to processed", path);
        if (max_loading_retries)
            zk_client->tryRemove(zookeeper_failed_path / (node_name + ".retriable"), -1);
        return;
    }

    if (!responses.empty() && responses[0]->error != Coordination::Error::ZOK)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot create a persistent node in /processed since it already exists");
    }

    LOG_WARNING(log,
                "Cannot set file ({}) as processed since ephemeral node in /processing"
                "does not exist with expected id, "
                "this could be a result of expired zookeeper session", path);
}


void S3QueueFilesMetadata::setFileProcessedForOrderedMode(ProcessingNodeHolderPtr holder)
{
    auto processed_node_path = isShardedProcessing()
        ? zookeeper_processed_path / toString(getProcessingIdForPath(holder->path))
        : zookeeper_processed_path;

    return setFileProcessedForOrderedModeImpl(holder->path, holder, processed_node_path);
}

void S3QueueFilesMetadata::setFileProcessedForOrderedModeImpl(
    const std::string & path, ProcessingNodeHolderPtr holder, const std::string & processed_node_path)
{
    /// Update a persistent node in /processed and remove ephemeral node from /processing.

    const auto node_name = getNodeName(path);
    const auto node_metadata = createNodeMetadata(path).toString();
    const auto zk_client = getZooKeeper();

    LOG_TEST(log, "Setting file `{}` as processed (at {})", path, processed_node_path);
    while (true)
    {
        std::string res;
        Coordination::Stat stat;
        bool exists = zk_client->tryGet(processed_node_path, res, &stat);
        Coordination::Requests requests;
        if (exists)
        {
            if (!res.empty())
            {
                auto metadata = NodeMetadata::fromString(res);
                if (metadata.file_path >= path)
                {
                    LOG_TRACE(log, "File {} is already processed, current max processed file: {}", path, metadata.file_path);
                    return;
                }
            }
            requests.push_back(zkutil::makeSetRequest(processed_node_path, node_metadata, stat.version));
        }
        else
        {
            requests.push_back(zkutil::makeCreateRequest(processed_node_path, node_metadata, zkutil::CreateMode::Persistent));
        }

        Coordination::Responses responses;
        if (holder)
        {
            if (holder->remove(&requests, &responses))
            {
                LOG_TEST(log, "Moved file `{}` to processed", path);
                if (max_loading_retries)
                    zk_client->tryRemove(zookeeper_failed_path / (node_name + ".retriable"), -1);
                return;
            }
        }
        else
        {
            auto code = zk_client->tryMulti(requests, responses);
            if (code == Coordination::Error::ZOK)
            {
                LOG_TEST(log, "Moved file `{}` to processed", path);
                return;
            }
        }

        /// Failed to update max processed node, retry.
        if (!responses.empty() && responses[0]->error != Coordination::Error::ZOK)
        {
            LOG_TRACE(log, "Failed to update processed node ({}). Will retry.", magic_enum::enum_name(responses[0]->error));
            continue;
        }

        LOG_WARNING(log, "Cannot set file ({}) as processed since processing node "
                    "does not exist with expected processing id does not exist, "
                    "this could be a result of expired zookeeper session", path);
        return;
    }
}

void S3QueueFilesMetadata::setFileProcessed(const std::string & path, size_t shard_id)
{
    if (mode != S3QueueMode::ORDERED)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can set file as preprocessed only for Ordered mode");

    if (isShardedProcessing())
    {
        for (const auto & processor : getProcessingIdsForShard(shard_id))
            setFileProcessedForOrderedModeImpl(path, nullptr, zookeeper_processed_path / toString(processor));
    }
    else
    {
        setFileProcessedForOrderedModeImpl(path, nullptr, zookeeper_processed_path);
    }
}

void S3QueueFilesMetadata::setFileFailed(ProcessingNodeHolderPtr holder, const String & exception_message)
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3QueueSetFileFailedMicroseconds);
    const auto & path = holder->path;

    auto file_status = holder->getFileStatus();
    {
        std::lock_guard lock(file_status->metadata_lock);
        file_status->state = FileStatus::State::Failed;
        file_status->last_exception = exception_message;
        file_status->processing_end_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    }

    SCOPE_EXIT({
        file_status->profile_counters.increment(ProfileEvents::S3QueueSetFileFailedMicroseconds, timer.get());
        timer.cancel();
    });

    const auto node_name = getNodeName(path);
    auto node_metadata = createNodeMetadata(path, exception_message);
    const auto zk_client = getZooKeeper();

    /// Is file retriable?
    if (max_loading_retries == 0)
    {
        /// File is not retriable,
        /// just create a node in /failed and remove a node from /processing.

        Coordination::Requests requests;
        requests.push_back(zkutil::makeCreateRequest(zookeeper_failed_path / node_name,
                                                     node_metadata.toString(),
                                                     zkutil::CreateMode::Persistent));
        Coordination::Responses responses;
        if (holder->remove(&requests, &responses))
        {
            LOG_TEST(log, "File `{}` failed to process and will not be retried. "
                     "Error: {}", path, exception_message);
            return;
        }

        if (responses[0]->error != Coordination::Error::ZOK)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Cannot create a persistent node in /failed since it already exists");
        }

        LOG_WARNING(log, "Cannot set file ({}) as processed since processing node "
                    "does not exist with expected processing id does not exist, "
                    "this could be a result of expired zookeeper session", path);
        return;
    }

    /// So file is retriable.
    /// Let's do an optimization here.
    /// Instead of creating a persistent /failed/node_hash node
    /// we create a persistent /failed/node_hash.retriable node.
    /// This allows us to make less zookeeper requests as we avoid checking
    /// the number of already done retries in trySetFileAsProcessing.

    const auto node_name_with_retriable_suffix = node_name + ".retriable";
    Coordination::Stat stat;
    std::string res;

    /// Extract the number of already done retries from node_hash.retriable node if it exists.
    if (zk_client->tryGet(zookeeper_failed_path / node_name_with_retriable_suffix, res, &stat))
    {
        auto failed_node_metadata = NodeMetadata::fromString(res);
        node_metadata.retries = failed_node_metadata.retries + 1;

        std::lock_guard lock(file_status->metadata_lock);
        file_status->retries = node_metadata.retries;
    }

    LOG_TEST(log, "File `{}` failed to process, try {}/{} (Error: {})",
             path, node_metadata.retries, max_loading_retries, exception_message);

    /// Check if file can be retried further or not.
    if (node_metadata.retries >= max_loading_retries)
    {
        /// File is no longer retriable.
        /// Make a persistent node /failed/node_hash, remove /failed/node_hash.retriable node and node in /processing.

        Coordination::Requests requests;
        requests.push_back(zkutil::makeRemoveRequest(zookeeper_processing_path / node_name, -1));
        requests.push_back(zkutil::makeRemoveRequest(zookeeper_failed_path / node_name_with_retriable_suffix,
                                                     stat.version));
        requests.push_back(zkutil::makeCreateRequest(zookeeper_failed_path / node_name,
                                                     node_metadata.toString(),
                                                     zkutil::CreateMode::Persistent));

        Coordination::Responses responses;
        auto code = zk_client->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
            return;

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set file as failed");
    }
    else
    {
        /// File is still retriable, update retries count and remove node from /processing.

        Coordination::Requests requests;
        requests.push_back(zkutil::makeRemoveRequest(zookeeper_processing_path / node_name, -1));
        if (node_metadata.retries == 0)
        {
            requests.push_back(zkutil::makeCreateRequest(zookeeper_failed_path / node_name_with_retriable_suffix,
                                                         node_metadata.toString(),
                                                         zkutil::CreateMode::Persistent));
        }
        else
        {
            requests.push_back(zkutil::makeSetRequest(zookeeper_failed_path / node_name_with_retriable_suffix,
                                                      node_metadata.toString(),
                                                      stat.version));
        }
        Coordination::Responses responses;
        auto code = zk_client->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
            return;

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set file as failed");
    }
}

S3QueueFilesMetadata::ProcessingNodeHolder::ProcessingNodeHolder(
    const std::string & processing_id_,
    const std::string & path_,
    const std::string & zk_node_path_,
    FileStatusPtr file_status_,
    zkutil::ZooKeeperPtr zk_client_)
    : zk_client(zk_client_)
    , file_status(file_status_)
    , path(path_)
    , zk_node_path(zk_node_path_)
    , processing_id(processing_id_)
    , log(getLogger("ProcessingNodeHolder"))
{
}

S3QueueFilesMetadata::ProcessingNodeHolder::~ProcessingNodeHolder()
{
    if (!removed)
        remove();
}

bool S3QueueFilesMetadata::ProcessingNodeHolder::remove(Coordination::Requests * requests, Coordination::Responses * responses)
{
    if (removed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Processing node is already removed");

    LOG_TEST(log, "Removing processing node {} ({})", zk_node_path, path);

    try
    {
        if (!zk_client->expired())
        {
            /// Is is possible that we created an ephemeral processing node
            /// but session expired and someone other created an ephemeral processing node.
            /// To avoid deleting this new node, check processing_id.
            std::string res;
            Coordination::Stat stat;
            if (zk_client->tryGet(zk_node_path, res, &stat))
            {
                auto node_metadata = NodeMetadata::fromString(res);
                if (node_metadata.processing_id == processing_id)
                {
                    if (requests)
                    {
                        requests->push_back(zkutil::makeRemoveRequest(zk_node_path, stat.version));
                        auto code = zk_client->tryMulti(*requests, *responses);
                        removed = code == Coordination::Error::ZOK;
                    }
                    else
                    {
                        zk_client->remove(zk_node_path);
                        removed = true;
                    }
                    return removed;
                }
                else
                    LOG_WARNING(log, "Cannot remove {} since processing id changed: {} -> {}",
                                zk_node_path, processing_id, node_metadata.processing_id);
            }
            else
                LOG_DEBUG(log, "Cannot remove {}, node doesn't exist, "
                          "probably because of session expiration", zk_node_path);

            /// TODO: this actually would mean that we already processed (or partially processed)
            /// the data but another thread will try processing it again and data can be duplicated.
            /// This can be solved via persistenly saving last processed offset in the file.
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::CannotRemoveEphemeralNode);
            LOG_DEBUG(log, "Cannot remove {} since session has been expired", zk_node_path);
        }
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::CannotRemoveEphemeralNode);
        DB::tryLogCurrentException(__PRETTY_FUNCTION__, "Cannot remove " + zk_node_path);
    }
    return false;
}

void S3QueueFilesMetadata::cleanupThreadFunc()
{
    /// A background task is responsible for maintaining
    /// max_set_size and max_set_age settings for `unordered` processing mode.

    if (shutdown)
        return;

    try
    {
        cleanupThreadFuncImpl();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    if (shutdown)
        return;

    task->scheduleAfter(generateRescheduleInterval(min_cleanup_interval_ms, max_cleanup_interval_ms));
}

void S3QueueFilesMetadata::cleanupThreadFuncImpl()
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3QueueCleanupMaxSetSizeOrTTLMicroseconds);

    chassert(max_set_size || max_set_age_sec);

    const bool check_nodes_limit = max_set_size > 0;
    const bool check_nodes_ttl = max_set_age_sec > 0;

    const auto zk_client = getZooKeeper();
    Strings nodes;
    auto code = zk_client->tryGetChildren(zookeeper_processed_path, nodes);
    if (code != Coordination::Error::ZOK)
    {
        if (code == Coordination::Error::ZNONODE)
        {
            LOG_TEST(log, "A `processed` not is not yet created");
            return;
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected error: {}", magic_enum::enum_name(code));
    }

    if (nodes.empty())
    {
        LOG_TEST(log, "A set of nodes is empty");
        return;
    }

    const bool nodes_limit_exceeded = nodes.size() > max_set_size;
    if (!nodes_limit_exceeded && check_nodes_limit && !check_nodes_ttl)
    {
        LOG_TEST(log, "No limit exceeded");
        return;
    }

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
        std::string name;
        NodeMetadata metadata;
    };
    auto node_cmp = [](const Node & a, const Node & b)
    {
        return std::tie(a.metadata.last_processed_timestamp, a.metadata.file_path)
            < std::tie(b.metadata.last_processed_timestamp, b.metadata.file_path);
    };

    /// Ordered in ascending order of timestamps.
    std::set<Node, decltype(node_cmp)> sorted_nodes(node_cmp);

    LOG_TRACE(log, "Found {} nodes", nodes.size());

    for (const auto & node : nodes)
    {
        try
        {
            std::string metadata_str;
            if (zk_client->tryGet(zookeeper_processed_path / node, metadata_str))
            {
                sorted_nodes.emplace(node, NodeMetadata::fromString(metadata_str));
                LOG_TEST(log, "Fetched metadata for node {}", node);
            }
            else
                LOG_TEST(log, "Failed to fetch node metadata {}", node);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
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

    size_t nodes_to_remove = check_nodes_limit && nodes_limit_exceeded ? nodes.size() - max_set_size : 0;
    for (const auto & node : sorted_nodes)
    {
        if (nodes_to_remove)
        {
            auto path = zookeeper_processed_path / node.name;
            LOG_TEST(log, "Removing node at path {} ({}) because max files limit is reached",
                     node.metadata.file_path, path.string());

            local_file_statuses.remove(node.metadata.file_path, /* if_exists */true);

            code = zk_client->tryRemove(path);
            if (code == Coordination::Error::ZOK)
                --nodes_to_remove;
            else
                LOG_ERROR(log, "Failed to remove a node `{}` (code: {})", path.string(), code);
        }
        else if (check_nodes_ttl)
        {
            UInt64 node_age = getCurrentTime() - node.metadata.last_processed_timestamp;
            if (node_age >= max_set_age_sec)
            {
                auto path = zookeeper_processed_path / node.name;
                LOG_TEST(log, "Removing node at path {} ({}) because file is reached",
                        node.metadata.file_path, path.string());

                local_file_statuses.remove(node.metadata.file_path, /* if_exists */true);

                code = zk_client->tryRemove(path);
                if (code != Coordination::Error::ZOK)
                    LOG_ERROR(log, "Failed to remove a node `{}` (code: {})", path.string(), code);
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
