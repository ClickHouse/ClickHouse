#include "config.h"

#include <base/sleep.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/randomSeed.h>
#include <Common/getRandomASCIIString.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/S3Queue/S3QueueFilesMetadata.h>
#include <Storages/S3Queue/S3QueueSettings.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/StorageSnapshot.h>
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

std::shared_ptr<S3QueueFilesMetadata::FileStatus> S3QueueFilesMetadata::LocalFileStatuses::get(const std::string & filename, bool create)
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
    , zookeeper_processing_path(zookeeper_path_ / "processing")
    , zookeeper_processed_path(zookeeper_path_ / "processed")
    , zookeeper_failed_path(zookeeper_path_ / "failed")
    , zookeeper_cleanup_lock_path(zookeeper_path_ / "cleanup_lock")
    , log(&Poco::Logger::get("S3QueueFilesMetadata"))
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

std::shared_ptr<S3QueueFilesMetadata::FileStatus> S3QueueFilesMetadata::getFileStatus(const std::string & path)
{
    return local_file_statuses.get(path, /* create */false);
}

std::string S3QueueFilesMetadata::getNodeName(const std::string & path)
{
    SipHash path_hash;
    path_hash.update(path);
    return toString(path_hash.get64());
}

S3QueueFilesMetadata::NodeMetadata S3QueueFilesMetadata::createNodeMetadata(
    const std::string & path,
    const std::string & exception,
    size_t retries)
{
    NodeMetadata metadata;
    metadata.file_path = path;
    metadata.last_processed_timestamp = getCurrentTime();
    metadata.last_exception = exception;
    metadata.retries = retries;
    return metadata;
}

S3QueueFilesMetadata::ProcessingNodeHolderPtr S3QueueFilesMetadata::trySetFileAsProcessing(const std::string & path)
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3QueueSetFileProcessingMicroseconds);
    auto file_status = local_file_statuses.get(path, /* create */true);

    /// Check locally cached file status.
    switch (file_status->state)
    {
        case FileStatus::State::Processing: [[fallthrough]];
        case FileStatus::State::Processed:
        {
            /// File is already processes or processing by current server.
            return nullptr;
        }
        case FileStatus::State::Failed:
        {
            /// max_loading_retries == 0 => file is not retriable.
            /// file_status->retries is a cached value, so in case file_status->retries >= max_loading retries
            /// we can fully rely that it is true, but in other case the value might be outdated,
            /// but this is ok, we will recheck with zookeeper.
            if (!max_loading_retries || file_status->retries >= max_loading_retries)
                return nullptr;
            break;
        }
        case FileStatus::State::None:
        {
            /// The file was not processed by current server,
            /// check metadata in zookeeper.
            break;
        }
    }
    std::unique_lock lock(file_status->processing_lock, std::defer_lock);
    if (!lock.try_lock())
    {
        /// Another thread is already trying to set file as processing.
        return nullptr;
    }

    SetFileProcessingResult result;
    ProcessingNodeHolderPtr processing_holder;
    switch (mode)
    {
        case S3QueueMode::ORDERED:
        {
            std::tie(result, processing_holder) = trySetFileAsProcessingForOrderedMode(path);
            break;
        }
        case S3QueueMode::UNORDERED:
        {
            std::tie(result, processing_holder) = trySetFileAsProcessingForUnorderedMode(path);
            break;
        }
    }
    switch (result)
    {
        case SetFileProcessingResult::Success:
        {
            file_status->state = FileStatus::State::Processing;
            file_status->profile_counters.increment(ProfileEvents::S3QueueSetFileProcessingMicroseconds, timer.get());
            timer.cancel();
            if (!file_status->processing_start_time)
                file_status->processing_start_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            break;
        }
        case SetFileProcessingResult::AlreadyProcessed:
        {
            /// Cache the state.
            file_status->state = FileStatus::State::Processed;
            break;
        }
        case SetFileProcessingResult::AlreadyFailed:
        {
            /// Cache the state.
            file_status->state = FileStatus::State::Failed;
            break;
        }
        case SetFileProcessingResult::ProcessingByOtherNode:
        {
            /// We cannot save any local state.
            break;
        }
    }

    if (result != SetFileProcessingResult::Success)
        return nullptr;

    return processing_holder;
}

std::pair<S3QueueFilesMetadata::SetFileProcessingResult, S3QueueFilesMetadata::ProcessingNodeHolderPtr>
S3QueueFilesMetadata::trySetFileAsProcessingForUnorderedMode(const std::string & path)
{
    /// Create an ephemenral node in /processing
    /// if corresponding node does not exist in failed/, processed/ and processing/.
    /// Return false otherwise.

    const auto node_name = getNodeName(path);
    const auto zk_client = getZooKeeper();
    auto node_metadata = createNodeMetadata(path);
    node_metadata.processing_id = getRandomASCIIString(10);

    Coordination::Requests requests;
    zkutil::addCheckNotExistsRequest(requests, *zk_client, zookeeper_processed_path / node_name);
    zkutil::addCheckNotExistsRequest(requests, *zk_client, zookeeper_failed_path / node_name);
    requests.push_back(zkutil::makeCreateRequest(zookeeper_processing_path / node_name, node_metadata.toString(), zkutil::CreateMode::Ephemeral));

    Coordination::Responses responses;
    auto code = zk_client->tryMulti(requests, responses);

    if (code == Coordination::Error::ZOK)
    {
        auto holder = std::make_unique<ProcessingNodeHolder>(node_metadata.processing_id, path, zookeeper_processing_path / node_name, zk_client);
        return std::pair{SetFileProcessingResult::Success, std::move(holder)};
    }

    if (responses[0]->error != Coordination::Error::ZOK)
        return std::pair{SetFileProcessingResult::AlreadyProcessed, nullptr};

    if (responses[1]->error != Coordination::Error::ZOK)
        return std::pair{SetFileProcessingResult::AlreadyFailed, nullptr};

    chassert(responses[2]->error != Coordination::Error::ZOK);
    return std::pair{SetFileProcessingResult::ProcessingByOtherNode, nullptr};
}

std::pair<S3QueueFilesMetadata::SetFileProcessingResult, S3QueueFilesMetadata::ProcessingNodeHolderPtr>
S3QueueFilesMetadata::trySetFileAsProcessingForOrderedMode(const std::string & path)
{
    /// Create an ephemenral node in /processing
    /// if corresponding it does not exist in failed/, processing/ and satisfied max processed file check.
    /// Return false otherwise.

    const auto node_name = getNodeName(path);
    const auto zk_client = getZooKeeper();
    auto node_metadata = createNodeMetadata(path);
    node_metadata.processing_id = getRandomASCIIString(10);

    while (true)
    {
        Coordination::Requests requests;
        zkutil::addCheckNotExistsRequest(requests, *zk_client, zookeeper_failed_path / node_name);
        zkutil::addCheckNotExistsRequest(requests, *zk_client, zookeeper_processing_path / node_name);

        Coordination::Responses responses;
        auto code = zk_client->tryMulti(requests, responses);
        if (code != Coordination::Error::ZOK)
        {
            if (responses[0]->error == Coordination::Error::ZOK)
            {
                LOG_TEST(log, "Skipping file `{}`: already processing", path);
                return std::pair{SetFileProcessingResult::ProcessingByOtherNode, nullptr};
            }
            else
            {
                LOG_TEST(log, "Skipping file `{}`: failed", path);
                return std::pair{SetFileProcessingResult::AlreadyFailed, nullptr};
            }
        }

        Coordination::Stat processed_node_stat;
        auto data = zk_client->get(zookeeper_processed_path, &processed_node_stat);
        NodeMetadata processed_node_metadata;
        if (!data.empty())
            processed_node_metadata = NodeMetadata::fromString(data);

        auto max_processed_file_path = processed_node_metadata.file_path;
        if (!max_processed_file_path.empty() && path <= max_processed_file_path)
            return std::pair{SetFileProcessingResult::AlreadyProcessed, nullptr};

        requests.clear();
        responses.clear();

        zkutil::addCheckNotExistsRequest(requests, *zk_client, zookeeper_failed_path / node_name);
        requests.push_back(zkutil::makeCreateRequest(zookeeper_processing_path / node_name, node_metadata.toString(), zkutil::CreateMode::Ephemeral));
        requests.push_back(zkutil::makeCheckRequest(zookeeper_processed_path, processed_node_stat.version));

        code = zk_client->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
        {
            auto holder = std::make_unique<ProcessingNodeHolder>(node_metadata.processing_id, path, zookeeper_processing_path / node_name, zk_client);
            return std::pair{SetFileProcessingResult::Success, std::move(holder)};
        }

        if (responses[0]->error != Coordination::Error::ZOK)
        {
            LOG_TEST(log, "Skipping file `{}`: failed", path);
            return std::pair{SetFileProcessingResult::AlreadyFailed, nullptr};
        }
        else if (responses[1]->error != Coordination::Error::ZOK)
        {
            LOG_TEST(log, "Skipping file `{}`: already processing", path);
            return std::pair{SetFileProcessingResult::ProcessingByOtherNode, nullptr};
        }
        else
        {
            LOG_TEST(log, "Version of max processed file changed. Retring the check for file `{}`", path);
        }
    }
}

void S3QueueFilesMetadata::setFileProcessed(ProcessingNodeHolderPtr holder)
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3QueueSetFileProcessedMicroseconds);
    const auto & path = holder->path;

    SCOPE_EXIT({
        auto file_status = local_file_statuses.get(path, /* create */false);
        file_status->state = FileStatus::State::Processed;
        file_status->profile_counters.increment(ProfileEvents::S3QueueSetFileProcessedMicroseconds, timer.get());
        timer.cancel();
        file_status->processing_end_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
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
        return;
    }

    if (!responses.empty() && responses[0]->error != Coordination::Error::ZOK)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to set file as processed but it is already processed");

    LOG_WARNING(log, "Cannot set file ({}) as processed since processing node "
                "does not exist with expected processing id does not exist, "
                "this could be a result of expired zookeeper session", path);
}

void S3QueueFilesMetadata::setFileProcessedForOrderedMode(ProcessingNodeHolderPtr holder)
{
    const auto & path = holder->path;
    const auto node_name = getNodeName(path);
    const auto node_metadata = createNodeMetadata(path).toString();
    const auto zk_client = getZooKeeper();

    while (true)
    {
        std::string res;
        Coordination::Stat stat;
        bool exists = zk_client->tryGet(zookeeper_processed_path, res, &stat);
        Coordination::Requests requests;
        if (exists)
        {
            if (!res.empty())
            {
                auto metadata = NodeMetadata::fromString(res);
                if (metadata.file_path >= path)
                    return;
            }
            requests.push_back(zkutil::makeSetRequest(zookeeper_processed_path, node_metadata, stat.version));
        }
        else
        {
            requests.push_back(zkutil::makeCreateRequest(zookeeper_processed_path, node_metadata, zkutil::CreateMode::Persistent));
        }

        Coordination::Responses responses;
        if (holder->remove(&requests, &responses))
            return;

        if (!responses.empty() && responses[0]->error != Coordination::Error::ZOK)
            continue;

        LOG_WARNING(log, "Cannot set file ({}) as processed since processing node "
                    "does not exist with expected processing id does not exist, "
                    "this could be a result of expired zookeeper session", path);
        return;
    }
}

void S3QueueFilesMetadata::setFileFailed(ProcessingNodeHolderPtr holder, const String & exception_message)
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3QueueSetFileFailedMicroseconds);
    const auto & path = holder->path;

    auto file_status = local_file_statuses.get(path, /* create */false);
    SCOPE_EXIT({
        file_status->state = FileStatus::State::Failed;
        file_status->profile_counters.increment(ProfileEvents::S3QueueSetFileFailedMicroseconds, timer.get());
        timer.cancel();
        file_status->processing_end_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    });

    const auto node_name = getNodeName(path);
    auto node_metadata = createNodeMetadata(path, exception_message);
    const auto zk_client = getZooKeeper();

    if (max_loading_retries == 0)
    {
        Coordination::Requests requests;
        requests.push_back(zkutil::makeCreateRequest(zookeeper_failed_path / node_name,
                                                     node_metadata.toString(),
                                                     zkutil::CreateMode::Persistent));

        Coordination::Responses responses;
        if (holder->remove(&requests,  &responses))
        {
            LOG_TEST(log, "File `{}` failed to process and will not be retried. "
                     "Error: {}", path, exception_message);
            return;
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set file as failed");
    }

    const auto node_name_with_retriable_suffix = node_name + ".retriable";

    Coordination::Stat stat;
    std::string res;
    if (zk_client->tryGet(zookeeper_failed_path / node_name_with_retriable_suffix, res, &stat))
    {
        auto failed_node_metadata = NodeMetadata::fromString(res);
        node_metadata.retries = failed_node_metadata.retries + 1;
        file_status->retries = node_metadata.retries;
    }

    LOG_TEST(log, "File `{}` failed to process, try {}/{} (Error: {})",
             path, node_metadata.retries, max_loading_retries, exception_message);

    if (node_metadata.retries >= max_loading_retries)
    {
        /// File is no longer retriable.
        /// Make a failed/node_name node and remove failed/node_name.retriable node.

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
    zkutil::ZooKeeperPtr zk_client_)
    : zk_client(zk_client_)
    , path(path_)
    , zk_node_path(zk_node_path_)
    , processing_id(processing_id_)
    , log(&Poco::Logger::get("ProcessingNodeHolder"))
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
                    LOG_WARNING(log, "Cannot remove {} since precessing id changed: {} -> {}",
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
    auto nodes = zk_client->getChildren(zookeeper_processed_path);
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
    auto ephemeral_node = zkutil::EphemeralNodeHolder::create(zookeeper_cleanup_lock_path, *zk_client, toString(getCurrentTime()));
    if (!ephemeral_node)
    {
        LOG_TEST(log, "Cleanup is already being executed by another node");
        return;
    }

    struct Node
    {
        std::string name;
        NodeMetadata metadata;
    };
    auto node_cmp = [](const Node & a, const Node & b)
    {
        if (a.metadata.last_processed_timestamp == b.metadata.last_processed_timestamp)
            return a.metadata.file_path < b.metadata.file_path;
        else
            return a.metadata.last_processed_timestamp < b.metadata.last_processed_timestamp;
    };

    /// Ordered in ascending order of timestamps.
    std::multiset<Node, decltype(node_cmp)> sorted_nodes(node_cmp);

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
    for  (const auto & node : sorted_nodes)
    {
        if (nodes_to_remove)
        {
            auto path = zookeeper_processed_path / node.name;
            LOG_TEST(log, "Removing node at path {} ({}) because max files limit is reached",
                     node.metadata.file_path, path.string());

            local_file_statuses.remove(node.metadata.file_path, /* if_exists */true);

            auto code = zk_client->tryRemove(path);
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

                auto code = zk_client->tryRemove(path);
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
