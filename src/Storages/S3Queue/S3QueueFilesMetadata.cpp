#include <set>
#include "Common/Exception.h"
#include "Common/ZooKeeper/Types.h"
#include "Common/scope_guard_safe.h"
#include "Interpreters/Context_fwd.h"
#include "Storages/S3Queue/S3QueueSettings.h"
#include "config.h"

#if USE_AWS_S3
#include <base/sleep.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/randomSeed.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Storages/S3Queue/S3QueueFilesMetadata.h>
#include <Storages/S3Queue/StorageS3Queue.h>
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
};

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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

S3QueueFilesMetadata::S3QueueFilesMetadata(
    const StorageS3Queue * storage_,
    const S3QueueSettings & settings_,
    ContextPtr context)
    : storage(storage_)
    , mode(settings_.mode)
    , max_set_size(settings_.s3queue_tracked_files_limit.value)
    , max_set_age_sec(settings_.s3queue_tracked_file_ttl_sec.value)
    , max_loading_retries(settings_.s3queue_loading_retries.value)
    , min_cleanup_interval_ms(settings_.s3queue_cleanup_interval_min_ms.value)
    , max_cleanup_interval_ms(settings_.s3queue_cleanup_interval_max_ms.value)
    , zookeeper_processing_path(storage->getZooKeeperPath() / "processing")
    , zookeeper_processed_path(storage->getZooKeeperPath() / "processed")
    , zookeeper_failed_path(storage->getZooKeeperPath() / "failed")
    , zookeeper_cleanup_lock_path(storage->getZooKeeperPath() / "cleanup_lock")
    , log(&Poco::Logger::get("S3QueueFilesMetadata"))
{
    if (mode == S3QueueMode::UNORDERED && (max_set_size || max_set_age_sec))
    {
        task = context->getSchedulePool().createTask("S3QueueCleanupFunc", [this] { cleanupThreadFunc(); });
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

std::string S3QueueFilesMetadata::NodeMetadata::toString() const
{
    Poco::JSON::Object json;
    json.set("file_path", file_path);
    json.set("last_processed_timestamp", getCurrentTime());
    json.set("last_exception", last_exception);
    json.set("retries", retries);

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
    return metadata;
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

std::shared_ptr<S3QueueFilesMetadata::FileStatus> S3QueueFilesMetadata::getFileStatus(const std::string & path)
{
    std::lock_guard lock(file_statuses_mutex);
    return file_statuses.at(path);
}

S3QueueFilesMetadata::FileStatuses S3QueueFilesMetadata::getFileStateses() const
{
    std::lock_guard lock(file_statuses_mutex);
    return file_statuses;
}

bool S3QueueFilesMetadata::trySetFileAsProcessing(const std::string & path)
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3QueueSetFileProcessingMicroseconds);

    bool result;
    switch (mode)
    {
        case S3QueueMode::ORDERED:
        {
            result = trySetFileAsProcessingForOrderedMode(path);
            break;
        }
        case S3QueueMode::UNORDERED:
        {
            result = trySetFileAsProcessingForUnorderedMode(path);
            break;
        }
    }
    if (result)
    {
        std::lock_guard lock(file_statuses_mutex);
        auto it = file_statuses.emplace(path, std::make_shared<FileStatus>()).first;
        auto & file_status = it->second;
        file_status->state = FileStatus::State::Processing;
        file_status->profile_counters.increment(ProfileEvents::S3QueueSetFileProcessingMicroseconds, timer.get());
        timer.cancel();
        if (!file_status->processing_start_time)
            file_status->processing_start_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    }
    return result;
}

bool S3QueueFilesMetadata::trySetFileAsProcessingForUnorderedMode(const std::string & path)
{
    /// Create an ephemenral node in /processing
    /// if corresponding node does not exist in failed/, processed/ and processing/.
    /// Return false otherwise.

    const auto node_name = getNodeName(path);
    const auto node_metadata = createNodeMetadata(path).toString();
    const auto zk_client = storage->getZooKeeper();

    Coordination::Requests requests;
    zkutil::addCheckNotExistsRequest(requests, *zk_client, zookeeper_processed_path / node_name);
    zkutil::addCheckNotExistsRequest(requests, *zk_client, zookeeper_failed_path / node_name);
    requests.push_back(zkutil::makeCreateRequest(zookeeper_processing_path / node_name, node_metadata, zkutil::CreateMode::Ephemeral));

    Coordination::Responses responses;
    auto code = zk_client->tryMulti(requests, responses);
    return code == Coordination::Error::ZOK;
}

bool S3QueueFilesMetadata::trySetFileAsProcessingForOrderedMode(const std::string & path)
{
    /// Create an ephemenral node in /processing
    /// if corresponding it does not exist in failed/, processing/ and satisfied max processed file check.
    /// Return false otherwise.

    const auto node_name = getNodeName(path);
    const auto node_metadata = createNodeMetadata(path).toString();
    const auto zk_client = storage->getZooKeeper();

    while (true)
    {
        Coordination::Requests requests;
        zkutil::addCheckNotExistsRequest(requests, *zk_client, zookeeper_failed_path / node_name);
        zkutil::addCheckNotExistsRequest(requests, *zk_client, zookeeper_processing_path / node_name);

        Coordination::Responses responses;
        auto code = zk_client->tryMulti(requests, responses);
        if (code != Coordination::Error::ZOK)
        {
            LOG_TEST(log, "Skipping file `{}`: {}",
                    path, responses[0]->error != Coordination::Error::ZOK ? "failed" : "processing");
            return false;
        }

        Coordination::Stat processed_node_stat;
        auto data = zk_client->get(zookeeper_processed_path, &processed_node_stat);
        NodeMetadata processed_node_metadata;
        if (!data.empty())
            processed_node_metadata = NodeMetadata::fromString(data);

        auto max_processed_file_path = processed_node_metadata.file_path;
        if (!max_processed_file_path.empty() && path <= max_processed_file_path)
            return false;

        requests.clear();
        responses.clear();
        zkutil::addCheckNotExistsRequest(requests, *zk_client, zookeeper_failed_path / node_name);
        requests.push_back(zkutil::makeCreateRequest(zookeeper_processing_path / node_name, node_metadata, zkutil::CreateMode::Ephemeral));
        requests.push_back(zkutil::makeCheckRequest(zookeeper_processed_path, processed_node_stat.version));

        code = zk_client->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
            return true;

        if (responses[0]->error != Coordination::Error::ZOK
            || responses[1]->error != Coordination::Error::ZOK)
        {
            LOG_TEST(log, "Skipping file `{}`: {}",
                    path, responses[0]->error != Coordination::Error::ZOK ? "failed" : "processing");
            return false;
        }
        else
        {
            LOG_TEST(log, "Version of max processed file changed. Retring the check for file `{}`", path);
        }
    }
}

void S3QueueFilesMetadata::setFileProcessed(const String & path)
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3QueueSetFileProcessedMicroseconds);
    SCOPE_EXIT({
        std::lock_guard lock(file_statuses_mutex);
        auto & file_status = file_statuses.at(path);
        file_status->state = FileStatus::State::Processed;
        file_status->profile_counters.increment(ProfileEvents::S3QueueSetFileProcessedMicroseconds, timer.get());
        timer.cancel();
        file_status->processing_end_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    });

    switch (mode)
    {
        case S3QueueMode::ORDERED:
        {
            return setFileProcessedForOrderedMode(path);
        }
        case S3QueueMode::UNORDERED:
        {
            return setFileProcessedForUnorderedMode(path);
        }
    }
}

void S3QueueFilesMetadata::setFileProcessedForUnorderedMode(const String & path)
{
    /// Create a persistent node in /processed and remove ephemeral node from /processing.

    const auto node_name = getNodeName(path);
    const auto node_metadata = createNodeMetadata(path).toString();
    const auto zk_client = storage->getZooKeeper();

    Coordination::Requests requests;
    requests.push_back(zkutil::makeRemoveRequest(zookeeper_processing_path / node_name, -1));
    requests.push_back(zkutil::makeCreateRequest(zookeeper_processed_path / node_name, node_metadata, zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    auto code = zk_client->tryMulti(requests, responses);
    if (code == Coordination::Error::ZOK)
    {
        LOG_TEST(log, "Moved file `{}` to processed", path);
        return;
    }

    /// TODO this could be because of the expired session.
    if (responses[0]->error != Coordination::Error::ZOK)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to set file as processed but it is not processing");
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to set file as processed but it is already processed");
}

void S3QueueFilesMetadata::setFileProcessedForOrderedMode(const String & path)
{
    const auto node_name = getNodeName(path);
    const auto node_metadata = createNodeMetadata(path).toString();
    const auto zk_client = storage->getZooKeeper();

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
        auto code = zk_client->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
            return;
    }
}

void S3QueueFilesMetadata::setFileFailed(const String & path, const String & exception_message)
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3QueueSetFileFailedMicroseconds);

    SCOPE_EXIT_SAFE({
        std::lock_guard lock(file_statuses_mutex);
        auto & file_status = file_statuses.at(path);
        file_status->state = FileStatus::State::Failed;
        file_status->profile_counters.increment(ProfileEvents::S3QueueSetFileFailedMicroseconds, timer.get());
        timer.cancel();
        file_status->processing_end_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    });

    const auto node_name = getNodeName(path);
    auto node_metadata = createNodeMetadata(path, exception_message);
    const auto zk_client = storage->getZooKeeper();

    if (max_loading_retries == 0)
    {
        Coordination::Requests requests;
        requests.push_back(zkutil::makeRemoveRequest(zookeeper_processing_path / node_name, -1));
        requests.push_back(zkutil::makeCreateRequest(zookeeper_failed_path / node_name,
                                                     node_metadata.toString(),
                                                     zkutil::CreateMode::Persistent));

        Coordination::Responses responses;
        auto code = zk_client->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
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
    }

    LOG_TEST(log, "File `{}` failed to process, try {}/{} (Error: {})",
             path, node_metadata.retries, max_loading_retries, exception_message);

    if (node_metadata.retries >= max_loading_retries)
    {
        /// File is no longer retriable.
        /// Make a failed/node_name node and remove failed/node_name.retriable node.
        /// TODO: always add version for processing node.

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

    const auto zk_client = storage->getZooKeeper();
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
    Coordination::Error code = zk_client->tryCreate(zookeeper_cleanup_lock_path,
                                                    toString(getCurrentTime()),
                                                    zkutil::CreateMode::Ephemeral);
    if (code == Coordination::Error::ZNODEEXISTS)
    {
        LOG_TEST(log, "Cleanup is already being executed by another node");
        return;
    }
    else if (code != Coordination::Error::ZOK)
    {
        throw Coordination::Exception::fromPath(code, zookeeper_cleanup_lock_path);
    }

    SCOPE_EXIT_SAFE({
        try
        {
            zk_client->remove(zookeeper_cleanup_lock_path);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            chassert(false);
        }
    });

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

}

#endif
