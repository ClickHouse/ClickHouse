#include "S3QueueIFileMetadata.h"
#include <Common/SipHash.h>
#include <Common/CurrentThread.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>


namespace ProfileEvents
{
    extern const Event S3QueueSetFileProcessingMicroseconds;
    extern const Event S3QueueSetFileProcessedMicroseconds;
    extern const Event S3QueueSetFileFailedMicroseconds;
    extern const Event S3QueueProcessedFiles;
    extern const Event S3QueueFailedFiles;
};

namespace DB
{
namespace
{
    UInt64 getCurrentTime()
    {
        return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    }

    zkutil::ZooKeeperPtr getZooKeeper()
    {
        return Context::getGlobalContextInstance()->getZooKeeper();
    }
}

std::string IFileMetadata::NodeMetadata::toString() const
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

IFileMetadata::NodeMetadata IFileMetadata::NodeMetadata::fromString(const std::string & metadata_str)
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

IFileMetadata::IFileMetadata(
    const std::string & path_,
    const std::string & processing_node_path_,
    const std::string & processed_node_path_,
    const std::string & failed_node_path_,
    FileStatusPtr file_status_,
    size_t max_loading_retries_,
    LoggerPtr log_)
    : path(path_)
    , node_name(getNodeName(path_))
    , file_status(file_status_)
    , max_loading_retries(max_loading_retries_)
    , processing_node_path(processing_node_path_)
    , processed_node_path(processed_node_path_)
    , failed_node_path(failed_node_path_)
    , node_metadata(createNodeMetadata(path))
    , log(log_)
{
    LOG_TEST(log, "Path: {}, node_name: {}, max_loading_retries: {}"
             "processed_path: {}, processing_path: {}, failed_path: {}",
             path, node_name, max_loading_retries,
             processed_node_path, processing_node_path, failed_node_path);
}

std::string IFileMetadata::getNodeName(const std::string & path)
{
    /// Since with are dealing with paths in s3 which can have "/",
    /// we cannot create a zookeeper node with the name equal to path.
    /// Therefore we use a hash of the path as a node name.

    SipHash path_hash;
    path_hash.update(path);
    return toString(path_hash.get64());
}

IFileMetadata::NodeMetadata IFileMetadata::createNodeMetadata(
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

bool IFileMetadata::setProcessing()
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3QueueSetFileProcessingMicroseconds);

    auto state = file_status->state;
    if (state == FileStatus::State::Processing
        || state == FileStatus::State::Processed
        || (state == FileStatus::State::Failed && file_status->retries >= max_loading_retries))
    {
        LOG_TEST(log, "File {} has non-processable state `{}`", path, file_status->state);
        return false;
    }

    /// An optimization for local parallel processing.
    std::unique_lock processing_lock(file_status->processing_lock, std::defer_lock);
    if (!processing_lock.try_lock())
        return {};

    auto [success, file_state] = setProcessingImpl();
    if (success)
        file_status->updateState(FileStatus::State::Processing, std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));
    else
        file_status->updateState(file_state);

    LOG_TEST(log, "File {} has state `{}`", path, file_state);
    return success;
}

void IFileMetadata::setProcessed()
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3QueueSetFileProcessedMicroseconds);
    {
        std::lock_guard lock(file_status->metadata_lock);
        file_status->state = FileStatus::State::Processed;
        file_status->processing_end_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    }

    SCOPE_EXIT({
        file_status->profile_counters.increment(ProfileEvents::S3QueueSetFileProcessedMicroseconds, timer.get());
        timer.cancel();
    });

    setProcessedImpl();
    ProfileEvents::increment(ProfileEvents::S3QueueProcessedFiles);
}

void IFileMetadata::setFailed(const std::string & exception)
{
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3QueueSetFileFailedMicroseconds);

    {
        std::lock_guard lock(file_status->metadata_lock);
        file_status->state = FileStatus::State::Failed;
        file_status->last_exception = exception;
        file_status->processing_end_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    }

    ProfileEvents::increment(ProfileEvents::S3QueueFailedFiles);

    SCOPE_EXIT({
        file_status->profile_counters.increment(ProfileEvents::S3QueueSetFileFailedMicroseconds, timer.get());
        timer.cancel();
    });

    auto zk_client = getZooKeeper();
    node_metadata.last_exception = exception;

    /// Is file retriable?
    if (max_loading_retries == 0)
    {
        /// File is not retriable,
        /// just create a node in /failed and remove a node from /processing.

        Coordination::Requests requests;
        requests.push_back(zkutil::makeCreateRequest(
                               failed_node_path, node_metadata.toString(), zkutil::CreateMode::Persistent));
        requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));

        Coordination::Responses responses;
        const auto code = zk_client->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
        {
            LOG_TRACE(log,
                      "File `{}` failed to process and will not be retried. "
                     "Error: {}", path, exception);
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

    auto retrieable_failed_node_path = failed_node_path + ".retriable";
    Coordination::Stat stat;
    std::string res;

    /// Extract the number of already done retries from node_hash.retriable node if it exists.
    if (zk_client->tryGet(retrieable_failed_node_path, res, &stat))
    {
        auto failed_node_metadata = NodeMetadata::fromString(res);
        node_metadata.retries = failed_node_metadata.retries + 1;

        std::lock_guard lock(file_status->metadata_lock);
        file_status->retries = node_metadata.retries;
    }

    LOG_TRACE(log, "File `{}` failed to process, try {}/{} (Error: {})",
             path, node_metadata.retries, max_loading_retries, exception);

    /// Check if file can be retried further or not.
    if (node_metadata.retries >= max_loading_retries)
    {
        /// File is no longer retriable.
        /// Make a persistent node /failed/node_hash, remove /failed/node_hash.retriable node and node in /processing.

        Coordination::Requests requests;
        requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));
        requests.push_back(zkutil::makeRemoveRequest(retrieable_failed_node_path,
                                                     stat.version));
        requests.push_back(zkutil::makeCreateRequest(failed_node_path,
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
        requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));
        if (node_metadata.retries == 0)
        {
            requests.push_back(zkutil::makeCreateRequest(retrieable_failed_node_path,
                                                         node_metadata.toString(),
                                                         zkutil::CreateMode::Persistent));
        }
        else
        {
            requests.push_back(zkutil::makeSetRequest(retrieable_failed_node_path,
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

}
