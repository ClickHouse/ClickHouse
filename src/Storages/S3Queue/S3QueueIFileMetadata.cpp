#include <Storages/S3Queue/S3QueueIFileMetadata.h>
#include <Common/SipHash.h>
#include <Common/CurrentThread.h>
#include <Common/DNSResolver.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>


namespace ProfileEvents
{
    extern const Event S3QueueProcessedFiles;
    extern const Event S3QueueFailedFiles;
};

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    zkutil::ZooKeeperPtr getZooKeeper()
    {
        return Context::getGlobalContextInstance()->getZooKeeper();
    }

    time_t now()
    {
        return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    }
}

void S3QueueIFileMetadata::FileStatus::onProcessing()
{
    state = FileStatus::State::Processing;
    processing_start_time = now();
}

void S3QueueIFileMetadata::FileStatus::onProcessed()
{
    state = FileStatus::State::Processed;
    processing_end_time = now();
}

void S3QueueIFileMetadata::FileStatus::onFailed(const std::string & exception)
{
    state = FileStatus::State::Failed;
    processing_end_time = now();
    std::lock_guard lock(last_exception_mutex);
    last_exception = exception;
}

std::string S3QueueIFileMetadata::FileStatus::getException() const
{
    std::lock_guard lock(last_exception_mutex);
    return last_exception;
}

std::string S3QueueIFileMetadata::NodeMetadata::toString() const
{
    Poco::JSON::Object json;
    json.set("file_path", file_path);
    json.set("last_processed_timestamp", now());
    json.set("last_exception", last_exception);
    json.set("retries", retries);
    json.set("processing_id", processing_id);

    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);
    return oss.str();
}

S3QueueIFileMetadata::NodeMetadata S3QueueIFileMetadata::NodeMetadata::fromString(const std::string & metadata_str)
{
    Poco::JSON::Parser parser;
    auto json = parser.parse(metadata_str).extract<Poco::JSON::Object::Ptr>();
    chassert(json);

    NodeMetadata metadata;
    metadata.file_path = json->getValue<String>("file_path");
    metadata.last_processed_timestamp = json->getValue<UInt64>("last_processed_timestamp");
    metadata.last_exception = json->getValue<String>("last_exception");
    metadata.retries = json->getValue<UInt64>("retries");
    metadata.processing_id = json->getValue<String>("processing_id");
    return metadata;
}

S3QueueIFileMetadata::S3QueueIFileMetadata(
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
    , processing_node_id_path(processing_node_path + "_processing_id")
{
    LOG_TEST(log, "Path: {}, node_name: {}, max_loading_retries: {}, "
             "processed_path: {}, processing_path: {}, failed_path: {}",
             path, node_name, max_loading_retries,
             processed_node_path, processing_node_path, failed_node_path);
}

S3QueueIFileMetadata::~S3QueueIFileMetadata()
{
    if (processing_id_version.has_value())
    {
        file_status->onFailed("Uncaught exception");
        LOG_TEST(log, "Removing processing node in destructor for file: {}", path);
        try
        {
            auto zk_client = getZooKeeper();

            Coordination::Requests requests;
            requests.push_back(zkutil::makeCheckRequest(processing_node_id_path, processing_id_version.value()));
            requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));

            Coordination::Responses responses;
            const auto code = zk_client->tryMulti(requests, responses);
            if (code != Coordination::Error::ZOK
                && !Coordination::isHardwareError(code)
                && code != Coordination::Error::ZBADVERSION
                && code != Coordination::Error::ZNONODE)
            {
                LOG_WARNING(log, "Unexpected error while removing processing node: {}", code);
                chassert(false);
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

std::string S3QueueIFileMetadata::getNodeName(const std::string & path)
{
    /// Since with are dealing with paths in s3 which can have "/",
    /// we cannot create a zookeeper node with the name equal to path.
    /// Therefore we use a hash of the path as a node name.

    SipHash path_hash;
    path_hash.update(path);
    return toString(path_hash.get64());
}

S3QueueIFileMetadata::NodeMetadata S3QueueIFileMetadata::createNodeMetadata(
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
    metadata.last_processed_timestamp = now();
    metadata.last_exception = exception;
    metadata.retries = retries;
    return metadata;
}

std::string S3QueueIFileMetadata::getProcessorInfo(const std::string & processor_id)
{
    /// Add information which will be useful for debugging just in case.
    Poco::JSON::Object json;
    json.set("hostname", DNSResolver::instance().getHostName());
    json.set("processor_id", processor_id);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);
    return oss.str();
}

bool S3QueueIFileMetadata::setProcessing()
{
    auto state = file_status->state.load();
    if (state == FileStatus::State::Processing
        || state == FileStatus::State::Processed
        || (state == FileStatus::State::Failed && file_status->retries >= max_loading_retries))
    {
        LOG_TEST(log, "File {} has non-processable state `{}`", path, file_status->state.load());
        return false;
    }

    /// An optimization for local parallel processing.
    std::unique_lock processing_lock(file_status->processing_lock, std::defer_lock);
    if (!processing_lock.try_lock())
        return {};

    auto [success, file_state] = setProcessingImpl();
    if (success)
        file_status->onProcessing();
    else
        file_status->updateState(file_state);

    LOG_TEST(log, "File {} has state `{}`: will {}process (processing id version: {})",
             path, file_state, success ? "" : "not ",
             processing_id_version.has_value() ? toString(processing_id_version.value()) : "None");

    return success;
}

void S3QueueIFileMetadata::setProcessed()
{
    LOG_TRACE(log, "Setting file {} as processed (path: {})", path, processed_node_path);

    ProfileEvents::increment(ProfileEvents::S3QueueProcessedFiles);
    file_status->onProcessed();
    setProcessedImpl();

    processing_id.reset();
    processing_id_version.reset();

    LOG_TRACE(log, "Set file {} as processed (rows: {})", path, file_status->processed_rows);
}

void S3QueueIFileMetadata::setFailed(const std::string & exception)
{
    LOG_TRACE(log, "Setting file {} as failed (exception: {}, path: {})", path, exception, failed_node_path);

    ProfileEvents::increment(ProfileEvents::S3QueueFailedFiles);
    file_status->onFailed(exception);
    node_metadata.last_exception = exception;

    if (max_loading_retries == 0)
        setFailedNonRetriable();
    else
        setFailedRetriable();

    processing_id.reset();
    processing_id_version.reset();

    LOG_TRACE(log, "Set file {} as failed (rows: {})", path, file_status->processed_rows);
}

void S3QueueIFileMetadata::setFailedNonRetriable()
{
    auto zk_client = getZooKeeper();
    Coordination::Requests requests;
    requests.push_back(zkutil::makeCreateRequest(failed_node_path, node_metadata.toString(), zkutil::CreateMode::Persistent));
    requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));

    Coordination::Responses responses;
    const auto code = zk_client->tryMulti(requests, responses);
    if (code == Coordination::Error::ZOK)
    {
        LOG_TRACE(log, "File `{}` failed to process and will not be retried. ", path);
        return;
    }

    if (Coordination::isHardwareError(responses[0]->error))
    {
        LOG_WARNING(log, "Cannot set file as failed: lost connection to keeper");
        return;
    }

    if (responses[0]->error == Coordination::Error::ZNODEEXISTS)
    {
        LOG_WARNING(log, "Cannot create a persistent node in /failed since it already exists");
        chassert(false);
        return;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected error while setting file as failed: {}", code);
}

void S3QueueIFileMetadata::setFailedRetriable()
{
    /// Instead of creating a persistent /failed/node_hash node
    /// we create a persistent /failed/node_hash.retriable node.
    /// This allows us to make less zookeeper requests as we avoid checking
    /// the number of already done retries in trySetFileAsProcessing.

    auto retrieable_failed_node_path = failed_node_path + ".retriable";
    auto zk_client = getZooKeeper();

    /// Extract the number of already done retries from node_hash.retriable node if it exists.
    Coordination::Stat stat;
    std::string res;
    if (zk_client->tryGet(retrieable_failed_node_path, res, &stat))
    {
        auto failed_node_metadata = NodeMetadata::fromString(res);
        node_metadata.retries = failed_node_metadata.retries + 1;
        file_status->retries = node_metadata.retries;
    }

    LOG_TRACE(log, "File `{}` failed to process, try {}/{}",
              path, node_metadata.retries, max_loading_retries);

    Coordination::Requests requests;
    if (node_metadata.retries >= max_loading_retries)
    {
        /// File is no longer retriable.
        /// Make a persistent node /failed/node_hash,
        /// remove /failed/node_hash.retriable node and node in /processing.

        requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));
        requests.push_back(zkutil::makeRemoveRequest(retrieable_failed_node_path, stat.version));
        requests.push_back(
            zkutil::makeCreateRequest(
                failed_node_path, node_metadata.toString(), zkutil::CreateMode::Persistent));

    }
    else
    {
        /// File is still retriable,
        /// update retries count and remove node from /processing.

        requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));
        if (node_metadata.retries == 0)
        {
            requests.push_back(
                zkutil::makeCreateRequest(
                    retrieable_failed_node_path, node_metadata.toString(), zkutil::CreateMode::Persistent));
        }
        else
        {
            requests.push_back(
                zkutil::makeSetRequest(
                    retrieable_failed_node_path, node_metadata.toString(), stat.version));
        }
    }

    Coordination::Responses responses;
    auto code = zk_client->tryMulti(requests, responses);
    if (code == Coordination::Error::ZOK)
        return;

    throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Failed to set file {} as failed (code: {})", path, code);
}

}
