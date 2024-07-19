#include <Storages/ObjectStorageQueue/ObjectStorageQueueIFileMetadata.h>
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
    extern const Event ObjectStorageQueueProcessedFiles;
    extern const Event ObjectStorageQueueFailedFiles;
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

void ObjectStorageQueueIFileMetadata::FileStatus::setProcessingEndTime()
{
    processing_end_time = now();
}

void ObjectStorageQueueIFileMetadata::FileStatus::onProcessing()
{
    state = FileStatus::State::Processing;
    processing_start_time = now();
}

void ObjectStorageQueueIFileMetadata::FileStatus::onProcessed()
{
    state = FileStatus::State::Processed;
    if (!processing_end_time)
        setProcessingEndTime();
}

void ObjectStorageQueueIFileMetadata::FileStatus::onFailed(const std::string & exception)
{
    state = FileStatus::State::Failed;
    if (!processing_end_time)
        setProcessingEndTime();
    std::lock_guard lock(last_exception_mutex);
    last_exception = exception;
}

void ObjectStorageQueueIFileMetadata::FileStatus::updateState(State state_)
{
    state = state_;
}

std::string ObjectStorageQueueIFileMetadata::FileStatus::getException() const
{
    std::lock_guard lock(last_exception_mutex);
    return last_exception;
}

std::string ObjectStorageQueueIFileMetadata::NodeMetadata::toString() const
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

ObjectStorageQueueIFileMetadata::NodeMetadata ObjectStorageQueueIFileMetadata::NodeMetadata::fromString(const std::string & metadata_str)
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

ObjectStorageQueueIFileMetadata::ObjectStorageQueueIFileMetadata(
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

ObjectStorageQueueIFileMetadata::~ObjectStorageQueueIFileMetadata()
{
    if (processing_id_version.has_value())
    {
        if (file_status->getException().empty())
        {
            if (std::current_exception())
                file_status->onFailed(getCurrentExceptionMessage(true));
            else
                file_status->onFailed("Unprocessed exception");
        }

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

std::string ObjectStorageQueueIFileMetadata::getNodeName(const std::string & path)
{
    /// Since with are dealing with paths in object storage which can have "/",
    /// we cannot create a zookeeper node with the name equal to path.
    /// Therefore we use a hash of the path as a node name.

    SipHash path_hash;
    path_hash.update(path);
    return toString(path_hash.get64());
}

ObjectStorageQueueIFileMetadata::NodeMetadata ObjectStorageQueueIFileMetadata::createNodeMetadata(
    const std::string & path,
    const std::string & exception,
    size_t retries)
{
    /// Create a metadata which will be stored in a node named as getNodeName(path).

    /// Since node name is just a hash we want to know to which file it corresponds,
    /// so we keep "file_path" in nodes data.
    /// "last_processed_timestamp" is needed for TTL metadata nodes enabled by tracked_file_ttl_sec.
    /// "last_exception" is kept for introspection, should also be visible in system.s3(azure)queue_log if it is enabled.
    /// "retries" is kept for retrying the processing enabled by loading_retries.
    NodeMetadata metadata;
    metadata.file_path = path;
    metadata.last_processed_timestamp = now();
    metadata.last_exception = exception;
    metadata.retries = retries;
    return metadata;
}

std::string ObjectStorageQueueIFileMetadata::getProcessorInfo(const std::string & processor_id)
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

bool ObjectStorageQueueIFileMetadata::setProcessing()
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
    {
        file_status->onProcessing();
    }
    else
    {
        LOG_TEST(log, "Updating state of {} from {} to {}", path, file_status->state.load(), file_state);
        file_status->updateState(file_state);
    }

    LOG_TEST(log, "File {} has state `{}`: will {}process (processing id version: {})",
             path, file_state, success ? "" : "not ",
             processing_id_version.has_value() ? toString(processing_id_version.value()) : "None");

    return success;
}

void ObjectStorageQueueIFileMetadata::setProcessed()
{
    LOG_TRACE(log, "Setting file {} as processed (path: {})", path, processed_node_path);

    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueProcessedFiles);
    file_status->onProcessed();

    try
    {
        setProcessedImpl();
    }
    catch (...)
    {
        file_status->onFailed(getCurrentExceptionMessage(true));
        throw;
    }

    processing_id.reset();
    processing_id_version.reset();

    LOG_TRACE(log, "Set file {} as processed (rows: {})", path, file_status->processed_rows);
}

void ObjectStorageQueueIFileMetadata::setFailed(const std::string & exception_message, bool reduce_retry_count, bool overwrite_status)
{
    LOG_TRACE(log, "Setting file {} as failed (path: {}, reduce retry count: {}, exception: {})",
              path, failed_node_path, reduce_retry_count, exception_message);

    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueFailedFiles);
    if (overwrite_status || file_status->state != FileStatus::State::Failed)
        file_status->onFailed(exception_message);

    node_metadata.last_exception = exception_message;

    if (reduce_retry_count)
    {
        try
        {
            if (max_loading_retries == 0)
                setFailedNonRetriable();
            else
                setFailedRetriable();
        }
        catch (...)
        {
            auto full_exception = fmt::format(
                "First exception: {}, exception while setting file as failed: {}",
                exception_message, getCurrentExceptionMessage(true));

            file_status->onFailed(full_exception);
            throw;
        }
    }

    processing_id.reset();
    processing_id_version.reset();

    LOG_TRACE(log, "Set file {} as failed (rows: {})", path, file_status->processed_rows);
}

void ObjectStorageQueueIFileMetadata::setFailedNonRetriable()
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

void ObjectStorageQueueIFileMetadata::setFailedRetriable()
{
    /// Instead of creating a persistent /failed/node_hash node
    /// we create a persistent /failed/node_hash.retriable node.
    /// This allows us to make less zookeeper requests as we avoid checking
    /// the number of already done retries in trySetFileAsProcessing.

    auto retrieable_failed_node_path = failed_node_path + ".retriable";
    auto zk_client = getZooKeeper();

    /// Extract the number of already done retries from node_hash.retriable node if it exists.
    Coordination::Requests requests;
    Coordination::Stat stat;
    std::string res;
    bool has_failed_before = zk_client->tryGet(retrieable_failed_node_path, res, &stat);
    if (has_failed_before)
    {
        auto failed_node_metadata = NodeMetadata::fromString(res);
        node_metadata.retries = failed_node_metadata.retries + 1;
        file_status->retries = node_metadata.retries;
    }

    LOG_TRACE(log, "File `{}` failed to process, try {}/{}, retries node exists: {} (failed node path: {})",
              path, node_metadata.retries, max_loading_retries, has_failed_before, failed_node_path);

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
