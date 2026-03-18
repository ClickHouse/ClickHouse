#include <Storages/ObjectStorageQueue/ObjectStorageQueueIFileMetadata.h>
#include <Common/SipHash.h>
#include <Common/CurrentThread.h>
#include <Common/DNSResolver.h>
#include <Core/Field.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <base/scope_guard.h>


namespace ProfileEvents
{
    extern const Event ObjectStorageQueueProcessedFiles;
    extern const Event ObjectStorageQueueFailedFiles;
    extern const Event ObjectStorageQueueTrySetProcessingRequests;
    extern const Event ObjectStorageQueueTrySetProcessingSucceeded;
    extern const Event ObjectStorageQueueTrySetProcessingFailed;
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
    processing_end_time = {};
    processed_rows = 0;
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

void ObjectStorageQueueIFileMetadata::FileStatus::reset()
{
    state = FileStatus::State::None;
    processing_start_time = {};
    processing_end_time = {};
    processed_rows = 0;
    retries = 0;
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
    std::atomic<size_t> & metadata_ref_count_,
    LoggerPtr log_)
    : path(path_)
    , node_name(getNodeName(path_))
    , file_status(file_status_)
    , max_loading_retries(max_loading_retries_)
    , metadata_ref_count(metadata_ref_count_)
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

bool ObjectStorageQueueIFileMetadata::trySetProcessing()
{
    auto state = file_status->state.load();
    if (state == FileStatus::State::Processing
        || state == FileStatus::State::Processed
        || (state == FileStatus::State::Failed
            && file_status->retries
            && file_status->retries >= max_loading_retries))
    {
        LOG_TEST(log, "File {} has non-processable state `{}` (retries: {}/{})",
                 path, state, file_status->retries.load(), max_loading_retries);
        return false;
    }

    /// An optimization for local parallel processing.
    std::unique_lock processing_lock(file_status->processing_lock, std::defer_lock);
    if (!processing_lock.try_lock())
        return {};

    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueTrySetProcessingRequests);

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

    if (success)
        ProfileEvents::increment(ProfileEvents::ObjectStorageQueueTrySetProcessingSucceeded);
    else
        ProfileEvents::increment(ProfileEvents::ObjectStorageQueueTrySetProcessingFailed);

    return success;
}

std::optional<ObjectStorageQueueIFileMetadata::SetProcessingResponseIndexes>
ObjectStorageQueueIFileMetadata::prepareSetProcessingRequests(Coordination::Requests & requests)
{
    if (metadata_ref_count.load() > 1)
    {
        std::unique_lock processing_lock(file_status->processing_lock, std::defer_lock);
        if (!processing_lock.try_lock())
        {
            /// This is possible in case on the same server
            /// there are more than one S3(Azure)Queue table processing the same keeper path.
            LOG_TEST(log, "File {} is being processed on this server by another table on this server", path);
            return std::nullopt;
        }

        auto state = file_status->state.load();
        if (state == FileStatus::State::Processing
            || state == FileStatus::State::Processed
            || (state == FileStatus::State::Failed
                && file_status->retries
                && file_status->retries >= max_loading_retries))
        {
            LOG_TEST(log, "File {} has non-processable state `{}` (retries: {}/{})",
                    path, state, file_status->retries.load(), max_loading_retries);

            /// This is possible in case on the same server
            /// there are more than one S3(Azure)Queue table processing the same keeper path.
            LOG_TEST(log, "File {} is being processed on this server by another table on this server", path);
            return std::nullopt;
        }
    }

    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueTrySetProcessingRequests);
    return prepareProcessingRequestsImpl(requests);
}

void ObjectStorageQueueIFileMetadata::finalizeProcessing(int processing_id_version_)
{
    processing_id_version = processing_id_version_;
    file_status->onProcessing();
}

void ObjectStorageQueueIFileMetadata::resetProcessing()
{
    auto state = file_status->state.load();
    if (state != FileStatus::State::Processing)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot reset non-processing state: {}", state);

    SCOPE_EXIT({
        file_status->reset();
    });

    if (!processing_id_version.has_value())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "No processing id version set, but state is `Processing` ({})",
            node_metadata.toString());
    }

    Coordination::Requests requests;
    prepareResetProcessingRequests(requests);

    Coordination::Responses responses;
    const auto zk_client = getZooKeeper();
    const auto code = zk_client->tryMulti(requests, responses);
    if (code == Coordination::Error::ZOK)
        return;

    if (Coordination::isHardwareError(code))
    {
        LOG_TRACE(log, "Keeper session expired, processing will be automatically reset");
        return;
    }

    if (responses[0]->error == Coordination::Error::ZBADVERSION
        || responses[0]->error == Coordination::Error::ZNONODE
        || responses[1]->error == Coordination::Error::ZNONODE)
    {
        LOG_TRACE(
            log, "Processing node no longer exists ({}) "
            "while resetting processing state. "
            "This could be as a result of expired keeper session. ",
            processing_node_path);
        return;
    }

    const auto failed_path = responses[0]->error != Coordination::Error::ZOK
        ? requests[0]->getPath()
        : requests[1]->getPath();

    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Failed to reset processing for file {}: (code: {}, path: {})",
        path, code, failed_path);
}

void ObjectStorageQueueIFileMetadata::prepareResetProcessingRequests(Coordination::Requests & requests)
{
    requests.push_back(zkutil::makeRemoveRequest(processing_node_id_path, processing_id_version.value()));
    requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));
}

void ObjectStorageQueueIFileMetadata::prepareProcessedRequests(Coordination::Requests & requests)
{
    LOG_TRACE(log, "Setting file {} as processed (keeper path: {})", path, processed_node_path);

    try
    {
        prepareProcessedRequestsImpl(requests);
    }
    catch (...)
    {
        auto full_exception = fmt::format(
            "Exception while setting file as failed: {}",
            getCurrentExceptionMessage(true));

        file_status->onFailed(full_exception);
        throw;
    }
}

void ObjectStorageQueueIFileMetadata::prepareFailedRequests(
    Coordination::Requests & requests,
    const std::string & exception_message,
    bool reduce_retry_count)
{
    LOG_TRACE(
        log,
        "Setting file {} as failed "
        "(keeper path: {}, reduce retry count: {}, exception: {})",
        path, failed_node_path, reduce_retry_count, exception_message);

    node_metadata.last_exception = exception_message;

    if (!reduce_retry_count)
    {
        prepareResetProcessingRequests(requests);
        return;
    }

    try
    {
        prepareFailedRequestsImpl(requests, /* retriable */max_loading_retries != 0);
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

void ObjectStorageQueueIFileMetadata::finalizeProcessed()
{
    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueProcessedFiles);
    file_status->onProcessed();

    processing_id.reset();
    processing_id_version.reset();

    LOG_TRACE(log, "Set file {} as processed (rows: {})", path, file_status->processed_rows.load());
}

void ObjectStorageQueueIFileMetadata::finalizeFailed(const std::string & exception_message)
{
    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueFailedFiles);
    file_status->onFailed(exception_message);

    processing_id.reset();
    processing_id_version.reset();

    LOG_TRACE(log, "Set file {} as failed (rows: {})", path, file_status->processed_rows.load());
}

void ObjectStorageQueueIFileMetadata::prepareFailedRequestsImpl(
    Coordination::Requests & requests,
    bool retriable)
{
    if (!processing_id_version.has_value())
    {
        chassert(false);
        return;
    }

    if (!retriable)
    {
        LOG_TEST(log, "File {} failed to process and will not be retried. ({})", path, failed_node_path);

        /// Check Processing node id and remove processing_node_id node.
        requests.push_back(zkutil::makeRemoveRequest(processing_node_id_path, processing_id_version.value()));
        /// Remove Processing node.
        requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));
        /// Created Failed node.
        requests.push_back(zkutil::makeCreateRequest(failed_node_path, node_metadata.toString(), zkutil::CreateMode::Persistent));
        return;
    }

    /// Instead of creating a persistent /failed/node_hash node
    /// we create a persistent /failed/node_hash.retriable node.
    /// This allows us to make less zookeeper requests as we avoid checking
    /// the number of already done retries in trySetProcessing.

    auto retrieable_failed_node_path = failed_node_path + ".retriable";
    auto zk_client = getZooKeeper();

    /// Extract the number of already done retries from node_hash.retriable node if it exists.
    Coordination::Stat retriable_failed_node_stat;
    std::string res;
    bool has_failed_before = zk_client->tryGet(retrieable_failed_node_path, res, &retriable_failed_node_stat);
    if (has_failed_before)
        file_status->retries = node_metadata.retries = NodeMetadata::fromString(res).retries + 1;
    else
        chassert(!file_status->retries && !node_metadata.retries);

    LOG_TRACE(
        log,
        "File {} failed at try {}/{}, "
        "retries node exists: {} (failed node path: {})",
        path, node_metadata.retries, max_loading_retries, has_failed_before, failed_node_path);

    if (node_metadata.retries >= max_loading_retries)
    {
        LOG_TEST(log, "File {} failed to process and will not be retried. ({})", path, failed_node_path);

        /// Check Processing node id and remove processing_node_id node.
        requests.push_back(zkutil::makeRemoveRequest(processing_node_id_path, processing_id_version.value()));
        /// Remove Processing node.
        requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));
        /// Remove /failed/node_hash.retriable node.
        requests.push_back(zkutil::makeRemoveRequest(retrieable_failed_node_path, retriable_failed_node_stat.version));
        /// Create a persistent node /failed/node_hash.
        requests.push_back(zkutil::makeCreateRequest(failed_node_path, node_metadata.toString(), zkutil::CreateMode::Persistent));
    }
    else
    {
        /// Check Processing node id (without removing, because processing retries are not over).
        requests.push_back(zkutil::makeCheckRequest(processing_node_id_path, processing_id_version.value()));
        /// Remove Processing node.
        requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));

        if (node_metadata.retries == 0)
        {
            /// Create /failed/node_hash.retriable node.
            requests.push_back(
                zkutil::makeCreateRequest(
                    retrieable_failed_node_path, node_metadata.toString(), zkutil::CreateMode::Persistent));
        }
        else
        {
            /// Update retries count.
            requests.push_back(
                zkutil::makeSetRequest(
                    retrieable_failed_node_path, node_metadata.toString(), retriable_failed_node_stat.version));
        }
    }
}

}
