#include <Storages/ObjectStorageQueue/ObjectStorageQueueIFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include <Common/getRandomASCIIString.h>
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
    time_t now()
    {
        return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    }
}

void ObjectStorageQueueIFileMetadata::FileStatus::setProcessingEndTime()
{
    processing_end_time = now();
}

void ObjectStorageQueueIFileMetadata::FileStatus::setGetObjectTime(size_t elapsed_ms)
{
    get_object_time_ms = elapsed_ms;
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
    chassert(processing_end_time);
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
    json.set("processor_id", ""); /// Remains for compatibility

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
    bool use_persistent_processing_nodes_,
    LoggerPtr log_)
    : path(path_)
    , node_name(getNodeName(path_))
    , file_status(file_status_)
    , max_loading_retries(max_loading_retries_)
    , metadata_ref_count(metadata_ref_count_)
    , use_persistent_processing_nodes(use_persistent_processing_nodes_)
    , processing_node_path(processing_node_path_)
    , processed_node_path(processed_node_path_)
    , failed_node_path(failed_node_path_)
    , node_metadata(createNodeMetadata(path))
    , log(log_)
{
    LOG_TEST(log, "Path: {}, node_name: {}, max_loading_retries: {}, "
             "processed_path: {}, processing_path: {}, failed_path: {}",
             path, node_name, max_loading_retries,
             processed_node_path, processing_node_path, failed_node_path);
}

ObjectStorageQueueIFileMetadata::~ObjectStorageQueueIFileMetadata()
{
    if (created_processing_node)
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
            Coordination::Error code;
            auto zk_retry = ObjectStorageQueueMetadata::getKeeperRetriesControl(log);
            zk_retry.retryLoop([&]
            {
                auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log);
                if (zk_retry.isRetry())
                {
                    /// It is possible that we fail "after operation",
                    /// e.g. we successfully removed the node, but did not get confirmation,
                    /// but then if we retry - we can remove a newly recreated node,
                    /// therefore avoid this with this check.
                    if (!checkProcessingOwnership(zk_client))
                    {
                        LOG_TEST(log, "Will not remove processing node, ownership changed");
                        code = Coordination::Error::ZOK;
                        return;
                    }
                }
                else
                {
                    chassert(checkProcessingOwnership(zk_client));
                }
                code = zk_client->tryRemove(processing_node_path);
            });

            if (code == Coordination::Error::ZOK)
                return;

            if (Coordination::isHardwareError(code))
            {
                LOG_WARNING(log, "Keeper session expired and retries did not help. "
                            "Will rely on automatic processing node cleanup");
                return;
            }

            LOG_WARNING(
                log, "Unexpected error while removing processing node: {} (path: {})",
                code, processing_node_path);

            chassert(false);
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

std::string ObjectStorageQueueIFileMetadata::generateProcessingID()
{
    return getRandomASCIIString(10);
}

bool ObjectStorageQueueIFileMetadata::checkProcessingOwnership(std::shared_ptr<ZooKeeperWithFaultInjection> zk_client)
{
    if (processor_info.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor info is not set");

    std::string data;
    /// No retries, because they must be done on a higher level.
    if (!zk_client->tryGet(processing_node_path, data))
        return false;

    LOG_TEST(
        log, "Processing node {} has processor: {}, current processor: {}",
        processing_node_path, data, processor_info);

    return data == processor_info;
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
    afterSetProcessing(success, file_state);

    LOG_TEST(log, "File {} has state `{}`: will {}process", path, file_state, success ? "" : "not ");
    return success;
}

std::optional<ObjectStorageQueueIFileMetadata::SetProcessingResponseIndexes>
ObjectStorageQueueIFileMetadata::prepareSetProcessingRequests(Coordination::Requests & requests, const std::string & processing_id)
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
    return prepareProcessingRequestsImpl(requests, processing_id);
}

void ObjectStorageQueueIFileMetadata::afterSetProcessing(bool success, std::optional<FileStatus::State> file_state)
{
    if (success)
    {
        chassert(!file_state.has_value() || *file_state == FileStatus::State::None);
        chassert(!processor_info.empty());

        created_processing_node = true;
        file_status->onProcessing();
        ProfileEvents::increment(ProfileEvents::ObjectStorageQueueTrySetProcessingSucceeded);
    }
    else
    {
        chassert(!created_processing_node);
        ProfileEvents::increment(ProfileEvents::ObjectStorageQueueTrySetProcessingFailed);

        if (file_state.has_value())
        {
            LOG_TEST(log, "Updating state of {} from {} to {}", path, file_status->state.load(), file_state.value());
            file_status->updateState(file_state.value());
        }
    }
}

void ObjectStorageQueueIFileMetadata::resetProcessing()
{
    chassert(created_processing_node);

    auto state = file_status->state.load();
    if (state != FileStatus::State::Processing)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot reset non-processing state: {}", state);

    SCOPE_EXIT({
        (*file_status).reset();
    });

    Coordination::Requests requests;
    prepareResetProcessingRequests(requests);

    Coordination::Responses responses;
    Coordination::Error code;
    auto zk_retry = ObjectStorageQueueMetadata::getKeeperRetriesControl(log);
    zk_retry.retryLoop([&]
    {
        auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log);
        if (zk_retry.isRetry())
        {
            /// It is possible that we fail "after operation",
            /// e.g. we successfully removed the node, but did not get confirmation,
            /// but then if we retry - we can remove a newly recreated node,
            /// therefore avoid this with this check.
            if (!checkProcessingOwnership(zk_client))
            {
                LOG_TEST(log, "Will not remove processing node, ownership changed");
                code = Coordination::Error::ZOK;
                return;
            }
        }
        else
        {
            chassert(checkProcessingOwnership(zk_client));
        }
        code = zk_client->tryMulti(requests, responses);
    });

    if (code == Coordination::Error::ZOK)
        return;

    if (Coordination::isHardwareError(code))
    {
        LOG_WARNING(log, "Keeper session expired and retries did not help. "
                    "Will rely on automatic processing node cleanup");
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
    LOG_TEST(log, "Resetting processing for {}", path);
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

#ifdef DEBUG_OR_SANITIZER_BUILD
    ObjectStorageQueueMetadata::getKeeperRetriesControl(log).retryLoop([&]
    {
        auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log);
        chassert(
            !zk_client->exists(processing_node_path),
            fmt::format("Expected path {} not to exist while finalizing {}", processing_node_path, path));

        chassert(
            !zk_client->exists(failed_node_path),
            fmt::format("Expected path {} not to exist while finalizing {}", failed_node_path, path));

        chassert(
            zk_client->exists(processed_node_path),
            fmt::format("Expected path {} to exist while finalizing {}", processed_node_path, path));
    });
#endif

    file_status->onProcessed();
    created_processing_node = false;

    LOG_TRACE(log, "Set file {} as processed (rows: {})", path, file_status->processed_rows.load());
}

void ObjectStorageQueueIFileMetadata::finalizeFailed(const std::string & exception_message)
{
    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueFailedFiles);

#ifdef DEBUG_OR_SANITIZER_BUILD
    ObjectStorageQueueMetadata::getKeeperRetriesControl(log).retryLoop([&]
    {
        auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log);
        chassert(
            !zk_client->exists(processing_node_path),
            fmt::format("Expected path {} not to exist while finalizing {}", processing_node_path, path));

        if (!useBucketsForProcessing())
            chassert(
                !zk_client->exists(processed_node_path),
                fmt::format("Expected path {} not to exist while finalizing {}", processed_node_path, path));

        chassert(
            zk_client->exists(failed_node_path) || zk_client->exists(failed_node_path + ".retriable"),
            fmt::format("Expected path {} to exist while finalizing {}", failed_node_path, path));

    });
#endif

    file_status->onFailed(exception_message);
    created_processing_node = false;

    LOG_TRACE(log, "Set file {} as failed (rows: {})", path, file_status->processed_rows.load());
}

void ObjectStorageQueueIFileMetadata::prepareFailedRequestsImpl(
    Coordination::Requests & requests,
    bool retriable)
{
    if (!retriable)
    {
        LOG_TEST(log, "File {} failed to process and will not be retried. ({})", path, failed_node_path);

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

    /// Extract the number of already done retries from node_hash.retriable node if it exists.
    Coordination::Stat retriable_failed_node_stat;
    std::string res;
    bool has_failed_before = false;
    ObjectStorageQueueMetadata::getKeeperRetriesControl(log).retryLoop([&]
    {
        auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log);
        has_failed_before = zk_client->tryGet(retrieable_failed_node_path, res, &retriable_failed_node_stat);
    });
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

        /// Remove Processing node.
        requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));
        /// Remove /failed/node_hash.retriable node.
        requests.push_back(zkutil::makeRemoveRequest(retrieable_failed_node_path, retriable_failed_node_stat.version));
        /// Create a persistent node /failed/node_hash.
        requests.push_back(zkutil::makeCreateRequest(failed_node_path, node_metadata.toString(), zkutil::CreateMode::Persistent));
    }
    else
    {
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
