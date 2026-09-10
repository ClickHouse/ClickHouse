#include <Storages/ObjectStorageQueue/ObjectStorageQueueIFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include <Common/FailPoint.h>
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
#include <filesystem>


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

namespace FailPoints
{
    extern const char object_storage_queue_skip_one_file_in_batch[];
}

namespace ErrorCodes
{
    extern const int ABORTED;
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
    std::lock_guard lock(last_exception_mutex);
    last_exception = {};
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
    const std::string & zookeeper_name_,
    const std::string & processing_node_path_,
    const std::string & processed_node_path_,
    const std::string & failed_node_path_,
    FileStatusPtr file_status_,
    size_t max_loading_retries_,
    std::atomic<size_t> & metadata_ref_count_,
    bool use_persistent_processing_nodes_,
    const std::string & active_registry_id_,
    LoggerPtr log_)
    : path(path_)
    , zookeeper_name(zookeeper_name_)
    , node_name(getNodeName(path_))
    , file_status(file_status_)
    , max_loading_retries(max_loading_retries_)
    , metadata_ref_count(metadata_ref_count_)
    , use_persistent_processing_nodes(use_persistent_processing_nodes_)
    , active_registry_id(use_persistent_processing_nodes_ ? active_registry_id_ : "")
    , processing_node_path(processing_node_path_)
    , processed_node_path(processed_node_path_)
    , failed_node_path(failed_node_path_)
    , node_metadata(createNodeMetadata(path))
    , log(log_)
{
}

ObjectStorageQueueIFileMetadata::~ObjectStorageQueueIFileMetadata()
{
    auto component_guard = Coordination::setCurrentComponent("ObjectStorageQueueIFileMetadata::~ObjectStorageQueueIFileMetadata");
    if (created_processing_node)
    {
        std::string current_exception;
        if (file_status->getException().empty())
        {
            if (std::current_exception())
            {
                current_exception = getCurrentExceptionMessage(true);
                file_status->onFailed(current_exception);
            }
            else
                file_status->onFailed("Unprocessed exception");
        }
        else
        {
            chassert(file_status->state == FileStatus::State::Failed);
        }

        LOG_TEST(log, "Removing processing node in destructor for file: {} "
                 "(state: {}, exception: {})",
                 path, file_status->state.load(), current_exception);
        try
        {
            Coordination::Error code = {};
            auto zk_retry = ObjectStorageQueueMetadata::getKeeperRetriesControl(log);
            zk_retry.retryLoop([&]
            {
                auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log, zookeeper_name);
                if (zk_retry.isRetry() || uncertain_commit)
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

std::string ObjectStorageQueueIFileMetadata::getProcessorInfo(
    const std::string & processor_id, const std::string & active_registry_id)
{
    /// Add information which will be useful for debugging just in case.
    Poco::JSON::Object json;
    json.set("hostname", DNSResolver::instance().getHostName());
    json.set("processor_id", processor_id);
    if (!active_registry_id.empty())
        json.set("active_registry_id", active_registry_id);

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

void ObjectStorageQueueIFileMetadata::refreshProcessingNode(size_t refresh_interval_seconds, bool force)
{
    if (!use_persistent_processing_nodes || !created_processing_node || active_registry_id.empty())
        return;
    if (!force && refresh_interval_seconds
        && processing_node_age_watch.elapsedSeconds() < static_cast<double>(refresh_interval_seconds))
        return;

    const auto queue_path = std::filesystem::path(processing_node_path).parent_path().parent_path();
    const auto registry_path = queue_path / "registry" / active_registry_id;
    bool ownership_lost = false;
    bool active_registry_lost = false;
    auto zk_retry = ObjectStorageQueueMetadata::getKeeperRetriesControl(log);
    zk_retry.retryLoop([&]
    {
        auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log, zookeeper_name);
        Coordination::Stat stat;
        std::string data;
        if (!zk_client->tryGet(processing_node_path, data, &stat) || data != processor_info)
        {
            ownership_lost = true;
            return;
        }

        Coordination::Requests requests;
        requests.push_back(zkutil::makeCheckRequest(registry_path, -1));
        requests.push_back(zkutil::makeSetRequest(processing_node_path, data, stat.version));
        Coordination::Responses responses;
        const auto code = zk_client->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
        {
            processing_node_version = stat.version + 1;
            return;
        }
        if (code == Coordination::Error::ZNONODE
            && !responses.empty()
            && responses.front()->error == Coordination::Error::ZNONODE)
        {
            /// The claim is still ours, but this process's ephemeral registry node was
            /// lost with the Keeper session. Keep ownership set so teardown removes the
            /// persistent claim; the next streaming cycle will recreate the registry.
            active_registry_lost = true;
            return;
        }
        if (code == Coordination::Error::ZBADVERSION)
        {
            Coordination::Stat current_stat;
            std::string current_data;
            if (zk_client->tryGet(processing_node_path, current_data, &current_stat)
                && current_data == processor_info)
            {
                /// Another refresh by this owner won the race.
                processing_node_version = current_stat.version;
                return;
            }
            ownership_lost = true;
            return;
        }
        if (code == Coordination::Error::ZNONODE)
        {
            ownership_lost = true;
            return;
        }
        zkutil::KeeperMultiException::check(code, requests, responses);
    });

    if (ownership_lost)
    {
        created_processing_node = false;
        (*file_status).reset();
        /// Not a logical error: a claim of a process which was unregistered and did not
        /// refresh for longer than the abandoned-reclaim TTL (e.g. it was paused with
        /// SYSTEM STOP, or lost its Keeper session for that long) is legitimately
        /// reclaimed by cleanup. The owner detects it here and releases the work.
        throw Exception(
            ErrorCodes::ABORTED,
            "Lost ownership of persistent processing node {} (processor: {}, active registry: {})",
            processing_node_path,
            processor_info,
            active_registry_id);
    }
    if (active_registry_lost)
        throw zkutil::KeeperException::fromPath(Coordination::Error::ZNONODE, registry_path);

    processing_node_age_watch.restart();
}

void ObjectStorageQueueIFileMetadata::addProcessingNodeRemovalRequest(Coordination::Requests & requests) const
{
    if (!active_registry_id.empty())
    {
        const auto queue_path = std::filesystem::path(processing_node_path).parent_path().parent_path();
        requests.push_back(zkutil::makeCheckRequest(queue_path / "registry" / active_registry_id, -1));
    }
    requests.push_back(zkutil::makeRemoveRequest(processing_node_path, processing_node_version));
}

std::optional<ObjectStorageQueueIFileMetadata::SetProcessingResponseIndexes>
ObjectStorageQueueIFileMetadata::prepareSetProcessingRequests(Coordination::Requests & requests, const std::string & processing_id)
{
    std::unique_lock processing_lock(file_status->processing_lock, std::defer_lock);
    bool processing_lock_acquired = processing_lock.try_lock();

    /// Test-only: simulate the file being grabbed by another consumer (a processing-lock conflict).
    /// ONCE, so it skips the first file after being enabled, exercising the batch compaction path.
    fiu_do_on(FailPoints::object_storage_queue_skip_one_file_in_batch, { processing_lock_acquired = false; });

    if (!processing_lock_acquired)
    {
        /// This is possible in case on the same server
        /// there are more than one S3(Azure)Queue table processing the same keeper path.
        LOG_TEST(log, "File {} is being processed by another table on this server or"
                 " another process insert thread in case parallel_insert = 1", path);
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
        processing_node_version = 0;
        processing_node_age_watch.restart();
        file_status->onProcessing();
        ProfileEvents::increment(ProfileEvents::ObjectStorageQueueTrySetProcessingSucceeded);
    }
    else
    {
        chassert(!created_processing_node);
        ProfileEvents::increment(ProfileEvents::ObjectStorageQueueTrySetProcessingFailed);

        if (file_state.has_value() && file_state.value() != FileStatus::State::None)
        {
            LOG_TEST(log, "Updating state of {} from {} to {}", path, file_status->state.load(), file_state.value());
            file_status->updateState(file_state.value());
        }
    }
}

void ObjectStorageQueueIFileMetadata::resetProcessing()
{
    chassert(created_processing_node);
    SCOPE_EXIT({
        created_processing_node = false;
    });

    auto state = file_status->state.load();
    if (state != FileStatus::State::Processing)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot reset non-processing state: {}", state);

    SCOPE_EXIT({
        (*file_status).reset();
    });

    Coordination::Requests requests;
    /// Resetting releases work without marking the object processed or deleting it.
    /// It is therefore safe to remove a versioned claim that still contains our exact
    /// processor identity even if the ephemeral active-registry entry was just lost.
    requests.push_back(zkutil::makeRemoveRequest(processing_node_path, processing_node_version));

    Coordination::Responses responses;
    Coordination::Error code = {};
    auto zk_retry = ObjectStorageQueueMetadata::getKeeperRetriesControl(log);
    zk_retry.retryLoop([&]
    {
        auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log, zookeeper_name);
        /// On retry: it is possible that we fail "after operation", e.g. we successfully
        /// removed the node, but did not get confirmation, and a retry could remove a
        /// newly recreated node. On the first attempt: the claim may have been reclaimed
        /// by abandoned-claim cleanup (e.g. after a long pause or Keeper disconnection)
        /// and possibly recreated by another server. Either way ownership is gone, the
        /// work is already released, and the node must not be touched.
        if (!checkProcessingOwnership(zk_client))
        {
            LOG_TEST(log, "Will not remove processing node, ownership changed");
            code = Coordination::Error::ZOK;
            return;
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

    if (code == Coordination::Error::ZNONODE || code == Coordination::Error::ZBADVERSION)
    {
        /// The claim was reclaimed (and possibly recreated by another server) between the
        /// ownership check above and the removal. The reset's goal is already achieved.
        LOG_TEST(log, "Will not remove processing node {}, it was already removed or recreated", processing_node_path);
        return;
    }

    std::string failed_path = processing_node_path;
    for (size_t i = 0; i < responses.size(); ++i)
    {
        if (responses[i]->error != Coordination::Error::ZOK)
        {
            failed_path = requests[i]->getPath();
            break;
        }
    }

    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Failed to reset processing for file {}: (code: {}, path: {})",
        path, code, failed_path);
}

void ObjectStorageQueueIFileMetadata::prepareResetProcessingRequests(Coordination::Requests & requests)
{
    LOG_TEST(log, "Resetting processing for {}", path);
    addProcessingNodeRemovalRequest(requests);
}

void ObjectStorageQueueIFileMetadata::prepareProcessedRequests(Coordination::Requests & requests,
    LastProcessedFileInfoMapPtr created_nodes)
{
    LOG_TRACE(log, "Setting file {} as processed (keeper path: {})", path, processed_node_path);

    try
    {
        prepareProcessedRequestsImpl(requests, created_nodes);
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
        processing_reset_without_failure = true;
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

    SCOPE_EXIT({
        file_status->onProcessed();
        created_processing_node = false;

        LOG_TRACE(log, "Set file {} as processed (rows: {})", path, file_status->processed_rows.load());
    });

#ifdef DEBUG_OR_SANITIZER_BUILD
    ObjectStorageQueueMetadata::getKeeperRetriesControl(log).retryLoop([&]
    {
        auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log, zookeeper_name);
        chassert(
            !zk_client->exists(processing_node_path),
            fmt::format("Expected path {} not to exist while finalizing {}", processing_node_path, path));

        chassert(
            !zk_client->exists(failed_node_path),
            fmt::format("Expected path {} not to exist while finalizing {}", failed_node_path, path));

        /// NOTE: we don't check that processed_node_path exists here because the cleanup thread
        /// may have already removed it (e.g. when `s3queue_tracked_files_limit` is reached).
    });
#endif
}

void ObjectStorageQueueIFileMetadata::finalizeResetProcessing()
{
    SCOPE_EXIT({
        (*file_status).reset();
        created_processing_node = false;
    });

    LOG_TRACE(log, "File {} processing was reset for retry (rows: {})", path, file_status->processed_rows.load());

#ifdef DEBUG_OR_SANITIZER_BUILD
    ObjectStorageQueueMetadata::getKeeperRetriesControl(log).retryLoop([&]
    {
        auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log, zookeeper_name);
        chassert(
            !zk_client->exists(processing_node_path),
            fmt::format("Expected path {} not to exist after reset for {}", processing_node_path, path));
    });
#endif
}

void ObjectStorageQueueIFileMetadata::finalizeFailed(const std::string & exception_message)
{
    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueFailedFiles);

    SCOPE_EXIT({
        file_status->onFailed(exception_message);
        created_processing_node = false;

        LOG_TRACE(log, "Set file {} as failed (rows: {})", path, file_status->processed_rows.load());
    });
#ifdef DEBUG_OR_SANITIZER_BUILD
    ObjectStorageQueueMetadata::getKeeperRetriesControl(log).retryLoop([&]
    {
        auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log, zookeeper_name);
        chassert(
            !zk_client->exists(processing_node_path),
            fmt::format("Expected path {} not to exist while finalizing {}", processing_node_path, path));

        chassert(
            zk_client->exists(failed_node_path) || zk_client->exists(failed_node_path + ".retriable"),
            fmt::format("Expected path {} to exist while finalizing {}", failed_node_path, path));

    });
#endif
}

void ObjectStorageQueueIFileMetadata::prepareFailedRequestsImpl(
    Coordination::Requests & requests,
    bool retriable)
{
    if (!retriable)
    {
        LOG_TEST(log, "File {} failed to process and will not be retried. ({})", path, failed_node_path);

        permanently_failed = true;

        /// Remove Processing node.
        addProcessingNodeRemovalRequest(requests);
        /// Create Failed node.
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
        auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log, zookeeper_name);
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

        permanently_failed = true;

        /// Remove Processing node.
        addProcessingNodeRemovalRequest(requests);
        /// Remove /failed/node_hash.retriable node.
        requests.push_back(zkutil::makeRemoveRequest(retrieable_failed_node_path, retriable_failed_node_stat.version));
        /// Create a persistent node /failed/node_hash.
        requests.push_back(zkutil::makeCreateRequest(failed_node_path, node_metadata.toString(), zkutil::CreateMode::Persistent));
    }
    else
    {
        /// Remove Processing node.
        addProcessingNodeRemovalRequest(requests);

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
