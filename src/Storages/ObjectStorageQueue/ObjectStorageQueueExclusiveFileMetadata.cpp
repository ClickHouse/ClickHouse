#include <Storages/ObjectStorageQueue/ObjectStorageQueueExclusiveFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>

namespace DB
{
ObjectStorageQueueExclusiveFileMetadata::ObjectStorageQueueExclusiveFileMetadata(
    const std::string & path_,
    FileStatusPtr file_status_,
    size_t max_loading_retries_,
    std::atomic<UInt64> & loading_retries_ref_,
    std::atomic<size_t> & metadata_ref_count_,
    ObjectStorageQueueMetadata & metadata_,
    const std::atomic<size_t> & processing_state_cache_ttl_seconds_,
    const std::string & zookeeper_name_,
    LoggerPtr log_)
    : ObjectStorageQueueIFileMetadata(
          path_,
          zookeeper_name_,
          /* processing_node_path */ std::string(),
          /* processed_node_path */ std::string(),
          /* failed_node_path */ std::string(),
          file_status_,
          max_loading_retries_,
          loading_retries_ref_,
          metadata_ref_count_,
          /* use_persistent_processing_nodes */ false,
          processing_state_cache_ttl_seconds_,
          log_)
    , metadata(metadata_)
{
    LOG_TRACE(log, "Exclusive mode {}", path);
}

ObjectStorageQueueExclusiveFileMetadata::~ObjectStorageQueueExclusiveFileMetadata()
{
    releaseProcessingGuard();
}

void ObjectStorageQueueExclusiveFileMetadata::releaseProcessingGuard()
{
    if (!holds_processing_guard)
        return;
    holds_processing_guard = false;
    metadata.releaseExclusiveProcessing(path);
}

ObjectStorageQueueExclusiveFileMetadata::SetProcessingResponseIndexes
ObjectStorageQueueExclusiveFileMetadata::prepareProcessingRequestsImpl(
    Coordination::Requests & requests, const std::string & /*processing_id*/)
{
    SetProcessingResponseIndexes result_indexes;

    result_indexes.processed_path_doesnt_exist_idx = requests.size();
    result_indexes.failed_path_doesnt_exist_idx = requests.size();
    result_indexes.create_processing_node_idx = requests.size();

    return result_indexes;
}

void ObjectStorageQueueExclusiveFileMetadata::prepareFailedRequestsImpl(Coordination::Requests & /*requests*/, bool retriable)
{
    if (retriable)
    {
        /// Read the live limit: `s3queue_loading_retries` can be lowered while the file is in flight.
        const UInt64 max_tries = loading_retries_ref.load();
        auto tries = file_status->retries.fetch_add(1, std::memory_order_relaxed);
        LOG_TRACE(log, "File {} failed at try {}/{}", path, tries + 1, max_tries);

        if (tries + 1 < max_tries)
        {
            releaseProcessingGuard();
            return;
        }
    }
    // Nothing is changed in zookeeper.
    LOG_TRACE(log, "File {} failed to process and will not be retried.", path);
}

std::pair<bool, ObjectStorageQueueIFileMetadata::FileStatus::State> ObjectStorageQueueExclusiveFileMetadata::setProcessingImpl()
{
    if (file_status->state.load() == ObjectStorageQueueIFileMetadata::FileStatus::State::Failed &&
        (!file_status->retries || file_status->retries >= loading_retries_ref.load()))
    {
        return std::pair{false, ObjectStorageQueueIFileMetadata::FileStatus::State::Failed};
    }

    if (!metadata.tryAcquireExclusiveProcessing(path))
        return std::pair{false, ObjectStorageQueueIFileMetadata::FileStatus::State::Processing};
    holds_processing_guard = true;

    processor_info = getProcessorInfo(generateProcessingID());

    // Nothing is changed in zookeeper.
    LOG_TRACE(log, "Setting {} as processing", path);
    return std::pair{true, ObjectStorageQueueIFileMetadata::FileStatus::State::None};
}

void ObjectStorageQueueExclusiveFileMetadata::prepareResetProcessingRequests(Coordination::Requests & /*requests*/, bool /*clear_retriable*/)
{
    releaseProcessingGuard();

    // Nothing is changed in zookeeper.
    LOG_TRACE(log, "Prepare {} reset processed request", path);
}

void ObjectStorageQueueExclusiveFileMetadata::prepareProcessedRequestsImpl(
    Coordination::Requests & /*requests*/, LastProcessedFileInfoMapPtr /* created_nodes */)
{
    // Nothing is changed in zookeeper.
    LOG_TRACE(log, "Prepare {} processed request", path);
}

void ObjectStorageQueueExclusiveFileMetadata::filterOutProcessedAndFailed(
    std::vector<std::string> & /*paths*/,
    const std::filesystem::path & /*zk_path_*/,
    const std::string & /*zookeeper_name_*/,
    LoggerPtr log_)
{
    // Nothing is changed in zookeeper.
    LOG_TRACE(log_, "Filter processed paths");
}

ObjectStorageQueueIFileMetadata::PathState ObjectStorageQueueExclusiveFileMetadata::getPathState(
    std::string & failure_message, UInt64 * retries_out, bool * is_terminal_out) const
{
    const auto state = file_status->state.load();

    switch (state)
    {
        case FileStatus::State::Processed: return PathState::Processed;
        case FileStatus::State::Failed:
            failure_message = file_status->getException();
            {
                const auto retries = file_status->retries.load();
                if (retries_out)
                    *retries_out = retries;
                /// Exclusive mode has no `.retriable` marker, but a failure is still retryable
                /// until the retry budget is exhausted. Mirror the claim check in setProcessingImpl().
                if (is_terminal_out)
                    *is_terminal_out = (retries == 0 || retries >= loading_retries_ref.load());
            }
            return PathState::Failed;
        default:
            return PathState::Unknown;
    }
}

void ObjectStorageQueueExclusiveFileMetadata::debugFinalizeProcessed() {}

void ObjectStorageQueueExclusiveFileMetadata::debugFinalizeResetProcessing() {}

void ObjectStorageQueueExclusiveFileMetadata::debugFinalizeFailed() {}

}
