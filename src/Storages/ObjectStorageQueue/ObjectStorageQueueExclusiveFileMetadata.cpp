#include <Storages/ObjectStorageQueue/ObjectStorageQueueExclusiveFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>

namespace DB
{
ObjectStorageQueueExclusiveFileMetadata::ObjectStorageQueueExclusiveFileMetadata(
    const std::string & path_,
    FileStatusPtr file_status_,
    size_t max_loading_retries_,
    std::atomic<size_t> & metadata_ref_count_,
    ObjectStorageQueueMetadata & metadata_,
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
          metadata_ref_count_,
          /* use_persistent_processing_nodes */ false,
          log_)
    , metadata(metadata_)
{
    LOG_TRACE(log, "Exclusive mode {}", path);
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
        file_status->retries.fetch_add(1, std::memory_order_relaxed);
    else
        file_status->retries = max_loading_retries ? max_loading_retries : 1;
    // Nothing is changed in zookeeper.
    LOG_TRACE(log, "Prepare {} failed request", path);
}

std::pair<bool, ObjectStorageQueueIFileMetadata::FileStatus::State> ObjectStorageQueueExclusiveFileMetadata::setProcessingImpl()
{
    const auto state = file_status->state.load();
    if (state == FileStatus::State::Processing || state == FileStatus::State::Processed
        || (state == FileStatus::State::Failed && file_status->retries && file_status->retries >= max_loading_retries))
    {
        LOG_TEST(
            log, "File {} has non-processable state `{}` (retries: {}/{})", path, state, file_status->retries.load(), max_loading_retries);
        return std::pair{false, state};
    }

    if (!metadata.tryAcquireExclusiveProcessing(path))
        return std::pair{false, ObjectStorageQueueIFileMetadata::FileStatus::State::Processing};

    processor_info = getProcessorInfo(generateProcessingID());

    // Nothing is changed in zookeeper.
    LOG_TRACE(log, "Setting {} as processing", path);
    return std::pair{true, ObjectStorageQueueIFileMetadata::FileStatus::State::None};
}

void ObjectStorageQueueExclusiveFileMetadata::prepareResetProcessingRequests(Coordination::Requests & /*requests*/)
{
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
    std::string & /*failure_message*/) const
{
    const auto state = file_status->state.load();

    switch (state) {

    case FileStatus::State::Processed:
        return PathState::Processed;

    case FileStatus::State::Failed:
        return PathState::Failed;

    default:
        return PathState::Unknown;
    }
}

}
