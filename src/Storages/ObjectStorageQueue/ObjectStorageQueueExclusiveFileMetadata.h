#pragma once
#include <Storages/ObjectStorageQueue/ObjectStorageQueueIFileMetadata.h>
#include <filesystem>
#include <Common/logger_useful.h>

namespace DB
{
class ObjectStorageQueueMetadata;

class ObjectStorageQueueExclusiveFileMetadata : public ObjectStorageQueueIFileMetadata
{
public:
    using Bucket = size_t;

    explicit ObjectStorageQueueExclusiveFileMetadata(
        const std::string & path_,
        FileStatusPtr file_status_,
        size_t max_loading_retries_,
        std::atomic<size_t> & metadata_ref_count_,
        ObjectStorageQueueMetadata & metadata_,
        const std::string & zookeeper_name_,
        LoggerPtr log_);

    static std::vector<std::string> getMetadataPaths() { return {}; }

    /// Return vector of indexes of filtered paths.
    static void filterOutProcessedAndFailed(
        std::vector<std::string> & paths,
        const std::filesystem::path & zk_path_,
        const std::string & zookeeper_name_,
        LoggerPtr log_);

    void prepareResetProcessingRequests(Coordination::Requests & requests) override;

    PathState getPathState(std::string & failure_message) const override;

private:
    std::pair<bool, FileStatus::State> setProcessingImpl() override;
    void prepareProcessedRequestsImpl(Coordination::Requests & requests, LastProcessedFileInfoMapPtr created_nodes) override;
    SetProcessingResponseIndexes prepareProcessingRequestsImpl(Coordination::Requests & requests, const std::string & processing_id) override;
    void prepareFailedRequestsImpl(Coordination::Requests & requests, bool retriable) override;

    ObjectStorageQueueMetadata & metadata;
};

}
