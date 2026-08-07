#pragma once
#include <Storages/ObjectStorageQueue/ObjectStorageQueueIFileMetadata.h>
#include <filesystem>
#include <Common/logger_useful.h>

namespace DB
{

class ObjectStorageQueueUnorderedFileMetadata : public ObjectStorageQueueIFileMetadata
{
public:
    using Bucket = size_t;

    explicit ObjectStorageQueueUnorderedFileMetadata(
        const std::filesystem::path & zk_path,
        const std::string & path_,
        FileStatusPtr file_status_,
        size_t max_loading_retries_,
        std::atomic<size_t> & metadata_ref_count_,
        bool use_persistent_processing_nodes_,
        const std::string & zookeeper_name_,
        LoggerPtr log_,
        /// Zero (the default) means to always check keeper.
        time_t foreign_processing_node_cache_ttl_sec_ = 0);

    static std::vector<std::string> getMetadataPaths() { return {"processed", "failed", "processing", "persistent_processing"}; }

    /// Remove the paths which have a terminal (`processed` or `failed`) node in keeper,
    /// recording the state of each removed path in `terminal_states`.
    static void filterOutProcessedAndFailed(
        std::vector<std::string> & paths,
        const std::filesystem::path & zk_path_,
        const std::string & zookeeper_name_,
        std::unordered_map<std::string, FileTerminalState> & terminal_states,
        LoggerPtr log_);

    PathState getPathState(std::string & failure_message) const override;

private:
    std::pair<bool, FileStatus::State> setProcessingImpl(std::optional<FileTerminalState> & terminal_state) override;
    void prepareProcessedRequestsImpl(Coordination::Requests & requests, LastProcessedFileInfoMapPtr created_nodes) override;
    SetProcessingResponseIndexes prepareProcessingRequestsImpl(
        Coordination::Requests & requests,
        const std::string & processing_id) override;
};

}
