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
        LoggerPtr log_);

    static std::vector<std::string> getMetadataPaths() { return {"processed", "failed", "processing"}; }

    void setProcessedAtStartRequests(
        Coordination::Requests & requests,
        const zkutil::ZooKeeperPtr & zk_client) override;

private:
    std::pair<bool, FileStatus::State> setProcessingImpl() override;
    void setProcessedImpl() override;
    void resetProcessingImpl() override;

    void setProcessedImpl(bool remove_processing_nodes_only);
};

}
