#pragma once
#include <Storages/S3Queue/S3QueueIFileMetadata.h>
#include <filesystem>
#include <Common/logger_useful.h>

namespace DB
{

class S3QueueUnorderedFileMetadata : public S3QueueIFileMetadata
{
public:
    using Bucket = size_t;

    explicit S3QueueUnorderedFileMetadata(
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
};

}
