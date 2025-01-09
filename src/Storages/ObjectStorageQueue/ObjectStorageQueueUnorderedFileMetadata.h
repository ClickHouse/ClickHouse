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
        const std::filesystem::path & zk_path_,
        const std::string & path_,
        FileStatusPtr file_status_,
        BucketInfoPtr bucket_info_,
        size_t buckets_num_,
        size_t max_loading_retries_,
        LoggerPtr log_);

    static std::vector<std::string> getMetadataPaths() { return {"processed", "failed", "processing"}; }

    void prepareProcessedAtStartRequests(
        Coordination::Requests & requests,
        const zkutil::ZooKeeperPtr & zk_client) override;

private:
    std::pair<bool, FileStatus::State> setProcessingImpl() override;
    void prepareProcessedRequestsImpl(Coordination::Requests & requests) override;
};

}
