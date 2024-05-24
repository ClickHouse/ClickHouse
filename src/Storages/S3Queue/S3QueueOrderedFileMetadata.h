#pragma once
#include "S3QueueIFileMetadata.h"
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <filesystem>

namespace DB
{

class OrderedFileMetadata : public IFileMetadata
{
public:
    using Processor = std::string;
    using Bucket = size_t;

    explicit OrderedFileMetadata(
        const std::filesystem::path & zk_path,
        const std::string & path_,
        FileStatusPtr file_status_,
        size_t buckets_num_,
        size_t max_loading_retries_,
        LoggerPtr log_);

    struct BucketHolder;
    using BucketHolderPtr = std::shared_ptr<BucketHolder>;

    static BucketHolderPtr tryAcquireBucket(
        const std::filesystem::path & zk_path,
        const Bucket & bucket,
        const Processor & processor);

    static OrderedFileMetadata::Bucket getBucketForPath(const std::string & path, size_t buckets_num);

private:
    const size_t buckets_num;

    std::pair<bool, FileStatus::State> setProcessingImpl() override;
    void setProcessedImpl() override;

    bool getMaxProcessedFile(
        NodeMetadata & result,
        Coordination::Stat * stat,
        const zkutil::ZooKeeperPtr & zk_client)
    {
        std::string data;
        if (zk_client->tryGet(processed_node_path, data, stat))
        {
            if (!data.empty())
                result = NodeMetadata::fromString(data);
            return true;
        }
        return false;
    }
};

}
