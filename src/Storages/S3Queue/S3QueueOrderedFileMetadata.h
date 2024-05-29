#pragma once
#include "S3QueueIFileMetadata.h"
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <filesystem>

namespace DB
{

class S3QueueOrderedFileMetadata : public S3QueueIFileMetadata
{
public:
    using Processor = std::string;
    using Bucket = size_t;

    explicit S3QueueOrderedFileMetadata(
        const std::filesystem::path & zk_path_,
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

    static S3QueueOrderedFileMetadata::Bucket getBucketForPath(const std::string & path, size_t buckets_num);

    static std::vector<std::string> getMetadataPaths(size_t buckets_num);

    void setProcessedAtStartRequests(
        Coordination::Requests & requests,
        const zkutil::ZooKeeperPtr & zk_client) override;

private:
    const size_t buckets_num;
    const std::string zk_path;

    std::pair<bool, FileStatus::State> setProcessingImpl() override;
    void setProcessedImpl() override;

    bool getMaxProcessedFile(
        NodeMetadata & result,
        Coordination::Stat * stat,
        const zkutil::ZooKeeperPtr & zk_client)
    {
        return getMaxProcessedFile(result, stat, processed_node_path, zk_client);
    }

    bool getMaxProcessedFile(
        NodeMetadata & result,
        Coordination::Stat * stat,
        const std::string & processed_node_path_,
        const zkutil::ZooKeeperPtr & zk_client)
    {
        std::string data;
        if (zk_client->tryGet(processed_node_path_, data, stat))
        {
            if (!data.empty())
                result = NodeMetadata::fromString(data);
            return true;
        }
        return false;
    }

    void setProcessedRequests(
        Coordination::Requests & requests,
        const zkutil::ZooKeeperPtr & zk_client,
        const std::string & processed_node_path_,
        bool ignore_if_exists);
};

struct S3QueueOrderedFileMetadata::BucketHolder
{
    BucketHolder(
        const Bucket & bucket_,
        const std::string & bucket_lock_path_,
        zkutil::ZooKeeperPtr zk_client_)
        : bucket(bucket_), bucket_lock_path(bucket_lock_path_), zk_client(zk_client_) {}

    Bucket getBucket() const { return bucket; }

    void release()
    {
        if (released)
            return;

        released = true;
        LOG_TEST(getLogger("S3QueueBucketHolder"), "Releasing bucket {}", bucket);

        zk_client->remove(bucket_lock_path);
    }

    ~BucketHolder()
    {
        try
        {
            release();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    const Bucket bucket;
    const std::string bucket_lock_path;
    const zkutil::ZooKeeperPtr zk_client;
    bool released = false;
};

}
