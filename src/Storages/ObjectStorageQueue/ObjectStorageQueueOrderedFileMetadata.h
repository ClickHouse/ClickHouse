#pragma once
#include <Storages/ObjectStorageQueue/ObjectStorageQueueIFileMetadata.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <filesystem>

#include <boost/noncopyable.hpp>

namespace DB
{

class ObjectStorageQueueOrderedFileMetadata : public ObjectStorageQueueIFileMetadata
{
public:
    using Processor = std::string;
    using Bucket = size_t;
    struct BucketInfo
    {
        Bucket bucket;
        int bucket_version;
        std::string bucket_lock_path;
        std::string bucket_lock_id_path;
    };
    using BucketInfoPtr = std::shared_ptr<const BucketInfo>;

    explicit ObjectStorageQueueOrderedFileMetadata(
        const std::filesystem::path & zk_path_,
        const std::string & path_,
        FileStatusPtr file_status_,
        BucketInfoPtr bucket_info_,
        size_t buckets_num_,
        size_t max_loading_retries_,
        LoggerPtr log_);

    struct BucketHolder;
    using BucketHolderPtr = std::shared_ptr<BucketHolder>;

    static BucketHolderPtr tryAcquireBucket(
        const std::filesystem::path & zk_path,
        const Bucket & bucket,
        const Processor & processor,
        LoggerPtr log_);

    static ObjectStorageQueueOrderedFileMetadata::Bucket getBucketForPath(const std::string & path, size_t buckets_num);

    static std::vector<std::string> getMetadataPaths(size_t buckets_num);

    void setProcessedAtStartRequests(
        Coordination::Requests & requests,
        const zkutil::ZooKeeperPtr & zk_client) override;

private:
    const size_t buckets_num;
    const std::string zk_path;
    const BucketInfoPtr bucket_info;

    std::pair<bool, FileStatus::State> setProcessingImpl() override;
    void setProcessedImpl() override;

    bool getMaxProcessedFile(
        NodeMetadata & result,
        Coordination::Stat * stat,
        const zkutil::ZooKeeperPtr & zk_client);

    bool getMaxProcessedFile(
        NodeMetadata & result,
        Coordination::Stat * stat,
        const std::string & processed_node_path_,
        const zkutil::ZooKeeperPtr & zk_client);

    void setProcessedRequests(
        Coordination::Requests & requests,
        const zkutil::ZooKeeperPtr & zk_client,
        const std::string & processed_node_path_,
        bool ignore_if_exists);
};

struct ObjectStorageQueueOrderedFileMetadata::BucketHolder : private boost::noncopyable
{
    BucketHolder(
        const Bucket & bucket_,
        int bucket_version_,
        const std::string & bucket_lock_path_,
        const std::string & bucket_lock_id_path_,
        zkutil::ZooKeeperPtr zk_client_,
        LoggerPtr log_);

    ~BucketHolder();

    Bucket getBucket() const { return bucket_info->bucket; }
    BucketInfoPtr getBucketInfo() const { return bucket_info; }

    void setFinished() { finished = true; }
    bool isFinished() const { return finished; }

    void release();

private:
    BucketInfoPtr bucket_info;
    const zkutil::ZooKeeperPtr zk_client;
    bool released = false;
    bool finished = false;
    LoggerPtr log;
};

}
