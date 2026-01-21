#pragma once
#include <Storages/ObjectStorageQueue/ObjectStorageQueueIFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueFilenameParser.h>
#include <Core/SettingsEnums.h>
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
        std::string bucket_lock_path;
        std::string processor_info;
        std::string toString() const;
    };
    using BucketInfoPtr = std::shared_ptr<const BucketInfo>;
    using LastProcessedFileInfo = ObjectStorageQueueIFileMetadata::LastProcessedFileInfo;

    explicit ObjectStorageQueueOrderedFileMetadata(
        const std::filesystem::path & zk_path_,
        const std::string & path_,
        FileStatusPtr file_status_,
        BucketInfoPtr bucket_info_,
        size_t buckets_num_,
        size_t max_loading_retries_,
        std::atomic<size_t> & metadata_ref_count_,
        bool use_persistent_processing_nodes_,
        ObjectStorageQueuePartitioningMode partitioning_mode_,
        const ObjectStorageQueueFilenameParser * parser_,
        LoggerPtr log_);

    struct BucketHolder;
    using BucketHolderPtr = std::shared_ptr<BucketHolder>;

    bool useBucketsForProcessing() const override;
    size_t getBucket() const override { chassert(useBucketsForProcessing() && bucket_info); return bucket_info->bucket; }

    static BucketHolderPtr tryAcquireBucket(
        const std::filesystem::path & zk_path,
        const Bucket & bucket,
        bool use_persistent_processing_nodes_,
        LoggerPtr log_);

    static ObjectStorageQueueOrderedFileMetadata::Bucket getBucketForPath(const std::string & path, size_t buckets_num);

    static std::vector<std::string> getMetadataPaths(size_t buckets_num);

    static void migrateToBuckets(const std::string & zk_path, size_t value, size_t prev_value);

    /// Return vector of indexes of filtered paths.
    static void filterOutProcessedAndFailed(
        std::vector<std::string> & paths,
        const std::filesystem::path & zk_path_,
        size_t buckets_num,
        ObjectStorageQueuePartitioningMode partitioning_mode,
        const ObjectStorageQueueFilenameParser * parser,
        LoggerPtr log);

    void prepareProcessedAtStartRequests(Coordination::Requests & requests);

private:
    const size_t buckets_num;
    const std::string zk_path;
    const BucketInfoPtr bucket_info;
    const ObjectStorageQueuePartitioningMode partitioning_mode;
    const ObjectStorageQueueFilenameParser * parser;

    std::pair<bool, FileStatus::State> setProcessingImpl() override;

    void prepareProcessedRequestsImpl(Coordination::Requests & requests,
        LastProcessedFileInfoMapPtr created_nodes) override;

    static bool getMaxProcessedNode(
        NodeMetadata & result,
        Coordination::Stat * stat,
        const std::string & processed_node_path_,
        LoggerPtr log_);

    struct ProcessingStateFromKeeper
    {
        explicit ProcessingStateFromKeeper(bool is_failed_) : is_failed(is_failed_) {}
        ProcessingStateFromKeeper(const std::string & path, const std::string & last_processed_path_, bool is_failed_);

        const std::optional<std::string> last_processed_path = std::nullopt;
        const bool is_failed = false;
        const bool is_processed = false;
    };

    ProcessingStateFromKeeper getProcessingStateFromKeeper(
        Coordination::Stat * processed_node_stat,
        bool check_failed = false,
        LoggerPtr log_ = nullptr);

    static ProcessingStateFromKeeper getProcessingStateFromKeeper(
        Coordination::Stat * processed_node_stat,
        const std::string & processed_node_path_,
        const std::string & file_path,
        std::optional<std::string> processed_node_hive_partitioning_path = std::nullopt,
        std::optional<std::string> failed_node_path = std::nullopt,
        LoggerPtr log_ = nullptr);

    static bool getMaxProcessedFilesByHivePartition(
        std::unordered_map<std::string, std::string> & last_processed_path_per_hive_partition,
        const std::string & processed_node_path_,
        LoggerPtr log_);

    void doPrepareProcessedRequests(
        Coordination::Requests & requests,
        const std::string & processed_node_path_,
        bool ignore_if_exists,
        LastProcessedFileInfoMapPtr created_nodes = nullptr);

    void preparePartitionProcessedMap(PartitionLastProcessedFileInfoMap & last_processed_file_per_partition) override;
};

struct ObjectStorageQueueOrderedFileMetadata::BucketHolder : private boost::noncopyable
{
    BucketHolder(
        const Bucket & bucket_,
        const std::string & bucket_lock_path_,
        const std::string & processor_info_,
        LoggerPtr log_);

    ~BucketHolder();

    Bucket getBucket() const { return bucket_info->bucket; }
    BucketInfoPtr getBucketInfo() const { return bucket_info; }

    void setFinished() { finished = true; }
    bool isFinished() const { return finished; }

    bool checkBucketOwnership(std::shared_ptr<ZooKeeperWithFaultInjection> zk_client);
    std::optional<std::string> getProcessorInfo(std::shared_ptr<ZooKeeperWithFaultInjection> zk_client);

    void release();

private:
    BucketInfoPtr bucket_info;
    bool released = false;
    bool finished = false;
    LoggerPtr log;
};

}
