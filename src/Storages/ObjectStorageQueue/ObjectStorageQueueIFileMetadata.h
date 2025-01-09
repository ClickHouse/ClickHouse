#pragma once
#include <Core/Types.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <boost/noncopyable.hpp>
#include <filesystem>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// A base class to work with single file metadata in keeper.
/// Metadata can have type Ordered or Unordered.
class ObjectStorageQueueIFileMetadata
{
public:
    struct FileStatus
    {
        enum class State : uint8_t
        {
            Processing,
            Processed,
            Failed,
            None
        };

        void setProcessingEndTime();
        void onProcessing();
        void onProcessed();
        void reset();
        void onFailed(const std::string & exception);
        void updateState(State state_);

        std::string getException() const;

        std::mutex processing_lock;

        std::atomic<State> state = State::None;
        std::atomic<size_t> processed_rows = 0;
        std::atomic<time_t> processing_start_time = 0;
        std::atomic<time_t> processing_end_time = 0;
        std::atomic<size_t> retries = 0;

    private:
        mutable std::mutex last_exception_mutex;
        std::string last_exception;
    };
    using FileStatusPtr = std::shared_ptr<FileStatus>;

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

    struct BucketHolder;
    using BucketHolderPtr = std::shared_ptr<BucketHolder>;

    explicit ObjectStorageQueueIFileMetadata(
        const std::string & path_,
        const std::filesystem::path & zk_path_,
        const std::string & processing_node_path_,
        const std::string & processed_node_path_,
        const std::string & failed_node_path_,
        FileStatusPtr file_status_,
        BucketInfoPtr bucket_info_,
        size_t buckets_num_,
        size_t max_loading_retries_,
        LoggerPtr log_);

    virtual ~ObjectStorageQueueIFileMetadata();

    /// Get path from current file metadata.
    const std::string & getPath() const { return path; }
    /// Get maximum number of retries for file processing.
    size_t getMaxTries() const { return max_loading_retries; }
    /// Get file status.
    /// File status is an in-memory processing info of the file, containing:
    /// number of processed rows, processing time, exception, etc.
    FileStatusPtr getFileStatus() { return file_status; }

    /// Try set file as Processing.
    bool trySetProcessing();
    /// Reset processing
    /// (file will not be set neither as Failed nor Processed,
    /// simply Processing state will be cancelled).
    void resetProcessing();

    /// Prepare keeper requests, required to set file as Processed.
    void prepareProcessedRequests(Coordination::Requests & requests);
    /// Prepare keeper requests, required to set file as Failed.
    void prepareFailedRequests(
        Coordination::Requests & requests,
        const std::string & exception_message,
        bool reduce_retry_count);
    void prepareResetProcessingRequests(Coordination::Requests & requests);

    /// Do some work after prepared requests to set file as Processed succeeded.
    void finalizeProcessed();
    /// Do some work after prepared requests to set file as Failed succeeded.
    void finalizeFailed(const std::string & exception_message);

    /// Set a starting point for processing.
    /// Done on table creation, when we want to tell the table
    /// that processing must be started from certain point,
    /// instead of from scratch.
    virtual void prepareProcessedAtStartRequests(
        Coordination::Requests & requests,
        const zkutil::ZooKeeperPtr & zk_client) = 0;

    /// Whether bucket-based implementation should be used for processing.
    /// Bucket-based implementation means splitting files into buckets
    /// based on the hash from the file name.
    bool useBucketsForProcessing() const { return useBucketsForProcessingImpl(buckets_num); }
    /// Get bucket id. Bucket ids start from 0.
    size_t getBucket() const { chassert(useBucketsForProcessing() && bucket_info); return bucket_info->bucket; }
    static size_t getBucketForPath(const std::string & path, size_t buckets_num_);
    /// "Acquire" bucket for processing.
    /// Each bucket needs to be "acquired" before its files could be processed,
    /// creating a unique access to the bucket,
    /// when no other processor would try to read files from that bucket.
    static BucketHolderPtr tryAcquireBucket(
        const std::filesystem::path & zk_path_,
        const Bucket & bucket,
        const Processor & processor,
        LoggerPtr log_);

    /// A struct, representing information stored in keeper for a single file.
    struct NodeMetadata
    {
        std::string file_path;
        UInt64 last_processed_timestamp = 0;
        std::string last_exception;
        UInt64 retries = 0;
        std::string processing_id; /// For ephemeral processing node.

        std::string toString() const;
        static NodeMetadata fromString(const std::string & metadata_str);
    };

protected:
    virtual std::pair<bool, FileStatus::State> setProcessingImpl() = 0;
    virtual void prepareProcessedRequestsImpl(Coordination::Requests & requests) = 0;
    void prepareFailedRequestsImpl(Coordination::Requests & requests, bool retriable);

    const std::string path;
    const std::string node_name;
    const FileStatusPtr file_status;
    const size_t max_loading_retries;

    const std::filesystem::path zk_path;
    const std::string processing_node_path;
    const std::string processed_node_path;
    const std::string failed_node_path;

    const size_t buckets_num;
    const BucketInfoPtr bucket_info;

    NodeMetadata node_metadata;
    LoggerPtr log;

    /// processing node is ephemeral, so we cannot verify with it if
    /// this node was created by a certain processor on a previous processing stage,
    /// because we could get a session expired in between the stages
    /// and someone else could just create this processing node.
    /// Therefore we also create a persistent processing node
    /// which is updated on each creation of ephemeral processing node.
    /// We use the version of this node to verify the version of the processing ephemeral node.
    const std::string processing_node_id_path;
    /// Id of the processor.
    std::optional<std::string> processing_id;
    /// Version of the processing id persistent node.
    std::optional<int> processing_id_version;

    static std::string getNodeName(const std::string & path);

    static NodeMetadata createNodeMetadata(const std::string & path, const std::string & exception = {}, size_t retries = 0);

    static std::string getProcessorInfo(const std::string & processor_id);

    static bool useBucketsForProcessingImpl(size_t buckets_num_) { return buckets_num_ > 1; }

    static zkutil::ZooKeeperPtr getZooKeeper();
};

struct ObjectStorageQueueIFileMetadata::BucketHolder : private boost::noncopyable
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
