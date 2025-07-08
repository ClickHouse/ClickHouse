#pragma once
#include <Core/Types.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
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
        /// Set how much time it took to list this object from s3.
        void setGetObjectTime(size_t elapsed_ms);
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
        std::atomic<UInt64> get_object_time_ms = 0;

    private:
        mutable std::mutex last_exception_mutex;
        std::string last_exception;
    };
    using FileStatusPtr = std::shared_ptr<FileStatus>;

    struct HiveLastProcessedFileInfo
    {
        bool exists;
        std::string file_path;
    };

    using HiveLastProcessedFileInfoMap = std::unordered_map<std::string, HiveLastProcessedFileInfo>;

    struct LastProcessedFileInfo
    {
        std::string file_path;
        /// Position of record in `requests` list with keeper commands.
        /// Used to avoid double creation Keeper node with same path.
        /// Instead more actual record overrides old one in `requests` list.
        size_t index;
    };

    using LastProcessedFileInfoMap = std::unordered_map<std::string, LastProcessedFileInfo>;
    using LastProcessedFileInfoMapPtr = std::shared_ptr<LastProcessedFileInfoMap>;

    explicit ObjectStorageQueueIFileMetadata(
        const std::string & path_,
        const std::string & processing_node_path_,
        const std::string & processed_node_path_,
        const std::string & failed_node_path_,
        FileStatusPtr file_status_,
        size_t max_loading_retries_,
        std::atomic<size_t> & metadata_ref_count_,
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

    virtual bool useBucketsForProcessing() const { return false; }
    virtual size_t getBucket() const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Buckets are not supported"); }

    /// Try set file as Processing.
    bool trySetProcessing();
    /// Reset processing
    /// (file will not be set neither as Failed nor Processed,
    /// simply Processing state will be cancelled).
    void resetProcessing();

    /// Prepare keeper requests, required to set file as Processed.
    /// `created_nodes` is a helper index for hive partitioning case,
    /// keeps values and indexes of already inserted commands
    /// to avoid double creation with the same path.
    void prepareProcessedRequests(Coordination::Requests & requests,
        LastProcessedFileInfoMapPtr created_nodes = nullptr);
    /// Prepare keeper requests, required to set file as Failed.
    void prepareFailedRequests(
        Coordination::Requests & requests,
        const std::string & exception_message,
        bool reduce_retry_count);

    /// Prepare keeper requests to save hive last processed files.
    virtual void prepareHiveProcessedMap(HiveLastProcessedFileInfoMap & /* file_map */) {}

    struct SetProcessingResponseIndexes
    {
        size_t processed_path_doesnt_exist_idx = 0;
        size_t failed_path_doesnt_exist_idx = 0;
        size_t create_processing_node_idx = 0;
        size_t set_processing_id_node_idx = 0;
    };
    std::optional<SetProcessingResponseIndexes> prepareSetProcessingRequests(Coordination::Requests & requests);
    void prepareResetProcessingRequests(Coordination::Requests & requests);

    /// Do some work after prepared requests to set file as Processed succeeded.
    void finalizeProcessed();
    /// Do some work after prepared requests to set file as Failed succeeded.
    void finalizeFailed(const std::string & exception_message);
    /// Do some work after prepared requests to set file as Processing succeeded.
    void finalizeProcessing(int processing_id_version_);

    /// A struct, representing information stored in keeper for a single file.
    struct NodeMetadata
    {
        std::string file_path; /// Ignored in hive partitioning case, subnodes hive_path=>file_name used instead.
        UInt64 last_processed_timestamp = 0;
        std::string last_exception;
        UInt64 retries = 0;
        std::string processing_id; /// For ephemeral processing node.

        std::string toString() const;
        static NodeMetadata fromString(const std::string & metadata_str);
    };

protected:
    virtual std::pair<bool, FileStatus::State> setProcessingImpl() = 0;
    virtual void prepareProcessedRequestsImpl(Coordination::Requests & requests,
        LastProcessedFileInfoMapPtr created_nodes) = 0;

    virtual SetProcessingResponseIndexes prepareProcessingRequestsImpl(Coordination::Requests &)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method prepareProcesingRequestsImpl is not implemented");
    }
    void prepareFailedRequestsImpl(Coordination::Requests & requests, bool retriable);

    const std::string path;
    const std::string node_name;
    const FileStatusPtr file_status;
    const size_t max_loading_retries;
    const std::atomic<size_t> & metadata_ref_count;

    const std::string processing_node_path;
    const std::string processed_node_path;
    const std::string failed_node_path;

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
};

}
