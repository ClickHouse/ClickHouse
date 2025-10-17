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

class ZooKeeperWithFaultInjection;

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

    explicit ObjectStorageQueueIFileMetadata(
        const std::string & path_,
        const std::string & processing_node_path_,
        const std::string & processed_node_path_,
        const std::string & failed_node_path_,
        FileStatusPtr file_status_,
        size_t max_loading_retries_,
        std::atomic<size_t> & metadata_ref_count_,
        bool use_persistent_processing_nodes_,
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

    const std::string & getProcessingPath() const { return processing_node_path; }
    const std::string & getProcessorInfo() const { return processor_info; }

    static std::string generateProcessingID();

    virtual bool useBucketsForProcessing() const { return false; }
    virtual size_t getBucket() const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Buckets are not supported"); }

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

    struct SetProcessingResponseIndexes
    {
        size_t processed_path_doesnt_exist_idx = 0;
        size_t failed_path_doesnt_exist_idx = 0;
        size_t create_processing_node_idx = 0;
    };
    /// Prepare requests, required to set file as processing.
    std::optional<SetProcessingResponseIndexes> prepareSetProcessingRequests(
        Coordination::Requests & requests,
        const std::string & processing_id);
    /// Prepare requests, required to reset file's processing state.
    void prepareResetProcessingRequests(Coordination::Requests & requests);

    /// Do some work after prepared requests to set file as Processed succeeded.
    void finalizeProcessed();
    /// Do some work after prepared requests to set file as Failed succeeded.
    void finalizeFailed(const std::string & exception_message);
    /// Do some work after prepared requests to set file as Processing succeeded.
    /// `file_state` is a file state,
    /// which we find out after unsuccessfully attempting to set file as processing.
    void afterSetProcessing(bool success, std::optional<FileStatus::State> file_state);

    /// Set a starting point for processing.
    /// Done on table creation, when we want to tell the table
    /// that processing must be started from certain point,
    /// instead of from scratch.
    virtual void prepareProcessedAtStartRequests(Coordination::Requests & requests) = 0;

    /// A struct, representing information stored in keeper for a single file.
    struct NodeMetadata
    {
        std::string file_path;
        UInt64 last_processed_timestamp = 0;
        std::string last_exception;
        UInt64 retries = 0;

        std::string toString() const;
        static NodeMetadata fromString(const std::string & metadata_str);
    };

protected:
    virtual std::pair<bool, FileStatus::State> setProcessingImpl() = 0;
    virtual void prepareProcessedRequestsImpl(Coordination::Requests & requests) = 0;

    virtual SetProcessingResponseIndexes prepareProcessingRequestsImpl(Coordination::Requests &, const std::string &)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method prepareProcesingRequestsImpl is not implemented");
    }
    void prepareFailedRequestsImpl(Coordination::Requests & requests, bool retriable);

    const std::string path;
    const std::string node_name;
    const FileStatusPtr file_status;
    const size_t max_loading_retries;
    const std::atomic<size_t> & metadata_ref_count;
    const bool use_persistent_processing_nodes;

    const std::string processing_node_path;
    const std::string processed_node_path;
    const std::string failed_node_path;

    NodeMetadata node_metadata;
    LoggerPtr log;

    /// Whether processing node was created by us.
    bool created_processing_node = false;
    /// Id of the processor, which is put into processing node.
    /// Can be used to check if processing node was created by us or by someone else.
    std::string processor_info;

    bool checkProcessingOwnership(std::shared_ptr<ZooKeeperWithFaultInjection> zk_client);

    static std::string getNodeName(const std::string & path);

    static NodeMetadata createNodeMetadata(const std::string & path, const std::string & exception = {}, size_t retries = 0);

    static std::string getProcessorInfo(const std::string & processor_id);
};

}
