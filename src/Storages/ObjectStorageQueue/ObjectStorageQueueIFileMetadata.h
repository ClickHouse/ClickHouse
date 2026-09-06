#pragma once
#include <Core/Types.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

class ZooKeeperWithFaultInjection;

/// Keeper paths of processing nodes and bucket locks owned (or about to be owned)
/// by executions running in this process. The TTL-based cleanup thread must not
/// delete these: losing a live execution's node fails its commit ("Transaction
/// failed: No node"), and losing a live bucket lock lets another server acquire
/// the same bucket.
///
/// Fencing protocol (closes the revalidate/remove vs create race completely for
/// local owners): a creator registers the path with `tryAdd` BEFORE issuing the
/// Keeper create and backs off when it fails; the cleanup wraps its revalidate +
/// remove window in `tryLockForRemoval`/`unlockRemoval`, which fails while the
/// path is registered. Both sides only try — nobody blocks.
class ObjectStorageQueueLocalActiveNodes
{
public:
    /// Register intent to own `path`. Returns false while the cleanup holds a
    /// removal lock on it — the caller must not create the node and retry later.
    bool tryAdd(const std::string & path);
    void remove(const std::string & path);
    bool contains(const std::string & path) const;

    /// Exclusive lock for the cleanup's revalidate+remove window.
    /// Fails while `path` is registered to a live local owner.
    bool tryLockForRemoval(const std::string & path);
    void unlockRemoval(const std::string & path);

private:
    mutable std::mutex mutex;
    /// Reference-counted: separate owners (e.g. an uncertain-commit straggler and
    /// a retry, or sibling tables on the same keeper path) can overlap.
    std::unordered_map<std::string, size_t> path_counts;
    std::unordered_set<std::string> removal_locks;
};
using ObjectStorageQueueLocalActiveNodesPtr = std::shared_ptr<ObjectStorageQueueLocalActiveNodes>;

/// A base class to work with single file metadata in keeper.
/// Metadata can have type Ordered or Unordered.
class ObjectStorageQueueIFileMetadata
{
public:
    struct FileStatus
    {
        explicit FileStatus(const std::string & path_) : path(path_) {}

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

        const std::string path;
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

    /// Helper structure for storing the flag of presence or absence of a node in the keeper.
    struct PartitionLastProcessedFileInfo
    {
        /// Flag if node exists. For existing node need to call `set`, for non-existing - `create`.
        bool exists;
        /// Last processed path for this partition.
        std::string file_path;
    };

    /// Used only in Ordered mode with partitioning (HIVE or REGEX mode).
    /// Key: <processed_node_path>/<partition_key>
    /// Value: last processed file info
    /// Explanation:
    ///    Used to collect `requests` list via preparePartitionProcessedRequests
    ///    to set values for each <processed_node_path>/<partition_key>.
    using PartitionLastProcessedFileInfoMap = std::unordered_map<std::string, PartitionLastProcessedFileInfo>;

    struct LastProcessedFileInfo
    {
        /// Last processed path for some hive partition.
        std::string file_path;
        /// Position of record in `requests` list with keeper commands.
        /// Used to avoid double creation of Keeper node with same path.
        /// Instead more actual record overrides old one in the `requests` list.
        size_t index;
    };

    /// Used only in Ordered mode.
    /// Key: <processed_node_path> (without <hive_part>)
    /// Value: last processed file info
    /// Explanation:
    ///    Used to collect `requests` list via prepareProcessedRequests
    ///    to set values for each <processed_node_path>.
    using LastProcessedFileInfoMap = std::unordered_map<std::string, LastProcessedFileInfo>;
    using LastProcessedFileInfoMapPtr = std::shared_ptr<LastProcessedFileInfoMap>;

    explicit ObjectStorageQueueIFileMetadata(
        const std::string & path_,
        const std::string & zookeeper_name_,
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

    enum class PathState
    {
        /// The path has been successfully processed.
        Processed,
        /// The path has permanently failed; the failure message is populated.
        Failed,
        /// The path has not been processed yet (or its status is unknown).
        Unknown,
    };

    /// Check Keeper to determine whether this file has already been processed or failed.
    /// Sets `failure_message` when the result is `Failed`.
    virtual PathState getPathState(std::string & failure_message) const = 0;

    const std::string & getFailedNodePath() const { return failed_node_path; }
    const std::string & getProcessedNodePath() const { return processed_node_path; }

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

    /// Prepare keeper requests to save partition last processed files (for HIVE or REGEX partitioning modes).
    virtual void preparePartitionProcessedMap(PartitionLastProcessedFileInfoMap & /* file_map */) {}

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
    /// Do some work after prepared requests reset processing without marking as failed.
    void finalizeResetProcessing();
    /// Whether prepareFailedRequests just reset processing
    /// without actually marking the file as failed.
    bool wasProcessingResetWithoutFailure() const { return processing_reset_without_failure; }
    /// Whether the file was given up on for good (see `permanently_failed`).
    bool wasPermanentlyFailed() const { return permanently_failed; }
    /// Do some work after prepared requests to set file as Processing succeeded.
    /// `file_state` is a file state,
    /// which we find out after unsuccessfully attempting to set file as processing.
    void afterSetProcessing(bool success, std::optional<FileStatus::State> file_state);

    void setUncertainCommit() { uncertain_commit = true; }

    /// Set the registry of node paths owned by live local executions
    /// (kept in sync with `created_processing_node`). May stay unset when
    /// processing nodes are never created through this object.
    void setLocalActiveNodes(ObjectStorageQueueLocalActiveNodesPtr nodes) { local_active_nodes = std::move(nodes); }

    /// A struct, representing information stored in keeper for a single file.
    struct NodeMetadata
    {
        std::string file_path; /// Ignored in hive partitioning case, subnodes hive_path=>file_name used instead.
        UInt64 last_processed_timestamp = 0;
        std::string last_exception;
        UInt64 retries = 0;

        std::string toString() const;
        static NodeMetadata fromString(const std::string & metadata_str);
    };

protected:
    /// Returns a single-component Keeper node name for the given file path.
    /// Raw file paths contain '/' and cannot be used directly as Keeper node names,
    /// so SipHash64 of the path is used instead.
    static std::string getNodeName(const std::string & path);

    virtual std::pair<bool, FileStatus::State> setProcessingImpl() = 0;
    virtual void prepareProcessedRequestsImpl(Coordination::Requests & requests,
        LastProcessedFileInfoMapPtr created_nodes) = 0;

    virtual SetProcessingResponseIndexes prepareProcessingRequestsImpl(Coordination::Requests &,
        const std::string &)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method prepareProcesingRequestsImpl is not implemented");
    }
    void prepareFailedRequestsImpl(Coordination::Requests & requests, bool retriable);

    const std::string path;
    const std::string zookeeper_name;
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

    /// Flip `created_processing_node` and keep `local_active_nodes` in sync.
    void setProcessingNodeCreated();
    void clearProcessingNodeCreated();

    /// Register in `local_active_nodes` BEFORE the processing node is created in
    /// Keeper (see the fencing protocol above). False = cleanup is inspecting the
    /// path right now, back off. Both are idempotent per object.
    bool tryRegisterInLocalActiveNodes();
    void unregisterFromLocalActiveNodes();

    ObjectStorageQueueLocalActiveNodesPtr local_active_nodes;
    bool registered_in_local_active_nodes = false;
    /// Whether processing node was created by us.
    bool created_processing_node = false;
    /// Set when a commit failed after a ZooKeeper retry (possible "failed after operation"):
    /// the multi-op may have succeeded in ZK but the connection was lost before we received
    /// the response. In this case the destructor must check ownership before removing the
    /// processing node rather than asserting it.
    bool uncertain_commit = false;
    /// Whether prepareFailedRequests just reset processing without actually
    /// marking the file as failed (when reduce_retry_count was false).
    bool processing_reset_without_failure = false;
    /// Whether prepareFailedRequests gave up on the file for good, i.e. created
    /// the terminal /failed node rather than a retriable one (retries exhausted,
    /// or retries are disabled altogether).
    bool permanently_failed = false;
    /// Id of the processor, which is put into processing node.
    /// Can be used to check if processing node was created by us or by someone else.
    std::string processor_info;

    bool checkProcessingOwnership(std::shared_ptr<ZooKeeperWithFaultInjection> zk_client);

    static NodeMetadata createNodeMetadata(const std::string & path, const std::string & exception = {}, size_t retries = 0);

    static std::string getProcessorInfo(const std::string & processor_id);
};

}
