#pragma once
#include <Core/Types.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <list>
#include <memory>
#include <unordered_map>

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
    /// Per-table foreign-node observations. This must stay outside `FileStatus`: file
    /// statuses are held in a byte-accounted cache and their weight cannot grow after
    /// insertion. The registry follows the table's metadata-cache entry limit, where
    /// zero means unlimited.
    /// An observation belongs to one foreign hold of the path, identified by the
    /// generation of the shared `FileStatus`: an observation of an earlier hold must
    /// never be reused for a later one (the file may have been released in between).
    class ForeignProcessingObservers
    {
    public:
        explicit ForeignProcessingObservers(size_t max_entries_, size_t max_bytes_ = 0)
            : max_entries(max_entries_), max_bytes(max_bytes_) {}

        void set(const String & path, UInt64 generation, time_t since);
        /// Zero if the path was not observed by this table in this generation.
        time_t get(const String & path, UInt64 generation) const;
        void setMaxEntries(size_t max_entries_);
        void setMaxSizeInBytes(size_t max_bytes_);
        /// The whole heap footprint of the registry: the entries and the bucket array.
        size_t sizeInBytes() const;
        size_t count() const;

    private:
        struct Observation
        {
            UInt64 generation;
            time_t since;
            std::list<String>::iterator lru_position;
            size_t entry_weight;
        };

        /// The registry holds this many bytes per observation: the path is stored twice
        /// (as the key of `observations` and as an element of the `lru` list).
        static size_t weight(const String & key, const String & lru_entry);
        void evictWhileOverLimitsUnlocked() TSA_REQUIRES(mutex);
        bool overLimitsUnlocked() const TSA_REQUIRES(mutex);
        size_t sizeInBytesUnlocked() const TSA_REQUIRES(mutex);
        /// `std::unordered_map::erase` never shrinks the bucket array, so the registry would
        /// keep its high-water mark forever. Returns whether the bucket array became smaller.
        bool reclaimBucketArrayUnlocked() TSA_REQUIRES(mutex);

        size_t max_entries;
        /// Zero means that only `max_entries` bounds the registry.
        size_t max_bytes;
        mutable std::mutex mutex;
        mutable std::list<String> lru TSA_GUARDED_BY(mutex);
        mutable std::unordered_map<String, Observation> observations TSA_GUARDED_BY(mutex);
        size_t entries_size_in_bytes TSA_GUARDED_BY(mutex) = 0;
    };

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
        /// The `processing` node in keeper is held by another processor
        /// (another server, or another table on this server).
        void onProcessingByAnotherProcessor(ForeignProcessingObservers & observers);
        /// The file was committed by another processor: replace the data of a previous
        /// local attempt with the terminal state discovered in keeper.
        void onTerminalStateByAnotherProcessor(State state_, const std::string & exception, size_t retries_);
        /// Whether the `Processing` state is only a cached observation of a foreign node.
        bool isProcessingByAnotherProcessor() const { return processing_by_another_processor_since.load() != 0; }
        /// When the foreign `processing` node was observed; zero if the state is not foreign.
        time_t processingByAnotherProcessorSince(const ForeignProcessingObservers & observers) const;
        /// Whether a file in `Processing` state may be attempted again: only if the state is a
        /// cached observation of a foreign node and the observation is older than `ttl_sec`.
        bool shouldRetryProcessing(const ForeignProcessingObservers & observers, time_t ttl_sec) const;

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
        /// When the `processing` node of another processor was observed the last time.
        /// Zero means that the state, if it is `Processing`, belongs to this processor.
        std::atomic<time_t> processing_by_another_processor_since = 0;
        /// Incremented on every transition into the foreign `Processing` state.
        std::atomic<UInt64> foreign_processing_generation = 0;
        mutable std::mutex last_exception_mutex;
        std::string last_exception;
    };
    using FileStatusPtr = std::shared_ptr<FileStatus>;

    /// A terminal state of a file discovered in keeper (a `processed` or `failed` node
    /// committed by another processor).
    struct FileTerminalState
    {
        FileStatus::State state;
        /// The exception from the `failed` node; empty for `Processed`.
        std::string exception = {};
        size_t retries = 0;
    };

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
        LoggerPtr log_,
        /// Zero (the default) means to always check keeper.
        time_t foreign_processing_node_cache_ttl_sec_ = 0,
        std::shared_ptr<ForeignProcessingObservers> foreign_processing_observers_ = {});

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
    /// `terminal_state` carries the `failed` node metadata when `file_state` is terminal.
    void afterSetProcessing(
        bool success,
        std::optional<FileStatus::State> file_state,
        std::optional<FileTerminalState> terminal_state = std::nullopt);

    void setUncertainCommit() { uncertain_commit = true; }

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

    /// `terminal_state` is filled with the discovered node metadata
    /// when the returned state is `Processed` or `Failed`.
    virtual std::pair<bool, FileStatus::State> setProcessingImpl(std::optional<FileTerminalState> & terminal_state) = 0;
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
    /// How long an observation of a `processing` node of another processor is trusted.
    const time_t foreign_processing_node_cache_ttl_sec;
    const std::shared_ptr<ForeignProcessingObservers> foreign_processing_observers;
    const std::string processing_node_path;
    const std::string processed_node_path;
    const std::string failed_node_path;

    NodeMetadata node_metadata;
    LoggerPtr log;

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
