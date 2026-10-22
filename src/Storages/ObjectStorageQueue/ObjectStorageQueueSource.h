#pragma once

#include <Interpreters/ObjectStorageQueueLog.h>
#include <Processors/ISource.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueuePostProcessor.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSettings.h>
#include <base/defines.h>
#include <condition_variable>
#include <unordered_set>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/ZooKeeper.h>


namespace Poco { class Logger; }

namespace DB
{

class IStreamingStorage;
struct ObjectMetadata;

class ObjectStorageQueueSource final : public ISource, WithContext
{
public:
    using Storage = StorageObjectStorage;
    using Source = StorageObjectStorageSource;
    using BucketHolder = ObjectStorageQueueOrderedFileMetadata::BucketHolder;
    using BucketHolderPtr = ObjectStorageQueueOrderedFileMetadata::BucketHolderPtr;
    using BucketHolders = std::vector<BucketHolderPtr>;
    using FileMetadataPtr = ObjectStorageQueueMetadata::FileMetadataPtr;
    using PartitionLastProcessedFileInfoMap = ObjectStorageQueueIFileMetadata::PartitionLastProcessedFileInfoMap;
    using LastProcessedFileInfoMapPtr = ObjectStorageQueueIFileMetadata::LastProcessedFileInfoMapPtr;

    struct ObjectStorageQueueObjectInfo : public ObjectInfo
    {
        ObjectStorageQueueObjectInfo(
            const ObjectInfo & object_info,
            FileMetadataPtr file_metadata_);

        FileMetadataPtr file_metadata;
    };

    class FileIterator : public IObjectIterator, private WithContext
    {
    public:
        FileIterator(
            std::shared_ptr<ObjectStorageQueueMetadata> metadata_,
            ObjectStoragePtr object_storage_,
            StorageObjectStorageConfigurationPtr configuration_,
            const StorageID & storage_id_,
            size_t list_objects_batch_size_,
            const ActionsDAG::Node * predicate_,
            const NamesAndTypesList & virtual_columns_,
            const NamesAndTypesList & hive_partition_columns_to_read_from_file_path_,
            ContextPtr context_,
            LoggerPtr logger_,
            bool enable_hash_ring_filtering_,
            bool file_deletion_on_processed_enabled_,
            std::atomic<bool> & shutdown_called_);

        bool isFinished();

        ObjectInfoPtr next(size_t processor) override;

        size_t estimatedKeysCount() override;

        /// If the key was taken from iterator via next() call,
        /// we might later want to return it back for retrying.
        void returnForRetry(ObjectInfoPtr object_info, FileMetadataPtr file_metadata);

        /// Release hold buckets.
        /// In fact, they could be released in destructors of BucketHolder,
        /// but we anyway try to release them explicitly,
        /// because we want to be able to rethrow exceptions if they might happen.
        void releaseFinishedBuckets();

        /// Refresh bucket locks which were not refreshed for more than a quarter of
        /// the TTL, after which the cleanup removes them as abandoned (the TTL is
        /// meant to remove locks of dead servers).
        void refreshExpiringBucketLocks();

        bool useBucketsForProcessing() const { return use_buckets_for_processing; }

        /// The earliest time at which a file skipped because of a fresh foreign `processing`
        /// observation becomes due for a recheck; `std::nullopt` if there are no such files.
        /// The streaming task schedules the next cycle no later than this time, so that
        /// `processing_state_cache_ttl_seconds` bounds the retry latency even when
        /// the queue is otherwise idle and the polling backoff is large.
        std::optional<time_t> earliestForeignProcessingRecheckTime();

        /// Takes over the pending rechecks of files skipped because of a fresh foreign
        /// `processing` observation from the previous iterator of the streaming task, whose
        /// listing pass is over. Otherwise such a file would wait until the new listing pass
        /// reaches it again, which in a large namespace can take much longer than
        /// `processing_state_cache_ttl_seconds`.
        void inheritForeignProcessingRechecks(FileIterator & previous);

    private:
        using Bucket = ObjectStorageQueueMetadata::Bucket;
        using Processor = ObjectStorageQueueMetadata::Processor;

        const std::shared_ptr<ObjectStorageQueueMetadata> metadata;
        const ObjectStoragePtr object_storage;
        const StorageObjectStorageConfigurationPtr configuration;
        const NamesAndTypesList virtual_columns;
        const NamesAndTypesList hive_partition_columns_to_read_from_file_path;
        const bool file_deletion_on_processed_enabled;
        const ObjectStorageQueueMode mode;
        const bool enable_hash_ring_filtering;
        const StorageID storage_id;
        const bool use_buckets_for_processing;
        const size_t buckets_num = 0;

        ObjectStorageIteratorPtr object_storage_iterator;
        std::unique_ptr<re2::RE2> matcher;
        ExpressionActionsPtr filter_expr;
        bool recursive{false};

        ObjectInfos object_infos TSA_GUARDED_BY(next_mutex);
        std::vector<FileMetadataPtr> file_metadatas;
        bool is_finished = false;
        std::mutex next_mutex;
        size_t index = 0;

        /// Files skipped because a foreign `processing` node observation was fresh.
        /// They are rechecked at the next batch boundary after the observation expires
        /// (`processing_state_cache_ttl_seconds`), so the recheck does not wait
        /// for the current listing pass to end. Entries left when the listing is
        /// exhausted are taken over by the next iterator, see `inheritForeignProcessingRechecks`.
        /// Each entry keeps its own recheck deadline: the file status cache may evict the status
        /// of a pending file, and an evicted status must not make the file due immediately -
        /// a failed claim refreshes the observation, which can evict the statuses of the other
        /// pending files again, and the rechecks would then starve the listing.
        struct PendingRecheck
        {
            ObjectInfoPtr object;
            /// When the recheck is due if the status of the file is not cached any more.
            time_t due_time = 0;
            /// Ordered mode only. The file was not foreign-held itself: it is replayed after the
            /// blocker of its ordering domain resolved (or it is queued behind such a replay),
            /// see `pending_replays_per_domain`.
            bool is_replay = false;
        };
        std::deque<PendingRecheck> foreign_processing_files_to_recheck TSA_GUARDED_BY(next_mutex);

        /// The object-storage listing of this iterator is over: only the pending rechecks remain.
        bool listing_exhausted TSA_GUARDED_BY(next_mutex) = false;
        /// The previous batch was made of rechecks: the next batch boundary takes a listing batch
        /// first, so that the rechecks cannot indefinitely prevent the listing from progressing.
        bool last_batch_was_recheck TSA_GUARDED_BY(next_mutex) = false;

        /// Paths taken over from the previous iterator which the listing pass of this iterator
        /// has not listed again yet. A path stays here after its inherited recheck is taken,
        /// because the file can be queued for a recheck again. When the pass lists the path,
        /// the listed object replaces every pending recheck of it, so that the file is neither
        /// handed out twice nor queued twice.
        std::unordered_set<std::string> inherited_foreign_processing_rechecks TSA_GUARDED_BY(next_mutex);

        /// Ordered mode only. An ordering domain is the scope of one `processed` pointer:
        /// a bucket, and a partition within it when partitioning is used.
        using OrderingDomain = std::pair<Bucket, std::string>;

        /// Ordered mode only. Later files dropped while a smaller foreign-held file
        /// blocks their ordering domain. Once the smallest blocker of a domain resolves,
        /// put the files below the next blocker (all of them if it was the last one) back through the regular filtering path instead of waiting
        /// for the object-storage listing to start another full pass.
        /// This is only a shortcut, so the number of retained files is capped: a blocker
        /// near the beginning of a large namespace would otherwise buffer the whole rest
        /// of its domain in memory. Beyond the cap the files are simply not retained and
        /// the next listing pass lists them again, which is why the domain has to stay blocked
        /// from the smallest dropped path on for the rest of this pass, see
        /// `dropped_blocked_files_per_domain`.
        static constexpr size_t max_blocked_files_to_replay = 1000;
        std::map<OrderingDomain, ObjectInfos> blocked_files_per_domain TSA_GUARDED_BY(next_mutex);
        size_t blocked_files_count TSA_GUARDED_BY(next_mutex) = 0;
        bool blocked_files_replay_capped TSA_GUARDED_BY(next_mutex) = false;

        /// Ordered mode only. The smallest path of a domain dropped by the cap above.
        /// Dropping a blocked file is only safe while the whole range above the blocker stays
        /// unprocessed. The blocker can resolve in the middle of the listing pass (a foreign
        /// observation expires and is rechecked at a batch boundary, or the foreign processor
        /// commits), after which this iterator would hand out the later files of the domain
        /// and advance the `processed` pointer past the dropped range, which declares it
        /// processed forever. So a domain which lost a file to the cap stays blocked from that
        /// path on until a fresh listing pass - a new iterator - lists the range again.
        std::map<OrderingDomain, std::string> dropped_blocked_files_per_domain TSA_GUARDED_BY(next_mutex);

        /// Ordered mode only. Committing a file declares every smaller path of its domain
        /// processed, so while a file of the domain
        /// is held by a foreign `processing` node, later files of the domain must not be
        /// processed: the pointer would advance past the held file and lose it forever.
        std::map<OrderingDomain, std::set<std::string>> foreign_held_files_per_domain TSA_GUARDED_BY(next_mutex);

        /// Ordered mode only. The replayed files (`PendingRecheck::is_replay`) still waiting in
        /// `foreign_processing_files_to_recheck`. A replayed file is neither foreign-held nor handed
        /// out yet, so nothing else keeps a later file of its domain from being committed first and
        /// advancing the `processed` pointer past it: a later file found behind a pending replay is
        /// queued as a replay as well, and the recheck batch hands both out in path order.
        std::map<OrderingDomain, std::multiset<std::string>> pending_replays_per_domain TSA_GUARDED_BY(next_mutex);

        /// Ordered mode only. The replayed files taken from `foreign_processing_files_to_recheck`
        /// which are not handed out to a processing thread yet. With buckets, a replay can wait in
        /// `keys_cache_per_bucket` behind a later file of its domain cached before it, so it stays
        /// an ordering barrier for the hand-out gate until it is handed out, where the unresolved
        /// set-processing guard takes over.
        std::map<OrderingDomain, std::multiset<std::string>> in_flight_replays_per_domain TSA_GUARDED_BY(next_mutex);

        /// Ordered mode only. Files handed out to a processing thread whose `trySetProcessing`
        /// outcome is not known yet. A later file of the domain must not start processing
        /// while a smaller file can still turn out to be foreign-held: with several processing
        /// threads, committing the later file would advance the `processed` pointer past it.
        /// Registered under `mutex` (the hand-out order), resolved under `next_mutex`.
        std::map<OrderingDomain, std::set<std::string>> unresolved_set_processing_per_domain TSA_GUARDED_BY(next_mutex);
        std::condition_variable set_processing_resolved_cv;

        OrderingDomain getOrderingDomain(const std::string & path) const;
        void recordForeignHeldFile(const std::string & path) TSA_REQUIRES(next_mutex);
        void resolveForeignHeldFile(const std::string & path) TSA_REQUIRES(next_mutex);
        void rememberBlockedFile(ObjectInfoPtr object) TSA_REQUIRES(next_mutex);
        void recheckBlockedFilesForDomain(const OrderingDomain & domain, const std::optional<std::string> & smallest_remaining_blocker) TSA_REQUIRES(next_mutex);
        bool isBlockedByForeignHeldFile(const std::string & path) TSA_REQUIRES(next_mutex);
        /// `due_time` delays the replay, see `PendingRecheck::due_time`.
        void queueReplay(ObjectInfoPtr object, time_t due_time = 0) TSA_REQUIRES(next_mutex);
        /// Ordered mode only. The set-processing attempt of a handed-out file ended without
        /// establishing its outcome (the claim lost a local race, or threw): retry the file
        /// later, keeping it as an ordering barrier of its domain meanwhile.
        void retryUnresolvedSetProcessingLater(ObjectInfoPtr object) TSA_REQUIRES(next_mutex);
        /// The set-processing attempt of a handed-out file threw.
        void onSetProcessingException(ObjectInfoPtr object);
        void forgetPendingReplay(const std::string & path) TSA_REQUIRES(next_mutex);
        bool isBehindPendingReplay(const std::string & path) TSA_REQUIRES(next_mutex);
        void forgetInFlightReplay(const std::string & path) TSA_REQUIRES(next_mutex);
        bool isBehindInFlightReplay(const std::string & path) TSA_REQUIRES(next_mutex);

        /// Called when the file is handed out to a processing thread: it also stops being an in-flight replay.
        void registerUnresolvedSetProcessing(const std::string & path);
        void resolveSetProcessing(const std::string & path);
        void resolveSetProcessingUnlocked(const std::string & path) TSA_REQUIRES(next_mutex);
        bool hasSmallerUnresolvedSetProcessing(const std::string & path) TSA_REQUIRES(next_mutex);
        /// Waits until the file is allowed to start a set-processing attempt. A file which may not
        /// proceed (`Blocked`, `BehindReplay`) is moved into its deferred queue under the same lock
        /// which removes its unresolved set-processing barrier, so that no later file of the domain
        /// can pass the gate in between; `object_info` is empty afterwards.
        /// TSA does not understand the `std::unique_lock` needed by the condition variable.
        enum class OrderingDomainGate { Proceed, Blocked, BehindReplay, Shutdown };
        OrderingDomainGate waitOrderingDomainGate(ObjectInfoPtr & object_info) TSA_NO_THREAD_SAFETY_ANALYSIS;
        /// Ordered mode only. Waits until the set-processing outcomes of all handed-out files are known.
        void waitAllSetProcessingResolved() TSA_NO_THREAD_SAFETY_ANALYSIS;

        std::pair<ObjectInfoPtr, FileMetadataPtr> next();
        void filterProcessableFiles(ObjectInfos & objects) TSA_REQUIRES(next_mutex);
        /// The replays among the taken files are added to `in_flight_replays_per_domain` and to `taken_replays`.
        ObjectInfos takeDueForeignProcessingRechecks(std::vector<std::string> & taken_replays) TSA_REQUIRES(next_mutex);
        /// When the recheck of `pending` is due, judging by the most recent cached observation.
        time_t getRecheckDeadline(const PendingRecheck & pending, size_t ttl_sec) const;
        std::vector<PendingRecheck> takeForeignProcessingRechecksToInherit() TSA_REQUIRES(next_mutex);
        /// Queues a recheck of a file skipped because of the foreign `processing` observation in `status`.
        void queueForeignProcessingRecheck(ObjectInfoPtr object, const ObjectStorageQueueIFileMetadata::FileStatusPtr & status) TSA_REQUIRES(next_mutex);
        void recheckForeignProcessingLater(ObjectInfoPtr object_info, const FileMetadataPtr & file_metadata);

        std::atomic<bool> & shutdown_called;
        std::mutex mutex;
        LoggerPtr log;

        struct BucketInfo
        {
            std::deque<std::pair<ObjectInfoPtr, FileMetadataPtr>> keys;
            std::optional<size_t> processor;
        };
        /// A cache of keys which were iterated via glob_iterator, but not taken for processing.
        std::unordered_map<Bucket, std::unique_ptr<BucketInfo>> keys_cache_per_bucket TSA_GUARDED_BY(mutex);

        /// We store a vector of holders, because we cannot release them until processed files are committed.
        std::unordered_map<size_t, std::shared_ptr<BucketHolders>> bucket_holders TSA_GUARDED_BY(mutex);

        /// Is glob_iterator finished?
        std::atomic_bool iterator_finished = false;

        /// Set when a bucket lock refresh or release fails (e.g. lost ownership), or, in Ordered mode,
        /// when preparing a batch of files fails: next() stops returning keys, isFinished returns true.
        std::atomic_bool iterator_invalidated = false;

        /// Only for processing without buckets.
        std::deque<std::pair<ObjectInfoPtr, FileMetadataPtr>> objects_to_retry TSA_GUARDED_BY(mutex);

        struct NextKeyFromBucket
        {
            ObjectInfoPtr object_info;
            FileMetadataPtr file_metadata;
            ObjectStorageQueueOrderedFileMetadata::BucketInfoPtr bucket_info;
        };
        NextKeyFromBucket getNextKeyFromAcquiredBucket(size_t processor) TSA_REQUIRES(mutex);
        std::string bucketHoldersToString() const TSA_REQUIRES(mutex);

        BucketHolderPtr tryAcquireBucket(
            size_t bucket,
            BucketInfo & bucket_info,
            BucketHolders & acquired_buckets,
            size_t processor) const TSA_REQUIRES(mutex);
    };

    struct CommitSettings
    {
        size_t max_processed_files_before_commit;
        size_t max_processed_rows_before_commit;
        size_t max_processed_bytes_before_commit;
        size_t max_processing_time_sec_before_commit;
    };

    struct ProcessingProgress
    {
        std::atomic<size_t> processed_files = 0;
        std::atomic<size_t> processed_rows = 0;
        std::atomic<size_t> processed_bytes = 0;
        Stopwatch elapsed_time{CLOCK_MONOTONIC_COARSE};
    };
    using ProcessingProgressPtr = std::shared_ptr<ProcessingProgress>;
    using AfterProcessingSettings = ObjectStorageQueuePostProcessor::AfterProcessingSettings;

    ObjectStorageQueueSource(
        String name_,
        size_t processor_id_,
        std::shared_ptr<FileIterator> file_iterator_,
        StorageObjectStorageConfigurationPtr configuration_,
        ObjectStoragePtr object_storage_,
        ProcessingProgressPtr progress_,
        const ReadFromFormatInfo & read_from_format_info_,
        const std::optional<FormatSettings> & format_settings_,
        FormatParserSharedResourcesPtr parser_shared_resources_,
        const CommitSettings & commit_settings_,
        const AfterProcessingSettings & after_processing_settings_,
        std::shared_ptr<ObjectStorageQueueMetadata> files_metadata_,
        ContextPtr context_,
        size_t max_block_size_,
        const std::atomic<bool> & shutdown_called_,
        const std::atomic<bool> & table_is_being_dropped_,
        std::shared_ptr<ObjectStorageQueueLog> system_queue_log_,
        const StorageID & storage_id_,
        LoggerPtr log_,
        bool commit_once_processed_,
        bool is_direct_select_,
        bool add_deduplication_info_,
        bool is_deduplication_v2_,
        IStreamingStorage & streaming_storage_,
        std::atomic_bool * iterator_consumed_);

    static Block getHeader(Block sample_block, const NamesAndTypes & requested_virtual_columns);

    String getName() const override;

    Chunk generate() override;

    void onFinish() override;

    /// Commit files after insertion into storage finished.
    /// `success` defines whether insertion was successful or not.
    void prepareCommitRequests(
        Coordination::Requests & requests,
        bool insert_succeeded,
        StoredObjects & successful_files,
        PartitionLastProcessedFileInfoMap & file_map,
        LastProcessedFileInfoMapPtr created_nodes = nullptr,
        const std::string & exception_message = {},
        int error_code = 0);

    static void preparePartitionProcessedRequests(
        Coordination::Requests & requests,
        const PartitionLastProcessedFileInfoMap & last_processed_file_per_partition);

    /// Mark all processed files' metadata so that their destructors check ownership
    /// before removing the processing node (rather than asserting).
    /// Called when a commit may have succeeded in ZK but the connection was lost before
    /// we received the response ("failed after operation").
    void setUncertainCommit();

    /// Do some work after Processed/Failed files were successfully committed to keeper.
    void finalizeCommit(
        bool insert_succeeded,
        UInt64 commit_id,
        time_t commit_time,
        time_t transaction_start_time_,
        const std::string & exception_message = {},
        const UnorderedSetWithMemoryTracking<String> & post_processing_failed_paths = {});

private:
    Chunk generateImpl();
    /// Log to system.s3(azure)_queue_log.
    void appendLogElement(
        const FileMetadataPtr & file_metadata_,
        bool processed,
        UInt64 commit_id,
        time_t commit_time,
        time_t transaction_start_time_);
    /// Commit processed files.
    /// This method is only used for SELECT query, not for streaming to materialized views.
    /// Which is defined by passing a flag commit_once_processed.
    void commit(bool insert_succeeded, const std::string & exception_message = {}, int error_code = 0);

    const String name;
    const size_t processor_id;
    const std::shared_ptr<FileIterator> file_iterator;
    const StorageObjectStorageConfigurationPtr configuration;
    const ObjectStoragePtr object_storage;
    const ProcessingProgressPtr progress;
    ReadFromFormatInfo read_from_format_info;
    const std::optional<FormatSettings> format_settings;
    FormatParserSharedResourcesPtr parser_shared_resources;
    const CommitSettings commit_settings;
    const AfterProcessingSettings after_processing_settings;
    const std::shared_ptr<ObjectStorageQueueMetadata> files_metadata;
    const size_t max_block_size;
    const ObjectStorageQueueMode mode;

    const std::atomic<bool> & shutdown_called;
    const std::atomic<bool> & table_is_being_dropped;
    const std::shared_ptr<ObjectStorageQueueLog> system_queue_log;
    const StorageID storage_id;
    const bool commit_once_processed;
    const bool is_direct_select;
    IStreamingStorage & streaming_storage;
    const UInt64 cancel_epoch;
    const bool add_deduplication_info;
    /// Effective dedup: gates whether shutdown can abort mid-file.
    const bool is_deduplication_v2;
    std::atomic_bool * const iterator_consumed;
    time_t transaction_start_time;

    LoggerPtr log;

    enum class FileState
    {
        Processing,
        ErrorOnRead,
        Cancelled,
        Processed,
    };
    struct ProcessedFile
    {
        explicit ProcessedFile(FileMetadataPtr metadata_)
            : state(FileState::Processing), metadata(metadata_) {}

        FileState state;
        FileMetadataPtr metadata;
        std::string exception_during_read;
        int exception_during_read_code = 0;
        /// The object's own last-modified time, if object storage reported one.
        /// Used to update the "newest object committed" pipeline-lag watermark.
        time_t last_modified = 0;
    };
    std::vector<ProcessedFile> processed_files;
    Source::ReaderHolder reader;
};

}
