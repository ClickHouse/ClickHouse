#pragma once

#include <cstdint>
#include <optional>

#include <Common/ActionBlocker.h>
#include <Parsers/SyncReplicaMode.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeMutationEntry.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/MergeTree/PinnedPartUUIDs.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumAddedParts.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAltersSequence.h>
#include <Storages/MergeTree/DropPartsRanges.h>

#include <Common/ZooKeeper/ZooKeeper.h>


namespace DB
{

class StorageReplicatedMergeTree;
class MergeTreeDataMergerMutator;

class ReplicatedMergeTreeMergePredicate;
class ReplicatedMergeTreeMergeStrategyPicker;

using PartitionIdsHint = std::unordered_set<String>;

class ReplicatedMergeTreeQueue
{
private:
    friend class CurrentlyExecuting;
    friend class LocalMergePredicate;
    friend class ReplicatedMergeTreeMergePredicate;
    template<typename T, typename U> friend class BaseMergePredicate;
    friend class MergeFromLogEntryTask;
    friend class ReplicatedMergeMutateTaskBase;

    using LogEntry = ReplicatedMergeTreeLogEntry;
    using LogEntryPtr = LogEntry::Ptr;

    using Queue = std::list<LogEntryPtr>;

    using StringSet = std::set<String>;

    struct ByTime
    {
        bool operator()(const LogEntryPtr & lhs, const LogEntryPtr & rhs) const
        {
            return std::forward_as_tuple(lhs->create_time, lhs.get())
                 < std::forward_as_tuple(rhs->create_time, rhs.get());
        }
    };

    struct OperationsInQueue
    {
        size_t merges = 0;
        size_t mutations = 0;
        size_t merges_with_ttl = 0;
    };

    /// To calculate min_unprocessed_insert_time, max_processed_insert_time, for which the replica lag is calculated.
    using InsertsByTime = std::set<LogEntryPtr, ByTime>;

    StorageReplicatedMergeTree & storage;
    ReplicatedMergeTreeMergeStrategyPicker & merge_strategy_picker;
    MergeTreeDataFormatVersion format_version;

    String zookeeper_path;
    String replica_path;
    String logger_name;
    LoggerPtr log = nullptr;

    /// Protects the queue, future_parts and other queue state variables.
    mutable std::mutex state_mutex;

    /// A set of parts that should be on this replica according to the queue entries that have been done
    /// up to this point. The invariant holds: `virtual_parts` = `current_parts` + `queue`.
    /// Note: it can be different from the actual set of parts because the replica can decide to fetch
    /// a bigger part instead of the part mentioned in the log entry.
    ActiveDataPartSet current_parts;

    /** The queue of what you need to do on this line to catch up. It is taken from ZooKeeper (/replicas/me/queue/).
      * In ZK records in chronological order. Here they are executed in parallel and reorder after entry execution.
      * Order of execution is not "queue" at all. Look at selectEntryToProcess.
      */
    Queue queue;

    InsertsByTime inserts_by_time;
    std::atomic<time_t> min_unprocessed_insert_time = 0;
    std::atomic<time_t> max_processed_insert_time = 0;

    time_t last_queue_update = 0;

    /// parts that will appear as a result of actions performed right now by background threads (these actions are not in the queue).
    /// Used to block other actions on parts in the range covered by future_parts.
    using FuturePartsSet = std::map<String, LogEntryPtr>;
    FuturePartsSet future_parts;

    /// Avoid parallel execution of queue enties, which may remove other entries from the queue.
    std::set<MergeTreePartInfo> currently_executing_drop_replace_ranges;

    /** What will be the set of active parts after executing all log entries up to log_pointer.
      * Used to determine which merges can be assigned (see ReplicatedMergeTreeMergePredicate)
      */
    ActiveDataPartSet virtual_parts;

    /// Used to prevent operations to start in ranges which will be affected by DROP_RANGE/REPLACE_RANGE
    std::vector<MergeTreePartInfo> drop_replace_range_intents;

    /// We do not add DROP_PARTs to virtual_parts because they can intersect,
    /// so we store them separately in this structure.
    DropPartsRanges drop_parts;

    /// A set of mutations loaded from ZooKeeper.
    /// mutations_by_partition is an index partition ID -> block ID -> mutation into this set.
    /// Note that mutations are updated in such a way that they are always more recent than
    /// log_pointer (see pullLogsToQueue()).

    struct MutationStatus
    {
        MutationStatus(const ReplicatedMergeTreeMutationEntryPtr & entry_, MergeTreeDataFormatVersion format_version_)
            : entry(entry_)
            , parts_to_do(format_version_)
        {
        }

        ReplicatedMergeTreeMutationEntryPtr entry;

        /// Current parts we have to mutate to complete mutation.
        ///
        /// current_part_name =mutation> result_part_name
        /// ^~~parts_to_do~~^            ^~virtual_parts~^
        ///
        /// We use ActiveDataPartSet structure to be able to manage covering and
        /// covered parts.
        ActiveDataPartSet parts_to_do;

        /// Note that is_done is not equivalent to parts_to_do.size() == 0
        /// (even if parts_to_do.size() == 0 some relevant parts can still commit in the future).
        /// Also we can jump over mutation when we download mutated part from other replica.
        bool is_done = false;

        String latest_failed_part;
        MergeTreePartInfo latest_failed_part_info;
        time_t latest_fail_time = 0;
        String latest_fail_reason;
    };

    /// Mapping from znode path to Mutations Status
    std::map<String, MutationStatus> mutations_by_znode;

    /// Unfinished mutations that are required for AlterConversions.
    Int64 num_data_mutations_to_apply = 0;
    Int64 num_metadata_mutations_to_apply = 0;

    /// Partition -> (block_number -> MutationStatus)
    std::unordered_map<String, std::map<Int64, MutationStatus *>> mutations_by_partition;
    /// Znode ID of the latest mutation that is done.
    String mutation_pointer;

    /// Provides only one simultaneous call to pullLogsToQueue.
    std::mutex pull_logs_to_queue_mutex;

    /// This sequence control ALTERs execution in replication queue.
    /// We need it because alters have to be executed sequentially (one by one).
    ReplicatedMergeTreeAltersSequence alter_sequence;

    Strings broken_parts_to_enqueue_fetches_on_loading;

    /// List of subscribers
    /// A subscriber callback is called when an entry queue is deleted
    mutable std::mutex subscribers_mutex;

    using SubscriberCallBack = std::function<void(size_t /* queue_size */, const String * /* removed_log_entry_id */)>;
    using Subscribers = std::list<SubscriberCallBack>;
    using SubscriberIterator = Subscribers::iterator;

    friend class SubscriberHandler;
    struct SubscriberHandler : public boost::noncopyable
    {
        SubscriberHandler(SubscriberIterator it_, ReplicatedMergeTreeQueue & queue_) : it(it_), queue(queue_) {}
        ~SubscriberHandler();

    private:
        SubscriberIterator it;
        ReplicatedMergeTreeQueue & queue;
    };

    Subscribers subscribers;

    /// Notify subscribers about queue change (new queue size and entry that was removed)
    void notifySubscribers(size_t new_queue_size, const String * removed_log_entry_id);

    /// Check that entry_ptr is REPLACE_RANGE entry and can be removed from queue because current entry covers it
    bool checkReplaceRangeCanBeRemoved(
        const MergeTreePartInfo & part_info, LogEntryPtr entry_ptr, const ReplicatedMergeTreeLogEntryData & current) const;

    /// Ensures that only one thread is simultaneously updating mutations.
    std::mutex update_mutations_mutex;

    /// Insert new entry from log into queue
    void insertUnlocked(
        const LogEntryPtr & entry, std::optional<time_t> & min_unprocessed_insert_time_changed,
        std::lock_guard<std::mutex> & state_lock);

    void removeProcessedEntry(zkutil::ZooKeeperPtr zookeeper, LogEntryPtr & entry);

    /** Can I now try this action. If not, you need to leave it in the queue and try another one.
      * Called under the state_mutex.
      */
    bool shouldExecuteLogEntry(
        const LogEntry & entry, String & out_postpone_reason,
        MergeTreeDataMergerMutator & merger_mutator, MergeTreeData & data,
        std::unique_lock<std::mutex> & state_lock) const;

    /// Return the version (block number) of the last mutation that we don't need to apply to the part
    /// with getDataVersion() == data_version. (Either this mutation was already applied or the part
    /// was created after the mutation).
    /// If there is no such mutation or it has already been executed and deleted, return 0.
    Int64 getCurrentMutationVersion(const String & partition_id, Int64 data_version) const;

    /** Check that part isn't in currently generating parts and isn't covered by them.
      * Should be called under state_mutex.
      */
    bool isCoveredByFuturePartsImpl(
        const LogEntry & entry,
        const String & new_part_name, String & out_reason,
        std::unique_lock<std::mutex> & state_lock,
        std::vector<LogEntryPtr> * covered_entries_to_wait) const;

    /// After removing the queue element, update the insertion times in the RAM. Running under state_mutex.
    /// Returns information about what times have changed - this information can be passed to updateTimesInZooKeeper.
    void updateStateOnQueueEntryRemoval(const LogEntryPtr & entry,
        bool is_successful,
        std::optional<time_t> & min_unprocessed_insert_time_changed,
        std::optional<time_t> & max_processed_insert_time_changed,
        std::unique_lock<std::mutex> & state_lock);

    /// Add part for mutations with block_number > part.getDataVersion()
    void addPartToMutations(const String & part_name, const MergeTreePartInfo & part_info);

    /// Remove covered parts from mutations (parts_to_do) which were assigned
    /// for mutation. If remove_covered_parts = true, than remove parts covered
    /// by first argument. If remove_part == true, than also remove part itself.
    /// Both negative flags will throw exception.
    ///
    /// Part removed from mutations which satisfy conditions:
    /// block_number > part.getDataVersion()
    /// or block_number == part.getDataVersion()
    ///    ^ (this may happen if we downloaded mutated part from other replica)
    void removeCoveredPartsFromMutations(const String & part_name, bool remove_part, bool remove_covered_parts);

    /// Update the insertion times in ZooKeeper.
    void updateTimesInZooKeeper(zkutil::ZooKeeperPtr zookeeper,
        std::optional<time_t> min_unprocessed_insert_time_changed,
        std::optional<time_t> max_processed_insert_time_changed) const;

    bool isIntersectingWithDropReplaceIntent(
        const LogEntry & entry,
        const String & part_name, String & out_reason, std::unique_lock<std::mutex> & /*state_mutex lock*/) const;

    /// Marks the element of the queue as running.
    class CurrentlyExecuting
    {
    private:
        ReplicatedMergeTreeQueue::LogEntryPtr entry;
        ReplicatedMergeTreeQueue & queue;

        friend class ReplicatedMergeTreeQueue;

        /// Created only in the selectEntryToProcess function. It is called under mutex.
        CurrentlyExecuting(
            const ReplicatedMergeTreeQueue::LogEntryPtr & entry_,
            ReplicatedMergeTreeQueue & queue_,
            std::unique_lock<std::mutex> & state_lock);

        /// In case of fetch, we determine actual part during the execution, so we need to update entry. It is called under state_mutex.
        static void setActualPartName(
            ReplicatedMergeTreeQueue::LogEntry & entry,
            const String & actual_part_name,
            ReplicatedMergeTreeQueue & queue,
            std::unique_lock<std::mutex> & state_lock,
            std::vector<LogEntryPtr> & covered_entries_to_wait);

    public:
        ~CurrentlyExecuting();
    };

    using CurrentlyExecutingPtr = std::unique_ptr<CurrentlyExecuting>;
    /// ZK contains a limit on the number or total size of operations in a multi-request.
    /// If the limit is exceeded, the connection is simply closed.
    /// The constant is selected with a margin. The default limit in ZK is 1 MB of data in total.
    /// The average size of the node value in this case is less than 10 kilobytes.
    static constexpr size_t MAX_MULTI_OPS = 100;

    /// Very large queue entries may appear occasionally.
    /// We cannot process MAX_MULTI_OPS at once because it will fail.
    /// But we have to process more than one entry at once because otherwise lagged replicas keep up slowly.
    /// Let's start with one entry per transaction and increase it exponentially towards MAX_MULTI_OPS.
    /// It will allow to make some progress before failing and remain operational even in extreme cases.
    size_t current_multi_batch_size = 1;

public:
    ReplicatedMergeTreeQueue(StorageReplicatedMergeTree & storage_, ReplicatedMergeTreeMergeStrategyPicker & merge_strategy_picker_);
    ~ReplicatedMergeTreeQueue() = default;

    /// Clears queue state
    void clear();

    /// Get set of parts from zookeeper
    void initialize(zkutil::ZooKeeperPtr zookeeper);

    /** Inserts an action to the end of the queue.
      * To restore broken parts during operation.
      * Do not insert the action itself into ZK (do it yourself).
      */
    void insert(zkutil::ZooKeeperPtr zookeeper, LogEntryPtr & entry);

    /** Load (initialize) a queue from ZooKeeper (/replicas/me/queue/).
      * If queue was not empty load() would not load duplicate records.
      * return true, if we update queue.
      */
    bool load(zkutil::ZooKeeperPtr zookeeper);

    bool removeFailedQuorumPart(const MergeTreePartInfo & part_info);

    enum PullLogsReason
    {
        LOAD,
        UPDATE,
        MERGE_PREDICATE,
        SYNC,
        FIX_METADATA_VERSION,
        OTHER,
    };

    /** Copy the new entries from the shared log to the queue of this replica. Set the log_pointer to the appropriate value.
      * If watch_callback is not empty, will call it when new entries appear in the log.
      * If there were new entries, notifies storage.queue_task_handle.
      * Additionally loads mutations (so that the set of mutations is always more recent than the queue).
      * Return the version of "logs" node (that is updated for every merge/mutation/... added to the log)
      */
    std::pair<int32_t, int32_t> pullLogsToQueue(zkutil::ZooKeeperPtr zookeeper, Coordination::WatchCallback watch_callback = {}, PullLogsReason reason = OTHER);

    /// Load new mutation entries. If something new is loaded, schedule storage.merge_selecting_task.
    /// If watch_callback is not empty, will call it when new mutations appear in ZK.
    int32_t updateMutations(zkutil::ZooKeeperPtr zookeeper, Coordination::WatchCallbackPtr watch_callback = {});

    /// Remove a mutation from ZooKeeper and from the local set. Returns the removed entry or nullptr
    /// if it could not be found. Called during KILL MUTATION query execution.
    ReplicatedMergeTreeMutationEntryPtr removeMutation(zkutil::ZooKeeperPtr zookeeper, const String & mutation_id);

    /** Remove the action from the queue with the parts covered by part_name (from ZK and from the RAM).
      * And also wait for the completion of their execution, if they are now being executed.
      * covering_entry is as an entry that caused removal of entries in range (usually, DROP_RANGE)
      */
    void removePartProducingOpsInRange(zkutil::ZooKeeperPtr zookeeper,
                                       const MergeTreePartInfo & part_info,
                                       const std::optional<ReplicatedMergeTreeLogEntryData> & covering_entry);

    /// Wait for the execution of currently executing actions with virtual parts intersecting with part_info
    void waitForCurrentlyExecutingOpsInRange(const MergeTreePartInfo & part_info) const;

    /** In the case where there are not enough parts to perform the merge in part_name
      * - move actions with merged parts to the end of the queue
      * (in order to download a already merged part from another replica).
      */
    StringSet moveSiblingPartsForMergeToEndOfQueue(const String & part_name);

    /** Select the next action to process.
      * merger_mutator is used only to check if the merges are not suspended.
      */
    struct SelectedEntry
    {
        ReplicatedMergeTreeQueue::LogEntryPtr log_entry;
        CurrentlyExecutingPtr currently_executing_holder;

        SelectedEntry(const ReplicatedMergeTreeQueue::LogEntryPtr & log_entry_, CurrentlyExecutingPtr && currently_executing_holder_)
            : log_entry(log_entry_)
            , currently_executing_holder(std::move(currently_executing_holder_))
        {}
    };

    using SelectedEntryPtr = std::shared_ptr<SelectedEntry>;
    SelectedEntryPtr selectEntryToProcess(MergeTreeDataMergerMutator & merger_mutator, MergeTreeData & data);

    /** Execute `func` function to handle the action.
      * In this case, at runtime, mark the queue element as running
      *  (add into future_parts and more).
      * If there was an exception during processing, it saves it in `entry`.
      * Returns true if there were no exceptions during the processing.
      */
    bool processEntry(std::function<zkutil::ZooKeeperPtr()> get_zookeeper, LogEntryPtr & entry, std::function<bool(LogEntryPtr &)> func);

    /// Count the number of merges and mutations of single parts in the queue.
    OperationsInQueue countMergesAndPartMutations() const;

    /// Count the total number of active mutations.
    size_t countMutations() const;

    /// Count the total number of active mutations that are finished (is_done = true).
    size_t countFinishedMutations() const;

    std::map<std::string, MutationCommands> getUnfinishedMutations() const;

    /// Returns functor which used by MergeTreeMergerMutator to select parts for merge
    ReplicatedMergeTreeMergePredicate
    getMergePredicate(zkutil::ZooKeeperPtr & zookeeper, std::optional<PartitionIdsHint> && partition_ids_hint);

    MutationCommands getMutationCommands(const MergeTreeData::DataPartPtr & part, Int64 desired_mutation_version,
                                         Strings & mutation_ids) const;

    struct MutationsSnapshot : public MergeTreeData::IMutationsSnapshot
    {
    public:
        MutationsSnapshot() = default;
        MutationsSnapshot(Params params_, Info info_) : IMutationsSnapshot(std::move(params_), std::move(info_)) {}

        using Params = MergeTreeData::IMutationsSnapshot::Params;
        using MutationsByPartititon = std::unordered_map<String, std::map<Int64, ReplicatedMergeTreeMutationEntryPtr>>;

        MutationsByPartititon mutations_by_partition;

        MutationCommands getAlterMutationCommandsForPart(const MergeTreeData::DataPartPtr & part) const override;
        std::shared_ptr<MergeTreeData::IMutationsSnapshot> cloneEmpty() const override { return std::make_shared<MutationsSnapshot>(); }
        NameSet getAllUpdatedColumns() const override;
    };

    /// Return mutation commands for part which could be not applied to
    /// it according to part mutation version. Used when we apply alter commands on fly,
    /// without actual data modification on disk.
    MergeTreeData::MutationsSnapshotPtr getMutationsSnapshot(const MutationsSnapshot::Params & params) const;

    /// Mark finished mutations as done. If the function needs to be called again at some later time
    /// (because some mutations are probably done but we are not sure yet), returns true.
    bool tryFinalizeMutations(zkutil::ZooKeeperPtr zookeeper);

    /// Checks that part is already in virtual parts
    bool isVirtualPart(const MergeTreeData::DataPartPtr & data_part) const;

    /// Returns true if part_info is covered by some DROP_RANGE or DROP_PART
    bool isGoingToBeDropped(const MergeTreePartInfo & part_info, MergeTreePartInfo * out_drop_range_info = nullptr) const;
    bool isGoingToBeDroppedImpl(const MergeTreePartInfo & part_info, MergeTreePartInfo * out_drop_range_info) const;

    /// Check that part produced by some entry in queue and get source parts for it.
    /// If there are several entries return largest source_parts set. This rarely possible
    /// for example after replica clone.
    bool checkPartInQueueAndGetSourceParts(const String & part_name, Strings & source_parts) const;

    /// Check that part isn't in currently generating parts and isn't covered by them and add it to future_parts.
    /// Locks queue's mutex.
    bool addFuturePartIfNotCoveredByThem(const String & part_name, LogEntry & entry, String & reject_reason);

    /// A blocker that stops selects from the queue
    ActionBlocker actions_blocker;

    /// A blocker that stops pulling entries from replication log to queue
    ActionBlocker pull_log_blocker;

    /// Adds a subscriber
    SubscriberHandler addSubscriber(SubscriberCallBack && callback, std::unordered_set<String> & out_entry_names, SyncReplicaMode sync_mode, std::unordered_set<String> src_replicas);

    void notifySubscribersOnPartialShutdown();

    struct Status
    {
        /// TODO: consider using UInt64 here
        UInt32 future_parts;
        UInt32 queue_size;
        UInt32 inserts_in_queue;
        UInt32 merges_in_queue;
        UInt32 part_mutations_in_queue;
        UInt32 metadata_alters_in_queue;
        UInt32 queue_oldest_time;
        UInt32 inserts_oldest_time;
        UInt32 merges_oldest_time;
        UInt32 part_mutations_oldest_time;
        String oldest_part_to_get;
        String oldest_part_to_merge_to;
        String oldest_part_to_mutate_to;
        UInt32 last_queue_update;
    };

    /// Get information about the queue.
    Status getStatus() const;

    /// Get the data of the queue elements.
    using LogEntriesData = std::vector<ReplicatedMergeTreeLogEntryData>;
    void getEntries(LogEntriesData & res) const;

    /// Get information about the insertion times.
    void getInsertTimes(time_t & out_min_unprocessed_insert_time, time_t & out_max_processed_insert_time) const;


    /// Return empty optional if mutation was killed. Otherwise return partially
    /// filled mutation status with information about error (latest_fail*) and
    /// is_done. mutation_ids filled with all mutations with same errors,
    /// because they may be executed simultaneously as one mutation. Order is
    /// important for better readability of exception message. If mutation was
    /// killed doesn't return any ids.
    std::optional<MergeTreeMutationStatus> getIncompleteMutationsStatus(const String & znode_name, std::set<String> * mutation_ids = nullptr) const;

    std::vector<MergeTreeMutationStatus> getMutationsStatus() const;

    void removeCurrentPartsFromMutations();

    using QueueLocks = std::scoped_lock<std::mutex, std::mutex, std::mutex>;

    /// This method locks all important queue mutexes: state_mutex,
    /// pull_logs_to_queue and update_mutations_mutex. It should be used only
    /// once while we want to shutdown our queue and remove it's task from pool.
    /// It's needed because queue itself can trigger it's task handler and in
    /// this case race condition is possible.
    QueueLocks lockQueue();

    /// Can be called only on data parts loading.
    /// We need loaded queue to create GET_PART entry for broken (or missing) part,
    /// but queue is not loaded yet on data parts loading.
    void setBrokenPartsToEnqueueFetchesOnLoading(Strings && parts_to_fetch);
    /// Must be called right after queue loading.
    void createLogEntriesToFetchBrokenParts();

    /// Add an intent to block operations to start in the range. All intents must be removed by calling
    /// removeDropReplaceIntent(). The same intent can be added multiple times, but it has to be removed exactly
    /// the same amount of times.
    void addDropReplaceIntent(const MergeTreePartInfo& intent);
    void removeDropReplaceIntent(const MergeTreePartInfo& intent);
};

using CommittingBlocks = std::unordered_map<String, std::set<Int64>>;

template<typename VirtualPartsT, typename MutationsStateT>
class BaseMergePredicate
{
public:
    BaseMergePredicate() = default;
    explicit BaseMergePredicate(std::optional<PartitionIdsHint> && partition_ids_hint_) : partition_ids_hint(std::move(partition_ids_hint_)) {}

    /// Depending on the existence of left part checks a merge predicate for two parts or for single part.
    bool operator()(const MergeTreeData::DataPartPtr & left,
                    const MergeTreeData::DataPartPtr & right,
                    const MergeTreeTransaction * txn,
                    PreformattedMessage & out_reason) const;

    /// Can we assign a merge with these two parts?
    /// (assuming that no merge was assigned after the predicate was constructed)
    /// If we can't and out_reason is not nullptr, set it to the reason why we can't merge.
    bool canMergeTwoParts(const MergeTreeData::DataPartPtr & left,
                          const MergeTreeData::DataPartPtr & right,
                          PreformattedMessage & out_reason) const;

    /// Can we assign a merge this part and some other part?
    /// For example a merge of a part and itself is needed for TTL.
    /// This predicate is checked for the first part of each range.
    bool canMergeSinglePart(const MergeTreeData::DataPartPtr & part, PreformattedMessage & out_reason) const;

    CommittingBlocks getCommittingBlocks(zkutil::ZooKeeperPtr & zookeeper, const std::string & zookeeper_path, LoggerPtr log_);

protected:
    /// A list of partitions that can be used in the merge predicate
    std::optional<PartitionIdsHint> partition_ids_hint;

    /// A snapshot of active parts that would appear if the replica executes all log entries in its queue.
    const VirtualPartsT * prev_virtual_parts_ = nullptr;
    const VirtualPartsT * virtual_parts_ = nullptr;

    /// partition ID -> block numbers of the inserts and mutations that are about to commit
    /// (loaded at some later time than prev_virtual_parts).
    const CommittingBlocks * committing_blocks_ = nullptr;

    /// List of UUIDs for parts that have their identity "pinned".
    const PinnedPartUUIDs * pinned_part_uuids_ = nullptr;

    /// Quorum state taken at some later time than prev_virtual_parts.
    const String * inprogress_quorum_part_ = nullptr;

    /// An object that provides current mutation version for a part
    const MutationsStateT * mutations_state_ = nullptr;

    std::mutex * virtual_parts_mutex = nullptr;
};

/// Lightweight version of ReplicatedMergeTreeMergePredicate that do not make any ZooKeeper requests,
/// but may return false-positive results. Checks only a subset of required conditions.
class LocalMergePredicate : public BaseMergePredicate<ActiveDataPartSet, ReplicatedMergeTreeQueue>
{
public:
    explicit LocalMergePredicate(ReplicatedMergeTreeQueue & queue_);
};

class ReplicatedMergeTreeMergePredicate : public BaseMergePredicate<ActiveDataPartSet, ReplicatedMergeTreeQueue>
{
public:
    ReplicatedMergeTreeMergePredicate(ReplicatedMergeTreeQueue & queue_, zkutil::ZooKeeperPtr & zookeeper,
                                      std::optional<PartitionIdsHint> && partition_ids_hint_);

    /// Returns true if part is needed for some REPLACE_RANGE entry.
    /// We should not drop part in this case, because replication queue may stuck without that part.
    bool partParticipatesInReplaceRange(const MergeTreeData::DataPartPtr & part, PreformattedMessage & out_reason) const;

    /// Return nonempty optional of desired mutation version and alter version.
    /// If we have no alter (modify/drop) mutations in mutations queue, than we return biggest possible
    /// mutation version (and -1 as alter version). In other case, we return biggest mutation version with
    /// smallest alter version. This required, because we have to execute alter mutations sequentially and
    /// don't glue them together. Alter is rare operation, so it shouldn't affect performance.
    std::optional<std::pair<Int64, int>> getDesiredMutationVersion(const MergeTreeData::DataPartPtr & part) const;

    bool isMutationFinished(const std::string & znode_name, const std::map<String, int64_t> & block_numbers,
                            std::unordered_set<String> & checked_partitions_cache) const;

    /// The version of "log" node that is used to check that no new merges have appeared.
    int32_t getVersion() const { return merges_version; }

    /// Returns true if there's a drop range covering new_drop_range_info
    bool isGoingToBeDropped(const MergeTreePartInfo & new_drop_range_info, MergeTreePartInfo * out_drop_range_info = nullptr) const;

    /// Returns virtual part covering part_name (if any) or empty string
    String getCoveringVirtualPart(const String & part_name) const;

private:
    const ReplicatedMergeTreeQueue & queue;

    /// We copy a merge predicate when we cast it to AllowedMergingPredicate, let's keep the pointers valid
    std::shared_ptr<ActiveDataPartSet> prev_virtual_parts;
    std::shared_ptr<CommittingBlocks> committing_blocks;
    std::shared_ptr<PinnedPartUUIDs> pinned_part_uuids;
    std::shared_ptr<String> inprogress_quorum_part;

    int32_t merges_version = -1;
};

/** Convert a number to a string in the format of the suffixes of auto-incremental nodes in ZooKeeper.
  * Negative numbers are also supported - for them the name of the node looks somewhat silly
  *  and does not match any auto-incremented node in ZK.
  */
String padIndex(Int64 index);

}
