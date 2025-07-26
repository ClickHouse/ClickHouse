#pragma once

#include <cstdint>
#include <optional>
#include <expected>
#include <mutex>

#include <Common/ActionBlocker.h>
#include <Common/UniqueLock.h>
#include <Common/ZooKeeper/ZooKeeper.h>
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
#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/Compaction/MergePredicates/DistributedMergePredicate.h>
#include <Storages/MergeTree/AlterConversions.h>

namespace DB
{

class StorageReplicatedMergeTree;
class MergeTreeDataMergerMutator;

class ReplicatedMergeTreeBaseMergePredicate;
class ReplicatedMergeTreeLocalMergePredicate;
class ReplicatedMergeTreeZooKeeperMergePredicate;
class ReplicatedMergeTreeMergeStrategyPicker;

using PartitionIdsHint = std::unordered_set<String>;

class ReplicatedMergeTreeQueue
{
private:
    friend class CurrentlyExecuting;
    friend class ReplicatedMergeTreeBaseMergePredicate;
    friend class ReplicatedMergeTreeLocalMergePredicate;
    friend class ReplicatedMergeTreeZooKeeperMergePredicate;
    friend class DistributedMergePredicate<ActiveDataPartSet, ReplicatedMergeTreeQueue>;
    friend class MergeFromLogEntryTask;
    friend class ReplicatedMergeMutateTaskBase;
    friend class StorageReplicatedMergeTree;

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

    UInt64 getPostponeTimeMsForEntry(const LogEntry & entry, const MergeTreeData & data) const;
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
    Queue TSA_GUARDED_BY(state_mutex) queue;

    InsertsByTime TSA_GUARDED_BY(state_mutex) inserts_by_time;
    std::atomic<time_t> min_unprocessed_insert_time = 0;
    std::atomic<time_t> max_processed_insert_time = 0;

    time_t TSA_GUARDED_BY(state_mutex) last_queue_update = 0;

    /// parts that will appear as a result of actions performed right now by background threads (these actions are not in the queue).
    /// Used to block other actions on parts in the range covered by future_parts.
    using FuturePartsSet = std::map<String, LogEntryPtr>;
    FuturePartsSet TSA_GUARDED_BY(state_mutex) future_parts;

    bool emplaceFuturePart(const String & actual_part_name, LogEntryPtr entry)
    {
        std::lock_guard lock(state_mutex);
        return future_parts.emplace(actual_part_name, entry).second;
    }
    bool eraseFuturePart(const String & part_name)
    {
        std::lock_guard lock(state_mutex);
        return future_parts.erase(part_name);
    }

    /// Avoid parallel execution of queue enties, which may remove other entries from the queue.
    std::set<MergeTreePartInfo> currently_executing_drop_replace_ranges;

    /** What will be the set of active parts after executing all log entries up to log_pointer.
      * Used to determine which merges can be assigned (see ReplicatedMergeTreeZooKeeperMergePredicate)
      */
    ActiveDataPartSet TSA_GUARDED_BY(state_mutex) virtual_parts;

    /// Used to prevent operations to start in ranges which will be affected by DROP_RANGE/REPLACE_RANGE
    std::vector<MergeTreePartInfo> TSA_GUARDED_BY(state_mutex) drop_replace_range_intents;

    /// We do not add DROP_PARTs to virtual_parts because they can intersect,
    /// so we store them separately in this structure.
    DropPartsRanges TSA_GUARDED_BY(state_mutex) drop_parts;

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
        String latest_fail_error_code_name;
    };

    /// Mapping from znode path to Mutations Status
    std::map<String, MutationStatus> TSA_GUARDED_BY(state_mutex) mutations_by_znode;

    /// Unfinished mutations that are required for AlterConversions.
    MutationCounters TSA_GUARDED_BY(state_mutex) mutation_counters;

    /// Partition -> (block_number -> MutationStatus)
    std::unordered_map<String, std::map<Int64, MutationStatus *>> TSA_GUARDED_BY(state_mutex) mutations_by_partition;
    /// Znode ID of the latest mutation that is done.
    String TSA_GUARDED_BY(state_mutex) mutation_pointer;

    /// Provides only one simultaneous call to pullLogsToQueue.
    std::mutex pull_logs_to_queue_mutex;

    /// This sequence control ALTERs execution in replication queue.
    /// We need it because alters have to be executed sequentially (one by one).
    ReplicatedMergeTreeAltersSequence TSA_GUARDED_BY(state_mutex) alter_sequence;

    Strings TSA_GUARDED_BY(state_mutex) broken_parts_to_enqueue_fetches_on_loading;

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

    Subscribers TSA_GUARDED_BY(subscribers_mutex) subscribers;

    /// Notify subscribers about queue change (new queue size and entry that was removed)
    void notifySubscribers(size_t new_queue_size, const String * removed_log_entry_id);

    /// Check that entry_ptr is REPLACE_RANGE entry and can be removed from queue because current entry covers it
    bool checkReplaceRangeCanBeRemoved(
        const MergeTreePartInfo & part_info, LogEntryPtr entry_ptr, const ReplicatedMergeTreeLogEntryData & current) const;

    /// Ensures that only one thread is simultaneously updating mutations.
    std::mutex update_mutations_mutex;

    /// Insert new entry from log into queue
    void insertUnlocked(const LogEntryPtr & entry, std::optional<time_t> & min_unprocessed_insert_time_changed) TSA_REQUIRES(state_mutex);

    void removeProcessedEntry(zkutil::ZooKeeperPtr zookeeper, LogEntryPtr & entry);

    /** Can I now try this action. If not, you need to leave it in the queue and try another one.
      * Called under the state_mutex.
      */
    bool shouldExecuteLogEntry(
        const LogEntry & entry,
        String & out_postpone_reason,
        MergeTreeDataMergerMutator & merger_mutator,
        MergeTreeData & data,
        const CommittingBlocks & committing_blocks) const TSA_REQUIRES(state_mutex);

    /// Return the version (block number) of the last mutation that we don't need to apply to the part
    /// with getDataVersion() == data_version. (Either this mutation was already applied or the part
    /// was created after the mutation).
    /// If there is no such mutation or it has already been executed and deleted, return 0.
    Int64 getCurrentMutationVersion(const String & partition_id, Int64 data_version) const TSA_REQUIRES(state_mutex);
    Int64 getNextMutationVersion(const String & partition_id, int64_t data_version) const TSA_REQUIRES(state_mutex);

    /** Check that part isn't in currently generating parts and isn't covered by them.
      * Should be called under state_mutex.
      */
    bool isCoveredByFuturePartsImpl(
        const LogEntry & entry, const String & new_part_name, String & out_reason, std::vector<LogEntryPtr> * covered_entries_to_wait) const
        TSA_REQUIRES(state_mutex);

    /// After removing the queue element, update the insertion times in the RAM. Running under state_mutex.
    /// Returns information about what times have changed - this information can be passed to updateTimesInZooKeeper.
    void updateStateOnQueueEntryRemoval(
        const LogEntryPtr & entry,
        bool is_successful,
        std::optional<time_t> & min_unprocessed_insert_time_changed,
        std::optional<time_t> & max_processed_insert_time_changed) TSA_REQUIRES(state_mutex);

    /// Add part for mutations with block_number > part.getDataVersion()
    void addPartToMutations(const String & part_name, const MergeTreePartInfo & part_info) TSA_REQUIRES(state_mutex);

    /// Remove covered parts from mutations (parts_to_do) which were assigned
    /// for mutation. If remove_covered_parts = true, than remove parts covered
    /// by first argument. If remove_part == true, than also remove part itself.
    /// Both negative flags will throw exception.
    ///
    /// Part removed from mutations which satisfy conditions:
    /// block_number > part.getDataVersion()
    /// or block_number == part.getDataVersion()
    ///    ^ (this may happen if we downloaded mutated part from other replica)
    void removeCoveredPartsFromMutations(const String & part_name, bool remove_part, bool remove_covered_parts) TSA_REQUIRES(state_mutex);

    /// Update the insertion times in ZooKeeper.
    void updateTimesInZooKeeper(zkutil::ZooKeeperPtr zookeeper,
        std::optional<time_t> min_unprocessed_insert_time_changed,
        std::optional<time_t> max_processed_insert_time_changed) const;

    bool isIntersectingWithDropReplaceIntent(const LogEntry & entry, const String & part_name, String & out_reason) const
        TSA_REQUIRES(state_mutex);

    bool isMergeOfPatchPartsBlocked(const LogEntry & entry, String & out_reason) const TSA_REQUIRES(state_mutex);
    bool havePendingPatchPartsForMutation(const LogEntry & entry, String & out_reason, const CommittingBlocks & committing_blocks) const
        TSA_REQUIRES(state_mutex);

    /// Marks the element of the queue as running.
    class CurrentlyExecuting
    {
    private:
        ReplicatedMergeTreeQueue::LogEntryPtr entry;
        ReplicatedMergeTreeQueue & queue;

        friend class ReplicatedMergeTreeQueue;

        /// Created only in the selectEntryToProcess function. It is called under mutex.
        CurrentlyExecuting(const ReplicatedMergeTreeQueue::LogEntryPtr & entry_, ReplicatedMergeTreeQueue & queue_);

        /// In case of fetch, we determine actual part during the execution, so we need to update entry. It is called under state_mutex.
        static void setActualPartName(
            ReplicatedMergeTreeQueue::LogEntry & entry,
            const String & actual_part_name,
            ReplicatedMergeTreeQueue & queue,
            UniqueLock<std::mutex> & state_lock,
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
    void clear() TSA_NO_THREAD_SAFETY_ANALYSIS;

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
    std::shared_ptr<ReplicatedMergeTreeZooKeeperMergePredicate> getMergePredicate(zkutil::ZooKeeperPtr & zookeeper, std::optional<PartitionIdsHint> && partition_ids_hint);

    MutationCommands getMutationCommands(const MergeTreeData::DataPartPtr & part, Int64 desired_mutation_version,
                                         Strings & mutation_ids) const;

    struct MutationsSnapshot : public MergeTreeData::MutationsSnapshotBase
    {
    public:
        using Params = MergeTreeData::IMutationsSnapshot::Params;
        using MutationsByPartititon = std::unordered_map<String, std::map<Int64, ReplicatedMergeTreeMutationEntryPtr>>;
        MutationsByPartititon mutations_by_partition;

        MutationsSnapshot() = default;
        MutationsSnapshot(Params params_, MutationCounters counters_, MutationsByPartititon mutations_by_partition_, DataPartsVector patches_);

        MutationCommands getOnFlyMutationCommandsForPart(const MergeTreeData::DataPartPtr & part) const override;
        std::shared_ptr<MergeTreeData::IMutationsSnapshot> cloneEmpty() const override { return std::make_shared<MutationsSnapshot>(); }
        NameSet getAllUpdatedColumns() const override;
    };

    /// Return mutation commands for part which could be not applied to
    /// it according to part mutation version. Used when we apply alter commands on fly,
    /// without actual data modification on disk.
    MergeTreeData::MutationsSnapshotPtr getMutationsSnapshot(const MutationsSnapshot::Params & params) const;
    MutationCounters getMutationCounters() const;

    /// Mark finished mutations as done. If the function needs to be called again at some later time
    /// (because some mutations are probably done but we are not sure yet), returns true.
    bool tryFinalizeMutations(zkutil::ZooKeeperPtr zookeeper);

    /// Checks that part is already in virtual parts
    bool isVirtualPart(const MergeTreeData::DataPartPtr & data_part) const;

    /// Returns true if part_info is covered by some DROP_RANGE or DROP_PART
    bool isGoingToBeDropped(const MergeTreePartInfo & part_info, MergeTreePartInfo * out_drop_range_info = nullptr) const;
    bool isGoingToBeDroppedImpl(const MergeTreePartInfo & part_info, MergeTreePartInfo * out_drop_range_info) const
        TSA_REQUIRES(state_mutex);

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
    SubscriberHandler addSubscriber(
        SubscriberCallBack && callback,
        std::unordered_set<String> & out_entry_names,
        SyncReplicaMode sync_mode,
        std::unordered_set<String> && src_replicas);

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

/** Convert a number to a string in the format of the suffixes of auto-incremental nodes in ZooKeeper.
  * Negative numbers are also supported - for them the name of the node looks somewhat silly
  *  and does not match any auto-incremented node in ZK.
  */
String padIndex(Int64 index);

}
