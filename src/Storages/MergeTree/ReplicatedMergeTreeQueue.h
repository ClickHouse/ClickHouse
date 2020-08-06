#pragma once

#include <optional>

#include <Common/ActionBlocker.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeMutationEntry.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumAddedParts.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAltersSequence.h>

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/BackgroundSchedulePool.h>


namespace DB
{

class StorageReplicatedMergeTree;
class MergeTreeDataMergerMutator;

class ReplicatedMergeTreeMergePredicate;


class ReplicatedMergeTreeQueue
{
private:
    friend class CurrentlyExecuting;
    friend class ReplicatedMergeTreeMergePredicate;

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

    /// To calculate min_unprocessed_insert_time, max_processed_insert_time, for which the replica lag is calculated.
    using InsertsByTime = std::set<LogEntryPtr, ByTime>;


    StorageReplicatedMergeTree & storage;
    MergeTreeDataFormatVersion format_version;

    String zookeeper_path;
    String replica_path;
    String logger_name;
    Poco::Logger * log = nullptr;

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
    time_t min_unprocessed_insert_time = 0;
    time_t max_processed_insert_time = 0;

    time_t last_queue_update = 0;

    /// parts that will appear as a result of actions performed right now by background threads (these actions are not in the queue).
    /// Used to block other actions on parts in the range covered by future_parts.
    using FuturePartsSet = std::map<String, LogEntryPtr>;
    FuturePartsSet future_parts;

    /// Index of the first log entry that we didn't see yet.
    Int64 log_pointer = 0;

    /** What will be the set of active parts after executing all log entries up to log_pointer.
      * Used to determine which merges can be assigned (see ReplicatedMergeTreeMergePredicate)
      */
    ActiveDataPartSet virtual_parts;

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
        /// Also we can jump over mutation when we dowload mutated part from other replica.
        bool is_done = false;

        String latest_failed_part;
        MergeTreePartInfo latest_failed_part_info;
        time_t latest_fail_time = 0;
        String latest_fail_reason;
    };

    /// Mapping from znode path to Mutations Status
    std::map<String, MutationStatus> mutations_by_znode;
    /// Partition -> (block_number -> MutationStatus)
    std::unordered_map<String, std::map<Int64, MutationStatus *>> mutations_by_partition;
    /// Znode ID of the latest mutation that is done.
    String mutation_pointer;

    /// Provides only one simultaneous call to pullLogsToQueue.
    std::mutex pull_logs_to_queue_mutex;

    /// This sequence control ALTERs execution in replication queue.
    /// We need it because alters have to be executed sequentially (one by one).
    ReplicatedMergeTreeAltersSequence alter_sequence;

    /// List of subscribers
    /// A subscriber callback is called when an entry queue is deleted
    mutable std::mutex subscribers_mutex;

    using SubscriberCallBack = std::function<void(size_t /* queue_size */)>;
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

    /// Notify subscribers about queue change
    void notifySubscribers(size_t new_queue_size);

    /// Check that entry_ptr is REPLACE_RANGE entry and can be removed from queue because current entry covers it
    bool checkReplaceRangeCanBeRemoved(
        const MergeTreePartInfo & part_info, const LogEntryPtr entry_ptr, const ReplicatedMergeTreeLogEntryData & current) const;

    /// Ensures that only one thread is simultaneously updating mutations.
    std::mutex update_mutations_mutex;

    /// Put a set of (already existing) parts in virtual_parts.
    void addVirtualParts(const MergeTreeData::DataParts & parts);

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
        std::lock_guard<std::mutex> & state_lock) const;

    Int64 getCurrentMutationVersionImpl(const String & partition_id, Int64 data_version, std::lock_guard<std::mutex> & /* state_lock */) const;

    /** Check that part isn't in currently generating parts and isn't covered by them.
      * Should be called under state_mutex.
      */
    bool isNotCoveredByFuturePartsImpl(
        const String & new_part_name, String & out_reason,
        std::lock_guard<std::mutex> & state_lock) const;

    /// After removing the queue element, update the insertion times in the RAM. Running under state_mutex.
    /// Returns information about what times have changed - this information can be passed to updateTimesInZooKeeper.
    void updateStateOnQueueEntryRemoval(const LogEntryPtr & entry,
        bool is_successful,
        std::optional<time_t> & min_unprocessed_insert_time_changed,
        std::optional<time_t> & max_processed_insert_time_changed,
        std::unique_lock<std::mutex> & state_lock);

    /// Add part for mutations with block_number > part.getDataVersion()
    void addPartToMutations(const String & part_name);

    /// Remove covered parts from mutations (parts_to_do) which were assigned
    /// for mutation. If remove_covered_parts = true, than remove parts covered
    /// by first argument. If remove_part == true, than also remove part itself.
    /// Both negative flags will throw exception.
    ///
    /// Part removed from mutations which satisfy contitions:
    /// block_number > part.getDataVersion()
    /// or block_number == part.getDataVersion()
    ///    ^ (this may happen if we downloaded mutated part from other replica)
    void removeCoveredPartsFromMutations(const String & part_name, bool remove_part, bool remove_covered_parts);

    /// Update the insertion times in ZooKeeper.
    void updateTimesInZooKeeper(zkutil::ZooKeeperPtr zookeeper,
        std::optional<time_t> min_unprocessed_insert_time_changed,
        std::optional<time_t> max_processed_insert_time_changed) const;

    /// Returns list of currently executing parts blocking execution a command modifying specified range
    size_t getConflictsCountForRange(
        const MergeTreePartInfo & range, const LogEntry & entry, String * out_description,
        std::lock_guard<std::mutex> & state_lock) const;

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
        static void setActualPartName(ReplicatedMergeTreeQueue::LogEntry & entry, const String & actual_part_name,
            ReplicatedMergeTreeQueue & queue);
    public:
        ~CurrentlyExecuting();
    };

public:
    ReplicatedMergeTreeQueue(StorageReplicatedMergeTree & storage_);
    ~ReplicatedMergeTreeQueue();


    void initialize(const MergeTreeData::DataParts & parts);

    /** Inserts an action to the end of the queue.
      * To restore broken parts during operation.
      * Do not insert the action itself into ZK (do it yourself).
      */
    void insert(zkutil::ZooKeeperPtr zookeeper, LogEntryPtr & entry);

    /** Delete the action with the specified part (as new_part_name) from the queue.
      * Called for unreachable actions in the queue - old lost parts.
      */
    bool remove(zkutil::ZooKeeperPtr zookeeper, const String & part_name);

    /** Load (initialize) a queue from ZooKeeper (/replicas/me/queue/).
      * If queue was not empty load() would not load duplicate records.
      * return true, if we update queue.
      */
    bool load(zkutil::ZooKeeperPtr zookeeper);

    bool removeFromVirtualParts(const MergeTreePartInfo & part_info);

    /** Copy the new entries from the shared log to the queue of this replica. Set the log_pointer to the appropriate value.
      * If watch_callback is not empty, will call it when new entries appear in the log.
      * If there were new entries, notifies storage.queue_task_handle.
      * Additionally loads mutations (so that the set of mutations is always more recent than the queue).
      * Return the version of "logs" node (that is updated for every merge/mutation/... added to the log)
      */
    int32_t pullLogsToQueue(zkutil::ZooKeeperPtr zookeeper, Coordination::WatchCallback watch_callback = {});

    /// Load new mutation entries. If something new is loaded, schedule storage.merge_selecting_task.
    /// If watch_callback is not empty, will call it when new mutations appear in ZK.
    void updateMutations(zkutil::ZooKeeperPtr zookeeper, Coordination::WatchCallback watch_callback = {});

    /// Remove a mutation from ZooKeeper and from the local set. Returns the removed entry or nullptr
    /// if it could not be found. Called during KILL MUTATION query execution.
    ReplicatedMergeTreeMutationEntryPtr removeMutation(zkutil::ZooKeeperPtr zookeeper, const String & mutation_id);

    /** Remove the action from the queue with the parts covered by part_name (from ZK and from the RAM).
      * And also wait for the completion of their execution, if they are now being executed.
      */
    void removePartProducingOpsInRange(zkutil::ZooKeeperPtr zookeeper, const MergeTreePartInfo & part_info, const ReplicatedMergeTreeLogEntryData & current);

    /** Throws and exception if there are currently executing entries in the range .
     */
    void checkThereAreNoConflictsInRange(const MergeTreePartInfo & range, const LogEntry & entry);

    /** In the case where there are not enough parts to perform the merge in part_name
      * - move actions with merged parts to the end of the queue
      * (in order to download a already merged part from another replica).
      */
    StringSet moveSiblingPartsForMergeToEndOfQueue(const String & part_name);

    /** Select the next action to process.
      * merger_mutator is used only to check if the merges are not suspended.
      */
    using SelectedEntry = std::pair<ReplicatedMergeTreeQueue::LogEntryPtr, std::unique_ptr<CurrentlyExecuting>>;
    SelectedEntry selectEntryToProcess(MergeTreeDataMergerMutator & merger_mutator, MergeTreeData & data);

    /** Execute `func` function to handle the action.
      * In this case, at runtime, mark the queue element as running
      *  (add into future_parts and more).
      * If there was an exception during processing, it saves it in `entry`.
      * Returns true if there were no exceptions during the processing.
      */
    bool processEntry(std::function<zkutil::ZooKeeperPtr()> get_zookeeper, LogEntryPtr & entry, const std::function<bool(LogEntryPtr &)> func);

    /// Count the number of merges and mutations of single parts in the queue.
    std::pair<size_t, size_t> countMergesAndPartMutations() const;

    /// Count the total number of active mutations.
    size_t countMutations() const;

    /// Count the total number of active mutations that are finished (is_done = true).
    size_t countFinishedMutations() const;

    /// Returns functor which used by MergeTreeMergerMutator to select parts for merge
    ReplicatedMergeTreeMergePredicate getMergePredicate(zkutil::ZooKeeperPtr & zookeeper);

    /// Return the version (block number) of the last mutation that we don't need to apply to the part
    /// with getDataVersion() == data_version. (Either this mutation was already applied or the part
    /// was created after the mutation).
    /// If there is no such mutation or it has already been executed and deleted, return 0.
    Int64 getCurrentMutationVersion(const String & partition_id, Int64 data_version) const;

    MutationCommands getMutationCommands(const MergeTreeData::DataPartPtr & part, Int64 desired_mutation_version) const;

    /// Return mutation commands for part with smallest mutation version bigger
    /// than data part version. Used when we apply alter commands on fly,
    /// without actual data modification on disk.
    MutationCommands getFirstAlterMutationCommandsForPart(const MergeTreeData::DataPartPtr & part) const;

    /// Mark finished mutations as done. If the function needs to be called again at some later time
    /// (because some mutations are probably done but we are not sure yet), returns true.
    bool tryFinalizeMutations(zkutil::ZooKeeperPtr zookeeper);

    /// Prohibit merges in the specified blocks range.
    /// Add part to virtual_parts, which means that part must exist
    /// after processing replication log up to log_pointer.
    /// Part maybe fake (look at ReplicatedMergeTreeMergePredicate).
    void disableMergesInBlockRange(const String & part_name);

    /// Cheks that part is already in virtual parts
    bool isVirtualPart(const MergeTreeData::DataPartPtr & data_part) const;

    /// Check that part isn't in currently generating parts and isn't covered by them and add it to future_parts.
    /// Locks queue's mutex.
    bool addFuturePartIfNotCoveredByThem(const String & part_name, LogEntry & entry, String & reject_reason);

    /// A blocker that stops selects from the queue
    ActionBlocker actions_blocker;

    /// A blocker that stops pulling entries from replication log to queue
    ActionBlocker pull_log_blocker;

    /// Adds a subscriber
    SubscriberHandler addSubscriber(SubscriberCallBack && callback);

    struct Status
    {
        UInt32 future_parts;
        UInt32 queue_size;
        UInt32 inserts_in_queue;
        UInt32 merges_in_queue;
        UInt32 part_mutations_in_queue;
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
};

class ReplicatedMergeTreeMergePredicate
{
public:
    ReplicatedMergeTreeMergePredicate(ReplicatedMergeTreeQueue & queue_, zkutil::ZooKeeperPtr & zookeeper);

    /// Depending on the existence of left part checks a merge predicate for two parts or for single part.
    bool operator()(const MergeTreeData::DataPartPtr & left,
                    const MergeTreeData::DataPartPtr & right,
                    String * out_reason = nullptr) const;

    /// Can we assign a merge with these two parts?
    /// (assuming that no merge was assigned after the predicate was constructed)
    /// If we can't and out_reason is not nullptr, set it to the reason why we can't merge.
    bool canMergeTwoParts(const MergeTreeData::DataPartPtr & left,
                          const MergeTreeData::DataPartPtr & right,
                          String * out_reason = nullptr) const;

    /// Can we assign a merge this part and some other part?
    /// For example a merge of a part and itself is needed for TTL.
    /// This predicate is checked for the first part of each partitition.
    bool canMergeSinglePart(const MergeTreeData::DataPartPtr & part, String * out_reason) const;

    /// Return nonempty optional of desired mutation version and alter version.
    /// If we have no alter (modify/drop) mutations in mutations queue, than we return biggest possible
    /// mutation version (and -1 as alter version). In other case, we return biggest mutation version with
    /// smallest alter version. This required, because we have to execute alter mutations sequentially and
    /// don't glue them together. Alter is rare operation, so it shouldn't affect performance.
    std::optional<std::pair<Int64, int>> getDesiredMutationVersion(const MergeTreeData::DataPartPtr & part) const;

    bool isMutationFinished(const ReplicatedMergeTreeMutationEntry & mutation) const;

    /// The version of "log" node that is used to check that no new merges have appeared.
    int32_t getVersion() const { return merges_version; }

private:
    const ReplicatedMergeTreeQueue & queue;

    /// A snapshot of active parts that would appear if the replica executes all log entries in its queue.
    ActiveDataPartSet prev_virtual_parts;
    /// partition ID -> block numbers of the inserts and mutations that are about to commit
    /// (loaded at some later time than prev_virtual_parts).
    std::unordered_map<String, std::set<Int64>> committing_blocks;

    /// Quorum state taken at some later time than prev_virtual_parts.
    String inprogress_quorum_part;

    int32_t merges_version = -1;
};


/** Convert a number to a string in the format of the suffixes of auto-incremental nodes in ZooKeeper.
  * Negative numbers are also supported - for them the name of the node looks somewhat silly
  *  and does not match any auto-incremented node in ZK.
  */
String padIndex(Int64 index);

}
