#pragma once

#include <mutex>
#include <base/defines.h>
#include <Common/SimpleIncrement.h>
#include <Common/SharedMutex.h>
#include <Common/MultiVersion.h>
#include <Storages/IStorage.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/StoragePolicy.h>
#include <Processors/Merges/Algorithms/Graphite.h>
#include <Storages/MergeTree/BackgroundJobsAssignee.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartBuilder.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/MergeTreePartsMover.h>
#include <Storages/MergeTree/MergeTreeWriteAheadLog.h>
#include <Storages/MergeTree/PinnedPartUUIDs.h>
#include <Storages/MergeTree/ZeroCopyLock.h>
#include <Storages/MergeTree/TemporaryParts.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/DataDestinationType.h>
#include <Storages/extractKeyExpressionList.h>
#include <Storages/PartitionCommands.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/threadPoolCallbackRunner.h>


#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/global_fun.hpp>
#include <boost/range/iterator_range_core.hpp>

namespace DB
{

/// Number of streams is not number parts, but number or parts*files, hence 1000.
const size_t DEFAULT_DELAYED_STREAMS_FOR_PARALLEL_WRITE = 1000;

class AlterCommands;
class MergeTreePartsMover;
class MergeTreeDataMergerMutator;
class MutationCommands;
class Context;
using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;
struct JobAndPool;
class MergeTreeTransaction;
struct ZeroCopyLock;

class IBackupEntry;
using BackupEntries = std::vector<std::pair<String, std::shared_ptr<const IBackupEntry>>>;

class MergeTreeTransaction;
using MergeTreeTransactionPtr = std::shared_ptr<MergeTreeTransaction>;

/// Auxiliary struct holding information about the future merged or mutated part.
struct EmergingPartInfo
{
    String disk_name;
    String partition_id;
    size_t estimate_bytes;
};

struct CurrentlySubmergingEmergingTagger;

struct SelectQueryOptions;
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
using ManyExpressionActions = std::vector<ExpressionActionsPtr>;
class MergeTreeDeduplicationLog;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct DataPartsLock
{
    std::optional<Stopwatch> wait_watch;
    std::unique_lock<std::mutex> lock;
    std::optional<Stopwatch> lock_watch;
    DataPartsLock() = default;
    explicit DataPartsLock(std::mutex & data_parts_mutex_);

    ~DataPartsLock();
};

/// Data structure for *MergeTree engines.
/// Merge tree is used for incremental sorting of data.
/// The table consists of several sorted parts.
/// During insertion new data is sorted according to the primary key and is written to the new part.
/// Parts are merged in the background according to a heuristic algorithm.
/// For each part the index file is created containing primary key values for every n-th row.
/// This allows efficient selection by primary key range predicate.
///
/// Additionally:
///
/// The date column is specified. For each part min and max dates are remembered.
/// Essentially it is an index too.
///
/// Data is partitioned by the value of the partitioning expression.
/// Parts belonging to different partitions are not merged - for the ease of administration (data sync and backup).
///
/// File structure of old-style month-partitioned tables (format_version = 0):
/// Part directory - / min-date _ max-date _ min-id _ max-id _ level /
/// Inside the part directory:
/// checksums.txt - contains the list of all files along with their sizes and checksums.
/// columns.txt - contains the list of all columns and their types.
/// primary.idx - contains the primary index.
/// [Column].bin - contains compressed column data.
/// [Column].mrk - marks, pointing to seek positions allowing to skip n * k rows.
///
/// File structure of tables with custom partitioning (format_version >= 1):
/// Part directory - / partition-id _ min-id _ max-id _ level /
/// Inside the part directory:
/// The same files as for month-partitioned tables, plus
/// count.txt - contains total number of rows in this part.
/// partition.dat - contains the value of the partitioning expression.
/// minmax_[Column].idx - MinMax indexes (see IMergeTreeDataPart::MinMaxIndex class) for the columns required by the partitioning expression.
///
/// Several modes are implemented. Modes determine additional actions during merge:
/// - Ordinary - don't do anything special
/// - Collapsing - collapse pairs of rows with the opposite values of sign_columns for the same values
///   of primary key (cf. CollapsingSortedTransform.h)
/// - Replacing - for all rows with the same primary key keep only the latest one. Or, if the version
///   column is set, keep the latest row with the maximal version.
/// - Summing - sum all numeric columns not contained in the primary key for all rows with the same primary key.
/// - Aggregating - merge columns containing aggregate function states for all rows with the same primary key.
/// - Graphite - performs coarsening of historical data for Graphite (a system for quantitative monitoring).

/// The MergeTreeData class contains a list of parts and the data structure parameters.
/// To read and modify the data use other classes:
/// - MergeTreeDataSelectExecutor
/// - MergeTreeDataWriter
/// - MergeTreeDataMergerMutator

class MergeTreeData : public IStorage, public WithMutableContext
{
public:
    /// Function to call if the part is suspected to contain corrupt data.
    using BrokenPartCallback = std::function<void (const String &)>;
    using DataPart = IMergeTreeDataPart;

    using MutableDataPartPtr = std::shared_ptr<DataPart>;
    using MutableDataPartsVector = std::vector<MutableDataPartPtr>;
    /// After the DataPart is added to the working set, it cannot be changed.
    using DataPartPtr = std::shared_ptr<const DataPart>;

    using DataPartState = MergeTreeDataPartState;
    using DataPartStates = std::initializer_list<DataPartState>;
    using DataPartStateVector = std::vector<DataPartState>;

    using PinnedPartUUIDsPtr = std::shared_ptr<const PinnedPartUUIDs>;

    constexpr static auto FORMAT_VERSION_FILE_NAME = "format_version.txt";
    constexpr static auto DETACHED_DIR_NAME = "detached";
    constexpr static auto MOVING_DIR_NAME = "moving";

    /// Auxiliary structure for index comparison. Keep in mind lifetime of MergeTreePartInfo.
    struct DataPartStateAndInfo
    {
        DataPartState state;
        const MergeTreePartInfo & info;
    };

    /// Auxiliary structure for index comparison
    struct DataPartStateAndPartitionID
    {
        DataPartState state;
        String partition_id;
    };

    STRONG_TYPEDEF(String, PartitionID)

    struct LessDataPart
    {
        using is_transparent = void;

        bool operator()(const DataPartPtr & lhs, const MergeTreePartInfo & rhs) const { return lhs->info < rhs; }
        bool operator()(const MergeTreePartInfo & lhs, const DataPartPtr & rhs) const { return lhs < rhs->info; }
        bool operator()(const DataPartPtr & lhs, const DataPartPtr & rhs) const { return lhs->info < rhs->info; }
        bool operator()(const MergeTreePartInfo & lhs, const PartitionID & rhs) const { return lhs.partition_id < rhs.toUnderType(); }
        bool operator()(const PartitionID & lhs, const MergeTreePartInfo & rhs) const { return lhs.toUnderType() < rhs.partition_id; }
    };

    struct LessStateDataPart
    {
        using is_transparent = void;

        bool operator() (const DataPartStateAndInfo & lhs, const DataPartStateAndInfo & rhs) const
        {
            return std::forward_as_tuple(static_cast<UInt8>(lhs.state), lhs.info)
                   < std::forward_as_tuple(static_cast<UInt8>(rhs.state), rhs.info);
        }

        bool operator() (DataPartStateAndInfo info, const DataPartState & state) const
        {
            return static_cast<size_t>(info.state) < static_cast<size_t>(state);
        }

        bool operator() (const DataPartState & state, DataPartStateAndInfo info) const
        {
            return static_cast<size_t>(state) < static_cast<size_t>(info.state);
        }

        bool operator() (const DataPartStateAndInfo & lhs, const DataPartStateAndPartitionID & rhs) const
        {
            return std::forward_as_tuple(static_cast<UInt8>(lhs.state), lhs.info.partition_id)
                   < std::forward_as_tuple(static_cast<UInt8>(rhs.state), rhs.partition_id);
        }

        bool operator() (const DataPartStateAndPartitionID & lhs, const DataPartStateAndInfo & rhs) const
        {
            return std::forward_as_tuple(static_cast<UInt8>(lhs.state), lhs.partition_id)
                   < std::forward_as_tuple(static_cast<UInt8>(rhs.state), rhs.info.partition_id);
        }
    };

    using DataParts = std::set<DataPartPtr, LessDataPart>;
    using MutableDataParts = std::set<MutableDataPartPtr, LessDataPart>;
    using DataPartsVector = std::vector<DataPartPtr>;

    DataPartsLock lockParts() const { return DataPartsLock(data_parts_mutex); }

    using OperationDataPartsLock = std::unique_lock<std::mutex>;
    OperationDataPartsLock lockOperationsWithParts() const { return OperationDataPartsLock(operation_with_data_parts_mutex); }

    MergeTreeDataPartFormat choosePartFormat(size_t bytes_uncompressed, size_t rows_count) const;
    MergeTreeDataPartFormat choosePartFormatOnDisk(size_t bytes_uncompressed, size_t rows_count) const;
    MergeTreeDataPartBuilder getDataPartBuilder(const String & name, const VolumePtr & volume, const String & part_dir) const;

    /// Auxiliary object to add a set of parts into the working set in two steps:
    /// * First, as PreActive parts (the parts are ready, but not yet in the active set).
    /// * Next, if commit() is called, the parts are added to the active set and the parts that are
    ///   covered by them are marked Outdated.
    /// If neither commit() nor rollback() was called, the destructor rollbacks the operation.
    class Transaction : private boost::noncopyable
    {
    public:
        Transaction(MergeTreeData & data_, MergeTreeTransaction * txn_);

        DataPartsVector commit(DataPartsLock * acquired_parts_lock = nullptr);

        void addPart(MutableDataPartPtr & part);

        void rollback();

        /// Immediately remove parts from table's data_parts set and change part
        /// state to temporary. Useful for new parts which not present in table.
        void rollbackPartsToTemporaryState();

        size_t size() const { return precommitted_parts.size(); }
        bool isEmpty() const { return precommitted_parts.empty(); }

        ~Transaction()
        {
            try
            {
                rollback();
            }
            catch (...)
            {
                tryLogCurrentException("~MergeTreeData::Transaction");
            }
        }
        void clear();

        TransactionID getTID() const;

    private:
        friend class MergeTreeData;

        MergeTreeData & data;
        MergeTreeTransaction * txn;
        MutableDataParts precommitted_parts;
        MutableDataParts locked_parts;

    };

    using TransactionUniquePtr = std::unique_ptr<Transaction>;

    using PathWithDisk = std::pair<String, DiskPtr>;

    struct PartsTemporaryRename : private boost::noncopyable
    {
        PartsTemporaryRename(
            const MergeTreeData & storage_,
            const String & source_dir_)
            : storage(storage_)
            , source_dir(source_dir_)
        {
        }

        /// Adds part to rename. Both names are relative to relative_data_path.
        void addPart(const String & old_name, const String & new_name, const DiskPtr & disk);

        /// Renames part from old_name to new_name
        void tryRenameAll();

        /// Renames all added parts from new_name to old_name if old name is not empty
        ~PartsTemporaryRename();

        struct RenameInfo
        {
            String old_name;
            String new_name;
            /// Disk cannot be changed
            DiskPtr disk;
        };

        const MergeTreeData & storage;
        const String source_dir;
        std::vector<RenameInfo> old_and_new_names;
        bool renamed = false;
    };

    /// Parameters for various modes.
    struct MergingParams
    {
        /// Merging mode. See above.
        enum Mode
        {
            Ordinary            = 0,    /// Enum values are saved. Do not change them.
            Collapsing          = 1,
            Summing             = 2,
            Aggregating         = 3,
            Replacing           = 5,
            Graphite            = 6,
            VersionedCollapsing = 7,
        };

        Mode mode;

        /// For Collapsing and VersionedCollapsing mode.
        String sign_column;

        /// For Replacing mode. Can be empty for Replacing.
        String is_deleted_column;

        /// For Summing mode. If empty - columns_to_sum is determined automatically.
        Names columns_to_sum;

        /// For Replacing and VersionedCollapsing mode. Can be empty for Replacing.
        String version_column;

        /// For Graphite mode.
        Graphite::Params graphite_params;

        /// Check that needed columns are present and have correct types.
        void check(const StorageInMemoryMetadata & metadata) const;

        String getModeName() const;
    };

    /// Attach the table corresponding to the directory in full_path inside policy (must end with /), with the given columns.
    /// Correctness of names and paths is not checked.
    ///
    /// date_column_name - if not empty, the name of the Date column used for partitioning by month.
    ///     Otherwise, partition_by_ast is used for partitioning.
    ///
    /// order_by_ast - a single expression or a tuple. It is used as a sorting key
    ///     (an ASTExpressionList used for sorting data in parts);
    /// primary_key_ast - can be nullptr, an expression, or a tuple.
    ///     Used to determine an ASTExpressionList values of which are written in the primary.idx file
    ///     for one row in every `index_granularity` rows to speed up range queries.
    ///     Primary key must be a prefix of the sorting key;
    ///     If it is nullptr, then it will be determined from order_by_ast.
    ///
    /// require_part_metadata - should checksums.txt and columns.txt exist in the part directory.
    /// attach - whether the existing table is attached or the new table is created.
    MergeTreeData(const StorageID & table_id_,
                  const StorageInMemoryMetadata & metadata_,
                  ContextMutablePtr context_,
                  const String & date_column_name,
                  const MergingParams & merging_params_,
                  std::unique_ptr<MergeTreeSettings> settings_,
                  bool require_part_metadata_,
                  bool attach,
                  BrokenPartCallback broken_part_callback_ = [](const String &){});

    /// Build a block of minmax and count values of a MergeTree table. These values are extracted
    /// from minmax_indices, the first expression of primary key, and part rows.
    ///
    /// has_filter - if query has no filter, bypass partition pruning completely
    ///
    /// query_info - used to filter unneeded parts
    ///
    /// parts - part set to filter
    ///
    /// normal_parts - collects parts that don't have all the needed values to form the block.
    /// Specifically, this is when a part doesn't contain a final mark and the related max value is
    /// required.
    Block getMinMaxCountProjectionBlock(
        const StorageMetadataPtr & metadata_snapshot,
        const Names & required_columns,
        bool has_filter,
        const SelectQueryInfo & query_info,
        const DataPartsVector & parts,
        DataPartsVector & normal_parts,
        const PartitionIdToMaxBlock * max_block_numbers_to_read,
        ContextPtr query_context) const;

    std::optional<ProjectionCandidate> getQueryProcessingStageWithAggregateProjection(
        ContextPtr query_context, const StorageSnapshotPtr & storage_snapshot, SelectQueryInfo & query_info) const;

    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr query_context,
        QueryProcessingStage::Enum to_stage,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & info) const override;

    ReservationPtr reserveSpace(UInt64 expected_size, VolumePtr & volume) const;
    static ReservationPtr tryReserveSpace(UInt64 expected_size, const IDataPartStorage & data_part_storage);
    static ReservationPtr reserveSpace(UInt64 expected_size, const IDataPartStorage & data_part_storage);

    static bool partsContainSameProjections(const DataPartPtr & left, const DataPartPtr & right);

    StoragePolicyPtr getStoragePolicy() const override;

    bool isMergeTree() const override { return true; }

    bool supportsPrewhere() const override { return true; }

    bool supportsFinal() const override;

    bool supportsSubcolumns() const override { return true; }

    bool supportsTTL() const override { return true; }

    bool supportsDynamicSubcolumns() const override { return true; }

    bool supportsLightweightDelete() const override;

    bool areAsynchronousInsertsEnabled() const override { return getSettings()->async_insert; }

    bool supportsTrivialCountOptimization() const override { return !hasLightweightDeletedMask(); }

    NamesAndTypesList getVirtuals() const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, ContextPtr, const StorageMetadataPtr & metadata_snapshot) const override;

    /// Snapshot for MergeTree contains the current set of data parts
    /// at the moment of the start of query.
    struct SnapshotData : public StorageSnapshot::Data
    {
        DataPartsVector parts;
        std::vector<AlterConversionsPtr> alter_conversions;
    };

    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const override;

    /// The same as above but does not hold vector of data parts.
    StorageSnapshotPtr getStorageSnapshotWithoutData(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const override;

    /// Load the set of data parts from disk. Call once - immediately after the object is created.
    void loadDataParts(bool skip_sanity_checks);

    String getLogName() const { return *std::atomic_load(&log_name); }

    Int64 getMaxBlockNumber() const;

    struct ProjectionPartsVector
    {
        DataPartsVector projection_parts;
        DataPartsVector data_parts;
    };

    /// Returns a copy of the list so that the caller shouldn't worry about locks.
    DataParts getDataParts(const DataPartStates & affordable_states) const;

    DataPartsVector getDataPartsVectorForInternalUsage(
        const DataPartStates & affordable_states, const DataPartsLock & lock, DataPartStateVector * out_states = nullptr) const;

    /// Returns sorted list of the parts with specified states
    ///  out_states will contain snapshot of each part state
    DataPartsVector getDataPartsVectorForInternalUsage(
        const DataPartStates & affordable_states, DataPartStateVector * out_states = nullptr) const;
    /// Same as above but only returns projection parts
    ProjectionPartsVector getProjectionPartsVectorForInternalUsage(
        const DataPartStates & affordable_states, DataPartStateVector * out_states = nullptr) const;


    /// Returns absolutely all parts (and snapshot of their states)
    DataPartsVector getAllDataPartsVector(DataPartStateVector * out_states = nullptr) const;

    size_t getAllPartsCount() const;

    /// Return the number of marks in all parts
    size_t getTotalMarksCount() const;

    /// Same as above but only returns projection parts
    ProjectionPartsVector getAllProjectionPartsVector(MergeTreeData::DataPartStateVector * out_states = nullptr) const;

    /// Returns parts in Active state
    DataParts getDataPartsForInternalUsage() const;
    DataPartsVector getDataPartsVectorForInternalUsage() const;

    void filterVisibleDataParts(DataPartsVector & maybe_visible_parts, CSN snapshot_version, TransactionID current_tid) const;

    /// Returns parts that visible with current snapshot
    DataPartsVector getVisibleDataPartsVector(ContextPtr local_context) const;
    DataPartsVector getVisibleDataPartsVectorUnlocked(ContextPtr local_context, const DataPartsLock & lock) const;
    DataPartsVector getVisibleDataPartsVector(const MergeTreeTransactionPtr & txn) const;
    DataPartsVector getVisibleDataPartsVector(CSN snapshot_version, TransactionID current_tid) const;

    /// Returns a part in Active state with the given name or a part containing it. If there is no such part, returns nullptr.
    DataPartPtr getActiveContainingPart(const String & part_name) const;
    DataPartPtr getActiveContainingPart(const String & part_name, DataPartsLock & lock) const;
    DataPartPtr getActiveContainingPart(const MergeTreePartInfo & part_info) const;
    DataPartPtr getActiveContainingPart(const MergeTreePartInfo & part_info, DataPartState state, DataPartsLock & lock) const;

    /// Swap part with it's identical copy (possible with another path on another disk).
    /// If original part is not active or doesn't exist exception will be thrown.
    void swapActivePart(MergeTreeData::DataPartPtr part_copy, DataPartsLock &);

    /// Returns all parts in specified partition
    DataPartsVector getVisibleDataPartsVectorInPartition(MergeTreeTransaction * txn, const String & partition_id, DataPartsLock * acquired_lock = nullptr) const;
    DataPartsVector getVisibleDataPartsVectorInPartition(ContextPtr local_context, const String & partition_id, DataPartsLock & lock) const;
    DataPartsVector getVisibleDataPartsVectorInPartition(ContextPtr local_context, const String & partition_id) const;
    DataPartsVector getVisibleDataPartsVectorInPartitions(ContextPtr local_context, const std::unordered_set<String> & partition_ids) const;

    DataPartsVector getDataPartsVectorInPartitionForInternalUsage(const DataPartState & state, const String & partition_id, DataPartsLock * acquired_lock = nullptr) const;
    DataPartsVector getDataPartsVectorInPartitionForInternalUsage(const DataPartStates & affordable_states, const String & partition_id, DataPartsLock * acquired_lock = nullptr) const;

    /// Returns the part with the given name and state or nullptr if no such part.
    DataPartPtr getPartIfExistsUnlocked(const String & part_name, const DataPartStates & valid_states, DataPartsLock & acquired_lock) const;
    DataPartPtr getPartIfExistsUnlocked(const MergeTreePartInfo & part_info, const DataPartStates & valid_states, DataPartsLock & acquired_lock) const;
    DataPartPtr getPartIfExists(const String & part_name, const DataPartStates & valid_states) const;
    DataPartPtr getPartIfExists(const MergeTreePartInfo & part_info, const DataPartStates & valid_states) const;

    /// Total size of active parts in bytes.
    size_t getTotalActiveSizeInBytes() const;

    size_t getTotalActiveSizeInRows() const;

    size_t getActivePartsCount() const;

    size_t getOutdatedPartsCount() const;

    size_t getNumberOfOutdatedPartsWithExpiredRemovalTime() const;

    /// Returns a pair with: max number of parts in partition across partitions; sum size of parts inside that partition.
    /// (if there are multiple partitions with max number of parts, the sum size of parts is returned for arbitrary of them)
    std::pair<size_t, size_t> getMaxPartsCountAndSizeForPartitionWithState(DataPartState state) const;
    std::pair<size_t, size_t> getMaxPartsCountAndSizeForPartition() const;

    size_t getMaxOutdatedPartsCountForPartition() const;

    /// Get min value of part->info.getDataVersion() for all active parts.
    /// Makes sense only for ordinary MergeTree engines because for them block numbering doesn't depend on partition.
    std::optional<Int64> getMinPartDataVersion() const;

    /// Returns all detached parts
    DetachedPartsInfo getDetachedParts() const;

    static void validateDetachedPartName(const String & name);

    void dropDetached(const ASTPtr & partition, bool part, ContextPtr context);

    MutableDataPartsVector tryLoadPartsToAttach(const ASTPtr & partition, bool attach_part,
                                                ContextPtr context, PartsTemporaryRename & renamed_parts);

    /// If the table contains too many active parts, sleep for a while to give them time to merge.
    /// If until is non-null, wake up from the sleep earlier if the event happened.
    /// The decision to delay or throw is made according to settings 'parts_to_delay_insert' and 'parts_to_throw_insert'.
    void delayInsertOrThrowIfNeeded(Poco::Event * until, const ContextPtr & query_context, bool allow_throw) const;

    /// If the table contains too many unfinished mutations, sleep for a while to give them time to execute.
    /// If until is non-null, wake up from the sleep earlier if the event happened.
    /// The decision to delay or throw is made according to settings 'number_of_mutations_to_delay' and 'number_of_mutations_to_throw'.
    void delayMutationOrThrowIfNeeded(Poco::Event * until, const ContextPtr & query_context) const;

    /// Returns number of unfinished mutations (is_done = 0).
    virtual size_t getNumberOfUnfinishedMutations() const = 0;

    /// Renames temporary part to a permanent part and adds it to the parts set.
    /// It is assumed that the part does not intersect with existing parts.
    /// Adds the part in the PreActive state (the part will be added to the active set later with out_transaction->commit()).
    /// Returns true if part was added. Returns false if part is covered by bigger part.
    bool renameTempPartAndAdd(
        MutableDataPartPtr & part,
        Transaction & transaction,
        DataPartsLock & lock);

    /// The same as renameTempPartAndAdd but the block range of the part can contain existing parts.
    /// Returns all parts covered by the added part (in ascending order).
    DataPartsVector renameTempPartAndReplace(
        MutableDataPartPtr & part,
        Transaction & out_transaction);

    /// Unlocked version of previous one. Useful when added multiple parts with a single lock.
    bool renameTempPartAndReplaceUnlocked(
        MutableDataPartPtr & part,
        Transaction & out_transaction,
        DataPartsLock & lock,
        DataPartsVector * out_covered_parts = nullptr);

    /// Remove parts from working set immediately (without wait for background
    /// process). Transfer part state to temporary. Have very limited usage only
    /// for new parts which aren't already present in table.
    void removePartsFromWorkingSetImmediatelyAndSetTemporaryState(const DataPartsVector & remove);

    /// Removes parts from the working set parts.
    /// Parts in add must already be in data_parts with PreActive, Active, or Outdated states.
    /// If clear_without_timeout is true, the parts will be deleted at once, or during the next call to
    /// clearOldParts (ignoring old_parts_lifetime).
    void removePartsFromWorkingSet(MergeTreeTransaction * txn, const DataPartsVector & remove, bool clear_without_timeout, DataPartsLock * acquired_lock = nullptr);
    void removePartsFromWorkingSet(MergeTreeTransaction * txn, const DataPartsVector & remove, bool clear_without_timeout, DataPartsLock & acquired_lock);

    /// Removes all parts covered by drop_range from the working set parts.
    /// Used in REPLACE PARTITION command.
    void removePartsInRangeFromWorkingSet(MergeTreeTransaction * txn, const MergeTreePartInfo & drop_range, DataPartsLock & lock);

    DataPartsVector grabActivePartsToRemoveForDropRange(
        MergeTreeTransaction * txn, const MergeTreePartInfo & drop_range, DataPartsLock & lock);
    /// This wrapper is required to restrict access to parts in Deleting state
    class PartToRemoveFromZooKeeper
    {
        DataPartPtr part;
        bool was_active;

    public:
        explicit PartToRemoveFromZooKeeper(DataPartPtr && part_, bool was_active_ = true)
         : part(std::move(part_)), was_active(was_active_)
        {
        }

        /// It's safe to get name of any part
        const String & getPartName() const { return part->name; }

        DataPartPtr getPartIfItWasActive() const
        {
            return was_active ? part : nullptr;
        }
    };

    using PartsToRemoveFromZooKeeper = std::vector<PartToRemoveFromZooKeeper>;

    /// Same as above, but also returns list of parts to remove from ZooKeeper.
    /// It includes parts that have been just removed by these method
    /// and Outdated parts covered by drop_range that were removed earlier for any reason.
    PartsToRemoveFromZooKeeper removePartsInRangeFromWorkingSetAndGetPartsToRemoveFromZooKeeper(
        MergeTreeTransaction * txn, const MergeTreePartInfo & drop_range, DataPartsLock & lock);

    /// Restores Outdated part and adds it to working set
    void restoreAndActivatePart(const DataPartPtr & part, DataPartsLock * acquired_lock = nullptr);

    /// Renames the part to detached/<prefix>_<part> and removes it from data_parts,
    //// so it will not be deleted in clearOldParts.
    /// If restore_covered is true, adds to the working set inactive parts, which were merged into the deleted part.
    /// NOTE: This method is safe to use only for parts which nobody else holds (like on server start or for parts which was not committed).
    /// For active parts it's unsafe because this method modifies fields of part (rename) while some other thread can try to read it.
    void forcefullyMovePartToDetachedAndRemoveFromMemory(const DataPartPtr & part, const String & prefix = "", bool restore_covered = false);

    /// This method should not be here, but async loading of Outdated parts is implemented in MergeTreeData
    virtual void forcefullyRemoveBrokenOutdatedPartFromZooKeeperBeforeDetaching(const String & /*part_name*/) {}

    /// Outdate broken part, set remove time to zero (remove as fast as possible) and make clone in detached directory.
    void outdateUnexpectedPartAndCloneToDetached(const DataPartPtr & part);

    /// If the part is Obsolete and not used by anybody else, immediately delete it from filesystem and remove from memory.
    void tryRemovePartImmediately(DataPartPtr && part);

    /// Returns old inactive parts that can be deleted. At the same time removes them from the list of parts but not from the disk.
    /// If 'force' - don't wait for old_parts_lifetime.
    DataPartsVector grabOldParts(bool force = false);

    /// Reverts the changes made by grabOldParts(), parts should be in Deleting state.
    void rollbackDeletingParts(const DataPartsVector & parts);

    /// Removes parts from data_parts, they should be in Deleting state
    void removePartsFinally(const DataPartsVector & parts);

    /// Delete irrelevant parts from memory and disk.
    /// If 'force' - don't wait for old_parts_lifetime.
    size_t clearOldPartsFromFilesystem(bool force = false);
    /// Try to clear parts from filesystem. Throw exception in case of errors.
    void clearPartsFromFilesystem(const DataPartsVector & parts, bool throw_on_error = true, NameSet * parts_failed_to_delete = nullptr);

    /// Delete WAL files containing parts, that all already stored on disk.
    size_t clearOldWriteAheadLogs();

    size_t clearOldBrokenPartsFromDetachedDirectory();

    /// Delete all directories which names begin with "tmp"
    /// Must be called with locked lockForShare() because it's using relative_data_path.
    size_t clearOldTemporaryDirectories(size_t custom_directories_lifetime_seconds, const NameSet & valid_prefixes = {"tmp_", "tmp-fetch_"});
    size_t clearOldTemporaryDirectories(const String & root_path, size_t custom_directories_lifetime_seconds, const NameSet & valid_prefixes);

    size_t clearEmptyParts();

    /// After the call to dropAllData() no method can be called.
    /// Deletes the data directory and flushes the uncompressed blocks cache and the marks cache.
    void dropAllData();

    /// This flag is for hardening and assertions.
    bool all_data_dropped = false;

    /// Drop data directories if they are empty. It is safe to call this method if table creation was unsuccessful.
    void dropIfEmpty();

    /// Moves the entire data directory. Flushes the uncompressed blocks cache
    /// and the marks cache. Must be called with locked lockExclusively()
    /// because changes relative_data_path.
    void rename(const String & new_table_path, const StorageID & new_table_id) override;

    /// Also rename log names.
    void renameInMemory(const StorageID & new_table_id) override;

    /// Check if the ALTER can be performed:
    /// - all needed columns are present.
    /// - all type conversions can be done.
    /// - columns corresponding to primary key, indices, sign, sampling expression and date are not affected.
    /// If something is wrong, throws an exception.
    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    /// Checks if the Mutation can be performed.
    /// (currently no additional checks: always ok)
    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;

    /// Checks that partition name in all commands is valid
    void checkAlterPartitionIsPossible(const PartitionCommands & commands, const StorageMetadataPtr & metadata_snapshot, const Settings & settings) const override;

    /// Change MergeTreeSettings
    void changeSettings(
        const ASTPtr & new_settings,
        AlterLockHolder & table_lock_holder);

    /// Should be called if part data is suspected to be corrupted.
    /// Has the ability to check all other parts
    /// which reside on the same disk of the suspicious part.
    void reportBrokenPart(MergeTreeData::DataPartPtr data_part) const;

    /// TODO (alesap) Duplicate method required for compatibility.
    /// Must be removed.
    static ASTPtr extractKeyExpressionList(const ASTPtr & node)
    {
        return DB::extractKeyExpressionList(node);
    }

    /** Create local backup (snapshot) for parts with specified prefix.
      * Backup is created in directory clickhouse_dir/shadow/i/, where i - incremental number,
      *  or if 'with_name' is specified - backup is created in directory with specified name.
      */
    PartitionCommandsResultInfo freezePartition(
        const ASTPtr & partition,
        const StorageMetadataPtr & metadata_snapshot,
        const String & with_name,
        ContextPtr context,
        TableLockHolder & table_lock_holder);

    /// Freezes all parts.
    PartitionCommandsResultInfo freezeAll(
        const String & with_name,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        TableLockHolder & table_lock_holder);

    /// Unfreezes particular partition.
    PartitionCommandsResultInfo unfreezePartition(
        const ASTPtr & partition,
        const String & backup_name,
        ContextPtr context,
        TableLockHolder & table_lock_holder);

    /// Unfreezes all parts.
    PartitionCommandsResultInfo unfreezeAll(
        const String & backup_name,
        ContextPtr context,
        TableLockHolder & table_lock_holder);

    /// Extract data from the backup and put it to the storage.
    void restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;

    /// Returns true if the storage supports backup/restore for specific partitions.
    bool supportsBackupPartition() const override { return true; }

    /// Moves partition to specified Disk
    void movePartitionToDisk(const ASTPtr & partition, const String & name, bool moving_part, ContextPtr context);

    /// Moves partition to specified Volume
    void movePartitionToVolume(const ASTPtr & partition, const String & name, bool moving_part, ContextPtr context);

    /// Checks that Partition could be dropped right now
    /// Otherwise - throws an exception with detailed information.
    /// We do not use mutex because it is not very important that the size could change during the operation.
    void checkPartitionCanBeDropped(const ASTPtr & partition, ContextPtr local_context);

    void checkPartCanBeDropped(const String & part_name);

    Pipe alterPartition(
        const StorageMetadataPtr & metadata_snapshot,
        const PartitionCommands & commands,
        ContextPtr query_context) override;

    size_t getColumnCompressedSize(const std::string & name) const
    {
        auto lock = lockParts();
        const auto it = column_sizes.find(name);
        return it == std::end(column_sizes) ? 0 : it->second.data_compressed;
    }

    ColumnSizeByName getColumnSizes() const override
    {
        auto lock = lockParts();
        return column_sizes;
    }

    /// Creates description of columns of data type Object from the range of data parts.
    static ColumnsDescription getConcreteObjectColumns(
        const DataPartsVector & parts, const ColumnsDescription & storage_columns);

    IndexSizeByName getSecondaryIndexSizes() const override
    {
        auto lock = lockParts();
        return secondary_index_sizes;
    }

    /// For ATTACH/DETACH/DROP PARTITION.
    String getPartitionIDFromQuery(const ASTPtr & ast, ContextPtr context, DataPartsLock * acquired_lock = nullptr) const;
    std::unordered_set<String> getPartitionIDsFromQuery(const ASTs & asts, ContextPtr context) const;
    std::set<String> getPartitionIdsAffectedByCommands(const MutationCommands & commands, ContextPtr query_context) const;

    /// Returns set of partition_ids of all Active parts
    std::unordered_set<String> getAllPartitionIds() const;

    /// Extracts MergeTreeData of other *MergeTree* storage
    ///  and checks that their structure suitable for ALTER TABLE ATTACH PARTITION FROM
    /// Tables structure should be locked.
    MergeTreeData & checkStructureAndGetMergeTreeData(const StoragePtr & source_table, const StorageMetadataPtr & src_snapshot, const StorageMetadataPtr & my_snapshot) const;
    MergeTreeData & checkStructureAndGetMergeTreeData(IStorage & source_table, const StorageMetadataPtr & src_snapshot, const StorageMetadataPtr & my_snapshot) const;

    std::pair<MergeTreeData::MutableDataPartPtr, scope_guard> cloneAndLoadDataPartOnSameDisk(
        const MergeTreeData::DataPartPtr & src_part, const String & tmp_part_prefix,
        const MergeTreePartInfo & dst_part_info, const StorageMetadataPtr & metadata_snapshot,
        const IDataPartStorage::ClonePartParams & params);

    virtual std::vector<MergeTreeMutationStatus> getMutationsStatus() const = 0;

    /// Returns true if table can create new parts with adaptive granularity
    /// Has additional constraint in replicated version
    virtual bool canUseAdaptiveGranularity() const
    {
        const auto settings = getSettings();
        return settings->index_granularity_bytes != 0 &&
            (settings->enable_mixed_granularity_parts || !has_non_adaptive_index_granularity_parts);
    }

    /// Get constant pointer to storage settings.
    /// Copy this pointer into your scope and you will
    /// get consistent settings.
    MergeTreeSettingsPtr getSettings() const
    {
        return storage_settings.get();
    }

    String getRelativeDataPath() const { return relative_data_path; }

    /// Get table path on disk
    String getFullPathOnDisk(const DiskPtr & disk) const;

    /// Looks for detached part on all disks,
    /// returns pointer to the disk where part is found or nullptr (the second function throws an exception)
    DiskPtr tryGetDiskForDetachedPart(const String & part_name) const;
    DiskPtr getDiskForDetachedPart(const String & part_name) const;

    bool storesDataOnDisk() const override { return !isStaticStorage(); }
    Strings getDataPaths() const override;

    /// Reserves space at least 1MB.
    ReservationPtr reserveSpace(UInt64 expected_size) const;

    /// Reserves space at least 1MB on specific disk or volume.
    static ReservationPtr reserveSpace(UInt64 expected_size, SpacePtr space);
    static ReservationPtr tryReserveSpace(UInt64 expected_size, SpacePtr space);

    /// Reserves space at least 1MB preferring best destination according to `ttl_infos`.
    ReservationPtr reserveSpacePreferringTTLRules(
        const StorageMetadataPtr & metadata_snapshot,
        UInt64 expected_size,
        const IMergeTreeDataPart::TTLInfos & ttl_infos,
        time_t time_of_move,
        size_t min_volume_index = 0,
        bool is_insert = false,
        DiskPtr selected_disk = nullptr) const;

    ReservationPtr tryReserveSpacePreferringTTLRules(
        const StorageMetadataPtr & metadata_snapshot,
        UInt64 expected_size,
        const IMergeTreeDataPart::TTLInfos & ttl_infos,
        time_t time_of_move,
        size_t min_volume_index = 0,
        bool is_insert = false,
        DiskPtr selected_disk = nullptr) const;

    /// Reserves space for the part based on the distribution of "big parts" in the same partition.
    /// Parts with estimated size larger than `min_bytes_to_rebalance_partition_over_jbod` are
    /// considered as big. The priority is lower than TTL. If reservation fails, return nullptr.
    ReservationPtr balancedReservation(
        const StorageMetadataPtr & metadata_snapshot,
        size_t part_size,
        size_t max_volume_index,
        const String & part_name,
        const MergeTreePartInfo & part_info,
        MergeTreeData::DataPartsVector covered_parts,
        std::optional<CurrentlySubmergingEmergingTagger> * tagger_ptr,
        const IMergeTreeDataPart::TTLInfos * ttl_infos,
        bool is_insert = false);

    /// Choose disk with max available free space
    /// Reserves 0 bytes
    ReservationPtr makeEmptyReservationOnLargestDisk() const { return getStoragePolicy()->makeEmptyReservationOnLargestDisk(); }

    Disks getDisks() const { return getStoragePolicy()->getDisks(); }

    /// Return alter conversions for part which must be applied on fly.
    AlterConversionsPtr getAlterConversionsForPart(MergeTreeDataPartPtr part) const;

    /// Returns destination disk or volume for the TTL rule according to current storage policy.
    SpacePtr getDestinationForMoveTTL(const TTLDescription & move_ttl) const;

    /// Whether INSERT of a data part which is already expired should move it immediately to a volume/disk declared in move rule.
    bool shouldPerformTTLMoveOnInsert(const SpacePtr & move_destination) const;

    /// Checks if given part already belongs destination disk or volume for the
    /// TTL rule.
    bool isPartInTTLDestination(const TTLDescription & ttl, const IMergeTreeDataPart & part) const;

    /// Get count of total merges with TTL in MergeList (system.merges) for all
    /// tables (not only current table).
    /// Method is cheap and doesn't require any locks.
    size_t getTotalMergesWithTTLInMergeList() const;

    using WriteAheadLogPtr = std::shared_ptr<MergeTreeWriteAheadLog>;
    WriteAheadLogPtr getWriteAheadLog();

    constexpr static auto EMPTY_PART_TMP_PREFIX = "tmp_empty_";
    std::pair<MergeTreeData::MutableDataPartPtr, scope_guard> createEmptyPart(
        MergeTreePartInfo & new_part_info, const MergeTreePartition & partition,
        const String & new_part_name, const MergeTreeTransactionPtr & txn);

    MergeTreeDataFormatVersion format_version;

    /// Merging params - what additional actions to perform during merge.
    const MergingParams merging_params;

    bool is_custom_partitioned = false;

    /// Used only for old syntax tables. Never changes after init.
    Int64 minmax_idx_date_column_pos = -1; /// In a common case minmax index includes a date column.
    Int64 minmax_idx_time_column_pos = -1; /// In other cases, minmax index often includes a dateTime column.

    /// Get partition key expression on required columns
    static ExpressionActionsPtr getMinMaxExpr(const KeyDescription & partition_key, const ExpressionActionsSettings & settings);
    /// Get column names required for partition key
    static Names getMinMaxColumnsNames(const KeyDescription & partition_key);
    /// Get column types required for partition key
    static DataTypes getMinMaxColumnsTypes(const KeyDescription & partition_key);

    ExpressionActionsPtr
    getPrimaryKeyAndSkipIndicesExpression(const StorageMetadataPtr & metadata_snapshot, const MergeTreeIndices & indices) const;
    ExpressionActionsPtr
    getSortingKeyAndSkipIndicesExpression(const StorageMetadataPtr & metadata_snapshot, const MergeTreeIndices & indices) const;

    /// Get compression codec for part according to TTL rules and <compression>
    /// section from config.xml.
    CompressionCodecPtr getCompressionCodecForPart(size_t part_size_compressed, const IMergeTreeDataPart::TTLInfos & ttl_infos, time_t current_time) const;

    std::shared_ptr<QueryIdHolder> getQueryIdHolder(const String & query_id, UInt64 max_concurrent_queries) const;

    /// Record current query id where querying the table. Throw if there are already `max_queries` queries accessing the same table.
    /// Returns false if the `query_id` already exists in the running set, otherwise return true.
    bool insertQueryIdOrThrow(const String & query_id, size_t max_queries) const;
    bool insertQueryIdOrThrowNoLock(const String & query_id, size_t max_queries) const TSA_REQUIRES(query_id_set_mutex);

    /// Remove current query id after query finished.
    void removeQueryId(const String & query_id) const;
    void removeQueryIdNoLock(const String & query_id) const TSA_REQUIRES(query_id_set_mutex);

    /// Return the partition expression types as a Tuple type. Return DataTypeUInt8 if partition expression is empty.
    DataTypePtr getPartitionValueType() const;

    /// Construct a sample block of virtual columns.
    Block getSampleBlockWithVirtualColumns() const;

    /// Construct a block consisting only of possible virtual columns for part pruning.
    /// If one_part is true, fill in at most one part.
    Block getBlockWithVirtualPartColumns(const MergeTreeData::DataPartsVector & parts, bool one_part, bool ignore_empty = false) const;

    /// In merge tree we do inserts with several steps. One of them:
    /// X. write part to temporary directory with some temp name
    /// Y. rename temporary directory to final name with correct block number value
    /// As temp name MergeTree use just ordinary in memory counter, but in some cases
    /// it can be useful to add additional part in temp name to avoid collisions on FS.
    /// FIXME: Currently unused.
    virtual std::string getPostfixForTempInsertName() const { return ""; }

    /// For generating names of temporary parts during insertion.
    SimpleIncrement insert_increment;

    bool has_non_adaptive_index_granularity_parts = false;

    /// True if at least one part contains lightweight delete.
    mutable std::atomic_bool has_lightweight_delete_parts = false;

    /// Parts that currently moving from disk/volume to another.
    /// This set have to be used with `currently_processing_in_background_mutex`.
    /// Moving may conflict with merges and mutations, but this is OK, because
    /// if we decide to move some part to another disk, than we
    /// assuredly will choose this disk for containing part, which will appear
    /// as result of merge or mutation.
    DataParts currently_moving_parts;

    /// Mutex for currently_moving_parts
    mutable std::mutex moving_parts_mutex;

    PinnedPartUUIDsPtr getPinnedPartUUIDs() const;

    /// Schedules background job to like merge/mutate/fetch an executor
    virtual bool scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) = 0;
    /// Schedules job to move parts between disks/volumes and so on.
    bool scheduleDataMovingJob(BackgroundJobsAssignee & assignee);
    bool areBackgroundMovesNeeded() const;


    /// Lock part in zookeeper for shared data in several nodes
    /// Overridden in StorageReplicatedMergeTree
    virtual void lockSharedData(const IMergeTreeDataPart &, bool = false, std::optional<HardlinkedFiles> = {}) const {} /// NOLINT

    /// Unlock shared data part in zookeeper
    /// Overridden in StorageReplicatedMergeTree
    virtual std::pair<bool, NameSet> unlockSharedData(const IMergeTreeDataPart &) const { return std::make_pair(true, NameSet{}); }

    /// Fetch part only if some replica has it on shared storage like S3
    /// Overridden in StorageReplicatedMergeTree
    virtual MutableDataPartPtr tryToFetchIfShared(const IMergeTreeDataPart &, const DiskPtr &, const String &) { return nullptr; }

    /// Check shared data usage on other replicas for detached/freezed part
    /// Remove local files and remote files if needed
    virtual bool removeDetachedPart(DiskPtr disk, const String & path, const String & part_name);

    virtual String getTableSharedID() const { return ""; }

    /// Store metadata for replicated tables
    /// Do nothing for non-replicated tables
    virtual void createAndStoreFreezeMetadata(DiskPtr disk, DataPartPtr part, String backup_part_path) const;

    /// Parts that currently submerging (merging to bigger parts) or emerging
    /// (to be appeared after merging finished). These two variables have to be used
    /// with `currently_submerging_emerging_mutex`.
    DataParts currently_submerging_big_parts;
    std::map<String, EmergingPartInfo> currently_emerging_big_parts;
    /// Mutex for currently_submerging_parts and currently_emerging_parts
    mutable std::mutex currently_submerging_emerging_mutex;

    /// Used for freezePartitionsByMatcher and unfreezePartitionsByMatcher
    using MatcherFn = std::function<bool(const String &)>;

    /// Returns an object that protects temporary directory from cleanup
    scope_guard getTemporaryPartDirectoryHolder(const String & part_dir_name) const;

    void waitForOutdatedPartsToBeLoaded() const;
    bool canUsePolymorphicParts() const;

    /// TODO: make enabled by default in the next release if no problems found.
    bool allowRemoveStaleMovingParts() const;

protected:
    friend class IMergeTreeDataPart;
    friend class MergeTreeDataMergerMutator;
    friend struct ReplicatedMergeTreeTableMetadata;
    friend class StorageReplicatedMergeTree;
    friend class MergeTreeDataWriter;
    friend class MergeTask;
    friend class IPartMetadataManager;
    friend class IMergedBlockOutputStream; // for access to log

    bool require_part_metadata;

    /// Relative path data, changes during rename for ordinary databases use
    /// under lockForShare if rename is possible.
    String relative_data_path;

    /// Current column sizes in compressed and uncompressed form.
    ColumnSizeByName column_sizes;

    /// Current secondary index sizes in compressed and uncompressed form.
    IndexSizeByName secondary_index_sizes;

    /// Engine-specific methods
    BrokenPartCallback broken_part_callback;

    /// log_name will change during table RENAME. Use atomic_shared_ptr to allow concurrent RW.
    /// NOTE clang-14 doesn't have atomic_shared_ptr yet. Use std::atomic* operations for now.
    std::shared_ptr<String> log_name;
    std::atomic<Poco::Logger *> log;

    /// Storage settings.
    /// Use get and set to receive readonly versions.
    MultiVersion<MergeTreeSettings> storage_settings;

    /// Used to determine which UUIDs to send to root query executor for deduplication.
    mutable SharedMutex pinned_part_uuids_mutex;
    PinnedPartUUIDsPtr pinned_part_uuids;

    /// True if at least one part was created/removed with transaction.
    mutable std::atomic_bool transactions_enabled = false;

    std::atomic_bool data_parts_loading_finished = false;

    /// Work with data parts

    struct TagByInfo{};
    struct TagByStateAndInfo{};

    void initializeDirectoriesAndFormatVersion(const std::string & relative_data_path_, bool attach, const std::string & date_column_name, bool need_create_directories=true);

    static const MergeTreePartInfo & dataPartPtrToInfo(const DataPartPtr & part)
    {
        return part->info;
    }

    static DataPartStateAndInfo dataPartPtrToStateAndInfo(const DataPartPtr & part)
    {
        return {part->getState(), part->info};
    }

    using DataPartsIndexes = boost::multi_index_container<DataPartPtr,
        boost::multi_index::indexed_by<
            /// Index by Info
            boost::multi_index::ordered_unique<
                boost::multi_index::tag<TagByInfo>,
                boost::multi_index::global_fun<const DataPartPtr &, const MergeTreePartInfo &, dataPartPtrToInfo>
            >,
            /// Index by (State, Info), is used to obtain ordered slices of parts with the same state
            boost::multi_index::ordered_unique<
                boost::multi_index::tag<TagByStateAndInfo>,
                boost::multi_index::global_fun<const DataPartPtr &, DataPartStateAndInfo, dataPartPtrToStateAndInfo>,
                LessStateDataPart
            >
        >
    >;

    /// Current set of data parts.
    mutable std::mutex data_parts_mutex;
    DataPartsIndexes data_parts_indexes;
    DataPartsIndexes::index<TagByInfo>::type & data_parts_by_info;
    DataPartsIndexes::index<TagByStateAndInfo>::type & data_parts_by_state_and_info;

    /// Mutex for critical sections which alter set of parts
    /// It is like truncate, drop/detach partition
    mutable std::mutex operation_with_data_parts_mutex;

    /// Current description of columns of data type Object.
    /// It changes only when set of parts is changed and is
    /// protected by @data_parts_mutex.
    ColumnsDescription object_columns;

    MergeTreePartsMover parts_mover;

    /// Executors are common for both ReplicatedMergeTree and plain MergeTree
    /// but they are being started and finished in derived classes, so let them be protected.
    ///
    /// Why there are two executors, not one? Or an executor for each kind of operation?
    /// It is historically formed.
    /// Another explanation is that moving operations are common for Replicated and Plain MergeTree classes.
    /// Task that schedules this operations is executed with its own timetable and triggered in a specific places in code.
    /// And for ReplicatedMergeTree we don't have LogEntry type for this operation.
    BackgroundJobsAssignee background_operations_assignee;
    BackgroundJobsAssignee background_moves_assignee;
    bool use_metadata_cache;

    /// Strongly connected with two fields above.
    /// Every task that is finished will ask to assign a new one into an executor.
    /// These callbacks will be passed to the constructor of each task.
    IExecutableTask::TaskResultCallback common_assignee_trigger;
    IExecutableTask::TaskResultCallback moves_assignee_trigger;

    using DataPartIteratorByInfo = DataPartsIndexes::index<TagByInfo>::type::iterator;
    using DataPartIteratorByStateAndInfo = DataPartsIndexes::index<TagByStateAndInfo>::type::iterator;

    boost::iterator_range<DataPartIteratorByStateAndInfo> getDataPartsStateRange(DataPartState state) const
    {
        auto begin = data_parts_by_state_and_info.lower_bound(state, LessStateDataPart());
        auto end = data_parts_by_state_and_info.upper_bound(state, LessStateDataPart());
        return {begin, end};
    }

    boost::iterator_range<DataPartIteratorByInfo> getDataPartsPartitionRange(const String & partition_id) const
    {
        auto begin = data_parts_by_info.lower_bound(PartitionID(partition_id), LessDataPart());
        auto end = data_parts_by_info.upper_bound(PartitionID(partition_id), LessDataPart());
        return {begin, end};
    }

    /// Creates description of columns of data type Object from the range of data parts.
    static ColumnsDescription getConcreteObjectColumns(
        boost::iterator_range<DataPartIteratorByStateAndInfo> range, const ColumnsDescription & storage_columns);

    std::optional<UInt64> totalRowsByPartitionPredicateImpl(
        const SelectQueryInfo & query_info, ContextPtr context, const DataPartsVector & parts) const;

    static decltype(auto) getStateModifier(DataPartState state)
    {
        return [state] (const DataPartPtr & part) { part->setState(state); };
    }

    void modifyPartState(DataPartIteratorByStateAndInfo it, DataPartState state)
    {
        if (!data_parts_by_state_and_info.modify(it, getStateModifier(state)))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't modify {}", (*it)->getNameWithState());
    }

    void modifyPartState(DataPartIteratorByInfo it, DataPartState state)
    {
        if (!data_parts_by_state_and_info.modify(data_parts_indexes.project<TagByStateAndInfo>(it), getStateModifier(state)))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't modify {}", (*it)->getNameWithState());
    }

    void modifyPartState(const DataPartPtr & part, DataPartState state)
    {
        auto it = data_parts_by_info.find(part->info);
        if (it == data_parts_by_info.end() || (*it).get() != part.get())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} doesn't exist (info: {})", part->name, part->info.getPartNameForLogs());

        if (!data_parts_by_state_and_info.modify(data_parts_indexes.project<TagByStateAndInfo>(it), getStateModifier(state)))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't modify {}", (*it)->getNameWithState());
    }

    /// Used to serialize calls to grabOldParts.
    std::mutex grab_old_parts_mutex;
    /// The same for clearOldTemporaryDirectories.
    std::mutex clear_old_temporary_directories_mutex;

    void checkProperties(const StorageInMemoryMetadata & new_metadata, const StorageInMemoryMetadata & old_metadata, bool attach, bool allow_empty_sorting_key, ContextPtr local_context) const;

    void setProperties(const StorageInMemoryMetadata & new_metadata, const StorageInMemoryMetadata & old_metadata, bool attach = false, ContextPtr local_context = nullptr);

    void checkPartitionKeyAndInitMinMax(const KeyDescription & new_partition_key);

    void checkTTLExpressions(const StorageInMemoryMetadata & new_metadata, const StorageInMemoryMetadata & old_metadata) const;

    void checkStoragePolicy(const StoragePolicyPtr & new_storage_policy) const;

    /// Calculates column and secondary indexes sizes in compressed form for the current state of data_parts. Call with data_parts mutex locked.
    void calculateColumnAndSecondaryIndexSizesImpl();

    /// Adds or subtracts the contribution of the part to compressed column and secondary indexes sizes.
    void addPartContributionToColumnAndSecondaryIndexSizes(const DataPartPtr & part);
    void removePartContributionToColumnAndSecondaryIndexSizes(const DataPartPtr & part);

    /// If there is no part in the partition with ID `partition_id`, returns empty ptr. Should be called under the lock.
    DataPartPtr getAnyPartInPartition(const String & partition_id, DataPartsLock & data_parts_lock) const;

    /// Return parts in the Active set that are covered by the new_part_info or the part that covers it.
    /// Will check that the new part doesn't already exist and that it doesn't intersect existing part.
    DataPartsVector getActivePartsToReplace(
        const MergeTreePartInfo & new_part_info,
        const String & new_part_name,
        DataPartPtr & out_covering_part,
        DataPartsLock & data_parts_lock) const;

    DataPartsVector getCoveredOutdatedParts(
        const DataPartPtr & part,
        DataPartsLock & data_parts_lock) const;

    struct PartHierarchy
    {
        DataPartPtr duplicate_part;
        DataPartsVector covering_parts;
        DataPartsVector covered_parts;
        DataPartsVector intersected_parts;
    };

    PartHierarchy getPartHierarchy(
        const MergeTreePartInfo & part_info,
        DataPartState state,
        DataPartsLock & /* data_parts_lock */) const;

    /// Checks whether the column is in the primary key, possibly wrapped in a chain of functions with single argument.
    bool isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(const ASTPtr & node, const StorageMetadataPtr & metadata_snapshot) const;

    /// Common part for |freezePartition()| and |freezeAll()|.
    PartitionCommandsResultInfo freezePartitionsByMatcher(MatcherFn matcher, const StorageMetadataPtr & metadata_snapshot, const String & with_name, ContextPtr context);
    PartitionCommandsResultInfo unfreezePartitionsByMatcher(MatcherFn matcher, const String & backup_name, ContextPtr context);

    // Partition helpers
    bool canReplacePartition(const DataPartPtr & src_part) const;

    /// Tries to drop part in background without any waits or throwing exceptions in case of errors.
    virtual void dropPartNoWaitNoThrow(const String & part_name) = 0;

    virtual void dropPart(const String & part_name, bool detach, ContextPtr context) = 0;
    virtual void dropPartition(const ASTPtr & partition, bool detach, ContextPtr context) = 0;
    virtual PartitionCommandsResultInfo attachPartition(const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, bool part, ContextPtr context) = 0;
    virtual void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr context) = 0;
    virtual void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr context) = 0;

    virtual void fetchPartition(
        const ASTPtr & partition,
        const StorageMetadataPtr & metadata_snapshot,
        const String & from,
        bool fetch_part,
        ContextPtr query_context);

    virtual void movePartitionToShard(const ASTPtr & partition, bool move_part, const String & to, ContextPtr query_context);

    void writePartLog(
        PartLogElement::Type type,
        const ExecutionStatus & execution_status,
        UInt64 elapsed_ns,
        const String & new_part_name,
        const DataPartPtr & result_part,
        const DataPartsVector & source_parts,
        const MergeListEntry * merge_entry,
        std::shared_ptr<ProfileEvents::Counters::Snapshot> profile_counters);

    /// If part is assigned to merge or mutation (possibly replicated)
    /// Should be overridden by children, because they can have different
    /// mechanisms for parts locking
    virtual bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const = 0;

    /// Return most recent mutations commands for part which weren't applied
    /// Used to receive AlterConversions for part and apply them on fly. This
    /// method has different implementations for replicated and non replicated
    /// MergeTree because they store mutations in different way.
    virtual std::map<int64_t, MutationCommands> getAlterMutationCommandsForPart(const DataPartPtr & part) const = 0;
    /// Moves part to specified space, used in ALTER ... MOVE ... queries
    MovePartsOutcome movePartsToSpace(const DataPartsVector & parts, SpacePtr space);

    struct PartBackupEntries
    {
        String part_name;
        UInt128 part_checksum; /// same as MinimalisticDataPartChecksums::hash_of_all_files
        BackupEntries backup_entries;
    };
    using PartsBackupEntries = std::vector<PartBackupEntries>;

    /// Makes backup entries to backup the parts of this table.
    PartsBackupEntries backupParts(const DataPartsVector & data_parts, const String & data_path_in_backup, const BackupSettings & backup_settings, const ReadSettings & read_settings, const ContextPtr & local_context);

    class RestoredPartsHolder;

    /// Restores the parts of this table from backup.
    void restorePartsFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions);
    void restorePartFromBackup(std::shared_ptr<RestoredPartsHolder> restored_parts_holder, const MergeTreePartInfo & part_info, const String & part_path_in_backup) const;

    /// Attaches restored parts to the storage.
    virtual void attachRestoredParts(MutableDataPartsVector && parts) = 0;

    void resetObjectColumnsFromActiveParts(const DataPartsLock & lock);
    void updateObjectColumns(const DataPartPtr & part, const DataPartsLock & lock);

    /** A structure that explicitly represents a "merge tree" of parts
     *  which is implicitly presented by min-max block numbers and levels of parts.
     *  The children of node are parts which are covered by parent part.
     *  This tree provides the order of loading of parts.
     *
     *  We start to traverse tree from the top level and load parts
     *  corresposponded to nodes. If part is loaded successfully then
     *  we stop traversal at this node. Otherwise part is broken and we
     *  traverse its children and try to load covered parts which will
     *  replace broken covering part. Unloaded nodes represent outdated parts
     *  nd they are pushed to background task and loaded asynchronoulsy.
     */
    class PartLoadingTree
    {
    public:
        struct Node
        {
            Node(const MergeTreePartInfo & info_, const String & name_, const DiskPtr & disk_)
                : info(info_), name(name_), disk(disk_)
            {
            }

            const MergeTreePartInfo info;
            const String name;
            const DiskPtr disk;

            bool is_loaded = false;
            std::map<MergeTreePartInfo, std::shared_ptr<Node>> children;
        };

        struct PartLoadingInfo
        {
            PartLoadingInfo(const MergeTreePartInfo & info_, const String & name_, const DiskPtr & disk_)
                : info(info_), name(name_), disk(disk_)
            {
            }

            /// Store name explicitly because it cannot be easily
            /// retrieved from info in tables with old syntax.
            MergeTreePartInfo info;
            String name;
            DiskPtr disk;
        };

        using NodePtr = std::shared_ptr<Node>;
        using PartLoadingInfos = std::vector<PartLoadingInfo>;

        /// Builds a tree from the list of part infos.
        static PartLoadingTree build(PartLoadingInfos nodes);

        /// Traverses a tree and call @func on each node.
        /// If recursive is false traverses only the top level.
        template <typename Func>
        void traverse(bool recursive, Func && func);

    private:
        /// NOTE: Parts should be added in descending order of their levels
        /// because rearranging tree to the new root is not supported.
        void add(const MergeTreePartInfo & info, const String & name, const DiskPtr & disk);
        std::unordered_map<String, NodePtr> root_by_partition;
    };

    using PartLoadingTreeNodes = std::vector<PartLoadingTree::NodePtr>;

    struct LoadPartResult
    {
        bool is_broken = false;
        std::optional<size_t> size_of_part;
        MutableDataPartPtr part;
    };

    mutable std::mutex outdated_data_parts_mutex;
    mutable std::condition_variable outdated_data_parts_cv;

    BackgroundSchedulePool::TaskHolder outdated_data_parts_loading_task;
    PartLoadingTreeNodes outdated_unloaded_data_parts TSA_GUARDED_BY(outdated_data_parts_mutex);
    bool outdated_data_parts_loading_canceled TSA_GUARDED_BY(outdated_data_parts_mutex) = false;

    /// This has to be "true" by default, because in case of empty table or absence of Outdated parts
    /// it is automatically finished.
    bool outdated_data_parts_loading_finished TSA_GUARDED_BY(outdated_data_parts_mutex) = true;

    void loadOutdatedDataParts(bool is_async);
    void startOutdatedDataPartsLoadingTask();
    void stopOutdatedDataPartsLoadingTask();

    static void incrementInsertedPartsProfileEvent(MergeTreeDataPartType type);
    static void incrementMergedPartsProfileEvent(MergeTreeDataPartType type);

    bool addTempPart(
        MutableDataPartPtr & part,
        Transaction & out_transaction,
        DataPartsLock & lock,
        DataPartsVector * out_covered_parts);

private:
    /// Checking that candidate part doesn't break invariants: correct partition
    void checkPartPartition(MutableDataPartPtr & part, DataPartsLock & lock) const;
    void checkPartDuplicate(MutableDataPartPtr & part, Transaction & transaction, DataPartsLock & lock) const;
    void checkPartDynamicColumns(MutableDataPartPtr & part, DataPartsLock & lock) const;

    /// Preparing itself to be committed in memory: fill some fields inside part, add it to data_parts_indexes
    /// in precommitted state and to transaction
    void preparePartForCommit(MutableDataPartPtr & part, Transaction & out_transaction, bool need_rename);

    /// Low-level method for preparing parts for commit (in-memory).
    /// FIXME Merge MergeTreeTransaction and Transaction
    bool renameTempPartAndReplaceImpl(
        MutableDataPartPtr & part,
        Transaction & out_transaction,
        DataPartsLock & lock,
        DataPartsVector * out_covered_parts);

    /// RAII Wrapper for atomic work with currently moving parts
    /// Acquire them in constructor and remove them in destructor
    /// Uses data.currently_moving_parts_mutex
    struct CurrentlyMovingPartsTagger
    {
        MergeTreeMovingParts parts_to_move;
        MergeTreeData & data;
        CurrentlyMovingPartsTagger(MergeTreeMovingParts && moving_parts_, MergeTreeData & data_);

        ~CurrentlyMovingPartsTagger();
    };

    using CurrentlyMovingPartsTaggerPtr = std::shared_ptr<CurrentlyMovingPartsTagger>;

    /// Move selected parts to corresponding disks
    MovePartsOutcome moveParts(const CurrentlyMovingPartsTaggerPtr & moving_tagger, bool wait_for_move_if_zero_copy=false);

    /// Select parts for move and disks for them. Used in background moving processes.
    CurrentlyMovingPartsTaggerPtr selectPartsForMove();

    /// Check selected parts for movements. Used by ALTER ... MOVE queries.
    CurrentlyMovingPartsTaggerPtr checkPartsForMove(const DataPartsVector & parts, SpacePtr space);

    bool canUsePolymorphicParts(const MergeTreeSettings & settings, String & out_reason) const;

    std::mutex write_ahead_log_mutex;
    WriteAheadLogPtr write_ahead_log;

    virtual void startBackgroundMovesIfNeeded() = 0;

    bool allow_nullable_key{};

    void addPartContributionToDataVolume(const DataPartPtr & part);
    void removePartContributionToDataVolume(const DataPartPtr & part);

    void increaseDataVolume(ssize_t bytes, ssize_t rows, ssize_t parts);
    void setDataVolume(size_t bytes, size_t rows, size_t parts);

    std::atomic<size_t> total_active_size_bytes = 0;
    std::atomic<size_t> total_active_size_rows = 0;
    std::atomic<size_t> total_active_size_parts = 0;

    mutable std::atomic<size_t> total_outdated_parts_count = 0;

    // Record all query ids which access the table. It's guarded by `query_id_set_mutex` and is always mutable.
    mutable std::set<String> query_id_set TSA_GUARDED_BY(query_id_set_mutex);
    mutable std::mutex query_id_set_mutex;

    // Get partition matcher for FREEZE / UNFREEZE queries.
    MatcherFn getPartitionMatcher(const ASTPtr & partition, ContextPtr context) const;

    /// Returns default settings for storage with possible changes from global config.
    virtual std::unique_ptr<MergeTreeSettings> getDefaultSettings() const = 0;

    LoadPartResult loadDataPart(
        const MergeTreePartInfo & part_info,
        const String & part_name,
        const DiskPtr & part_disk_ptr,
        MergeTreeDataPartState to_state,
        std::mutex & part_loading_mutex);

    LoadPartResult loadDataPartWithRetries(
        const MergeTreePartInfo & part_info,
        const String & part_name,
        const DiskPtr & part_disk_ptr,
        MergeTreeDataPartState to_state,
        std::mutex & part_loading_mutex,
        size_t backoff_ms,
        size_t max_backoff_ms,
        size_t max_tries);

    std::vector<LoadPartResult> loadDataPartsFromDisk(PartLoadingTreeNodes & parts_to_load);

    void loadDataPartsFromWAL(MutableDataPartsVector & parts_from_wal);

    /// Create zero-copy exclusive lock for part and disk. Useful for coordination of
    /// distributed operations which can lead to data duplication. Implemented only in ReplicatedMergeTree.
    virtual std::optional<ZeroCopyLock> tryCreateZeroCopyExclusiveLock(const String &, const DiskPtr &) { return std::nullopt; }
    virtual bool waitZeroCopyLockToDisappear(const ZeroCopyLock &, size_t) { return false; }

    /// Remove parts from disk calling part->remove(). Can do it in parallel in case of big set of parts and enabled settings.
    /// If we fail to remove some part and throw_on_error equal to `true` will throw an exception on the first failed part.
    /// Otherwise, in non-parallel case will break and return.
    void clearPartsFromFilesystemImpl(const DataPartsVector & parts, NameSet * part_names_succeed);

    static MutableDataPartPtr asMutableDeletingPart(const DataPartPtr & part);

    mutable TemporaryParts temporary_parts;

    /// Estimate the number of marks to read to make a decision whether to enable parallel replicas (distributed processing) or not
    /// Note: it could be very rough.
    bool canUseParallelReplicasBasedOnPKAnalysis(
        ContextPtr query_context,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info) const;
};

/// RAII struct to record big parts that are submerging or emerging.
/// It's used to calculate the balanced statistics of JBOD array.
struct CurrentlySubmergingEmergingTagger
{
    MergeTreeData & storage;
    String emerging_part_name;
    MergeTreeData::DataPartsVector submerging_parts;
    Poco::Logger * log;

    CurrentlySubmergingEmergingTagger(
        MergeTreeData & storage_, const String & name_, MergeTreeData::DataPartsVector && parts_, Poco::Logger * log_)
        : storage(storage_), emerging_part_name(name_), submerging_parts(std::move(parts_)), log(log_)
    {
    }

    ~CurrentlySubmergingEmergingTagger();
};


/// TODO: move it somewhere
[[ maybe_unused ]] static bool needSyncPart(size_t input_rows, size_t input_bytes, const MergeTreeSettings & settings)
{
    return ((settings.min_rows_to_fsync_after_merge && input_rows >= settings.min_rows_to_fsync_after_merge)
        || (settings.min_compressed_bytes_to_fsync_after_merge && input_bytes >= settings.min_compressed_bytes_to_fsync_after_merge));
}

}
