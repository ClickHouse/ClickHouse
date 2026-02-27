#pragma once

#include <mutex>
#include <tuple>
#include <base/defines.h>
#include <Common/AggregatedMetrics.h>
#include <Common/SimpleIncrement.h>
#include <Common/SharedMutex.h>
#include <Common/MultiVersion.h>
#include <Common/Logger.h>
#include <Storages/IStorage.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/StoragePolicy.h>
#include <Processors/Merges/Algorithms/Graphite.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/BackgroundJobsAssignee.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartBuilder.h>
#include <Storages/MergeTree/MergeTreePartsMover.h>
#include <Storages/MergeTree/PinnedPartUUIDs.h>
#include <Storages/MergeTree/ZeroCopyLock.h>
#include <Storages/MergeTree/TemporaryParts.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/IndicesDescription.h>
#include <Storages/DataDestinationType.h>
#include <Storages/extractKeyExpressionList.h>
#include <Storages/PartitionCommands.h>
#include <Storages/MergeTree/EphemeralLockInZooKeeper.h>
#include <Interpreters/PartLog.h>
#include <Poco/Timestamp.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/global_fun.hpp>
#include <boost/range/iterator_range_core.hpp>

namespace DB
{

/// Number of streams is not number parts, but number or parts*files, hence 100.
const size_t DEFAULT_DELAYED_STREAMS_FOR_PARALLEL_WRITE = 100;

struct AlterCommand;
class AlterCommands;
class InterpreterSelectQuery;
class MergeTreePartsMover;
class MergeTreeDataMergerMutator;
class MutationCommands;
class Context;
struct JobAndPool;
class MergeTreeTransaction;
struct ZeroCopyLock;

class IBackupEntry;
using BackupEntries = std::vector<std::pair<String, std::shared_ptr<const IBackupEntry>>>;

class MergeTreeTransaction;
using MergeTreeTransactionPtr = std::shared_ptr<MergeTreeTransaction>;

struct MergeTreeSettings;
struct WriteSettings;

class MarkCache;
using MarkCachePtr = std::shared_ptr<MarkCache>;

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
using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct DataPartsLock
{
    explicit DataPartsLock(SharedMutex & data_parts_mutex_, const MergeTreeData * data_);
    ~DataPartsLock();

    DataPartsLock(const DataPartsLock &) = delete;
    DataPartsLock & operator=(const DataPartsLock &) = delete;
    DataPartsLock(DataPartsLock &&) = default;
    DataPartsLock & operator=(DataPartsLock &&) = default;

private:
    std::optional<Stopwatch> wait_watch;
    std::unique_lock<DB::SharedMutex> lock;
    std::optional<Stopwatch> lock_watch;
    const MergeTreeData * data;
};

struct DataPartsSharedLock
{
    explicit DataPartsSharedLock(DB::SharedMutex & data_parts_mutex_);
    ~DataPartsSharedLock();

    DataPartsSharedLock(const DataPartsSharedLock &) = delete;
    DataPartsSharedLock & operator=(const DataPartsSharedLock &) = delete;
    DataPartsSharedLock(DataPartsSharedLock &&) = default;
    DataPartsSharedLock & operator=(DataPartsSharedLock &&) = default;

    void unlock() { lock.unlock(); }

private:
    std::optional<Stopwatch> wait_watch;
    std::shared_lock<DB::SharedMutex> lock;
    std::optional<Stopwatch> lock_watch;
};

// Some functions can accept either type of lock at the caller's preference.
class DataPartsAnyLock final
{
public:
    DataPartsAnyLock(const DataPartsAnyLock &) = delete;
    DataPartsAnyLock(DataPartsAnyLock &&) = delete;

    DataPartsAnyLock(const DataPartsLock &) noexcept {} // NOLINT(google-explicit-constructor)
    DataPartsAnyLock(const DataPartsSharedLock &) noexcept {} // NOLINT(google-explicit-constructor)
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

class MergeTreeData : public WithMutableContext, public IStorage, public IBackgroundOperation
{
public:
    /// Function to call if the part is suspected to contain corrupt data.
    using BrokenPartCallback = std::function<void (const String &)>;
    using DataPart = IMergeTreeDataPart;

    using MutableDataPartPtr = std::shared_ptr<DataPart>;
    using MutableDataPartsVector = std::vector<MutableDataPartPtr>;
    /// After the DataPart is added to the working set, it cannot be changed.
    using DataPartPtr = std::shared_ptr<const DataPart>;

    using DataPartKind = MergeTreePartInfo::Kind;
    using DataPartsKinds = std::initializer_list<DataPartKind>;

    using DataPartState = MergeTreeDataPartState;
    using DataPartStates = std::initializer_list<DataPartState>;
    using DataPartStateVector = std::vector<DataPartState>;

    using PinnedPartUUIDsPtr = std::shared_ptr<const PinnedPartUUIDs>;

    using PartitionIdToMinBlock = std::unordered_map<String, Int64>;
    using PartitionIdToMinBlockPtr = std::shared_ptr<const PartitionIdToMinBlock>;

    constexpr static auto FORMAT_VERSION_FILE_NAME = "format_version.txt";
    constexpr static auto DETACHED_DIR_NAME = "detached";
    constexpr static auto MOVING_DIR_NAME = "moving";

    /// Auxiliary structure for index comparison. Keep in mind lifetime of MergeTreePartInfo.
    struct DataPartStateAndInfo
    {
        DataPartState state;
        const MergeTreePartInfo & info;
    };

    struct DataPartStateAndKind
    {
        DataPartState state;
        DataPartKind kind;
    };

    /// Auxiliary structure for index comparison
    struct DataPartStateAndPartitionID
    {
        DataPartStateAndPartitionID(DataPartState state_, const String & partition_id_)
            : state(state_), kind(MergeTreePartInfo::getKind(partition_id_)), partition_id(partition_id_)
        {
        }

        DataPartState state;
        MergeTreePartInfo::Kind kind;
        String partition_id;
    };

    struct PartitionID
    {
        explicit PartitionID(const String & partition_id_)
            : kind(MergeTreePartInfo::getKind(partition_id_)), partition_id(partition_id_)
        {
        }

        MergeTreePartInfo::Kind kind;
        String partition_id;
    };

    struct LessDataPart
    {
        using is_transparent = void;

        bool operator()(const DataPartPtr & lhs, const MergeTreePartInfo & rhs) const { return lhs->info < rhs; }
        bool operator()(const MergeTreePartInfo & lhs, const DataPartPtr & rhs) const { return lhs < rhs->info; }
        bool operator()(const DataPartPtr & lhs, const DataPartPtr & rhs) const { return lhs->info < rhs->info; }

        bool operator()(const MergeTreePartInfo & lhs, const PartitionID & rhs) const
        {
            return std::forward_as_tuple(lhs.getKind(), lhs.getPartitionId()) < std::forward_as_tuple(rhs.kind, rhs.partition_id);
        }

        bool operator()(const PartitionID & lhs, const MergeTreePartInfo & rhs) const
        {
            return std::forward_as_tuple(lhs.kind, lhs.partition_id) < std::forward_as_tuple(rhs.getKind(), rhs.getPartitionId());
        }
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
            return static_cast<UInt8>(info.state) < static_cast<UInt8>(state);
        }

        bool operator() (const DataPartState & state, DataPartStateAndInfo info) const
        {
            return static_cast<UInt8>(state) < static_cast<UInt8>(info.state);
        }

        bool operator() (const DataPartStateAndInfo & lhs, const DataPartStateAndPartitionID & rhs) const
        {
            return std::forward_as_tuple(static_cast<UInt8>(lhs.state), lhs.info.getKind(), lhs.info.getPartitionId())
                   < std::forward_as_tuple(static_cast<UInt8>(rhs.state), rhs.kind, rhs.partition_id);
        }

        bool operator() (const DataPartStateAndPartitionID & lhs, const DataPartStateAndInfo & rhs) const
        {
            return std::forward_as_tuple(static_cast<UInt8>(lhs.state), lhs.kind, lhs.partition_id)
                   < std::forward_as_tuple(static_cast<UInt8>(rhs.state), rhs.info.getKind(), rhs.info.getPartitionId());
        }

        bool operator() (const DataPartStateAndKind & lhs, const DataPartStateAndInfo & rhs) const
        {
            return std::forward_as_tuple(static_cast<UInt8>(lhs.state), lhs.kind)
                   < std::forward_as_tuple(static_cast<UInt8>(rhs.state), rhs.info.getKind());
        }

        bool operator() (const DataPartStateAndInfo & lhs, const DataPartStateAndKind & rhs) const
        {
            return std::forward_as_tuple(static_cast<UInt8>(lhs.state), lhs.info.getKind())
                   < std::forward_as_tuple(static_cast<UInt8>(rhs.state), rhs.kind);
        }
    };

    using DataParts = std::set<DataPartPtr, LessDataPart>;
    using MutableDataParts = std::set<MutableDataPartPtr, LessDataPart>;
    using DataPartsVector = std::vector<DataPartPtr>;
    using DataPartsVectorPtr = std::shared_ptr<const DataPartsVector>;

    DataPartsLock lockParts() { return DataPartsLock(data_parts_mutex, this); }
    DataPartsSharedLock readLockParts() const { return DataPartsSharedLock(data_parts_mutex); }

    using OperationDataPartsLock = std::unique_lock<std::mutex>;
    OperationDataPartsLock lockOperationsWithParts() const { return OperationDataPartsLock(operation_with_data_parts_mutex); }

    MergeTreeDataPartFormat
    choosePartFormat(size_t bytes_uncompressed, size_t rows_count, UInt32 part_level, ProjectionDescriptionRawPtr projection) const;

    MergeTreeDataPartFormat choosePartFormatOnDisk(size_t bytes_uncompressed, size_t rows_count) const;

    /// return pair <exists, pointer to part>. Sometimes we may fail to load existing part (network issues, oom and so on),
    /// in this case pair of <true, nullptr> is returned.
    std::pair<bool, MutableDataPartPtr> loadDataPartForRemovalIfExists(const String & name, bool ignore_no_such_key = false);

    MergeTreeDataPartBuilder getDataPartBuilder(
        const String & name,
        const VolumePtr & volume,
        const String & part_dir,
        const ReadSettings & read_settings) const;

    /// Auxiliary object to add a set of parts into the working set in two steps:
    /// * First, as PreActive parts (the parts are ready, but not yet in the active set).
    /// * Next, if commit() is called, the parts are added to the active set and the parts that are
    ///   covered by them are marked Outdated.
    /// If neither commit() nor rollback() was called, the destructor rollbacks the operation.
    class Transaction : private boost::noncopyable
    {
    public:
        Transaction(MergeTreeData & data_, MergeTreeTransaction * txn_);

        DataPartsVector commit();
        DataPartsVector commit(DataPartsLock & lock);

        /// Rename should be done explicitly, before calling commit(), to
        /// guarantee that no lock held during rename (since rename is IO
        /// bound, while data parts lock is the bottleneck)
        void renameParts();

        void addPart(MutableDataPartPtr & part, bool need_rename);

        void rollback(DataPartsLock * acquired_lock = nullptr);

        /// Immediately remove parts from table's data_parts set and change part
        /// state to temporary. Useful for new parts which not present in table.
        void rollbackPartsToTemporaryState();

        size_t size() const { return precommitted_parts.size(); }
        bool isEmpty() const { return precommitted_parts.empty(); }

        ~Transaction();
        void clear();

        TransactionID getTID() const;

    private:
        friend class MergeTreeData;

        MergeTreeData & data;
        MergeTreeTransaction * txn;

        MutableDataParts precommitted_parts;
        MutableDataParts precommitted_parts_need_rename;
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
        void addPart(const String & part_name, const String & old_dir, const String & new_dir, const DiskPtr & disk);

        /// Renames part from old_name to new_name
        void tryRenameAll();

        void rollBackAll();

        /// Renames all added parts from new_name to old_name if old name is not empty
        ~PartsTemporaryRename();

        struct RenameInfo
        {
            String part_name;
            String old_dir;
            String new_dir;
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
            Coalescing          = 8,
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
        void check(const MergeTreeSettings & settings, const StorageInMemoryMetadata & metadata) const;

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
                  LoadingStrictnessLevel mode,
                  BrokenPartCallback broken_part_callback_ = [](const String &){});

    /// Build a block of minmax and count values of a MergeTree table. These values are extracted
    /// from minmax_indices, the first expression of primary key, and part rows.
    ///
    /// has_filter - if query has no filter, bypass partition pruning completely
    ///
    /// query_info - used to filter unneeded parts
    ///
    /// parts - part set to filter
    Block getMinMaxCountProjectionBlock(
        const StorageMetadataPtr & metadata_snapshot,
        const Names & required_columns,
        const ActionsDAG * filter_dag,
        const RangesInDataParts & parts,
        const PartitionIdToMaxBlock * max_block_numbers_to_read,
        ContextPtr query_context) const;

    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr query_context,
        QueryProcessingStage::Enum to_stage,
        const StorageSnapshotPtr &,
        SelectQueryInfo & info) const override;

    ReservationPtr reserveSpace(UInt64 expected_size, VolumePtr & volume) const;
    static ReservationPtr tryReserveSpace(UInt64 expected_size, const IDataPartStorage & data_part_storage);
    static ReservationPtr reserveSpace(UInt64 expected_size, const IDataPartStorage & data_part_storage);

    StoragePolicyPtr getStoragePolicy() const override;

    bool isMergeTree() const override { return true; }

    bool supportsPrewhere() const override { return true; }

    ConditionSelectivityEstimatorPtr getConditionSelectivityEstimator(const RangesInDataParts & parts, const Names & required_columns, ContextPtr local_context) const override;

    bool supportsFinal() const override;

    bool supportsSubcolumns() const override { return true; }

    bool supportsTTL() const override { return true; }

    bool supportsDynamicSubcolumns() const override { return true; }
    bool supportsSparseSerialization() const override { return true; }

    bool supportsLightweightDelete() const override;

    bool hasProjection() const override;

    bool areAsynchronousInsertsEnabled() const override;

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr & storage_snapshot, ContextPtr query_context) const override;

    /// A snapshot of pending mutations that weren't applied to some of the parts yet
    /// and should be applied on the fly (i.e. when reading from the part).
    /// Mutations not supported by AlterConversions (isSupported*Mutation) can be omitted.
    struct IMutationsSnapshot
    {
        /// Contains info that doesn't depend on state of mutations.
        struct Params
        {
            Int64 metadata_version = -1;
            Int64 min_part_metadata_version = -1;
            PartitionIdToMinBlockPtr min_part_data_versions = nullptr;
            PartitionIdToMaxBlockPtr max_mutation_versions = nullptr;
            bool need_data_mutations = false;
            bool need_alter_mutations = false;
            bool need_patch_parts = false;
        };

        static Int64 getMinPartDataVersionForPartition(const Params & params, const String & partition_id);
        static Int64 getMaxMutationVersionForPartition(const Params & params, const String & partition_id);

        static bool needIncludeMutationToSnapshot(const Params & params, const MutationCommands & commands);

        virtual ~IMutationsSnapshot() = default;
        virtual void addPatches(DataPartsVector patches_) = 0;

        /// Returns mutation commands that are required to be applied to the `part`.
        /// @return list of mutation commands in order: oldest to newest.
        virtual MutationCommands getOnFlyMutationCommandsForPart(const DataPartPtr & part) const = 0;
        virtual PatchParts getPatchesForPart(const DataPartPtr & part) const = 0;
        virtual std::shared_ptr<IMutationsSnapshot> cloneEmpty() const = 0;
        virtual NameSet getAllUpdatedColumns() const = 0;

        virtual bool hasPatchParts() const = 0;
        virtual bool hasDataMutations() const = 0;
        virtual bool hasAlterMutations() const = 0;
        virtual bool hasMetadataMutations() const = 0;
    };

    struct MutationsSnapshotBase : public IMutationsSnapshot
    {
    public:
        Params params;
        MutationCounters counters;
        PatchesByPartition patches_by_partition;

        MutationsSnapshotBase() = default;
        MutationsSnapshotBase(Params params_, MutationCounters counters_, DataPartsVector patches_);

        void addPatches(DataPartsVector patches_) override;
        PatchParts getPatchesForPart(const DataPartPtr & part) const final;

        bool hasPatchParts() const final { return !patches_by_partition.empty(); }
        bool hasDataMutations() const final { return counters.num_data > 0; }
        bool hasAlterMutations() const final { return counters.num_alter > 0; }
        bool hasMetadataMutations() const final { return counters.num_metadata > 0; }
        bool hasAnyMutations() const { return hasDataMutations() || hasAlterMutations() || hasMetadataMutations(); }

    protected:
        NameSet getColumnsUpdatedInPatches() const;
        void addSupportedCommands(const MutationCommands & commands, UInt64 mutation_version, MutationCommands & result_commands) const;
    };

    using MutationsSnapshotPtr = std::shared_ptr<const IMutationsSnapshot>;

    /// Snapshot for MergeTree contains the current set of data parts
    /// and mutations required to be applied at the moment of the start of query.
    struct SnapshotData : public StorageSnapshot::Data
    {
        /// Hold a reference to the storage since the snapshot cache in query context
        /// may outlive the storage and delay destruction of data parts.
        ConstStoragePtr storage;

        // shared_ptr because lifetime is as long as some query still reading it
        // const because we are sharing across multiple queries, we cannot have things mutating this.
        RangesInDataPartsPtr parts;

        MutationsSnapshotPtr mutations_snapshot;
    };

    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const override;

    /// The same as above but does not hold vector of data parts.
    StorageSnapshotPtr getStorageSnapshotWithoutData(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const override;

    /// Load the set of data parts from disk. Call once - immediately after the object is created.
    void loadDataParts(bool skip_sanity_checks, std::optional<std::unordered_set<std::string>> expected_parts);

    /// Check the set of data parts on disk and load if needed, assuming the data on disk can change under the hood.
    /// This method allows read-only replicas of tables on a shared storage.
    void refreshDataParts(UInt64 interval_milliseconds);

    /// Returns a pointer to primary index cache if it is enabled.
    PrimaryIndexCachePtr getPrimaryIndexCache() const;
    /// Returns a pointer to primary index cache if it is enabled and required to be prewarmed.
    PrimaryIndexCachePtr getPrimaryIndexCacheToPrewarm(size_t part_uncompressed_bytes) const;
    /// Returns a pointer to primary mark cache if it is required to be prewarmed.
    MarkCachePtr getMarkCacheToPrewarm(size_t part_uncompressed_bytes) const;

    /// Prewarm mark cache and primary index cache for the most recent data parts.
    void prewarmCaches(ThreadPool & pool, MarkCachePtr mark_cache, PrimaryIndexCachePtr index_cache);

    String getLogName() const { return log.loadName(); }

    Int64 getMaxBlockNumber() const;

    struct ProjectionPartsVector
    {
        DataPartsVector data_parts;

        DataPartsVector projection_parts;
        DataPartStateVector projection_parts_states;

        DataPartsVector broken_projection_parts;
        DataPartStateVector broken_projection_parts_states;
    };

    /// Returns a copy of the list so that the caller shouldn't worry about locks.
    DataParts getDataParts(const DataPartStates & affordable_states, const DataPartsKinds & affordable_kinds) const;

    /// Returns sorted list of the parts with specified states
    /// out_states will contain snapshot of each part state
    DataPartsVector getDataPartsVectorForInternalUsage(const DataPartStates & affordable_states, const DataPartsKinds & affordable_kinds, const DataPartsAnyLock & lock, DataPartStateVector * out_states = nullptr) const;
    DataPartsVector getDataPartsVectorForInternalUsage(const DataPartStates & affordable_states, const DataPartsKinds & affordable_kinds, DataPartStateVector * out_states = nullptr) const;
    DataPartsVector getDataPartsVectorForInternalUsage(const DataPartStates & affordable_states, const DataPartsAnyLock & lock, DataPartStateVector * out_states = nullptr) const;
    DataPartsVector getDataPartsVectorForInternalUsage(const DataPartStates & affordable_states, DataPartStateVector * out_states = nullptr) const;

    /// Returns parts in Active state.
    DataParts getDataPartsForInternalUsage() const;
    DataPartsVector getDataPartsVectorForInternalUsage() const;

    /// Returns patch parts.
    DataPartsVector getPatchPartsVectorForInternalUsage(const DataPartStates & affordable_states, const DataPartsLock & lock, DataPartStateVector * out_states = nullptr) const;
    DataPartsVector getPatchPartsVectorForInternalUsage(const DataPartStates & affordable_states, DataPartStateVector * out_states = nullptr) const;
    /// Returns patch parts in Active state
    DataPartsVector getPatchPartsVectorForInternalUsage() const;
    /// Returns patch parts in Active state that relate to partition_id.
    DataPartsVector getPatchPartsVectorForPartition(const String & partition_id, const DataPartsAnyLock & lock) const;
    DataPartsVector getPatchPartsVectorForPartition(const String & partition_id) const;

    /// Returns absolutely all parts (and snapshot of their states)
    DataPartsVector getAllDataPartsVector(DataPartStateVector * out_states = nullptr) const;

    DataPartsVector getDataPartsVectorInPartitionForInternalUsage(const DataPartState & state, const String & partition_id, const DataPartsAnyLock & acquired_lock) const;
    DataPartsVector getDataPartsVectorInPartitionForInternalUsage(const DataPartStates & affordable_states, const String & partition_id, const DataPartsAnyLock & acquired_lock) const;

    /// Returns the number of data mutations suitable for applying on the fly.
    virtual MutationCounters getMutationCounters() const = 0;

    /// Same as above but only returns projection parts
    ProjectionPartsVector getAllProjectionPartsVector(MergeTreeData::DataPartStateVector * out_states = nullptr) const;

    /// Same as above but only returns projection parts
    ProjectionPartsVector getProjectionPartsVectorForInternalUsage(
        const DataPartStates & affordable_states,
        MergeTreeData::DataPartStateVector * out_states) const;

    void filterVisibleDataParts(DataPartsVector & maybe_visible_parts, CSN snapshot_version, TransactionID current_tid) const;

    /// Returns parts that visible with current snapshot
    DataPartsVector getVisibleDataPartsVector(ContextPtr local_context) const;
    /// If using a shared lock (it guarantees no mutation has happened) and there is no transactions, we can return a shared copy of parts ranges
    std::tuple<RangesInDataPartsPtr, DataPartsVectorPtr> getPossiblySharedVisibleDataPartsRanges(ContextPtr local_context) const;
    /// Whereas if a unique lock is used, mutations could have happened, meaning shared part list *may* have been invalidated.
    DataPartsVector getVisibleDataPartsVectorUnlocked(ContextPtr local_context, const DataPartsLock & lock) const;
    DataPartsVector getVisibleDataPartsVector(const MergeTreeTransactionPtr & txn) const;
    DataPartsVector getVisibleDataPartsVector(CSN snapshot_version, TransactionID current_tid) const;

    /// Returns all parts in specified partition
    DataPartsVector getVisibleDataPartsVectorInPartition(MergeTreeTransaction * txn, const String & partition_id, const DataPartsAnyLock & acquired_lock) const;
    DataPartsVector getVisibleDataPartsVectorInPartition(ContextPtr local_context, const String & partition_id, const DataPartsAnyLock & lock) const;
    DataPartsVector getVisibleDataPartsVectorInPartition(ContextPtr local_context, const String & partition_id) const;
    DataPartsVector getVisibleDataPartsVectorInPartitions(ContextPtr local_context, const std::unordered_set<String> & partition_ids) const;

    /// Return the number of marks in all parts
    size_t getTotalMarksCount() const;

    /// Returns a part in Active state with the given name or a part containing it. If there is no such part, returns nullptr.
    DataPartPtr getActiveContainingPart(const String & part_name) const;
    DataPartPtr getActiveContainingPart(const String & part_name, const DataPartsAnyLock & lock) const;
    DataPartPtr getActiveContainingPart(const MergeTreePartInfo & part_info) const;
    DataPartPtr getActiveContainingPart(const MergeTreePartInfo & part_info, DataPartState state, const DataPartsAnyLock & lock) const;

    /// Swap part with it's identical copy (possible with another path on another disk).
    /// If original part is not active or doesn't exist exception will be thrown.
    void swapActivePart(MergeTreeData::DataPartPtr part_copy, DataPartsLock &);

    /// Returns the part with the given name and state or nullptr if no such part.
    DataPartPtr getPartIfExistsUnlocked(const String & part_name, const DataPartStates & valid_states, const DataPartsAnyLock & acquired_lock) const;
    DataPartPtr getPartIfExistsUnlocked(const MergeTreePartInfo & part_info, const DataPartStates & valid_states, const DataPartsAnyLock & acquired_lock) const;
    DataPartPtr getPartIfExists(const String & part_name, const DataPartStates & valid_states) const;
    DataPartPtr getPartIfExists(const MergeTreePartInfo & part_info, const DataPartStates & valid_states) const;

    /// Total size of active parts in bytes.
    size_t getTotalActiveSizeInBytes() const;
    size_t getTotalActiveSizeInRows() const;
    size_t getTotalUncompressedBytesInPatches() const;

    size_t getAllPartsCount() const;
    size_t getActivePartsCount() const;
    size_t getOutdatedPartsCount() const;

    size_t getNumberOfOutdatedPartsWithExpiredRemovalTime() const;

    /// Returns a pair with: max number of parts in partition across partitions; sum size of parts inside that partition.
    /// (if there are multiple partitions with max number of parts, the sum size of parts is returned for arbitrary of them)
    std::pair<size_t, size_t> getMaxPartsCountAndSizeForPartitionWithState(DataPartState state) const;

    virtual std::pair<size_t, size_t> getMaxPartsCountAndSizeForPartition() const;
    virtual size_t getMaxOutdatedPartsCountForPartition() const;

    /// Get min value of part->info.getDataVersion() for all active parts.
    /// Makes sense only for ordinary MergeTree engines because for them block numbering doesn't depend on partition.
    std::optional<Int64> getMinPartDataVersion() const;

    /// Returns all detached parts
    DetachedPartsInfo getDetachedParts() const;

    static void validateDetachedPartName(const String & name);

    void dropDetached(const ASTPtr & partition, bool part, ContextPtr context);

    /// Execute a merge of the specified parts to a temporary directory without committing.
    /// Used by OPTIMIZE ... DRY RUN PARTS.
    void optimizeDryRun(
        const Names & part_names,
        const StorageMetadataPtr & metadata_snapshot,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        ContextPtr context);

    MutableDataPartsVector tryLoadPartsToAttach(const PartitionCommand & command, ContextPtr context, PartsTemporaryRename & renamed_parts);

    bool assertNoPatchesForParts(const DataPartsVector & parts, const DataPartsVector & patches, std::string_view command, bool throw_on_error = true) const;

    /// If the table contains too many active parts, sleep for a while to give them time to merge.
    /// If until is non-null, wake up from the sleep earlier if the event happened.
    /// The decision to delay or throw is made according to settings 'parts_to_delay_insert' and 'parts_to_throw_insert'.
    void delayInsertOrThrowIfNeeded(Poco::Event * until, const ContextPtr & query_context, bool allow_throw) const;

    /// If the table contains too many unfinished mutations, sleep for a while to give them time to execute.
    /// If until is non-null, wake up from the sleep earlier if the event happened.
    /// The decision to delay or throw is made according to settings 'number_of_mutations_to_delay' and 'number_of_mutations_to_throw'.
    void delayMutationOrThrowIfNeeded(Poco::Event * until, const ContextPtr & query_context) const;

    /// If the table contains too many uncompressed bytes in patches, throw an exception.
    void throwLightweightUpdateIfNeeded(UInt64 added_uncompressed_bytes) const;

    /// Renames temporary part to a permanent part and adds it to the parts set.
    /// It is assumed that the part does not intersect with existing parts.
    /// Adds the part in the PreActive state (the part will be added to the active set later with out_transaction->commit()).
    /// Returns true if part was added. Returns false if part is covered by bigger part.
    bool renameTempPartAndAdd(
        MutableDataPartPtr & part,
        Transaction & transaction,
        DataPartsLock & lock,
        bool rename_in_transaction);

    /// The same as renameTempPartAndAdd but the block range of the part can contain existing parts.
    /// Returns all parts covered by the added part (in ascending order).
    DataPartsVector renameTempPartAndReplace(
        MutableDataPartPtr & part,
        Transaction & out_transaction,
        bool rename_in_transaction);
    DataPartsVector renameTempPartAndReplaceUnlocked(
        MutableDataPartPtr & part,
        DataPartsLock & lock,
        Transaction & out_transaction,
        bool rename_in_transaction);

    /// Unlocked version of previous one. Useful when added multiple parts with a single lock.
    bool renameTempPartAndReplaceUnlocked(
        MutableDataPartPtr & part,
        Transaction & out_transaction,
        DataPartsLock & lock,
        bool rename_in_transaction);

    /// Remove parts from working set immediately (without wait for background
    /// process). Transfer part state to temporary. Have very limited usage only
    /// for new parts which aren't already present in table.
    void removePartsFromWorkingSetImmediatelyAndSetTemporaryState(const DataPartsVector & remove);
    void removePartsFromWorkingSetImmediatelyAndSetTemporaryState(const DataPartsVector & remove, DataPartsLock & lock);

    /// Removes parts from the working set parts.
    /// Parts in add must already be in data_parts with PreActive, Active, or Outdated states.
    /// If clear_without_timeout is true, the parts will be deleted at once, or during the next call to
    /// clearOldParts (ignoring old_parts_lifetime).
    void removePartsFromWorkingSet(MergeTreeTransaction * txn, const DataPartsVector & remove, bool clear_without_timeout);
    void removePartsFromWorkingSet(MergeTreeTransaction * txn, const DataPartsVector & remove, bool clear_without_timeout, DataPartsLock & acquired_lock);

    /// Removes all parts covered by drop_range from the working set parts.
    /// Used in REPLACE PARTITION command.
    void removePartsInRangeFromWorkingSet(MergeTreeTransaction * txn, const MergeTreePartInfo & drop_range, DataPartsLock & lock);

    DataPartsVector grabActivePartsToRemoveForDropRange(
        MergeTreeTransaction * txn, const MergeTreePartInfo & drop_range, const DataPartsAnyLock & lock);

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
        MergeTreeTransaction * txn, const MergeTreePartInfo & drop_range, DataPartsLock & lock, bool create_empty_part = true, bool clone_to_detached = false);

    /// Restores Outdated part and adds it to working set
    void restoreAndActivatePart(const DataPartPtr & part);

    /// Renames the part to detached/<prefix>_<part> and removes it from data_parts,
    //// so it will not be deleted in clearOldParts.
    /// NOTE: This method is safe to use only for parts which nobody else holds (like on server start or for parts which was not committed).
    /// For active parts it's unsafe because this method modifies fields of part (rename) while some other thread can try to read it.
    void forcefullyMovePartToDetachedAndRemoveFromMemory(const DataPartPtr & part, const String & prefix = "");

    /// This method should not be here, but async loading of Outdated parts is implemented in MergeTreeData
    virtual void forcefullyRemoveBrokenOutdatedPartFromZooKeeperBeforeDetaching(const String & /*part_name*/) {}

    /// Outdate broken part, set remove time to zero (remove as fast as possible) and make clone in detached directory.
    void outdateUnexpectedPartAndCloneToDetached(const DataPartPtr & part);

    /// If the part is Obsolete and not used by anybody else, immediately delete it from filesystem and remove from memory.
    bool tryRemovePartImmediately(DataPartPtr && part);

    /// Returns old inactive parts that can be deleted. At the same time removes them from the list of parts but not from the disk.
    /// If 'force' - don't wait for old_parts_lifetime.
    DataPartsVector grabOldParts(bool force = false);

    /// Reverts the changes made by grabOldParts(), parts should be in Deleting state.
    void rollbackDeletingParts(const DataPartsVector & parts);

    /// Removes parts from data_parts, they should be in Deleting state
    void removePartsFinally(const MergeTreeData::DataPartsVector & parts, MergeTreeData::DataPartsVector * removed_parts = nullptr);

    /// Try to clear parts from filesystem.
    /// If we fail to remove some part and throw_on_error equal to `true` will throw an exception on the first failed part.
    void clearPartsFromFilesystemImpl(const DataPartsVector & parts, bool throw_on_error, NameSet * parts_failed_to_delete);

    /// Remove parts from disk calling part->remove(). Can do it in parallel in case of big set of parts and enabled settings.
    /// Throw exception in case of errors.
    /// Otherwise, in non-parallel case will break and return.
    void clearPartsFromFilesystemImplMaybeInParallel(const DataPartsVector & parts_to_remove, NameSet * part_names_succeed);

    /// Try to clear parts from filesystem.
    /// In case of error at some point for the rest of the parts its part's state is rollback Deleting - > Outdated.
    /// That allows to schedule them for deletion a bit later
    size_t clearPartsFromFilesystemAndRollbackIfError(const DataPartsVector & parts_to_delete, const String & parts_type);

    /// Delete all directories which names begin with "tmp"
    /// Must be called with locked lockForShare() because it's using relative_data_path.
    size_t clearOldTemporaryDirectories(size_t custom_directories_lifetime_seconds, const NameSet & valid_prefixes = {"tmp_", "tmp-fetch_"});
    size_t clearOldTemporaryDirectories(const String & root_path, size_t custom_directories_lifetime_seconds, const NameSet & valid_prefixes);

    size_t clearEmptyParts();

    /// Moves to outdated state patch parts that do not need to be applied to regular parts.
    size_t clearUnusedPatchParts();

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
    /// - columns corresponding to primary key, indices, sign, sampling expression, summed columns, and date are not affected.
    /// If something is wrong, throws an exception.
    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    /// Throw exception if command is some kind of DROP command (drop column, drop index, etc) or rename command
    /// and we have unfinished mutation which need this column to finish.
    void checkDropOrRenameCommandDoesntAffectInProgressMutations(
        const AlterCommand & command, const std::map<std::string, MutationCommands> & unfinished_mutations, ContextPtr context) const;

    /// Return mapping unfinished mutation name -> Mutation command
    virtual std::map<std::string, MutationCommands> getUnfinishedMutationCommands() const = 0;

    /// Checks if the Mutation can be performed.
    /// (currently no additional checks: always ok)
    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;

    /// Checks that partition name in all commands is valid
    void checkAlterPartitionIsPossible(
        const PartitionCommands & commands,
        const StorageMetadataPtr & metadata_snapshot,
        const Settings & settings,
        ContextPtr local_context) const override;

    /// Change MergeTreeSettings
    void changeSettings(
        const ASTPtr & new_settings,
        AlterLockHolder & table_lock_holder);

    static std::pair<String, bool> getNewImplicitStatisticsTypes(const StorageInMemoryMetadata & new_metadata, const MergeTreeSettings & old_settings);
    static void verifySortingKey(const KeyDescription & sorting_key);

    /// Should be called if part data is suspected to be corrupted.
    /// Has the ability to check all other parts
    /// which reside on the same disk of the suspicious part.
    virtual void reportBrokenPart(MergeTreeData::DataPartPtr data_part) const;

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
        const String & with_name,
        ContextPtr context,
        TableLockHolder & table_lock_holder);

    /// Freezes all parts.
    PartitionCommandsResultInfo freezeAll(
        const String & with_name,
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

    /// Moves partition to specified Table
    void movePartitionToTable(const PartitionCommand & command, ContextPtr query_context);

    /// Checks that Partition could be dropped right now
    /// Otherwise - throws an exception with detailed information.
    /// We do not use mutex because it is not very important that the size could change during the operation.
    void checkPartitionCanBeDropped(const ASTPtr & partition, ContextPtr local_context);

    void checkPartCanBeDropped(const String & part_name, ContextPtr local_context);

    Pipe alterPartition(
        const StorageMetadataPtr & metadata_snapshot,
        const PartitionCommands & commands,
        ContextPtr query_context) override;

    size_t getColumnCompressedSize(const std::string & name) const
    {
        /// Always keep locks order parts_lock -> sizes_lock
        auto parts_lock = readLockParts();
        std::unique_lock sizes_lock(columns_and_secondary_indices_sizes_mutex);
        calculateColumnAndSecondaryIndexSizesLazily(parts_lock, sizes_lock);
        const auto it = column_sizes.find(name);
        return it == std::end(column_sizes) ? 0 : it->second.data_compressed;
    }

    ColumnSizeByName getColumnSizes() const override
    {
        /// Always keep locks order parts_lock -> sizes_lock
        auto parts_lock = readLockParts();
        std::unique_lock sizes_lock(columns_and_secondary_indices_sizes_mutex);
        calculateColumnAndSecondaryIndexSizesLazily(parts_lock, sizes_lock);
        return column_sizes;
    }

    IndexSizeByName getSecondaryIndexSizes() const override
    {
        /// Always keep locks order parts_lock -> sizes_lock
        auto parts_lock = readLockParts();
        std::unique_lock sizes_lock(columns_and_secondary_indices_sizes_mutex);
        calculateColumnAndSecondaryIndexSizesLazily(parts_lock, sizes_lock);
        return secondary_index_sizes;
    }

    IndexSize getPrimaryIndexSize() const
    {
        /// Always keep locks order parts_lock -> sizes_lock
        auto parts_lock = readLockParts();
        std::unique_lock sizes_lock(columns_and_secondary_indices_sizes_mutex);
        calculateColumnAndSecondaryIndexSizesLazily(parts_lock, sizes_lock);
        return primary_index_size;
    }

    /// For ATTACH/DETACH/DROP/FORGET PARTITION.
    String getPartitionIDFromQuery(const ASTPtr & ast, ContextPtr context, const DataPartsLock * acquired_lock = nullptr) const;
    std::unordered_set<String> getPartitionIDsFromQuery(const ASTs & asts, ContextPtr context) const;
    std::set<String> getPartitionIdsAffectedByCommands(const MutationCommands & commands, ContextPtr query_context) const;

    /// Returns set of partition_ids of all Active parts
    std::unordered_set<String> getAllPartitionIds() const;

    /// Extracts MergeTreeData of other *MergeTree* storage
    ///  and checks that their structure suitable for ALTER TABLE ATTACH PARTITION FROM
    /// Tables structure should be locked.
    MergeTreeData & checkStructureAndGetMergeTreeData(const StoragePtr & source_table, const StorageMetadataPtr & src_snapshot, const StorageMetadataPtr & my_snapshot) const;
    MergeTreeData & checkStructureAndGetMergeTreeData(IStorage & source_table, const StorageMetadataPtr & src_snapshot, const StorageMetadataPtr & my_snapshot) const;

    std::pair<MergeTreeData::MutableDataPartPtr, scope_guard> cloneAndLoadDataPart(
        const MergeTreeData::DataPartPtr & src_part,
        const String & tmp_part_prefix,
        const MergeTreePartInfo & dst_part_info,
        const StorageMetadataPtr & metadata_snapshot,
        const IDataPartStorage::ClonePartParams & params,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        bool must_on_same_disk);

    /// Clone part for shared merge tree
    MergeTreeData::MutableDataPartPtr cloneAndLoadDataPartOnSameDiskTransacitonal(
        const MergeTreeData::DataPartPtr & src_part,
        const String & tmp_part_prefix,
        const MergeTreePartInfo & dst_part_info,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings);

    virtual std::vector<MergeTreeMutationStatus> getMutationsStatus() const = 0;

    /// Returns true if table can create new parts with adaptive granularity
    /// Has additional constraint in replicated version
    virtual bool canUseAdaptiveGranularity() const;

    /// Get constant pointer to storage settings.
    /// Copy this pointer into your scope and you will get consistent settings.
    /// When `projection` is provided, apply projection-level overrides on top of the table settings.
    MergeTreeSettingsPtr getSettings(ProjectionDescriptionRawPtr projection = nullptr) const;

    StorageMetadataPtr getInMemoryMetadataPtr(bool bypass_metadata_cache = false) const override; /// NOLINT

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

    std::map<std::string, DiskPtr> getDistinctDisksForParts(const DataPartsVector & parts_list) const;

    /// Returns a snapshot of mutations that probably will be applied on the fly to parts during reading.
    virtual MutationsSnapshotPtr getMutationsSnapshot(const IMutationsSnapshot::Params & params) const = 0;

    /// Returns the minimum version of metadata among parts.
    static Int64 getMinMetadataVersion(const DataPartsVector & parts);

    /// Returns minimum data version among parts inside each of the partitions.
    static PartitionIdToMinBlockPtr getMinDataVersionForEachPartition(const DataPartsVector & parts);

    /// Return alter conversions for part which must be applied on fly.
    static AlterConversionsPtr getAlterConversionsForPart(
        const MergeTreeDataPartPtr & part,
        const MutationsSnapshotPtr & mutations,
        const ContextPtr & query_context);

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

    static const Names virtuals_useful_for_filter;

    /// Construct a sample block of virtual columns.
    Block getHeaderWithVirtualsForFilter(const StorageMetadataPtr & metadata) const;

    /// Construct a block consisting only of possible virtual columns for part pruning.
    Block getBlockWithVirtualsForFilter(
        const StorageMetadataPtr & metadata, const RangesInDataParts & parts, bool ignore_empty = false) const;

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

    /// Schedules job to move parts between disks/volumes and so on.
    bool scheduleDataMovingJob(BackgroundJobsAssignee & assignee) override;
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

    /// Check shared data usage on other replicas for detached/frozen part
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
    void waitForUnexpectedPartsToBeLoaded() const;
    bool canUsePolymorphicParts() const;

    /// Returns cached metadata snapshot of a patch part that contains the following columns.
    StorageMetadataPtr getPatchPartMetadata(const ColumnsDescription & patch_part_desc, const String & patch_partition_id, ContextPtr local_context) const;

    static MergingParams getMergingParamsForPatchParts();

    std::expected<void, PreformattedMessage> supportsLightweightUpdate() const override;

    /// TODO: make enabled by default in the next release if no problems found.
    bool allowRemoveStaleMovingParts() const;

    /// Generate DAG filters based on query info (for PK analysis)
    static struct ActionDAGNodes getFiltersForPrimaryKeyAnalysis(const InterpreterSelectQuery & select);

    /// Estimate the number of rows to read based on primary key analysis (which could be very rough)
    /// It is used to make a decision whether to enable parallel replicas (distributed processing) or not and how
    /// many to replicas to use
    UInt64 estimateNumberOfRowsToRead(
        ContextPtr query_context, const StorageSnapshotPtr & storage_snapshot, const SelectQueryInfo & query_info) const;

    bool initializeDiskOnConfigChange(const std::set<String> & /*new_added_disks*/) override;

    static VirtualColumnsDescription createVirtuals(const StorageInMemoryMetadata & metadata);
    static VirtualColumnsDescription createProjectionVirtuals(const StorageInMemoryMetadata & metadata);

    /// Similar to IStorage::getVirtuals but returns only virtual columns valid in projection.
    VirtualsDescriptionPtr getProjectionVirtualsPtr() const { return projection_virtuals.get(); }

    /// Load/unload primary keys of all data parts
    void loadPrimaryKeys() const;
    void unloadPrimaryKeys();

    /// Unloads primary keys of outdated parts that are not used by any query.
    /// Returns the number of parts for which index was unloaded.
    size_t unloadPrimaryKeysAndClearCachesOfOutdatedParts();

protected:
    friend class IMergeTreeDataPart;
    friend class MergeTreeDataMergerMutator;
    friend struct ReplicatedMergeTreeTableMetadata;
    friend class StorageReplicatedMergeTree;
    friend class MergeTreeDataWriter;
    friend class MergeTask;
    friend class IPartMetadataManager;
    friend class IMergedBlockOutputStream; // for access to log
    friend struct DataPartsLock; // for access to shared_parts_list/shared_ranges_in_parts

    bool require_part_metadata;

    /// Relative path data, changes during rename for ordinary databases use
    /// under lockForShare if rename is possible.
    String relative_data_path;

private:
    /// Columns and secondary indices sizes can be calculated lazily.
    mutable std::mutex columns_and_secondary_indices_sizes_mutex;
    mutable bool are_columns_and_secondary_indices_sizes_calculated = false;
    /// Current column sizes in compressed and uncompressed form.
    mutable ColumnSizeByName column_sizes;
    /// Current secondary index sizes in compressed and uncompressed form.
    mutable IndexSizeByName secondary_index_sizes;
    mutable IndexSize primary_index_size;

protected:
    void loadPartAndFixMetadataImpl(MergeTreeData::MutableDataPartPtr part, ContextPtr local_context) const;

    void resetColumnSizes()
    {
        column_sizes.clear();
        are_columns_and_secondary_indices_sizes_calculated = false;
    }

private:
    struct NamesAndTypesListHash
    {
        size_t operator()(const NamesAndTypesList & list) const noexcept;
    };
    struct ColumnsDescriptionCache
    {
        std::shared_ptr<const ColumnsDescription> original;
        std::shared_ptr<const ColumnsDescription> with_collected_nested;
    };
    mutable AggregatedMetrics::GlobalSum columns_descriptions_metric_handle;
    mutable std::mutex columns_descriptions_cache_mutex;
    mutable std::unordered_map<NamesAndTypesList, ColumnsDescriptionCache, NamesAndTypesListHash> columns_descriptions_cache TSA_GUARDED_BY(columns_descriptions_cache_mutex);

public:
    ColumnsDescriptionCache getColumnsDescriptionForColumns(const NamesAndTypesList & columns) const;
    void decrefColumnsDescriptionForColumns(const NamesAndTypesList & columns) const;
    size_t getColumnsDescriptionsCacheSize() const;

protected:
    /// Engine-specific methods
    BrokenPartCallback broken_part_callback;

    AtomicLogger log;

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

    void initializeDirectoriesAndFormatVersion(const std::string & relative_data_path_, bool attach, const std::string & date_column_name, bool need_create_directories = true);

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
    /// On updates shared_parts_list/shared_ranges_in_parts should be reset (will be updated in getPossiblySharedVisibleDataPartsRanges())
    mutable DB::SharedMutex data_parts_mutex;

    /// Notified when parts transition out of PreActive state (via Transaction::commit or rollback).
    /// Used by waitForPreActivePartsInRange to avoid a race between INSERT and DROP_RANGE.
    mutable std::condition_variable_any preactive_parts_cv;

    DataPartsIndexes data_parts_indexes;
    DataPartsIndexes::index<TagByInfo>::type & data_parts_by_info;
    DataPartsIndexes::index<TagByStateAndInfo>::type & data_parts_by_state_and_info;

    /// A readonly shared copy of the `data_parts_by_state_and_info`, so we don't need to copy it for every query
    /// Pointer may only be updated if you have a DataPartsLock
    /// Pointer may only be shared if you have a DataPartsSharedLock
    mutable DataPartsVectorPtr shared_parts_list;
    /// Same as above, but this time caching a copy of RangesInDataParts
    mutable RangesInDataPartsPtr shared_ranges_in_parts;

    /// Mutex for critical sections which alter set of parts
    /// It is like truncate, drop/detach partition
    mutable std::mutex operation_with_data_parts_mutex;

    /// Serialization info accumulated among all active parts.
    /// It changes only when set of parts is changed and is
    /// protected by @data_parts_mutex.
    SerializationInfoByName serialization_hints{{}};

    /// A cache for metadata snapshots for patch parts.
    /// The key is a partition id of patch part.
    /// Patch parts in one partition always have the same structure.
    mutable std::mutex patch_parts_metadata_mutex;
    mutable std::unordered_map<String, StorageMetadataPtr> patch_parts_metadata_cache;

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

    /// Strongly connected with two fields above.
    /// Every task that is finished will ask to assign a new one into an executor.
    /// These callbacks will be passed to the constructor of each task.
    IExecutableTask::TaskResultCallback common_assignee_trigger;
    IExecutableTask::TaskResultCallback moves_assignee_trigger;

    using DataPartIteratorByInfo = DataPartsIndexes::index<TagByInfo>::type::iterator;
    using DataPartIteratorByStateAndInfo = DataPartsIndexes::index<TagByStateAndInfo>::type::iterator;

    boost::iterator_range<DataPartIteratorByStateAndInfo> getDataPartsStateRange(DataPartState state, DataPartKind kind) const
    {
        return data_parts_by_state_and_info.equal_range(DataPartStateAndKind{state, kind}, LessStateDataPart());
    }

    boost::iterator_range<DataPartIteratorByStateAndInfo> getDataPartsStateRange(DataPartState state) const
    {
        return data_parts_by_state_and_info.equal_range(state, LessStateDataPart());
    }

    boost::iterator_range<DataPartIteratorByInfo> getDataPartsPartitionRange(const String & partition_id) const
    {
        return data_parts_by_info.equal_range(PartitionID(partition_id), LessDataPart());
    }

    std::optional<UInt64> totalRowsByPartitionPredicateImpl(
        const ActionsDAG & filter_actions_dag, ContextPtr context, const RangesInDataParts & parts) const;

    static decltype(auto) getStateModifier(DataPartState state)
    {
        return [state] (const DataPartPtr & part) { part->setState(state); };
    }

    void modifyPartState(DataPartIteratorByStateAndInfo it, DataPartState state, DataPartsLock & /* parts_lock */)
    {
        if (!data_parts_by_state_and_info.modify(it, getStateModifier(state)))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't modify {}", (*it)->getNameWithState());
    }

    void modifyPartState(DataPartIteratorByInfo it, DataPartState state, DataPartsLock & /* parts_lock */)
    {
        if (!data_parts_by_state_and_info.modify(data_parts_indexes.project<TagByStateAndInfo>(it), getStateModifier(state)))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't modify {}", (*it)->getNameWithState());
    }

    void modifyPartState(const DataPartPtr & part, DataPartState state, DataPartsLock & /* parts_lock */)
    {
        auto it = data_parts_by_info.find(part->info);
        if (it == data_parts_by_info.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} doesn't exist (info: {})", part->name, part->info.getPartNameForLogs());

        if ((*it).get() != part.get())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot modify part state {} (info: {}) to {}, because there is another copy of the same part in {} state", part->name, part->info.getPartNameForLogs(), state, (*it)->getNameWithState());

        if (!data_parts_by_state_and_info.modify(data_parts_indexes.project<TagByStateAndInfo>(it), getStateModifier(state)))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't modify {}", (*it)->getNameWithState());
    }

    /// Used to serialize calls to grabOldParts.
    std::mutex grab_old_parts_mutex;
    /// The same for clearOldTemporaryDirectories.
    std::mutex clear_old_temporary_directories_mutex;
    /// The same for unloadPrimaryKeysAndClearCachesOfOutdatedParts.
    std::mutex unload_primary_key_mutex;

    void checkProperties(
        const StorageInMemoryMetadata & new_metadata,
        const StorageInMemoryMetadata & old_metadata,
        bool attach,
        bool allow_empty_sorting_key,
        bool allow_reverse_sorting_key,
        bool allow_nullable_key_,
        ContextPtr local_context) const;

    void setProperties(
        const StorageInMemoryMetadata & new_metadata,
        const StorageInMemoryMetadata & old_metadata,
        bool attach = false,
        ContextPtr local_context = nullptr);

    void checkPartitionKeyAndInitMinMax(const KeyDescription & new_partition_key);

    void checkTTLExpressions(const StorageInMemoryMetadata & new_metadata, const StorageInMemoryMetadata & old_metadata) const;

    void checkStoragePolicy(const StoragePolicyPtr & new_storage_policy) const;

    /// Calculates column and secondary indexes sizes in compressed form for the current state of data_parts. Call with data_parts mutex under lock.
    void calculateColumnAndSecondaryIndexSizesImpl(DataPartsLock & parts_lock) const;

    /// Similar to above but should be called before accessing column and secondary indexes sizes for possible lazy calculation.
    /// Call it with data_parts mutex under a shared lock, and columns_and_secondary_indices_sizes_mutex mutex under an unique lock.
    void calculateColumnAndSecondaryIndexSizesLazily(DataPartsSharedLock & parts_lock, std::unique_lock<std::mutex> & sizes_lock) const;

    /// Adds or subtracts the contribution of the part to compressed column and secondary indexes sizes.
    void addPartContributionToColumnAndSecondaryIndexSizes(const DataPartPtr & part) const;
    void addPartContributionToColumnAndSecondaryIndexSizesUnlocked(const DataPartPtr & part) const;
    void removePartContributionToColumnAndSecondaryIndexSizes(const DataPartPtr & part) const;

    /// If there is no part in the partition with ID `partition_id`, returns empty ptr. Should be called under the lock.
    DataPartPtr getAnyPartInPartition(const String & partition_id, const DataPartsAnyLock & data_parts_lock) const;

    /// Return parts in the Active set that are covered by the new_part_info or the part that covers it.
    /// Will check that the new part doesn't already exist and that it doesn't intersect existing part.
    DataPartsVector getActivePartsToReplace(
        const MergeTreePartInfo & new_part_info,
        const String & new_part_name,
        DataPartPtr & out_covering_part,
        const DataPartsAnyLock & data_parts_lock) const;

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
        const DataPartsAnyLock & /* data_parts_lock */) const;

    /// Checks whether the column is in the primary key, possibly wrapped in a chain of functions with single argument.
    bool isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(const ASTPtr & node, const StorageMetadataPtr & metadata_snapshot) const;

    /// Common part for |freezePartition()| and |freezeAll()|.
    PartitionCommandsResultInfo freezePartitionsByMatcher(MatcherFn matcher, const String & with_name, ContextPtr context);
    PartitionCommandsResultInfo unfreezePartitionsByMatcher(MatcherFn matcher, const String & backup_name, ContextPtr context);

    // Partition helpers
    bool canReplacePartition(const DataPartPtr & src_part) const;
    void checkTableCanBeDropped(ContextPtr query_context) const override;

    /// Tries to drop part in background without any waits or throwing exceptions in case of errors.
    virtual void dropPartNoWaitNoThrow(const String & part_name) = 0;

    virtual void dropPart(const String & part_name, bool detach, ContextPtr context) = 0;
    virtual void dropPartition(const ASTPtr & partition, bool detach, ContextPtr context) = 0;
    virtual PartitionCommandsResultInfo attachPartition(const PartitionCommand & command, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) = 0;
    virtual void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr context) = 0;
    virtual void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr context) = 0;

    virtual void fetchPartition(
        const ASTPtr & partition,
        const StorageMetadataPtr & metadata_snapshot,
        const String & from,
        bool fetch_part,
        ContextPtr query_context);

    virtual void forgetPartition(const ASTPtr & partition, ContextPtr context);

    virtual void movePartitionToShard(const ASTPtr & partition, bool move_part, const String & to, ContextPtr query_context);

    void writePartLog(
        PartLogElement::Type type,
        const ExecutionStatus & execution_status,
        UInt64 elapsed_ns,
        const String & new_part_name,
        const DataPartPtr & result_part,
        const DataPartsVector & source_parts,
        const MergeListEntry * merge_entry,
        std::shared_ptr<ProfileEvents::Counters::Snapshot> profile_counters,
        const Strings & mutation_ids = {});

    /// If part is assigned to merge or mutation (possibly replicated)
    /// Should be overridden by children, because they can have different
    /// mechanisms for parts locking
    virtual bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const = 0;

    struct PartBackupEntries
    {
        String part_name;
        UInt128 part_checksum; /// same as MinimalisticDataPartChecksums::hash_of_all_files
        BackupEntries backup_entries;
    };
    using PartsBackupEntries = std::vector<PartBackupEntries>;

    /// Makes backup entries to backup the parts of this table.
    PartsBackupEntries backupParts(const DataPartsVector & data_parts, const String & data_path_in_backup, const BackupSettings & backup_settings, const ContextPtr & local_context);

    class RestoredPartsHolder;

    /// Restores the parts of this table from backup.
    void restorePartsFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions);
    void restorePartFromBackup(std::shared_ptr<RestoredPartsHolder> restored_parts_holder, const MergeTreePartInfo & part_info, const String & part_path_in_backup, bool detach_if_broken) const;
    MutableDataPartPtr loadPartRestoredFromBackup(const String & part_name, const DiskPtr & disk, const String & temp_part_dir, bool detach_if_broken) const;

    /// Attaches restored parts to the storage.
    virtual void attachRestoredParts(MutableDataPartsVector && parts) = 0;

    void resetSerializationHints(const DataPartsLock & lock);

    template <typename AddedParts, typename RemovedParts>
    void updateSerializationHints(const AddedParts & added_parts, const RemovedParts & removed_parts, const DataPartsLock & lock);

    SerializationInfoByName getSerializationHints() const override;

    /** A structure that explicitly represents a "merge tree" of parts
     *  which is implicitly presented by min-max block numbers and levels of parts.
     *  The children of node are parts which are covered by parent part.
     *  This tree provides the order of loading of parts.
     *
     *  We start to traverse tree from the top level and load parts
     *  corresponding to nodes. If part is loaded successfully then
     *  we stop traversal at this node. Otherwise part is broken and we
     *  traverse its children and try to load covered parts which will
     *  replace broken covering part. Unloaded nodes represent outdated parts
     *  and they are pushed to background task and loaded asynchronously.
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
        /// @param relative_data_path - path to the table data directory on disks,
        ///   used to check for transaction metadata when parts intersect.
        static PartLoadingTree build(PartLoadingInfos nodes, const String & relative_data_path);

        /// Traverses a tree and call @func on each node.
        /// If recursive is false traverses only the top level.
        template <typename Func>
        void traverse(bool recursive, Func && func);

    private:
        /// NOTE: Parts should be added in descending order of their levels
        /// because rearranging tree to the new root is not supported.
        void add(const MergeTreePartInfo & info, const String & name, const DiskPtr & disk);
        std::unordered_map<String, NodePtr> root_by_partition;
        String relative_data_path;
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

    BackgroundSchedulePoolTaskHolder outdated_data_parts_loading_task;
    PartLoadingTreeNodes outdated_unloaded_data_parts TSA_GUARDED_BY(outdated_data_parts_mutex);
    bool outdated_data_parts_loading_canceled TSA_GUARDED_BY(outdated_data_parts_mutex) = false;

    mutable std::mutex unexpected_data_parts_mutex;
    mutable std::condition_variable unexpected_data_parts_cv;

    struct UnexpectedPartLoadState
    {
        PartLoadingTree::NodePtr loading_info;
        /// if it is covered by any unexpected part
        bool uncovered = true;
        bool is_broken = false;
        MutableDataPartPtr part;
    };

    BackgroundSchedulePoolTaskHolder unexpected_data_parts_loading_task;
    std::vector<UnexpectedPartLoadState> unexpected_data_parts;
    bool unexpected_data_parts_loading_canceled TSA_GUARDED_BY(unexpected_data_parts_mutex) = false;

    void loadUnexpectedDataParts();
    void loadUnexpectedDataPart(UnexpectedPartLoadState & state);

    /// This has to be "true" by default, because in case of empty table or absence of Outdated parts
    /// it is automatically finished.
    std::atomic_bool outdated_data_parts_loading_finished = true;
    std::atomic_bool unexpected_data_parts_loading_finished = true;

    void loadOutdatedDataParts(bool is_async);
    void startOutdatedAndUnexpectedDataPartsLoadingTask();
    void stopOutdatedAndUnexpectedDataPartsLoadingTask();

    BackgroundSchedulePoolTaskHolder refresh_parts_task;

    BackgroundSchedulePoolTaskHolder refresh_stats_task;

    mutable std::mutex stats_mutex;
    ConditionSelectivityEstimatorPtr cached_estimator;

    void startStatisticsCache();
    void refreshStatistics(UInt64 interval_seconds);

    static void incrementInsertedPartsProfileEvent(MergeTreeDataPartType type);
    static void incrementMergedPartsProfileEvent(MergeTreeDataPartType type);

    bool addTempPart(
        MutableDataPartPtr & part,
        Transaction & out_transaction,
        DataPartsLock & lock,
        DataPartsVector * out_covered_parts);

    std::vector<LoadPartResult> loadDataPartsFromDisk(PartLoadingTreeNodes & parts_to_load);

    QueryPipeline updateLightweightImpl(const MutationCommands & commands, ContextPtr query_context);

    static MutableDataPartPtr asMutableDeletingPart(const DataPartPtr & part);

private:
    /// Checking that candidate part doesn't break invariants: correct partition
    void checkPartPartition(MutableDataPartPtr & part, const DataPartsAnyLock & lock) const;
    void checkPartDuplicate(MutableDataPartPtr & part, Transaction & transaction, const DataPartsAnyLock & lock) const;

    /// Preparing itself to be committed in memory: fill some fields inside part, add it to data_parts_indexes
    /// in precommitted state and to transaction
    ///
    /// @param need_rename - rename the part
    /// @param rename_in_transaction - if set, the rename will be done as part of transaction (without holding DataPartsLock), otherwise inplace (when it does not make sense).
    void preparePartForCommit(MutableDataPartPtr & part, Transaction & out_transaction, DataPartsLock & lock, bool need_rename, bool rename_in_transaction = false);

    /// Low-level method for preparing parts for commit (in-memory).
    /// FIXME Merge MergeTreeTransaction and Transaction
    bool renameTempPartAndReplaceImpl(
        MutableDataPartPtr & part,
        Transaction & out_transaction,
        DataPartsLock & lock,
        DataPartsVector * out_covered_parts,
        bool rename_in_transaction);

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

    /// Moves part to specified space, used in ALTER ... MOVE ... queries
    std::future<MovePartsOutcome> movePartsToSpace(const CurrentlyMovingPartsTaggerPtr & moving_tagger, const ReadSettings & read_settings, const WriteSettings & write_settings, bool async);

    /// Move selected parts to corresponding disks
    MovePartsOutcome moveParts(const CurrentlyMovingPartsTaggerPtr & moving_tagger, const ReadSettings & read_settings, const WriteSettings & write_settings, bool wait_for_move_if_zero_copy);

    /// Select parts for move and disks for them. Used in background moving processes.
    CurrentlyMovingPartsTaggerPtr selectPartsForMove();

    /// Check selected parts for movements. Used by ALTER ... MOVE queries.
    CurrentlyMovingPartsTaggerPtr checkPartsForMove(const DataPartsVector & parts, SpacePtr space);

    bool canUsePolymorphicParts(const MergeTreeSettings & settings, String & out_reason) const;

    virtual void startBackgroundMovesIfNeeded() = 0;

    bool allow_nullable_key = false;
    bool allow_reverse_key = false;

    void addPartContributionToDataVolume(const DataPartPtr & part);
    void removePartContributionToDataVolume(const DataPartPtr & part);

    void increaseDataVolume(ssize_t bytes, ssize_t rows, ssize_t parts);
    void setDataVolume(size_t bytes, size_t rows, size_t parts);

    void addPartContributionToUncompressedBytesInPatches(const DataPartPtr & part);
    void removePartContributionToUncompressedBytesInPatches(const DataPartPtr & part);

    std::atomic<size_t> total_active_size_bytes = 0;
    std::atomic<size_t> total_active_size_rows = 0;
    std::atomic<size_t> total_active_size_parts = 0;

    mutable std::atomic<size_t> total_outdated_parts_count = 0;
    std::atomic<size_t> total_uncompressed_bytes_in_patches = 0;

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
        DB::SharedMutex & part_loading_mutex);

    LoadPartResult loadDataPartWithRetries(
        const MergeTreePartInfo & part_info,
        const String & part_name,
        const DiskPtr & part_disk_ptr,
        MergeTreeDataPartState to_state,
        DB::SharedMutex & part_loading_mutex,
        size_t backoff_ms,
        size_t max_backoff_ms,
        size_t max_tries);

    /// Create zero-copy exclusive lock for part and disk. Useful for coordination of
    /// distributed operations which can lead to data duplication. Implemented only in ReplicatedMergeTree.
    virtual std::optional<ZeroCopyLock> tryCreateZeroCopyExclusiveLock(const String &, const DiskPtr &) { return std::nullopt; }
    virtual bool waitZeroCopyLockToDisappear(const ZeroCopyLock &, size_t) { return false; }

    /// Remove parts from disk calling part->remove(). Can do it in parallel in case of big set of parts and enabled settings.
    /// If we fail to remove some part and throw_on_error equal to `true` will throw an exception on the first failed part.
    /// Otherwise, in non-parallel case will break and return.
    void clearPartsFromFilesystemImpl(const DataPartsVector & parts, NameSet * part_names_succeed);

    mutable TemporaryParts temporary_parts;

    MultiVersionVirtualsDescriptionPtr projection_virtuals;

    /// A regexp for files that should be prewarmed in the cache if cache_populated_by_fetch is enabled.
    MultiVersion<re2::RE2> filename_regexp_for_cache_prewarming;

    /// Estimate the number of marks to read to make a decision whether to enable parallel replicas (distributed processing) or not
    /// Note: it could be very rough.
    bool canUseParallelReplicasBasedOnPKAnalysis(
        ContextPtr query_context,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info) const;

    void checkColumnFilenamesForCollision(const StorageInMemoryMetadata & metadata, bool throw_on_error) const;
    void checkColumnFilenamesForCollision(const ColumnsDescription & columns, const MergeTreeSettings & settings, bool throw_on_error) const;

    StorageSnapshotPtr
    createStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context, bool without_data) const;

    bool isReadonlySetting(const std::string & setting_name) const;

    /// Is the disk should be searched for orphaned parts (ones that belong to a table based on file names, but located
    ///   on disks that are not a part of storage policy of the table).
    /// Sometimes it is better to bypass a disk e.g. to avoid interactions with a remote storage
    bool isDiskEligibleForOrphanedPartsSearch(DiskPtr disk) const;

    ConditionSelectivityEstimatorPtr cached_selectivity_estimator;
};

/// RAII struct to record big parts that are submerging or emerging.
/// It's used to calculate the balanced statistics of JBOD array.
struct CurrentlySubmergingEmergingTagger
{
    MergeTreeData & storage;
    String emerging_part_name;
    MergeTreeData::DataPartsVector submerging_parts;
    LoggerPtr log;

    CurrentlySubmergingEmergingTagger(
        MergeTreeData & storage_, const String & name_, MergeTreeData::DataPartsVector && parts_, LoggerPtr log_)
        : storage(storage_), emerging_part_name(name_), submerging_parts(std::move(parts_)), log(log_)
    {
    }

    ~CurrentlySubmergingEmergingTagger();
};

/// Look at MutationCommands if it contains mutations for AlterConversions, update the counter.
void incrementMutationsCounters(MutationCounters & mutation_counters, const MutationCommands & commands);
void decrementMutationsCounters(MutationCounters & mutation_counters, const MutationCommands & commands);

String replaceFileNameToHashIfNeeded(const String & file_name, const MergeTreeSettings & storage_settings, const IDataPartStorage * data_part_storage);

}
