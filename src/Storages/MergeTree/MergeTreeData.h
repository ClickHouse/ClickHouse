#pragma once

#include <Common/SimpleIncrement.h>
#include <Common/MultiVersion.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/PartDestinationType.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Merges/Algorithms/Graphite.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/MergeTreePartsMover.h>
#include <Interpreters/PartLog.h>
#include <Disks/StoragePolicy.h>
#include <Interpreters/Aggregator.h>
#include <Storages/MergeTree/TTLMode.h>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/global_fun.hpp>
#include <boost/range/iterator_range_core.hpp>


namespace DB
{

class MergeListEntry;
class AlterCommands;
class MergeTreePartsMover;
class MutationCommands;
class Context;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


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
///   of primary key (cf. CollapsingSortedBlockInputStream.h)
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

class MergeTreeData : public IStorage
{
public:
    /// Function to call if the part is suspected to contain corrupt data.
    using BrokenPartCallback = std::function<void (const String &)>;
    using DataPart = IMergeTreeDataPart;

    using MutableDataPartPtr = std::shared_ptr<DataPart>;
    using MutableDataPartsVector = std::vector<MutableDataPartPtr>;
    /// After the DataPart is added to the working set, it cannot be changed.
    using DataPartPtr = std::shared_ptr<const DataPart>;

    using DataPartState = IMergeTreeDataPart::State;
    using DataPartStates = std::initializer_list<DataPartState>;
    using DataPartStateVector = std::vector<DataPartState>;

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

    /// Alter conversions which should be applied on-fly for part. Build from of
    /// the most recent mutation commands for part. Now we have only rename_map
    /// here (from ALTER_RENAME) command, because for all other type of alters
    /// we can deduce conversions for part from difference between
    /// part->getColumns() and storage->getColumns().
    struct AlterConversions
    {
        /// Rename map new_name -> old_name
        std::unordered_map<String, String> rename_map;

        bool isColumnRenamed(const String & new_name) const { return rename_map.count(new_name) > 0; }
        String getColumnOldName(const String & new_name) const { return rename_map.at(new_name); }
    };

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
    using DataPartsVector = std::vector<DataPartPtr>;

    using DataPartsLock = std::unique_lock<std::mutex>;
    DataPartsLock lockParts() const { return DataPartsLock(data_parts_mutex); }

    MergeTreeDataPartType choosePartType(size_t bytes_uncompressed, size_t rows_count) const;

    /// After this method setColumns must be called
    MutableDataPartPtr createPart(const String & name,
        MergeTreeDataPartType type, const MergeTreePartInfo & part_info,
        const VolumePtr & volume, const String & relative_path) const;

    /// After this methods 'loadColumnsChecksumsIndexes' must be called
    MutableDataPartPtr createPart(const String & name,
        const VolumePtr & volume, const String & relative_path) const;

    MutableDataPartPtr createPart(const String & name, const MergeTreePartInfo & part_info,
        const VolumePtr & volume, const String & relative_path) const;

    /// Auxiliary object to add a set of parts into the working set in two steps:
    /// * First, as PreCommitted parts (the parts are ready, but not yet in the active set).
    /// * Next, if commit() is called, the parts are added to the active set and the parts that are
    ///   covered by them are marked Outdated.
    /// If neither commit() nor rollback() was called, the destructor rollbacks the operation.
    class Transaction : private boost::noncopyable
    {
    public:
        Transaction(MergeTreeData & data_) : data(data_) {}

        DataPartsVector commit(MergeTreeData::DataPartsLock * acquired_parts_lock = nullptr);

        void rollback();

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

    private:
        friend class MergeTreeData;

        MergeTreeData & data;
        DataParts precommitted_parts;

        void clear() { precommitted_parts.clear(); }
    };

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

        void addPart(const String & old_name, const String & new_name);

        /// Renames part from old_name to new_name
        void tryRenameAll();

        /// Renames all added parts from new_name to old_name if old name is not empty
        ~PartsTemporaryRename();

        const MergeTreeData & storage;
        const String source_dir;
        std::vector<std::pair<String, String>> old_and_new_names;
        std::unordered_map<String, PathWithDisk> old_part_name_to_path_and_disk;
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

        /// For Summing mode. If empty - columns_to_sum is determined automatically.
        Names columns_to_sum;

        /// For Replacing and VersionedCollapsing mode. Can be empty for Replacing.
        String version_column;

        /// For Graphite mode.
        Graphite::Params graphite_params;

        /// Check that needed columns are present and have correct types.
        void check(const NamesAndTypesList & columns) const;

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
                  const String & relative_data_path_,
                  const StorageInMemoryMetadata & metadata,
                  Context & context_,
                  const String & date_column_name,
                  const MergingParams & merging_params_,
                  std::unique_ptr<MergeTreeSettings> settings_,
                  bool require_part_metadata_,
                  bool attach,
                  BrokenPartCallback broken_part_callback_ = [](const String &){});


    /// See comments about methods below in IStorage interface
    StorageInMemoryMetadata getInMemoryMetadata() const override;

    ColumnDependencies getColumnDependencies(const NameSet & updated_columns) const override;

    StoragePolicyPtr getStoragePolicy() const override;

    bool supportsPrewhere() const override { return true; }

    bool supportsFinal() const override
    {
        return merging_params.mode == MergingParams::Collapsing
            || merging_params.mode == MergingParams::Summing
            || merging_params.mode == MergingParams::Aggregating
            || merging_params.mode == MergingParams::Replacing
            || merging_params.mode == MergingParams::VersionedCollapsing;
    }

    bool supportsSettings() const override { return true; }
    NamesAndTypesList getVirtuals() const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, const Context &) const override;

    /// Load the set of data parts from disk. Call once - immediately after the object is created.
    void loadDataParts(bool skip_sanity_checks);

    String getLogName() const { return log_name; }

    Int64 getMaxBlockNumber() const;

    /// Returns a copy of the list so that the caller shouldn't worry about locks.
    DataParts getDataParts(const DataPartStates & affordable_states) const;
    /// Returns sorted list of the parts with specified states
    ///  out_states will contain snapshot of each part state
    DataPartsVector getDataPartsVector(const DataPartStates & affordable_states, DataPartStateVector * out_states = nullptr) const;

    /// Returns absolutely all parts (and snapshot of their states)
    DataPartsVector getAllDataPartsVector(DataPartStateVector * out_states = nullptr) const;

    /// Returns all detached parts
    DetachedPartsInfo getDetachedParts() const;

    void validateDetachedPartName(const String & name) const;

    void dropDetached(const ASTPtr & partition, bool part, const Context & context);

    MutableDataPartsVector tryLoadPartsToAttach(const ASTPtr & partition, bool attach_part,
            const Context & context, PartsTemporaryRename & renamed_parts);

    /// Returns Committed parts
    DataParts getDataParts() const;
    DataPartsVector getDataPartsVector() const;

    /// Returns a committed part with the given name or a part containing it. If there is no such part, returns nullptr.
    DataPartPtr getActiveContainingPart(const String & part_name) const;
    DataPartPtr getActiveContainingPart(const MergeTreePartInfo & part_info) const;
    DataPartPtr getActiveContainingPart(const MergeTreePartInfo & part_info, DataPartState state, DataPartsLock & lock) const;

    /// Swap part with it's identical copy (possible with another path on another disk).
    /// If original part is not active or doesn't exist exception will be thrown.
    void swapActivePart(MergeTreeData::DataPartPtr part_copy);

    /// Returns all parts in specified partition
    DataPartsVector getDataPartsVectorInPartition(DataPartState state, const String & partition_id);

    /// Returns the part with the given name and state or nullptr if no such part.
    DataPartPtr getPartIfExists(const String & part_name, const DataPartStates & valid_states);
    DataPartPtr getPartIfExists(const MergeTreePartInfo & part_info, const DataPartStates & valid_states);

    /// Total size of active parts in bytes.
    size_t getTotalActiveSizeInBytes() const;

    size_t getTotalActiveSizeInRows() const;

    size_t getPartsCount() const;
    size_t getMaxPartsCountForPartition() const;

    /// Get min value of part->info.getDataVersion() for all active parts.
    /// Makes sense only for ordinary MergeTree engines because for them block numbering doesn't depend on partition.
    std::optional<Int64> getMinPartDataVersion() const;

    /// If the table contains too many active parts, sleep for a while to give them time to merge.
    /// If until is non-null, wake up from the sleep earlier if the event happened.
    void delayInsertOrThrowIfNeeded(Poco::Event * until = nullptr) const;
    void throwInsertIfNeeded() const;

    /// Renames temporary part to a permanent part and adds it to the parts set.
    /// It is assumed that the part does not intersect with existing parts.
    /// If increment != nullptr, part index is determing using increment. Otherwise part index remains unchanged.
    /// If out_transaction != nullptr, adds the part in the PreCommitted state (the part will be added to the
    /// active set later with out_transaction->commit()).
    /// Else, commits the part immediately.
    void renameTempPartAndAdd(MutableDataPartPtr & part, SimpleIncrement * increment = nullptr, Transaction * out_transaction = nullptr);

    /// The same as renameTempPartAndAdd but the block range of the part can contain existing parts.
    /// Returns all parts covered by the added part (in ascending order).
    /// If out_transaction == nullptr, marks covered parts as Outdated.
    DataPartsVector renameTempPartAndReplace(
        MutableDataPartPtr & part, SimpleIncrement * increment = nullptr, Transaction * out_transaction = nullptr);

    /// Low-level version of previous one, doesn't lock mutex
    void renameTempPartAndReplace(
            MutableDataPartPtr & part, SimpleIncrement * increment, Transaction * out_transaction, DataPartsLock & lock,
            DataPartsVector * out_covered_parts = nullptr);

    /// Removes parts from the working set parts.
    /// Parts in add must already be in data_parts with PreCommitted, Committed, or Outdated states.
    /// If clear_without_timeout is true, the parts will be deleted at once, or during the next call to
    /// clearOldParts (ignoring old_parts_lifetime).
    void removePartsFromWorkingSet(const DataPartsVector & remove, bool clear_without_timeout, DataPartsLock * acquired_lock = nullptr);
    void removePartsFromWorkingSet(const DataPartsVector & remove, bool clear_without_timeout, DataPartsLock & acquired_lock);

    /// Removes all parts from the working set parts
    ///  for which (partition_id = drop_range.partition_id && min_block >= drop_range.min_block && max_block <= drop_range.max_block).
    /// If a part intersecting drop_range.max_block is found, an exception will be thrown.
    /// Used in REPLACE PARTITION command;
    DataPartsVector removePartsInRangeFromWorkingSet(const MergeTreePartInfo & drop_range, bool clear_without_timeout,
                                                     bool skip_intersecting_parts, DataPartsLock & lock);

    /// Renames the part to detached/<prefix>_<part> and removes it from working set.
    void removePartsFromWorkingSetAndCloneToDetached(const DataPartsVector & parts, bool clear_without_timeout, const String & prefix = "");

    /// Renames the part to detached/<prefix>_<part> and removes it from data_parts,
    //// so it will not be deleted in clearOldParts.
    /// If restore_covered is true, adds to the working set inactive parts, which were merged into the deleted part.
    void forgetPartAndMoveToDetached(const DataPartPtr & part, const String & prefix = "", bool restore_covered = false);

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
    void clearOldPartsFromFilesystem(bool force = false);
    void clearPartsFromFilesystem(const DataPartsVector & parts);

    /// Delete all directories which names begin with "tmp"
    /// Set non-negative parameter value to override MergeTreeSettings temporary_directories_lifetime
    /// Must be called with locked lockStructureForShare().
    void clearOldTemporaryDirectories(ssize_t custom_directories_lifetime_seconds = -1);

    /// After the call to dropAllData() no method can be called.
    /// Deletes the data directory and flushes the uncompressed blocks cache and the marks cache.
    void dropAllData();

    /// Moves the entire data directory.
    /// Flushes the uncompressed blocks cache and the marks cache.
    /// Must be called with locked lockStructureForAlter().
    void rename(const String & new_table_path, const StorageID & new_table_id) override;

    /// Check if the ALTER can be performed:
    /// - all needed columns are present.
    /// - all type conversions can be done.
    /// - columns corresponding to primary key, indices, sign, sampling expression and date are not affected.
    /// If something is wrong, throws an exception.
    void checkAlterIsPossible(const AlterCommands & commands, const Settings & settings) override;

    /// Change MergeTreeSettings
    void changeSettings(
           const ASTPtr & new_settings,
           TableStructureWriteLockHolder & table_lock_holder);

    /// Freezes all parts.
    void freezeAll(const String & with_name, const Context & context, TableStructureReadLockHolder & table_lock_holder);

    /// Should be called if part data is suspected to be corrupted.
    void reportBrokenPart(const String & name) const
    {
        broken_part_callback(name);
    }

    /** Get the key expression AST as an ASTExpressionList. It can be specified
     *  in the tuple: (CounterID, Date), or as one column: CounterID.
     */
    static ASTPtr extractKeyExpressionList(const ASTPtr & node);

    bool hasSkipIndices() const { return !skip_indices.empty(); }

    bool hasAnyColumnTTL() const { return !column_ttl_entries_by_name.empty(); }
    bool hasAnyMoveTTL() const { return !move_ttl_entries.empty(); }
    bool hasRowsTTL() const override { return !rows_ttl_entry.isEmpty(); }
    bool hasAnyTTL() const override { return hasRowsTTL() || hasAnyMoveTTL() || hasAnyColumnTTL(); }

    /// Check that the part is not broken and calculate the checksums for it if they are not present.
    MutableDataPartPtr loadPartAndFixMetadata(const VolumePtr & volume, const String & relative_path) const;

    /** Create local backup (snapshot) for parts with specified prefix.
      * Backup is created in directory clickhouse_dir/shadow/i/, where i - incremental number,
      *  or if 'with_name' is specified - backup is created in directory with specified name.
      */
    void freezePartition(const ASTPtr & partition, const String & with_name, const Context & context, TableStructureReadLockHolder & table_lock_holder);


public:
    /// Moves partition to specified Disk
    void movePartitionToDisk(const ASTPtr & partition, const String & name, bool moving_part, const Context & context);

    /// Moves partition to specified Volume
    void movePartitionToVolume(const ASTPtr & partition, const String & name, bool moving_part, const Context & context);

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

    /// For ATTACH/DETACH/DROP PARTITION.
    String getPartitionIDFromQuery(const ASTPtr & ast, const Context & context);

    /// Extracts MergeTreeData of other *MergeTree* storage
    ///  and checks that their structure suitable for ALTER TABLE ATTACH PARTITION FROM
    /// Tables structure should be locked.
    MergeTreeData & checkStructureAndGetMergeTreeData(const StoragePtr & source_table) const;
    MergeTreeData & checkStructureAndGetMergeTreeData(IStorage & source_table) const;

    MergeTreeData::MutableDataPartPtr cloneAndLoadDataPartOnSameDisk(
        const MergeTreeData::DataPartPtr & src_part, const String & tmp_part_prefix, const MergeTreePartInfo & dst_part_info);

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

    /// Get disk where part is located.
    /// `additional_path` can be set if part is not located directly in table data path (e.g. 'detached/')
    DiskPtr getDiskForPart(const String & part_name, const String & additional_path = "") const;

    /// Get full path for part. Uses getDiskForPart and returns the full relative path.
    /// `additional_path` can be set if part is not located directly in table data path (e.g. 'detached/')
    std::optional<String> getFullRelativePathForPart(const String & part_name, const String & additional_path = "") const;

    Strings getDataPaths() const override;

    using PathsWithDisks = std::vector<PathWithDisk>;
    PathsWithDisks getRelativeDataPathsWithDisks() const;

    /// Reserves space at least 1MB.
    ReservationPtr reserveSpace(UInt64 expected_size) const;

    /// Reserves space at least 1MB on specific disk or volume.
    static ReservationPtr reserveSpace(UInt64 expected_size, SpacePtr space);
    static ReservationPtr tryReserveSpace(UInt64 expected_size, SpacePtr space);

    /// Reserves space at least 1MB preferring best destination according to `ttl_infos`.
    ReservationPtr reserveSpacePreferringTTLRules(UInt64 expected_size,
                                                                const IMergeTreeDataPart::TTLInfos & ttl_infos,
                                                                time_t time_of_move,
                                                                size_t min_volume_index = 0) const;
    ReservationPtr tryReserveSpacePreferringTTLRules(UInt64 expected_size,
                                                                const IMergeTreeDataPart::TTLInfos & ttl_infos,
                                                                time_t time_of_move,
                                                                size_t min_volume_index = 0) const;
    /// Choose disk with max available free space
    /// Reserves 0 bytes
    ReservationPtr makeEmptyReservationOnLargestDisk() { return getStoragePolicy()->makeEmptyReservationOnLargestDisk(); }

    /// Return alter conversions for part which must be applied on fly.
    AlterConversions getAlterConversionsForPart(const MergeTreeDataPartPtr part) const;

    MergeTreeDataFormatVersion format_version;

    Context & global_context;

    /// Merging params - what additional actions to perform during merge.
    const MergingParams merging_params;

    bool is_custom_partitioned = false;

    ExpressionActionsPtr minmax_idx_expr;
    Names minmax_idx_columns;
    DataTypes minmax_idx_column_types;
    Int64 minmax_idx_date_column_pos = -1; /// In a common case minmax index includes a date column.
    Int64 minmax_idx_time_column_pos = -1; /// In other cases, minmax index often includes a dateTime column.

    /// Secondary (data skipping) indices for MergeTree
    MergeTreeIndices skip_indices;

    ExpressionActionsPtr primary_key_and_skip_indices_expr;
    ExpressionActionsPtr sorting_key_and_skip_indices_expr;

    struct TTLEntry
    {
        TTLMode mode;

        ExpressionActionsPtr expression;
        String result_column;

        ExpressionActionsPtr where_expression;
        String where_result_column;

        Names group_by_keys;
        std::vector<std::tuple<String, String, ExpressionActionsPtr>> group_by_aggregations;
        AggregateDescriptions aggregate_descriptions;

        /// Name and type of a destination are only valid in table-level context.
        PartDestinationType destination_type;
        String destination_name;

        ASTPtr entry_ast;

        /// Returns destination disk or volume for this rule.
        SpacePtr getDestination(StoragePolicyPtr policy) const;

        /// Checks if given part already belongs destination disk or volume for this rule.
        bool isPartInDestination(StoragePolicyPtr policy, const IMergeTreeDataPart & part) const;

        bool isEmpty() const { return expression == nullptr; }
    };

    std::optional<TTLEntry> selectTTLEntryForTTLInfos(const IMergeTreeDataPart::TTLInfos & ttl_infos, time_t time_of_move) const;

    using TTLEntriesByName = std::unordered_map<String, TTLEntry>;
    TTLEntriesByName column_ttl_entries_by_name;

    TTLEntry rows_ttl_entry;

    /// This mutex is required for background move operations which do not obtain global locks.
    mutable std::mutex move_ttl_entries_mutex;

    /// Vector rw operations have to be done under "move_ttl_entries_mutex".
    std::vector<TTLEntry> move_ttl_entries;

    /// Limiting parallel sends per one table, used in DataPartsExchange
    std::atomic_uint current_table_sends {0};

    /// For generating names of temporary parts during insertion.
    SimpleIncrement insert_increment;

    bool has_non_adaptive_index_granularity_parts = false;

    /// Parts that currently moving from disk/volume to another.
    /// This set have to be used with `currently_processing_in_background_mutex`.
    /// Moving may conflict with merges and mutations, but this is OK, because
    /// if we decide to move some part to another disk, than we
    /// assuredly will choose this disk for containing part, which will appear
    /// as result of merge or mutation.
    DataParts currently_moving_parts;

    /// Mutex for currently_moving_parts
    mutable std::mutex moving_parts_mutex;

protected:

    friend class IMergeTreeDataPart;
    friend class MergeTreeDataMergerMutator;
    friend struct ReplicatedMergeTreeTableMetadata;
    friend class StorageReplicatedMergeTree;

    ASTPtr ttl_table_ast;
    ASTPtr settings_ast;

    bool require_part_metadata;

    String relative_data_path;


    /// Current column sizes in compressed and uncompressed form.
    ColumnSizeByName column_sizes;

    /// Engine-specific methods
    BrokenPartCallback broken_part_callback;

    String log_name;
    Logger * log;

    /// Storage settings.
    /// Use get and set to receive readonly versions.
    MultiVersion<MergeTreeSettings> storage_settings;

    /// Work with data parts

    struct TagByInfo{};
    struct TagByStateAndInfo{};

    static const MergeTreePartInfo & dataPartPtrToInfo(const DataPartPtr & part)
    {
        return part->info;
    }

    static DataPartStateAndInfo dataPartPtrToStateAndInfo(const DataPartPtr & part)
    {
        return {part->state, part->info};
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

    MergeTreePartsMover parts_mover;

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

    static decltype(auto) getStateModifier(DataPartState state)
    {
        return [state] (const DataPartPtr & part) { part->state = state; };
    }

    void modifyPartState(DataPartIteratorByStateAndInfo it, DataPartState state)
    {
        if (!data_parts_by_state_and_info.modify(it, getStateModifier(state)))
            throw Exception("Can't modify " + (*it)->getNameWithState(), ErrorCodes::LOGICAL_ERROR);
    }

    void modifyPartState(DataPartIteratorByInfo it, DataPartState state)
    {
        if (!data_parts_by_state_and_info.modify(data_parts_indexes.project<TagByStateAndInfo>(it), getStateModifier(state)))
            throw Exception("Can't modify " + (*it)->getNameWithState(), ErrorCodes::LOGICAL_ERROR);
    }

    void modifyPartState(const DataPartPtr & part, DataPartState state)
    {
        auto it = data_parts_by_info.find(part->info);
        if (it == data_parts_by_info.end() || (*it).get() != part.get())
            throw Exception("Part " + part->name + " doesn't exist", ErrorCodes::LOGICAL_ERROR);

        if (!data_parts_by_state_and_info.modify(data_parts_indexes.project<TagByStateAndInfo>(it), getStateModifier(state)))
            throw Exception("Can't modify " + (*it)->getNameWithState(), ErrorCodes::LOGICAL_ERROR);
    }

    /// Used to serialize calls to grabOldParts.
    std::mutex grab_old_parts_mutex;
    /// The same for clearOldTemporaryDirectories.
    std::mutex clear_old_temporary_directories_mutex;

    void setProperties(const StorageInMemoryMetadata & metadata, bool only_check = false, bool attach = false);

    void initPartitionKey(ASTPtr partition_by_ast);

    void setTTLExpressions(const ColumnsDescription & columns,
        const ASTPtr & new_ttl_table_ast, bool only_check = false);

    void checkStoragePolicy(const StoragePolicyPtr & new_storage_policy) const;

    void setStoragePolicy(const String & new_storage_policy_name, bool only_check = false);

    /// Calculates column sizes in compressed form for the current state of data_parts. Call with data_parts mutex locked.
    void calculateColumnSizesImpl();
    /// Adds or subtracts the contribution of the part to compressed column sizes.
    void addPartContributionToColumnSizes(const DataPartPtr & part);
    void removePartContributionToColumnSizes(const DataPartPtr & part);

    /// If there is no part in the partition with ID `partition_id`, returns empty ptr. Should be called under the lock.
    DataPartPtr getAnyPartInPartition(const String & partition_id, DataPartsLock & data_parts_lock);

    /// Return parts in the Committed set that are covered by the new_part_info or the part that covers it.
    /// Will check that the new part doesn't already exist and that it doesn't intersect existing part.
    DataPartsVector getActivePartsToReplace(
        const MergeTreePartInfo & new_part_info,
        const String & new_part_name,
        DataPartPtr & out_covering_part,
        DataPartsLock & data_parts_lock) const;

    /// Checks whether the column is in the primary key, possibly wrapped in a chain of functions with single argument.
    bool isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(const ASTPtr & node) const;

    /// Common part for |freezePartition()| and |freezeAll()|.
    using MatcherFn = std::function<bool(const DataPartPtr &)>;
    void freezePartitionsByMatcher(MatcherFn matcher, const String & with_name, const Context & context);

    bool canReplacePartition(const DataPartPtr & src_part) const;

    void writePartLog(
        PartLogElement::Type type,
        const ExecutionStatus & execution_status,
        UInt64 elapsed_ns,
        const String & new_part_name,
        const DataPartPtr & result_part,
        const DataPartsVector & source_parts,
        const MergeListEntry * merge_entry);

    /// If part is assigned to merge or mutation (possibly replicated)
    /// Should be overriden by childs, because they can have different
    /// mechanisms for parts locking
    virtual bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const = 0;

    /// Return most recent mutations commands for part which weren't applied
    /// Used to receive AlterConversions for part and apply them on fly. This
    /// method has different implementations for replicated and non replicated
    /// MergeTree because they store mutations in different way.
    virtual MutationCommands getFirtsAlterMutationCommandsForPart(const DataPartPtr & part) const = 0;
    /// Moves part to specified space, used in ALTER ... MOVE ... queries
    bool movePartsToSpace(const DataPartsVector & parts, SpacePtr space);

    /// Selects parts for move and moves them, used in background process
    bool selectPartsAndMove();

    bool areBackgroundMovesNeeded() const;

private:
    /// RAII Wrapper for atomic work with currently moving parts
    /// Acuire them in constructor and remove them in destructor
    /// Uses data.currently_moving_parts_mutex
    struct CurrentlyMovingPartsTagger
    {
        MergeTreeMovingParts parts_to_move;
        MergeTreeData & data;
        CurrentlyMovingPartsTagger(MergeTreeMovingParts && moving_parts_, MergeTreeData & data_);

        CurrentlyMovingPartsTagger(const CurrentlyMovingPartsTagger & other) = delete;
        ~CurrentlyMovingPartsTagger();
    };

    /// Move selected parts to corresponding disks
    bool moveParts(CurrentlyMovingPartsTagger && moving_tagger);

    /// Select parts for move and disks for them. Used in background moving processes.
    CurrentlyMovingPartsTagger selectPartsForMove();

    /// Check selected parts for movements. Used by ALTER ... MOVE queries.
    CurrentlyMovingPartsTagger checkPartsForMove(const DataPartsVector & parts, SpacePtr space);

    bool canUsePolymorphicParts(const MergeTreeSettings & settings, String * out_reason) const;
};

}
