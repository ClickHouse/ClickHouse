#pragma once

#include <Core/SortDescription.h>
#include <Common/SimpleIncrement.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/GraphiteRollupSortedBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_PARTITION_NAME;
    extern const int TOO_MUCH_PARTS;
    extern const int NO_SUCH_DATA_PART;
    extern const int DUPLICATE_DATA_PART;
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int TOO_MANY_UNEXPECTED_DATA_PARTS;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int TABLE_DIFFERS_TOO_MUCH;
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
/// Data is partitioned by month. Parts belonging to different months are not merged - for the ease of
/// administration (data sync and backup).
///
/// File structure:
/// Part directory - / min-date _ max-date _ min-id _ max-id _ level /
/// Inside the part directory:
/// checksums.txt - contains the list of all files along with their sizes and checksums.
/// columns.txt - contains the list of all columns and their types.
/// primary.idx - contains the primary index.
/// [Column].bin - contains compressed column data.
/// [Column].mrk - marks, pointing to seek positions allowing to skip n * k rows.
///
/// Several modes are implemented. Modes determine additional actions during merge:
/// - Ordinary - don't do anything special
/// - Collapsing - collapse pairs of rows with the opposite values of sign_columns for the same values
///   of primary key (cf. CollapsingSortedBlockInputStream.h)
/// - Replacing - for all rows with the same primary key keep only the latest one. Or, if the version
///   column is set, keep the latest row with the maximal version.
/// - Summing - sum all numeric columns not contained in the primary key for all rows with the same primary key.
/// - Aggregating - merge columns containing aggregate function states for all rows with the same primary key.
/// - Unsorted - during the merge the data is not sorted but merely concatenated; this allows reading the data
///   in the same batches as they were written.
/// - Graphite - performs coarsening of historical data for Graphite (a system for quantitative monitoring).

/// The MergeTreeData class contains a list of parts and the data structure parameters.
/// To read and modify the data use other classes:
/// - MergeTreeDataSelectExecutor
/// - MergeTreeDataWriter
/// - MergeTreeDataMerger

class MergeTreeData : public ITableDeclaration
{
    friend class ReshardingWorker;

public:
    /// Function to call if the part is suspected to contain corrupt data.
    using BrokenPartCallback = std::function<void (const String &)>;
    /// Callback to delete outdated parts immediately
    using PartsCleanCallback = std::function<void ()>;
    using DataPart = MergeTreeDataPart;

    using MutableDataPartPtr = std::shared_ptr<DataPart>;
    /// After the DataPart is added to the working set, it cannot be changed.
    using DataPartPtr = std::shared_ptr<const DataPart>;

    struct DataPartPtrLess
    {
        using is_transparent = void;

        bool operator()(const DataPartPtr & lhs, const MergeTreePartInfo & rhs) const { return lhs->info < rhs; }
        bool operator()(const MergeTreePartInfo & lhs, const DataPartPtr & rhs) const { return lhs < rhs->info; }
        bool operator()(const DataPartPtr & lhs, const DataPartPtr & rhs) const { return lhs->info < rhs->info; }
    };

    using DataParts = std::set<DataPartPtr, DataPartPtrLess>;
    using DataPartsVector = std::vector<DataPartPtr>;

    /// For resharding.
    using MutableDataParts = std::set<MutableDataPartPtr, DataPartPtrLess>;
    using PerShardDataParts = std::unordered_map<size_t, MutableDataPartPtr>;

    /// Some operations on the set of parts return a Transaction object.
    /// If neither commit() nor rollback() was called, the destructor rollbacks the operation.
    class Transaction : private boost::noncopyable
    {
    public:
        Transaction() {}

        void commit()
        {
            clear();
        }

        void rollback();

        ~Transaction()
        {
            try
            {
                rollback();
            }
            catch(...)
            {
                tryLogCurrentException("~MergeTreeData::Transaction");
            }
        }
    private:
        friend class MergeTreeData;

        MergeTreeData * data = nullptr;

        /// What to do on rollback.
        DataPartsVector parts_to_remove_on_rollback;
        DataPartsVector parts_to_add_on_rollback;

        void clear()
        {
            data = nullptr;
            parts_to_remove_on_rollback.clear();
            parts_to_add_on_rollback.clear();
        }
    };

    /// An object that stores the names of temporary files created in the part directory during ALTER of its
    /// columns.
    class AlterDataPartTransaction : private boost::noncopyable
    {
    public:
        /// Renames temporary files, finishing the ALTER of the part.
        void commit();

        /// If commit() was not called, deletes temporary files, canceling the ALTER.
        ~AlterDataPartTransaction();

        /// Review the changes before the commit.
        const NamesAndTypesList & getNewColumns() const { return new_columns; }
        const DataPart::Checksums & getNewChecksums() const { return new_checksums; }

    private:
        friend class MergeTreeData;

        AlterDataPartTransaction(DataPartPtr data_part_) : data_part(data_part_), alter_lock(data_part->alter_mutex) {}

        void clear()
        {
            alter_lock.unlock();
            data_part = nullptr;
        }

        DataPartPtr data_part;
        std::unique_lock<std::mutex> alter_lock;

        DataPart::Checksums new_checksums;
        NamesAndTypesList new_columns;
        /// If the value is an empty string, the file is not temporary, and it must be deleted.
        NameToNameMap rename_map;
    };

    using AlterDataPartTransactionPtr = std::unique_ptr<AlterDataPartTransaction>;


    /// Parameters for various modes.
    struct MergingParams
    {
        /// Merging mode. See above.
        enum Mode
        {
            Ordinary    = 0,    /// Enum values are saved. Do not change them.
            Collapsing  = 1,
            Summing     = 2,
            Aggregating = 3,
            Unsorted    = 4,
            Replacing   = 5,
            Graphite    = 6,
        };

        Mode mode;

        /// For collapsing mode.
        String sign_column;

        /// For Summing mode. If empty - columns_to_sum is determined automatically.
        Names columns_to_sum;

        /// For Replacing mode. Can be empty.
        String version_column;

        /// For Graphite mode.
        Graphite::Params graphite_params;

        /// Check that needed columns are present and have correct types.
        void check(const NamesAndTypesList & columns) const;

        String getModeName() const;
    };


    /// Attach the table corresponding to the directory in full_path (must end with /), with the given columns.
    /// Correctness of names and paths is not checked.
    ///
    /// primary_expr_ast - expression used for sorting; empty for UnsortedMergeTree.
    /// index_granularity - how many rows correspond to one primary key value.
    /// require_part_metadata - should checksums.txt and columns.txt exist in the part directory.
    /// attach - whether the existing table is attached or the new table is created.
    MergeTreeData(  const String & database_, const String & table_,
                    const String & full_path_, NamesAndTypesListPtr columns_,
                    const NamesAndTypesList & materialized_columns_,
                    const NamesAndTypesList & alias_columns_,
                    const ColumnDefaults & column_defaults_,
                    Context & context_,
                    ASTPtr & primary_expr_ast_,
                    const String & date_column_name_,
                    const ASTPtr & sampling_expression_, /// nullptr, if sampling is not supported.
                    size_t index_granularity_,
                    const MergingParams & merging_params_,
                    const MergeTreeSettings & settings_,
                    const String & log_name_,
                    bool require_part_metadata_,
                    bool attach,
                    BrokenPartCallback broken_part_callback_ = [](const String &){},
                    PartsCleanCallback parts_clean_callback_ = nullptr
                 );

    /// Load the set of data parts from disk. Call once - immediately after the object is created.
    void loadDataParts(bool skip_sanity_checks);

    bool supportsSampling() const { return !!sampling_expression; }
    bool supportsPrewhere() const { return true; }

    bool supportsFinal() const
    {
        return merging_params.mode == MergingParams::Collapsing
            || merging_params.mode == MergingParams::Summing
            || merging_params.mode == MergingParams::Aggregating
            || merging_params.mode == MergingParams::Replacing;
    }

    Int64 getMaxDataPartIndex();

    const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }

    NameAndTypePair getColumn(const String & column_name) const override
    {
        if (column_name == "_part")
            return NameAndTypePair("_part", std::make_shared<DataTypeString>());
        if (column_name == "_part_index")
            return NameAndTypePair("_part_index", std::make_shared<DataTypeUInt64>());
        if (column_name == "_sample_factor")
            return NameAndTypePair("_sample_factor", std::make_shared<DataTypeFloat64>());

        return ITableDeclaration::getColumn(column_name);
    }

    bool hasColumn(const String & column_name) const override
    {
        return ITableDeclaration::hasColumn(column_name)
            || column_name == "_part"
            || column_name == "_part_index"
            || column_name == "_sample_factor";
    }

    String getDatabaseName() const { return database_name; }

    String getTableName() const override { return table_name; }

    String getFullPath() const { return full_path; }

    String getLogName() const { return log_name; }

    /// Returns a copy of the list so that the caller shouldn't worry about locks.
    DataParts getDataParts() const;
    DataPartsVector getDataPartsVector() const;
    DataParts getAllDataParts() const;

    /// Total size of active parts in bytes.
    size_t getTotalActiveSizeInBytes() const;

    size_t getMaxPartsCountForPartition() const;

    /// If the table contains too many active parts, sleep for a while to give them time to merge.
    /// If until is non-null, wake up from the sleep earlier if the event happened.
    void delayInsertIfNeeded(Poco::Event * until = nullptr);

    /// Returns an active part with the given name or a part containing it. If there is no such part,
    /// returns nullptr.
    DataPartPtr getActiveContainingPart(const String & part_name);

    /// Returns the part with the given name or nullptr if no such part.
    DataPartPtr getPartIfExists(const String & part_name);
    DataPartPtr getShardedPartIfExists(const String & part_name, size_t shard_no);

    /// Renames temporary part to a permanent part and adds it to the working set.
    /// If increment != nullptr, part index is determing using increment. Otherwise part index remains unchanged.
    /// It is assumed that the part does not intersect with existing parts.
    /// If out_transaction != nullptr, sets it to an object allowing to rollback part addition (but not the renaming).
    void renameTempPartAndAdd(MutableDataPartPtr & part, SimpleIncrement * increment = nullptr, Transaction * out_transaction = nullptr);

    /// The same as renameTempPartAndAdd but the part can intersect existing parts.
    /// Deletes and returns all parts covered by the added part (in ascending order).
    DataPartsVector renameTempPartAndReplace(
        MutableDataPartPtr & part, SimpleIncrement * increment = nullptr, Transaction * out_transaction = nullptr);

    /// Removes from the working set parts in remove and adds parts in add. Parts in add must already be in
    /// all_data_parts.
    /// If clear_without_timeout is true, the parts will be deleted at once, or during the next call to
    /// clearOldParts (ignoring old_parts_lifetime).
    void replaceParts(const DataPartsVector & remove, const DataPartsVector & add, bool clear_without_timeout);

    /// Adds new part to the list of known parts and to the working set.
    void attachPart(const DataPartPtr & part);

    /// Renames the part to detached/<prefix>_<part> and forgets about it. The data won't be deleted in
    /// clearOldParts.
    /// If restore_covered is true, adds to the working set inactive parts, which were merged into the deleted part.
    void renameAndDetachPart(const DataPartPtr & part, const String & prefix = "", bool restore_covered = false, bool move_to_detached = true);

    /// Removes the part from the list of parts (including all_data_parts), but doesn't move the directory.
    void detachPartInPlace(const DataPartPtr & part);

    /// Returns old inactive parts that can be deleted. At the same time removes them from the list of parts
    /// but not from the disk.
    DataPartsVector grabOldParts();

    /// Reverts the changes made by grabOldParts().
    void addOldParts(const DataPartsVector & parts);

    /// Delete irrelevant parts.
    void clearOldParts();

    /// Deleate all directories which names begin with "tmp"
    /// Set non-negative parameter value to override MergeTreeSettings temporary_directories_lifetime
    void clearOldTemporaryDirectories(ssize_t custom_directories_lifetime_seconds = -1);

    /// After the call to dropAllData() no method can be called.
    /// Deletes the data directory and flushes the uncompressed blocks cache and the marks cache.
    void dropAllData();

    /// Moves the entire data directory.
    /// Flushes the uncompressed blocks cache and the marks cache.
    /// Must be called with locked lockStructureForAlter().
    void setPath(const String & full_path, bool move_data);

    /// Check if the ALTER can be performed:
    /// - all needed columns are present.
    /// - all type conversions can be done.
    /// - columns corresponding to primary key, sign, sampling expression and date are not affected.
    /// If something is wrong, throws an exception.
    void checkAlter(const AlterCommands & commands);

    /// Performs ALTER of the data part, writes the result to temporary files.
    /// Returns an object allowing to rename temporary files to permanent files.
    /// If the number of affected columns is suspiciously high and skip_sanity_checks is false, throws an exception.
    /// If no data transformations are necessary, returns nullptr.
    AlterDataPartTransactionPtr alterDataPart(
        const DataPartPtr & part,
        const NamesAndTypesList & new_columns,
        const ASTPtr & new_primary_key,
        bool skip_sanity_checks);

    /// Must be called with locked lockStructureForAlter().
    void setColumnsList(const NamesAndTypesList & new_columns) { columns = std::make_shared<NamesAndTypesList>(new_columns); }

    /// Should be called if part data is suspected to be corrupted.
    void reportBrokenPart(const String & name)
    {
        broken_part_callback(name);
    }

    /// Delete old parts from disk and ZooKeeper (in replicated case)
    void clearOldPartsAndRemoveFromZK()
    {
        parts_clean_callback();
    }

    ExpressionActionsPtr getPrimaryExpression() const { return primary_expr; }
    SortDescription getSortDescription() const { return sort_descr; }

    /// Check that the part is not broken and calculate the checksums for it if they are not present.
    MutableDataPartPtr loadPartAndFixMetadata(const String & relative_path);

    /** Create local backup (snapshot) for parts with specified prefix.
      * Backup is created in directory clickhouse_dir/shadow/i/, where i - incremental number,
      *  or if 'with_name' is specified - backup is created in directory with specified name.
      */
    void freezePartition(const std::string & prefix, const String & with_name);

    /// Returns the size of partition in bytes.
    size_t getPartitionSize(const std::string & partition_id) const;

    struct ColumnSize
    {
        size_t marks = 0;
        size_t data_compressed = 0;
        size_t data_uncompressed = 0;

        size_t getTotalCompressedSize() const
        {
            return marks + data_compressed;
        }
    };

    size_t getColumnCompressedSize(const std::string & name) const
    {
        std::lock_guard<std::mutex> lock{data_parts_mutex};

        const auto it = column_sizes.find(name);
        return it == std::end(column_sizes) ? 0 : it->second.data_compressed;
    }

    using ColumnSizes = std::unordered_map<std::string, ColumnSize>;
    ColumnSizes getColumnSizes() const
    {
        std::lock_guard<std::mutex> lock{data_parts_mutex};
        return column_sizes;
    }

    /// NOTE Could be off after DROPped and MODIFYed columns in ALTER. Doesn't include primary.idx.
    size_t getTotalCompressedSize() const
    {
        std::lock_guard<std::mutex> lock{data_parts_mutex};
        size_t total_size = 0;

        for (const auto & col : column_sizes)
            total_size += col.second.getTotalCompressedSize();

        return total_size;
    }

    void recalculateColumnSizes()
    {
        std::lock_guard<std::mutex> lock{data_parts_mutex};
        calculateColumnSizesImpl();
    }

    /// For ATTACH/DETACH/DROP/RESHARD PARTITION.
    String getPartitionIDFromQuery(const Field & partition);

    /// For determining the partition id of inserted blocks.
    String getPartitionIDFromData(const Row & partition);

    MergeTreeDataFormatVersion format_version;

    Context & context;
    const String date_column_name;
    const ASTPtr sampling_expression;
    const size_t index_granularity;

    /// Merging params - what additional actions to perform during merge.
    const MergingParams merging_params;

    const MergeTreeSettings settings;

    ASTPtr primary_expr_ast;
    Block primary_key_sample;
    DataTypes primary_key_data_types;

    ASTPtr partition_expr_ast;
    ExpressionActionsPtr partition_expr;
    Names partition_expr_columns;
    DataTypes partition_expr_column_types;

    ExpressionActionsPtr minmax_idx_expr;
    Names minmax_idx_columns;
    DataTypes minmax_idx_column_types;
    Int64 minmax_idx_date_column_pos = -1; /// In a common case minmax index includes a date column.
    SortDescription minmax_idx_sort_descr; /// For use with PKCondition.

    /// Limiting parallel sends per one table, used in DataPartsExchange
    std::atomic_uint current_table_sends {0};

    /// For generating names of temporary parts during insertion.
    SimpleIncrement insert_increment;

private:
    friend struct MergeTreeDataPart;
    friend class StorageMergeTree;
    friend class ReplicatedMergeTreeAlterThread;
    friend class MergeTreeDataMerger;

    bool require_part_metadata;

    ExpressionActionsPtr primary_expr;
    SortDescription sort_descr;

    String database_name;
    String table_name;
    String full_path;

    NamesAndTypesListPtr columns;

    /// Current column sizes in compressed and uncompressed form.
    ColumnSizes column_sizes;

    /// Engine-specific methods
    BrokenPartCallback broken_part_callback;
    /// Use to delete outdated parts immediately from memory, disk and ZooKeeper
    PartsCleanCallback parts_clean_callback;

    String log_name;
    Logger * log;

    /// Current set of data parts.
    DataParts data_parts;
    mutable std::mutex data_parts_mutex;

    /// The set of all data parts including already merged but not yet deleted. Usually it is small (tens of elements).
    /// The part is referenced from here, from the list of current parts and from each thread reading from it.
    /// This means that if reference count is 1 - the part is not used right now and can be deleted.
    DataParts all_data_parts;
    mutable std::mutex all_data_parts_mutex;

    /// Used to serialize calls to grabOldParts.
    std::mutex grab_old_parts_mutex;
    /// The same for clearOldTemporaryDirectories.
    std::mutex clear_old_temporary_directories_mutex;

    /// For each shard of the set of sharded parts.
    PerShardDataParts per_shard_data_parts;

    /// Check that columns list doesn't contain multidimensional arrays.
    /// If attach is true (attaching an existing table), writes an error message to log.
    /// Otherwise (new table or alter) throws an exception.
    void checkNoMultidimensionalArrays(const NamesAndTypesList & columns, bool attach) const;

    void initPrimaryKey();

    void initPartitionKey();

    /// Expression for column type conversion.
    /// If no conversions are needed, out_expression=nullptr.
    /// out_rename_map maps column files for the out_expression onto new table files.
    /// out_force_update_metadata denotes if metadata must be changed even if out_rename_map is empty (used
    /// for transformation-free changing of Enum values list).
    /// Files to be deleted are mapped to an empty string in out_rename_map.
    /// If part == nullptr, just checks that all type conversions are possible.
    void createConvertExpression(const DataPartPtr & part, const NamesAndTypesList & old_columns, const NamesAndTypesList & new_columns,
        ExpressionActionsPtr & out_expression, NameToNameMap & out_rename_map, bool & out_force_update_metadata) const;

    /// Calculates column sizes in compressed form for the current state of data_parts. Call with data_parts mutex locked.
    void calculateColumnSizesImpl();
    /// Adds or subtracts the contribution of the part to compressed column sizes.
    void addPartContributionToColumnSizes(const DataPartPtr & part);
    void removePartContributionToColumnSizes(const DataPartPtr & part);
};

}
