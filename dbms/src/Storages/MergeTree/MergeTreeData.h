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

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/global_fun.hpp>
#include <boost/range/iterator_range_core.hpp>

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
/// Part directory - / partiiton-id _ min-id _ max-id _ level /
/// Inside the part directory:
/// The same files as for month-partitioned tables, plus
/// count.txt - contains total number of rows in this part.
/// partition.dat - contains the value of the partitioning expression.
/// minmax_[Column].idx - MinMax indexes (see MergeTreeDataPart::MinMaxIndex class) for the columns required by the partitioning expression.
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
/// - MergeTreeDataMerger

class MergeTreeData : public ITableDeclaration
{
public:
    /// Function to call if the part is suspected to contain corrupt data.
    using BrokenPartCallback = std::function<void (const String &)>;
    using DataPart = MergeTreeDataPart;

    using MutableDataPartPtr = std::shared_ptr<DataPart>;
    /// After the DataPart is added to the working set, it cannot be changed.
    using DataPartPtr = std::shared_ptr<const DataPart>;

    using DataPartState = MergeTreeDataPart::State;
    using DataPartStates = std::initializer_list<DataPartState>;
    using DataPartStateVector = std::vector<DataPartState>;

    /// Auxiliary structure for index comparison. Keep in mind lifetime of MergeTreePartInfo.
    struct DataPartStateAndInfo
    {
        DataPartState state;
        const MergeTreePartInfo & info;

        DataPartStateAndInfo(DataPartState state, const MergeTreePartInfo & info) : state(state), info(info) {}
    };

    struct LessDataPart
    {
        using is_transparent = void;

        bool operator()(const DataPartPtr & lhs, const MergeTreePartInfo & rhs) const { return lhs->info < rhs; }
        bool operator()(const MergeTreePartInfo & lhs, const DataPartPtr & rhs) const { return lhs < rhs->info; }
        bool operator()(const DataPartPtr & lhs, const DataPartPtr & rhs) const { return lhs->info < rhs->info; }
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
    };

    using DataParts = std::set<DataPartPtr, LessDataPart>;
    using DataPartsVector = std::vector<DataPartPtr>;

    /// Auxiliary object to add a set of parts into the working set in two steps:
    /// * First, as PreCommitted parts (the parts are ready, but not yet in the active set).
    /// * Next, if commit() is called, the parts are added to the active set and the parts that are
    ///   covered by them are marked Outdated.
    /// If neither commit() nor rollback() was called, the destructor rollbacks the operation.
    class Transaction : private boost::noncopyable
    {
    public:
        Transaction() {}

        /// Return parts marked Obsolete as a result of the transaction commit.
        DataPartsVector commit();

        void rollback();

        bool isEmpty() const
        {
            return precommitted_parts.empty();
        }

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
        DataParts precommitted_parts;

        void clear()
        {
            data = nullptr;
            precommitted_parts.clear();
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


    /// Attach the table corresponding to the directory in full_path (must end with /), with the given columns.
    /// Correctness of names and paths is not checked.
    ///
    /// primary_expr_ast - expression used for sorting;
    /// date_column_name - if not empty, the name of the Date column used for partitioning by month.
    ///     Otherwise, partition_expr_ast is used for partitioning.
    /// require_part_metadata - should checksums.txt and columns.txt exist in the part directory.
    /// attach - whether the existing table is attached or the new table is created.
    MergeTreeData(const String & database_, const String & table_,
                  const String & full_path_, const NamesAndTypesList & columns_,
                  const NamesAndTypesList & materialized_columns_,
                  const NamesAndTypesList & alias_columns_,
                  const ColumnDefaults & column_defaults_,
                  Context & context_,
                  const ASTPtr & primary_expr_ast_,
                  const ASTPtr & secondary_sort_expr_ast_,
                  const String & date_column_name,
                  const ASTPtr & partition_expr_ast_,
                  const ASTPtr & sampling_expression_, /// nullptr, if sampling is not supported.
                  const MergingParams & merging_params_,
                  const MergeTreeSettings & settings_,
                  bool require_part_metadata_,
                  bool attach,
                  BrokenPartCallback broken_part_callback_ = [](const String &){});

    /// Load the set of data parts from disk. Call once - immediately after the object is created.
    void loadDataParts(bool skip_sanity_checks);

    bool supportsSampling() const { return sampling_expression != nullptr; }
    bool supportsPrewhere() const { return true; }

    bool supportsFinal() const
    {
        return merging_params.mode == MergingParams::Collapsing
            || merging_params.mode == MergingParams::Summing
            || merging_params.mode == MergingParams::Aggregating
            || merging_params.mode == MergingParams::Replacing
            || merging_params.mode == MergingParams::VersionedCollapsing;
    }

    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand) const;

    Int64 getMaxDataPartIndex();

    const NamesAndTypesList & getColumnsListImpl() const override { return columns; }

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
    DataParts getDataParts(const DataPartStates & affordable_states) const;
    /// Returns sorted list of the parts with specified states
    ///  out_states will contain snapshot of each part state
    DataPartsVector getDataPartsVector(const DataPartStates & affordable_states, DataPartStateVector * out_states = nullptr) const;

    /// Returns absolutely all parts (and snapshot of their states)
    DataPartsVector getAllDataPartsVector(DataPartStateVector * out_states = nullptr) const;

    /// Returns Committed parts
    DataParts getDataParts() const;
    DataPartsVector getDataPartsVector() const;

    /// Returns a committed part with the given name or a part containing it. If there is no such part, returns nullptr.
    DataPartPtr getActiveContainingPart(const String & part_name);

    /// Returns the part with the given name and state or nullptr if no such part.
    DataPartPtr getPartIfExists(const String & part_name, const DataPartStates & valid_states);

    /// Total size of active parts in bytes.
    size_t getTotalActiveSizeInBytes() const;

    size_t getMaxPartsCountForPartition() const;

    /// If the table contains too many active parts, sleep for a while to give them time to merge.
    /// If until is non-null, wake up from the sleep earlier if the event happened.
    void delayInsertIfNeeded(Poco::Event * until = nullptr);

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

    /// Removes parts from the working set parts.
    /// Parts in add must already be in data_parts with PreCommitted, Committed, or Outdated states.
    /// If clear_without_timeout is true, the parts will be deleted at once, or during the next call to
    /// clearOldParts (ignoring old_parts_lifetime).
    void removePartsFromWorkingSet(const DataPartsVector & remove, bool clear_without_timeout);

    /// Renames the part to detached/<prefix>_<part> and forgets about it. The data won't be deleted in
    /// clearOldParts.
    /// If restore_covered is true, adds to the working set inactive parts, which were merged into the deleted part.
    void renameAndDetachPart(const DataPartPtr & part, const String & prefix = "", bool restore_covered = false, bool move_to_detached = true);

    /// Returns old inactive parts that can be deleted. At the same time removes them from the list of parts
    /// but not from the disk.
    DataPartsVector grabOldParts();

    /// Reverts the changes made by grabOldParts(), parts should be in Deleting state.
    void rollbackDeletingParts(const DataPartsVector & parts);

    /// Removes parts from data_parts, they should be in Deleting state
    void removePartsFinally(const DataPartsVector & parts);

    /// Delete irrelevant parts from memory and disk.
    void clearOldPartsFromFilesystem();

    /// Deleate all directories which names begin with "tmp"
    /// Set non-negative parameter value to override MergeTreeSettings temporary_directories_lifetime
    void clearOldTemporaryDirectories(ssize_t custom_directories_lifetime_seconds = -1);

    /// After the call to dropAllData() no method can be called.
    /// Deletes the data directory and flushes the uncompressed blocks cache and the marks cache.
    void dropAllData();

    /// Moves the entire data directory.
    /// Flushes the uncompressed blocks cache and the marks cache.
    /// Must be called with locked lockStructureForAlter().
    void setPath(const String & full_path);

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
    void setColumnsList(const NamesAndTypesList & new_columns) { columns = new_columns; }

    /// Should be called if part data is suspected to be corrupted.
    void reportBrokenPart(const String & name)
    {
        broken_part_callback(name);
    }

    bool hasPrimaryKey() const { return !primary_sort_descr.empty(); }
    ExpressionActionsPtr getPrimaryExpression() const { return primary_expr; }
    ExpressionActionsPtr getSecondarySortExpression() const { return secondary_sort_expr; } /// may return nullptr
    SortDescription getPrimarySortDescription() const { return primary_sort_descr; }
    SortDescription getSortDescription() const { return sort_descr; }

    /// Check that the part is not broken and calculate the checksums for it if they are not present.
    MutableDataPartPtr loadPartAndFixMetadata(const String & relative_path);

    /** Create local backup (snapshot) for parts with specified prefix.
      * Backup is created in directory clickhouse_dir/shadow/i/, where i - incremental number,
      *  or if 'with_name' is specified - backup is created in directory with specified name.
      */
    void freezePartition(const ASTPtr & partition, const String & with_name, const Context & context);

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

    /// Calculates column sizes in compressed form for the current state of data_parts.
    void recalculateColumnSizes()
    {
        std::lock_guard<std::mutex> lock{data_parts_mutex};
        calculateColumnSizesImpl();
    }

    /// For ATTACH/DETACH/DROP PARTITION.
    String getPartitionIDFromQuery(const ASTPtr & partition, const Context & context);

    MergeTreeDataFormatVersion format_version;

    Context & context;
    const ASTPtr sampling_expression;
    const size_t index_granularity;

    /// Merging params - what additional actions to perform during merge.
    const MergingParams merging_params;

    const MergeTreeSettings settings;

    ASTPtr primary_expr_ast;
    ASTPtr secondary_sort_expr_ast;
    Block primary_key_sample;
    DataTypes primary_key_data_types;

    ASTPtr partition_expr_ast;
    ExpressionActionsPtr partition_expr;
    Block partition_key_sample;

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
    /// Additional expression for sorting (of rows with the same primary keys).
    ExpressionActionsPtr secondary_sort_expr;
    /// Sort description for primary key. Is the prefix of sort_descr.
    SortDescription primary_sort_descr;
    /// Sort description for primary key + secondary sorting columns.
    SortDescription sort_descr;

    String database_name;
    String table_name;
    String full_path;

    /// Current column sizes in compressed and uncompressed form.
    ColumnSizes column_sizes;

    /// Engine-specific methods
    BrokenPartCallback broken_part_callback;

    String log_name;
    Logger * log;


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
    };

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

    using DataPartIteratorByInfo = DataPartsIndexes::index<TagByInfo>::type::iterator;
    using DataPartIteratorByStateAndInfo = DataPartsIndexes::index<TagByStateAndInfo>::type::iterator;

    boost::iterator_range<DataPartIteratorByStateAndInfo> getDataPartsStateRange(DataPartState state) const
    {
        auto begin = data_parts_by_state_and_info.lower_bound(state, LessStateDataPart());
        auto end = data_parts_by_state_and_info.upper_bound(state, LessStateDataPart());
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

    /// If there is no part in the partition with ID `partition_id`, returns empty ptr. Should be called under the lock.
    DataPartPtr getAnyPartInPartition(const String & partition_id, std::lock_guard<std::mutex> & data_parts_lock);

    /// Return parts in the Committed set that are covered by the new_part_info or the part that covers it.
    /// Will check that the new part doesn't already exist and that it doesn't intersect existing part.
    DataPartsVector getActivePartsToReplace(
        const MergeTreePartInfo & new_part_info,
        const String & new_part_name,
        DataPartPtr & out_covering_part,
        std::lock_guard<std::mutex> & data_parts_lock) const;

    /// Checks whether the column is in the primary key.
    bool isPrimaryKeyColumn(const ASTPtr &node) const;
};

}
