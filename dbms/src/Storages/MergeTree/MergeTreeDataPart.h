#pragma once

#include <Core/Row.h>
#include <Core/Block.h>
#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Columns/IColumn.h>

#include <Poco/Path.h>

#include <shared_mutex>


namespace DB
{

class MergeTreeData;


/// Description of the data part.
struct MergeTreeDataPart
{
    using Checksums = MergeTreeDataPartChecksums;
    using Checksum = MergeTreeDataPartChecksums::Checksum;

    MergeTreeDataPart(const MergeTreeData & storage_, const String & name_, const MergeTreePartInfo & info_);

    MergeTreeDataPart(MergeTreeData & storage_, const String & name_);

    /// Returns the name of a column with minimum compressed size (as returned by getColumnSize()).
    /// If no checksums are present returns the name of the first physically existing column.
    String getColumnNameWithMinumumCompressedSize() const;

    struct ColumnSize
    {
        size_t marks = 0;
        size_t data_compressed = 0;
        size_t data_uncompressed = 0;

        void add(const ColumnSize & other)
        {
            marks += other.marks;
            data_compressed += other.data_compressed;
            data_uncompressed += other.data_uncompressed;
        }
    };

    /// NOTE: Returns zeros if column files are not found in checksums.
    /// NOTE: You must ensure that no ALTERs are in progress when calculating ColumnSizes.
    ///   (either by locking columns_lock, or by locking table structure).
    ColumnSize getColumnSize(const String & name, const IDataType & type) const;

    ColumnSize getTotalColumnsSize() const;

    size_t getFileSizeOrZero(const String & file_name) const;

    /// Returns full path to part dir
    String getFullPath() const;

    /// Returns part->name with prefixes like 'tmp_<name>'
    String getNameWithPrefix() const;

    /// Generate the new name for this part according to `new_part_info` and min/max dates from the old name.
    /// This is useful when you want to change e.g. block numbers or the mutation version of the part.
    String getNewName(const MergeTreePartInfo & new_part_info) const;

    bool contains(const MergeTreeDataPart & other) const { return info.contains(other.info); }

    /// If the partition key includes date column (a common case), these functions will return min and max values for this column.
    DayNum getMinDate() const;
    DayNum getMaxDate() const;

    /// otherwise, if the partition key includes dateTime column (also a common case), these functions will return min and max values for this column.
    time_t getMinTime() const;
    time_t getMaxTime() const;

    bool isEmpty() const { return rows_count == 0; }

    const MergeTreeData & storage;

    String name;
    MergeTreePartInfo info;

    /// A directory path (relative to storage's path) where part data is actually stored
    /// Examples: 'detached/tmp_fetch_<name>', 'tmp_<name>', '<name>'
    mutable String relative_path;

    size_t rows_count = 0;
    std::atomic<UInt64> bytes_on_disk {0};  /// 0 - if not counted;
                                            /// Is used from several threads without locks (it is changed with ALTER).
                                            /// May not contain size of checksums.txt and columns.txt
    time_t modification_time = 0;
    /// When the part is removed from the working set. Changes once.
    mutable std::atomic<time_t> remove_time { std::numeric_limits<time_t>::max() };

    /// If true, the destructor will delete the directory with the part.
    bool is_temp = false;

    /// If true it means that there are no ZooKeeper node for this part, so it should be deleted only from filesystem
    bool is_duplicate = false;

    /**
     * Part state is a stage of its lifetime. States are ordered and state of a part could be increased only.
     * Part state should be modified under data_parts mutex.
     *
     * Possible state transitions:
     * Temporary -> Precommitted: we are trying to commit a fetched, inserted or merged part to active set
     * Precommitted -> Outdated:  we could not to add a part to active set and doing a rollback (for example it is duplicated part)
     * Precommitted -> Commited:  we successfully committed a part to active dataset
     * Precommitted -> Outdated:  a part was replaced by a covering part or DROP PARTITION
     * Outdated -> Deleting:      a cleaner selected this part for deletion
     * Deleting -> Outdated:      if an ZooKeeper error occurred during the deletion, we will retry deletion
     */
    enum class State
    {
        Temporary,      /// the part is generating now, it is not in data_parts list
        PreCommitted,   /// the part is in data_parts, but not used for SELECTs
        Committed,      /// active data part, used by current and upcoming SELECTs
        Outdated,       /// not active data part, but could be used by only current SELECTs, could be deleted after SELECTs finishes
        Deleting        /// not active data part with identity refcounter, it is deleting right now by a cleaner
    };

    using TTLInfo = MergeTreeDataPartTTLInfo;
    using TTLInfos = MergeTreeDataPartTTLInfos;

    TTLInfos ttl_infos;

    /// Current state of the part. If the part is in working set already, it should be accessed via data_parts mutex
    mutable State state{State::Temporary};

    /// Returns name of state
    static String stateToString(State state);
    String stateString() const;

    String getNameWithState() const
    {
        return name + " (state " + stateString() + ")";
    }

    /// Returns true if state of part is one of affordable_states
    bool checkState(const std::initializer_list<State> & affordable_states) const
    {
        for (auto affordable_state : affordable_states)
        {
            if (state == affordable_state)
                return true;
        }
        return false;
    }

    /// Throws an exception if state of the part is not in affordable_states
    void assertState(const std::initializer_list<State> & affordable_states) const;

    /// In comparison with lambdas, it is move assignable and could has several overloaded operator()
    struct StatesFilter
    {
        std::initializer_list<State> affordable_states;
        StatesFilter(const std::initializer_list<State> & affordable_states) : affordable_states(affordable_states) {}

        bool operator() (const std::shared_ptr<const MergeTreeDataPart> & part) const
        {
            return part->checkState(affordable_states);
        }
    };

    /// Returns a lambda that returns true only for part with states from specified list
    static inline StatesFilter getStatesFilter(const std::initializer_list<State> & affordable_states)
    {
        return StatesFilter(affordable_states);
    }

    /// Primary key (correspond to primary.idx file).
    /// Always loaded in RAM. Contains each index_granularity-th value of primary key tuple.
    /// Note that marks (also correspond to primary key) is not always in RAM, but cached. See MarkCache.h.
    using Index = Columns;
    Index index;

    MergeTreePartition partition;

    /// Amount of rows between marks
    /// As index always loaded into memory
    MergeTreeIndexGranularity index_granularity;

    /// Index that for each part stores min and max values of a set of columns. This allows quickly excluding
    /// parts based on conditions on these columns imposed by a query.
    /// Currently this index is built using only columns required by partition expression, but in principle it
    /// can be built using any set of columns.
    struct MinMaxIndex
    {
        /// A direct product of ranges for each key column. See Storages/MergeTree/KeyCondition.cpp for details.
        std::vector<Range> parallelogram;
        bool initialized = false;

    public:
        MinMaxIndex() = default;

        /// For month-based partitioning.
        MinMaxIndex(DayNum min_date, DayNum max_date)
            : parallelogram(1, Range(min_date, true, max_date, true))
            , initialized(true)
        {
        }

        void load(const MergeTreeData & storage, const String & part_path);
        void store(const MergeTreeData & storage, const String & part_path, Checksums & checksums) const;
        void store(const Names & column_names, const DataTypes & data_types, const String & part_path, Checksums & checksums) const;

        void update(const Block & block, const Names & column_names);
        void merge(const MinMaxIndex & other);
    };

    MinMaxIndex minmax_idx;

    Checksums checksums;

    /// Columns description.
    NamesAndTypesList columns;

    /// Columns with values, that all have been zeroed by expired ttl
    NameSet empty_columns;

    using ColumnToSize = std::map<std::string, UInt64>;

    /** It is blocked for writing when changing columns, checksums or any part files.
        * Locked to read when reading columns, checksums or any part files.
        */
    mutable std::shared_mutex columns_lock;

    /** It is taken for the whole time ALTER a part: from the beginning of the recording of the temporary files to their renaming to permanent.
        * It is taken with unlocked `columns_lock`.
        *
        * NOTE: "You can" do without this mutex if you could turn ReadRWLock into WriteRWLock without removing the lock.
        * This transformation is impossible, because it would create a deadlock, if you do it from two threads at once.
        * Taking this mutex means that we want to lock columns_lock on read with intention then, not
        *  unblocking, block it for writing.
        */
    mutable std::mutex alter_mutex;

    ~MergeTreeDataPart();

    /// Calculate the total size of the entire directory with all the files
    static UInt64 calculateTotalSizeOnDisk(const String & from);

    void remove() const;

    /// Makes checks and move part to new directory
    /// Changes only relative_dir_name, you need to update other metadata (name, is_temp) explicitly
    void renameTo(const String & new_relative_path, bool remove_new_dir_if_exists = true) const;

    /// Generate unique path to detach part
    String getRelativePathForDetachedPart(const String & prefix) const;

    /// Moves a part to detached/ directory and adds prefix to its name
    void renameToDetached(const String & prefix) const;

    /// Makes clone of a part in detached/ directory via hard links
    void makeCloneInDetached(const String & prefix) const;

    /// Populates columns_to_size map (compressed size).
    void accumulateColumnSizes(ColumnToSize & column_to_size) const;

    /// Initialize columns (from columns.txt if exists, or create from column files if not).
    /// Load checksums from checksums.txt if exists. Load index if required.
    void loadColumnsChecksumsIndexes(bool require_columns_checksums, bool check_consistency);

    /// Checks that .bin and .mrk files exist
    bool hasColumnFiles(const String & column) const;

    /// For data in RAM ('index')
    UInt64 getIndexSizeInBytes() const;
    UInt64 getIndexSizeInAllocatedBytes() const;
    UInt64 getMarksCount() const;

private:
    /// Reads columns names and types from columns.txt
    void loadColumns(bool require);

    /// If checksums.txt exists, reads files' checksums (and sizes) from it
    void loadChecksums(bool require);

    /// Loads marks index granularity into memory
    void loadIndexGranularity();

    /// Loads index file.
    void loadIndex();

    /// Load rows count for this part from disk (for the newer storage format version).
    /// For the older format version calculates rows count from the size of a column with a fixed size.
    void loadRowsCount();

    /// Loads ttl infos in json format from file ttl.txt. If file doesn`t exists assigns ttl infos with all zeros
    void loadTTLInfos();

    void loadPartitionAndMinMaxIndex();

    void checkConsistency(bool require_part_metadata);

    ColumnSize getColumnSizeImpl(const String & name, const IDataType & type, std::unordered_set<String> * processed_substreams) const;
};


using MergeTreeDataPartState = MergeTreeDataPart::State;

}
