#pragma once

#include <Core/Row.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Columns/IColumn.h>
#include <shared_mutex>


class SipHash;


namespace DB
{


/// Checksum of one file.
struct MergeTreeDataPartChecksum
{
    using uint128 = CityHash_v1_0_2::uint128;

    size_t file_size {};
    uint128 file_hash {};

    bool is_compressed = false;
    size_t uncompressed_size {};
    uint128 uncompressed_hash {};

    MergeTreeDataPartChecksum() {}
    MergeTreeDataPartChecksum(size_t file_size_, uint128 file_hash_) : file_size(file_size_), file_hash(file_hash_) {}
    MergeTreeDataPartChecksum(size_t file_size_, uint128 file_hash_, size_t uncompressed_size_, uint128 uncompressed_hash_)
        : file_size(file_size_), file_hash(file_hash_), is_compressed(true),
        uncompressed_size(uncompressed_size_), uncompressed_hash(uncompressed_hash_) {}

    void checkEqual(const MergeTreeDataPartChecksum & rhs, bool have_uncompressed, const String & name) const;
    void checkSize(const String & path) const;
};


/** Checksums of all non-temporary files.
  * For compressed files, the check sum and the size of the decompressed data are stored to not depend on the compression method.
  */
struct MergeTreeDataPartChecksums
{
    using Checksum = MergeTreeDataPartChecksum;

    /// The order is important.
    using FileChecksums = std::map<String, Checksum>;
    FileChecksums files;

    void addFile(const String & file_name, size_t file_size, Checksum::uint128 file_hash);

    void add(MergeTreeDataPartChecksums && rhs_checksums);

    /// Checks that the set of columns and their checksums are the same. If not, throws an exception.
    /// If have_uncompressed, for compressed files it compares the checksums of the decompressed data. Otherwise, it compares only the checksums of the files.
    void checkEqual(const MergeTreeDataPartChecksums & rhs, bool have_uncompressed) const;

    /// Checks that the directory contains all the needed files of the correct size. Does not check the checksum.
    void checkSizes(const String & path) const;

    /// Serializes and deserializes in human readable form.
    bool read(ReadBuffer & in); /// Returns false if the checksum is too old.
    bool read_v2(ReadBuffer & in);
    bool read_v3(ReadBuffer & in);
    bool read_v4(ReadBuffer & in);
    void write(WriteBuffer & out) const;

    bool empty() const
    {
        return files.empty();
    }

    /// Checksum from the set of checksums of .bin files.
    void summaryDataChecksum(SipHash & hash) const;

    String toString() const;
    static MergeTreeDataPartChecksums parse(const String & s);
};


class MergeTreeData;


/// Description of the data part.
struct MergeTreeDataPart
{
    using Checksums = MergeTreeDataPartChecksums;
    using Checksum = MergeTreeDataPartChecksums::Checksum;

    MergeTreeDataPart(MergeTreeData & storage_, const String & name_, const MergeTreePartInfo & info_)
        : storage(storage_), name(name_), info(info_)
    {
    }

    MergeTreeDataPart(MergeTreeData & storage_, const String & name_);

    const Checksum * tryGetChecksum(const String & name, const String & ext) const;
    /// Returns checksum of column's binary file.
    const Checksum * tryGetBinChecksum(const String & name) const;
    /// Returns checksum of column's mrk file.
    const Checksum * tryGetMrkChecksum(const String & name) const;

    /// Returns the size of .bin file for column `name` if found, zero otherwise
    size_t getColumnCompressedSize(const String & name) const;
    size_t getColumnUncompressedSize(const String & name) const;
    /// Returns the size of .mrk file for column `name` if found, zero otherwise
    size_t getColumnMrkSize(const String & name) const;

    /// Returns the name of a column with minimum compressed size (as returned by getColumnSize()).
    /// If no checksums are present returns the name of the first physically existing column.
    String getColumnNameWithMinumumCompressedSize() const;

    /// Returns full path to part dir
    String getFullPath() const;

    /// Returns part->name with prefixes like 'tmp_<name>'
    String getNameWithPrefix() const;

    bool contains(const MergeTreeDataPart & other) const { return info.contains(other.info); }

    /// If the partition key includes date column (a common case), these functions will return min and max values for this column.
    DayNum_t getMinDate() const;
    DayNum_t getMaxDate() const;

    MergeTreeData & storage;

    String name;
    MergeTreePartInfo info;

    /// A directory path (relative to storage's path) where part data is actually stored
    /// Examples: 'detached/tmp_fetch_<name>', 'tmp_<name>', '<name>'
    mutable String relative_path;

    size_t rows_count = 0;
    size_t marks_count = 0;
    std::atomic<size_t> size_in_bytes {0};  /// size in bytes, 0 - if not counted;
                                            ///  is used from several threads without locks (it is changed with ALTER).
    time_t modification_time = 0;
    mutable time_t remove_time = std::numeric_limits<time_t>::max(); /// When the part is removed from the working set.

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

    /// Index that for each part stores min and max values of a set of columns. This allows quickly excluding
    /// parts based on conditions on these columns imposed by a query.
    /// Currently this index is built using only columns required by partition expression, but in principle it
    /// can be built using any set of columns.
    struct MinMaxIndex
    {
        Row min_values;
        Row max_values;
        bool initialized = false;

    public:
        MinMaxIndex() = default;

        /// For month-based partitioning.
        MinMaxIndex(DayNum_t min_date, DayNum_t max_date)
            : min_values(1, static_cast<UInt64>(min_date))
            , max_values(1, static_cast<UInt64>(max_date))
            , initialized(true)
        {
        }

        void load(const MergeTreeData & storage, const String & part_path);
        void store(const MergeTreeData & storage, const String & part_path, Checksums & checksums) const;

        void update(const Block & block, const Names & column_names);
        void merge(const MinMaxIndex & other);
    };

    MinMaxIndex minmax_idx;

    Checksums checksums;

    /// Columns description.
    NamesAndTypesList columns;

    using ColumnToSize = std::map<std::string, size_t>;

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
    static size_t calculateTotalSize(const String & from);

    void remove() const;

    /// Makes checks and move part to new directory
    /// Changes only relative_dir_name, you need to update other metadata (name, is_temp) explicitly
    void renameTo(const String & new_relative_path, bool remove_new_dir_if_exists = true) const;

    /// Renames a part by appending a prefix to the name. To_detached - also moved to the detached directory.
    void renameAddPrefix(bool to_detached, const String & prefix) const;

    /// Populates columns_to_size map (compressed size).
    void accumulateColumnSizes(ColumnToSize & column_to_size) const;

    /// Initialize columns (from columns.txt if exists, or create from column files if not).
    /// Load checksums from checksums.txt if exists. Load index if required.
    void loadColumnsChecksumsIndexes(bool require_columns_checksums, bool check_consistency);

    /// Checks that .bin and .mrk files exist
    bool hasColumnFiles(const String & column) const;

    /// For data in RAM ('index')
    size_t getIndexSizeInBytes() const;
    size_t getIndexSizeInAllocatedBytes() const;
    /// Total size of *.mrk files
    size_t getTotalMrkSizeInBytes() const;

private:
    /// Reads columns names and types from columns.txt
    void loadColumns(bool require);

    /// If checksums.txt exists, reads files' checksums (and sizes) from it
    void loadChecksums(bool require);

    /// Loads index file. Also calculates this->marks_count if marks_count = 0
    void loadIndex();

    /// Load rows count for this part from disk (for the newer storage format version).
    /// For the older format version calculates rows count from the size of a column with a fixed size.
    void loadRowsCount();

    void loadPartitionAndMinMaxIndex();

    void checkConsistency(bool require_part_metadata);
};


using MergeTreeDataPartState = MergeTreeDataPart::State;

}
