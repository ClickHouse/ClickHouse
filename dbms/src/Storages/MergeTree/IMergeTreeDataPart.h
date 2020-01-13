#pragma once

#include <DataStreams/IBlockInputStream.h>

#include <Core/Row.h>
#include <Core/Block.h>
#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/AlterAnalysisResult.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Columns/IColumn.h>

#include <Poco/Path.h>

#include <shared_mutex>

namespace DB
{

struct ColumnSize;
class MergeTreeData;
struct FutureMergedMutatedPart;

class IMergeTreeReader;
class IMergeTreeDataPartWriter;

namespace ErrorCodes
{
    extern const int NOT_IMPLEMETED;
}
class IMergeTreeDataPart : public std::enable_shared_from_this<IMergeTreeDataPart>
{
public:

    using Checksums = MergeTreeDataPartChecksums;
    using Checksum = MergeTreeDataPartChecksums::Checksum;
    using ValueSizeMap = std::map<std::string, double>;

    using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;
    using MergeTreeWriterPtr = std::unique_ptr<IMergeTreeDataPartWriter>;

    using ColumnSizeByName = std::unordered_map<std::string, ColumnSize>;
    using NameToPosition = std::unordered_map<std::string, size_t>;

    virtual MergeTreeReaderPtr getReader(
        const NamesAndTypesList & columns_,
        const MarkRanges & mark_ranges,
        UncompressedCache * uncompressed_cache,
        MarkCache * mark_cache,
        const MergeTreeReaderSettings & reader_settings_,
        const ValueSizeMap & avg_value_size_hints_ = ValueSizeMap{},
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_ = ReadBufferFromFileBase::ProfileCallback{}) const = 0;

    virtual MergeTreeWriterPtr getWriter(
        const NamesAndTypesList & columns_list,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        const MergeTreeIndexGranularity & computed_index_granularity = {}) const = 0;

    virtual bool isStoredOnDisk() const = 0;


    virtual bool supportsVerticalMerge() const { return false; }

    /// NOTE: Returns zeros if column files are not found in checksums.
    /// NOTE: You must ensure that no ALTERs are in progress when calculating ColumnSizes.
    ///   (either by locking columns_lock, or by locking table structure).
    virtual ColumnSize getColumnSize(const String & /* name */, const IDataType & /* type */) const { return {}; }

    virtual ColumnSize getTotalColumnsSize() const { return {}; }

    /// Returns the name of a column with minimum compressed size (as returned by getColumnSize()).
    /// If no checksums are present returns the name of the first physically existing column.
    virtual String getColumnNameWithMinumumCompressedSize() const { return columns.front().name; }

    virtual String getFileNameForColumn(const NameAndTypePair & column) const = 0;

    virtual void setColumns(const NamesAndTypesList & columns_);

    virtual NameToNameMap createRenameMapForAlter(
        AlterAnalysisResult & /* analysis_result */,
        const NamesAndTypesList & /* old_columns */) const { return {}; }

    virtual ~IMergeTreeDataPart();

    // virtual Checksums check(
    //     bool require_checksums,
    //     const DataTypes & primary_key_data_types,    /// Check the primary key. If it is not necessary, pass an empty array.
    //     const MergeTreeIndices & indices = {}, /// Check skip indices
    //     std::function<bool()> is_cancelled = []{ return false; })
    // {
    //     return {};
    // }

    using ColumnToSize = std::map<std::string, UInt64>;
    virtual void accumulateColumnSizes(ColumnToSize & column_to_size) const;

    using Type = MergeTreeDataPartType;
    virtual Type getType() const = 0;

    static String typeToString(Type type);
    String getTypeName() { return typeToString(getType()); }

    IMergeTreeDataPart(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const DiskPtr & disk = {},
        const std::optional<String> & relative_path = {});

    IMergeTreeDataPart(
        MergeTreeData & storage_,
        const String & name_,
        const DiskPtr & disk = {},
        const std::optional<String> & relative_path = {});

    void assertOnDisk() const;

    void remove() const;

    /// Initialize columns (from columns.txt if exists, or create from column files if not).
    /// Load checksums from checksums.txt if exists. Load index if required.
    void loadColumnsChecksumsIndexes(bool require_columns_checksums, bool check_consistency);

    String getMarksFileExtension() const { return index_granularity_info.marks_file_extension; }

    /// Generate the new name for this part according to `new_part_info` and min/max dates from the old name.
    /// This is useful when you want to change e.g. block numbers or the mutation version of the part.
    String getNewName(const MergeTreePartInfo & new_part_info) const;

    // Block sample_block;
    std::optional<size_t> getColumnPosition(const String & column_name) const;

    bool contains(const IMergeTreeDataPart & other) const { return info.contains(other.info); }

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
    MergeTreeIndexGranularityInfo index_granularity_info;

    DiskPtr disk;

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

    /// Frozen by ALTER TABLE ... FREEZE ... It is used for information purposes in system.parts table.
    mutable std::atomic<bool> is_frozen {false};

    /**
     * Part state is a stage of its lifetime. States are ordered and state of a part could be increased only.
     * Part state should be modified under data_parts mutex.
     *
     * Possible state transitions:
     * Temporary -> Precommitted:   we are trying to commit a fetched, inserted or merged part to active set
     * Precommitted -> Outdated:    we could not to add a part to active set and doing a rollback (for example it is duplicated part)
     * Precommitted -> Commited:    we successfully committed a part to active dataset
     * Precommitted -> Outdated:    a part was replaced by a covering part or DROP PARTITION
     * Outdated -> Deleting:        a cleaner selected this part for deletion
     * Deleting -> Outdated:        if an ZooKeeper error occurred during the deletion, we will retry deletion
     * Committed -> DeleteOnDestroy if part was moved to another disk
     */
    enum class State
    {
        Temporary,       /// the part is generating now, it is not in data_parts list
        PreCommitted,    /// the part is in data_parts, but not used for SELECTs
        Committed,       /// active data part, used by current and upcoming SELECTs
        Outdated,        /// not active data part, but could be used by only current SELECTs, could be deleted after SELECTs finishes
        Deleting,        /// not active data part with identity refcounter, it is deleting right now by a cleaner
        DeleteOnDestroy, /// part was moved to another disk and should be deleted in own destructor
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
    NameSet expired_columns;

    /** It is blocked for writing when changing columns, checksums or any part files.
        * Locked to read when reading columns, checksums or any part files.
        */
    mutable std::shared_mutex columns_lock;

    ColumnSizeByName columns_sizes;

    /// For data in RAM ('index')
    UInt64 getIndexSizeInBytes() const;
    UInt64 getIndexSizeInAllocatedBytes() const;
    UInt64 getMarksCount() const;

    size_t getFileSizeOrZero(const String & file_name) const;
    String getFullPath() const;
    void renameTo(const String & new_relative_path, bool remove_new_dir_if_exists = false) const;
    void renameToDetached(const String & prefix) const;
    void makeCloneInDetached(const String & prefix) const;

    /// Makes full clone of part in detached/ on another disk
    void makeCloneOnDiskDetached(const ReservationPtr & reservation) const;

    /// Checks that .bin and .mrk files exist
    virtual bool hasColumnFiles(const String & /* column */, const IDataType & /* type */) const{ return false; }

    static UInt64 calculateTotalSizeOnDisk(const String & from);

protected:

    void removeIfNeeded();
    virtual void checkConsistency(bool require_part_metadata) const;

private:
    /// In compact parts order of columns is necessary
    NameToPosition column_name_to_position;

    /// Reads columns names and types from columns.txt
    void loadColumns(bool require);

    /// If checksums.txt exists, reads files' checksums (and sizes) from it
    void loadChecksums(bool require);

    /// Loads marks index granularity into memory
    virtual void loadIndexGranularity();

    /// Loads index file.
    void loadIndex();

    /// Load rows count for this part from disk (for the newer storage format version).
    /// For the older format version calculates rows count from the size of a column with a fixed size.
    void loadRowsCount();

    /// Loads ttl infos in json format from file ttl.txt. If file doesn`t exists assigns ttl infos with all zeros
    void loadTTLInfos();

    void loadPartitionAndMinMaxIndex();

    void loadColumnSizes();

    String getRelativePathForDetachedPart(const String & prefix) const;
};

using MergeTreeDataPartState = IMergeTreeDataPart::State;
using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

bool isCompactPart(const MergeTreeDataPartPtr & data_part);
bool isWidePart(const MergeTreeDataPartPtr & data_part);

}
