#pragma once

#include <Core/Block.h>
#include <base/types.h>
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
#include <Storages/MergeTree/KeyCondition.h>
#include <Interpreters/TransactionVersionMetadata.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Storages/MergeTree/IPartMetadataManager.h>

#include <shared_mutex>

namespace zkutil
{
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{

struct ColumnSize;
class MergeTreeData;
struct FutureMergedMutatedPart;
class IReservation;
using ReservationPtr = std::unique_ptr<IReservation>;

class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;

class IMergeTreeReader;
class IMergeTreeDataPartWriter;
class MarkCache;
class UncompressedCache;
class MergeTreeTransaction;

/// Description of the data part.
class IMergeTreeDataPart : public std::enable_shared_from_this<IMergeTreeDataPart>
{
public:
    static constexpr auto DATA_FILE_EXTENSION = ".bin";

    using Checksums = MergeTreeDataPartChecksums;
    using Checksum = MergeTreeDataPartChecksums::Checksum;
    using ValueSizeMap = std::map<std::string, double>;

    using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;
    using MergeTreeWriterPtr = std::unique_ptr<IMergeTreeDataPartWriter>;

    using ColumnSizeByName = std::unordered_map<std::string, ColumnSize>;
    using NameToNumber = std::unordered_map<std::string, size_t>;

    using IndexSizeByName = std::unordered_map<std::string, ColumnSize>;

    using Type = MergeTreeDataPartType;

    using uint128 = IPartMetadataManager::uint128;


    IMergeTreeDataPart(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const VolumePtr & volume,
        const std::optional<String> & relative_path,
        Type part_type_,
        const IMergeTreeDataPart * parent_part_);

    IMergeTreeDataPart(
        const MergeTreeData & storage_,
        const String & name_,
        const VolumePtr & volume,
        const std::optional<String> & relative_path,
        Type part_type_,
        const IMergeTreeDataPart * parent_part_);

    virtual MergeTreeReaderPtr getReader(
        const NamesAndTypesList & columns_,
        const StorageMetadataPtr & metadata_snapshot,
        const MarkRanges & mark_ranges,
        UncompressedCache * uncompressed_cache,
        MarkCache * mark_cache,
        const MergeTreeReaderSettings & reader_settings_,
        const ValueSizeMap & avg_value_size_hints_ = ValueSizeMap{},
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_ = ReadBufferFromFileBase::ProfileCallback{}) const = 0;

    virtual MergeTreeWriterPtr getWriter(
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        const MergeTreeIndexGranularity & computed_index_granularity = {}) const = 0;

    virtual bool isStoredOnDisk() const = 0;

    virtual bool isStoredOnRemoteDisk() const = 0;

    virtual bool supportsVerticalMerge() const { return false; }

    /// NOTE: Returns zeros if column files are not found in checksums.
    /// Otherwise return information about column size on disk.
    ColumnSize getColumnSize(const String & column_name) const;

    /// NOTE: Returns zeros if secondary indexes are not found in checksums.
    /// Otherwise return information about secondary index size on disk.
    IndexSize getSecondaryIndexSize(const String & secondary_index_name) const;

    /// Return information about column size on disk for all columns in part
    ColumnSize getTotalColumnsSize() const { return total_columns_size; }

    /// Return information about secondary indexes size on disk for all indexes in part
    IndexSize getTotalSeconaryIndicesSize() const { return total_secondary_indices_size; }

    virtual String getFileNameForColumn(const NameAndTypePair & column) const = 0;

    virtual ~IMergeTreeDataPart();

    using ColumnToSize = std::map<std::string, UInt64>;
    /// Populates columns_to_size map (compressed size).
    void accumulateColumnSizes(ColumnToSize & /* column_to_size */) const;

    Type getType() const { return part_type; }

    String getTypeName() const { return getType().toString(); }

    void setColumns(const NamesAndTypesList & new_columns);

    const NamesAndTypesList & getColumns() const { return columns; }

    void setSerializationInfos(const SerializationInfoByName & new_infos);

    const SerializationInfoByName & getSerializationInfos() const { return serialization_infos; }

    SerializationPtr getSerialization(const NameAndTypePair & column) const;

    /// Throws an exception if part is not stored in on-disk format.
    void assertOnDisk() const;

    void remove() const;

    void projectionRemove(const String & parent_to, bool keep_shared_data = false) const;

    /// Initialize columns (from columns.txt if exists, or create from column files if not).
    /// Load checksums from checksums.txt if exists. Load index if required.
    void loadColumnsChecksumsIndexes(bool require_columns_checksums, bool check_consistency);
    void appendFilesOfColumnsChecksumsIndexes(Strings & files, bool include_projection = false) const;

    String getMarksFileExtension() const { return index_granularity_info.marks_file_extension; }

    /// Generate the new name for this part according to `new_part_info` and min/max dates from the old name.
    /// This is useful when you want to change e.g. block numbers or the mutation version of the part.
    String getNewName(const MergeTreePartInfo & new_part_info) const;

    /// Returns column position in part structure or std::nullopt if it's missing in part.
    ///
    /// NOTE: Doesn't take column renames into account, if some column renames
    /// take place, you must take original name of column for this part from
    /// storage and pass it to this method.
    std::optional<size_t> getColumnPosition(const String & column_name) const;

    /// Returns the name of a column with minimum compressed size (as returned by getColumnSize()).
    /// If no checksums are present returns the name of the first physically existing column.
    String getColumnNameWithMinimumCompressedSize(
        const StorageSnapshotPtr & storage_snapshot, bool with_subcolumns) const;

    bool contains(const IMergeTreeDataPart & other) const { return info.contains(other.info); }

    /// If the partition key includes date column (a common case), this function will return min and max values for that column.
    std::pair<DayNum, DayNum> getMinMaxDate() const;

    /// otherwise, if the partition key includes dateTime column (also a common case), this function will return min and max values for that column.
    std::pair<time_t, time_t> getMinMaxTime() const;

    bool isEmpty() const { return rows_count == 0; }

    /// Compute part block id for zero level part. Otherwise throws an exception.
    /// If token is not empty, block id is calculated based on it instead of block data
    String getZeroLevelPartBlockID(std::string_view token) const;

    const MergeTreeData & storage;

    String name;
    MergeTreePartInfo info;

    /// Part unique identifier.
    /// The intention is to use it for identifying cases where the same part is
    /// processed by multiple shards.
    UUID uuid = UUIDHelpers::Nil;

    VolumePtr volume;

    /// A directory path (relative to storage's path) where part data is actually stored
    /// Examples: 'detached/tmp_fetch_<name>', 'tmp_<name>', '<name>'
    /// NOTE: Cannot have trailing slash.
    mutable String relative_path;
    MergeTreeIndexGranularityInfo index_granularity_info;

    size_t rows_count = 0;

    time_t modification_time = 0;
    /// When the part is removed from the working set. Changes once.
    mutable std::atomic<time_t> remove_time { std::numeric_limits<time_t>::max() };

    /// If true, the destructor will delete the directory with the part.
    /// FIXME Why do we need this flag? What's difference from Temporary and DeleteOnDestroy state? Can we get rid of this?
    bool is_temp = false;

    /// If true it means that there are no ZooKeeper node for this part, so it should be deleted only from filesystem
    bool is_duplicate = false;

    /// Frozen by ALTER TABLE ... FREEZE ... It is used for information purposes in system.parts table.
    mutable std::atomic<bool> is_frozen {false};

    /// Flag for keep S3 data when zero-copy replication over S3 turned on.
    mutable bool force_keep_shared_data = false;

    /**
     * Part state is a stage of its lifetime. States are ordered and state of a part could be increased only.
     * Part state should be modified under data_parts mutex.
     *
     * Possible state transitions:
     * Temporary -> PreActive:       we are trying to add a fetched, inserted or merged part to active set
     * PreActive -> Outdated:        we could not add a part to active set and are doing a rollback (for example it is duplicated part)
     * PreActive -> Active:          we successfully added a part to active dataset
     * PreActive -> Outdated:        a part was replaced by a covering part or DROP PARTITION
     * Outdated -> Deleting:         a cleaner selected this part for deletion
     * Deleting -> Outdated:         if an ZooKeeper error occurred during the deletion, we will retry deletion
     * Active -> DeleteOnDestroy:    if part was moved to another disk
     */
    enum class State
    {
        Temporary,       /// the part is generating now, it is not in data_parts list
        PreActive,    /// the part is in data_parts, but not used for SELECTs
        Active,       /// active data part, used by current and upcoming SELECTs
        Outdated,        /// not active data part, but could be used by only current SELECTs, could be deleted after SELECTs finishes
        Deleting,        /// not active data part with identity refcounter, it is deleting right now by a cleaner
        DeleteOnDestroy, /// part was moved to another disk and should be deleted in own destructor
    };

    using TTLInfo = MergeTreeDataPartTTLInfo;
    using TTLInfos = MergeTreeDataPartTTLInfos;

    mutable TTLInfos ttl_infos;

    /// Current state of the part. If the part is in working set already, it should be accessed via data_parts mutex
    void setState(State new_state) const;
    State getState() const;

    static constexpr std::string_view stateString(State state) { return magic_enum::enum_name(state); }
    constexpr std::string_view stateString() const { return stateString(state); }

    String getNameWithState() const { return fmt::format("{} (state {})", name, stateString()); }

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
        std::vector<Range> hyperrectangle;
        bool initialized = false;

    public:
        MinMaxIndex() = default;

        /// For month-based partitioning.
        MinMaxIndex(DayNum min_date, DayNum max_date)
            : hyperrectangle(1, Range(min_date, true, max_date, true))
            , initialized(true)
        {
        }

        void load(const MergeTreeData & data, const PartMetadataManagerPtr & manager);

        using WrittenFiles = std::vector<std::unique_ptr<WriteBufferFromFileBase>>;

        [[nodiscard]] WrittenFiles store(const MergeTreeData & data, const DiskPtr & disk_, const String & part_path, Checksums & checksums) const;
        [[nodiscard]] WrittenFiles store(const Names & column_names, const DataTypes & data_types, const DiskPtr & disk_, const String & part_path, Checksums & checksums) const;

        void update(const Block & block, const Names & column_names);
        void merge(const MinMaxIndex & other);
        static void appendFiles(const MergeTreeData & data, Strings & files);
    };

    using MinMaxIndexPtr = std::shared_ptr<MinMaxIndex>;

    MinMaxIndexPtr minmax_idx;

    Checksums checksums;

    /// Columns with values, that all have been zeroed by expired ttl
    NameSet expired_columns;

    CompressionCodecPtr default_codec;

    mutable VersionMetadata version;

    /// For data in RAM ('index')
    UInt64 getIndexSizeInBytes() const;
    UInt64 getIndexSizeInAllocatedBytes() const;
    UInt64 getMarksCount() const;

    UInt64 getBytesOnDisk() const { return bytes_on_disk; }
    void setBytesOnDisk(UInt64 bytes_on_disk_) { bytes_on_disk = bytes_on_disk_; }

    size_t getFileSizeOrZero(const String & file_name) const;

    /// Returns path to part dir relatively to disk mount point
    String getFullRelativePath() const;

    /// Returns full path to part dir
    String getFullPath() const;

    /// Moves a part to detached/ directory and adds prefix to its name
    void renameToDetached(const String & prefix) const;

    /// Makes checks and move part to new directory
    /// Changes only relative_dir_name, you need to update other metadata (name, is_temp) explicitly
    virtual void renameTo(const String & new_relative_path, bool remove_new_dir_if_exists) const;

    /// Cleanup shared locks made with old name after part renaming
    virtual void cleanupOldName(const String & old_part_name) const;

    /// Makes clone of a part in detached/ directory via hard links
    virtual void makeCloneInDetached(const String & prefix, const StorageMetadataPtr & metadata_snapshot) const;

    /// Makes full clone of part in specified subdirectory (relative to storage data directory, e.g. "detached") on another disk
    void makeCloneOnDisk(const DiskPtr & disk, const String & directory_name) const;

    /// Checks that .bin and .mrk files exist.
    ///
    /// NOTE: Doesn't take column renames into account, if some column renames
    /// take place, you must take original name of column for this part from
    /// storage and pass it to this method.
    virtual bool hasColumnFiles(const NameAndTypePair & /* column */) const { return false; }

    /// Returns true if this part shall participate in merges according to
    /// settings of given storage policy.
    bool shallParticipateInMerges(const StoragePolicyPtr & storage_policy) const;

    /// Calculate the total size of the entire directory with all the files
    static UInt64 calculateTotalSizeOnDisk(const DiskPtr & disk_, const String & from);

    /// Calculate column and secondary indices sizes on disk.
    void calculateColumnsAndSecondaryIndicesSizesOnDisk();

    String getRelativePathForPrefix(const String & prefix, bool detached = false) const;

    bool isProjectionPart() const { return parent_part != nullptr; }

    const IMergeTreeDataPart * getParentPart() const { return parent_part; }

    const std::map<String, std::shared_ptr<IMergeTreeDataPart>> & getProjectionParts() const { return projection_parts; }

    void addProjectionPart(const String & projection_name, std::shared_ptr<IMergeTreeDataPart> && projection_part)
    {
        projection_parts.emplace(projection_name, std::move(projection_part));
    }

    bool hasProjection(const String & projection_name) const
    {
        return projection_parts.find(projection_name) != projection_parts.end();
    }

    void loadProjections(bool require_columns_checksums, bool check_consistency);

    /// Return set of metadata file names without checksums. For example,
    /// columns.txt or checksums.txt itself.
    NameSet getFileNamesWithoutChecksums() const;

    /// File with compression codec name which was used to compress part columns
    /// by default. Some columns may have their own compression codecs, but
    /// default will be stored in this file.
    static inline constexpr auto DEFAULT_COMPRESSION_CODEC_FILE_NAME = "default_compression_codec.txt";

    static inline constexpr auto DELETE_ON_DESTROY_MARKER_FILE_NAME = "delete-on-destroy.txt";

    static inline constexpr auto UUID_FILE_NAME = "uuid.txt";

    /// File that contains information about kinds of serialization of columns
    /// and information that helps to choose kind of serialization later during merging
    /// (number of rows, number of rows with default values, etc).
    static inline constexpr auto SERIALIZATION_FILE_NAME = "serialization.json";

    static inline constexpr auto TXN_VERSION_METADATA_FILE_NAME = "txn_version.txt";

    /// One of part files which is used to check how many references (I'd like
    /// to say hardlinks, but it will confuse even more) we have for the part
    /// for zero copy replication. Sadly it's very complex.
    ///
    /// NOTE: it's not a random "metadata" file for part like 'columns.txt'. If
    /// two relative parts (for example all_1_1_0 and all_1_1_0_100) has equal
    /// checksums.txt it means that one part was obtained by FREEZE operation or
    /// it was mutation without any change for source part. In this case we
    /// really don't need to remove data from remote FS and need only decrement
    /// reference counter locally.
    static inline constexpr auto FILE_FOR_REFERENCES_CHECK = "checksums.txt";

    /// Checks that all TTLs (table min/max, column ttls, so on) for part
    /// calculated. Part without calculated TTL may exist if TTL was added after
    /// part creation (using alter query with materialize_ttl setting).
    bool checkAllTTLCalculated(const StorageMetadataPtr & metadata_snapshot) const;

    /// Return some uniq string for file.
    /// Required for distinguish different copies of the same part on remote FS.
    String getUniqueId() const;

    /// Ensures that creation_tid was correctly set after part creation.
    void assertHasVersionMetadata(MergeTreeTransaction * txn) const;

    /// [Re]writes file with transactional metadata on disk
    void storeVersionMetadata() const;

    /// Appends the corresponding CSN to file on disk (without fsync)
    void appendCSNToVersionMetadata(VersionMetadata::WhichCSN which_csn) const;

    /// Appends removal TID to file on disk (with fsync)
    void appendRemovalTIDToVersionMetadata(bool clear = false) const;

    /// Loads transactional metadata from disk
    void loadVersionMetadata() const;

    /// Returns true if part was created or removed by a transaction
    bool wasInvolvedInTransaction() const;

    /// Moar hardening: this method is supposed to be used for debug assertions
    bool assertHasValidVersionMetadata() const;

    /// Return hardlink count for part.
    /// Required for keep data on remote FS when part has shadow copies.
    UInt32 getNumberOfRefereneces() const;

    /// Get checksums of metadata file in part directory
    IMergeTreeDataPart::uint128 getActualChecksumByFile(const String & file_path) const;

    /// Check metadata in cache is consistent with actual metadata on disk(if use_metadata_cache is true)
    std::unordered_map<String, uint128> checkMetadata() const;


protected:

    /// Total size of all columns, calculated once in calcuateColumnSizesOnDisk
    ColumnSize total_columns_size;

    /// Size for each column, calculated once in calcuateColumnSizesOnDisk
    ColumnSizeByName columns_sizes;

    ColumnSize total_secondary_indices_size;

    IndexSizeByName secondary_index_sizes;

    /// Total size on disk, not only columns. May not contain size of
    /// checksums.txt and columns.txt. 0 - if not counted;
    UInt64 bytes_on_disk{0};

    /// Columns description. Cannot be changed, after part initialization.
    NamesAndTypesList columns;

    const Type part_type;

    /// Not null when it's a projection part.
    const IMergeTreeDataPart * parent_part;

    std::map<String, std::shared_ptr<IMergeTreeDataPart>> projection_parts;

    /// Disabled when USE_ROCKSDB is OFF or use_metadata_cache is set to false in merge tree settings
    bool use_metadata_cache = false;

    mutable PartMetadataManagerPtr metadata_manager;

    void removeIfNeeded();

    virtual void checkConsistency(bool require_part_metadata) const;
    void checkConsistencyBase() const;

    /// Fill each_columns_size and total_size with sizes from columns files on
    /// disk using columns and checksums.
    virtual void calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const = 0;

    String getRelativePathForDetachedPart(const String & prefix) const;

    std::optional<bool> keepSharedDataInDecoupledStorage() const;

    void initializePartMetadataManager();


private:
    /// In compact parts order of columns is necessary
    NameToNumber column_name_to_position;

    /// Map from name of column to its serialization info.
    SerializationInfoByName serialization_infos;

    /// Reads part unique identifier (if exists) from uuid.txt
    void loadUUID();

    static void appendFilesOfUUID(Strings & files);

    /// Reads columns names and types from columns.txt
    void loadColumns(bool require);

    static void appendFilesOfColumns(Strings & files);

    /// If checksums.txt exists, reads file's checksums (and sizes) from it
    void loadChecksums(bool require);

    static void appendFilesOfChecksums(Strings & files);

    /// Loads marks index granularity into memory
    virtual void loadIndexGranularity();

    virtual void appendFilesOfIndexGranularity(Strings & files) const;

    /// Loads index file.
    void loadIndex();

    void appendFilesOfIndex(Strings & files) const;

    /// Load rows count for this part from disk (for the newer storage format version).
    /// For the older format version calculates rows count from the size of a column with a fixed size.
    void loadRowsCount();

    static void appendFilesOfRowsCount(Strings & files);

    /// Loads ttl infos in json format from file ttl.txt. If file doesn't exists assigns ttl infos with all zeros
    void loadTTLInfos();

    static void appendFilesOfTTLInfos(Strings & files);

    void loadPartitionAndMinMaxIndex();

    void calculateColumnsSizesOnDisk();

    void calculateSecondaryIndicesSizesOnDisk();

    void appendFilesOfPartitionAndMinMaxIndex(Strings & files) const;

    /// Load default compression codec from file default_compression_codec.txt
    /// if it not exists tries to deduce codec from compressed column without
    /// any specifial compression.
    void loadDefaultCompressionCodec();

    static void appendFilesOfDefaultCompressionCodec(Strings & files);

    /// Found column without specific compression and return codec
    /// for this column with default parameters.
    CompressionCodecPtr detectDefaultCompressionCodec() const;

    mutable State state{State::Temporary};

    /// This ugly flag is needed for debug assertions only
    mutable bool part_is_probably_removed_from_disk = false;
};

using MergeTreeDataPartState = IMergeTreeDataPart::State;
using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using MergeTreeMutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;

bool isCompactPart(const MergeTreeDataPartPtr & data_part);
bool isWidePart(const MergeTreeDataPartPtr & data_part);
bool isInMemoryPart(const MergeTreeDataPartPtr & data_part);

}
