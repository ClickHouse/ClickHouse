#pragma once

#include <atomic>
#include <unordered_map>
#include <IO/WriteSettings.h>
#include <Core/Block.h>
#include <base/types.h>
#include <base/defines.h>
#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeDataPartState.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeDataPartBuilder.h>
#include <Storages/ColumnsDescription.h>
#include <Interpreters/TransactionVersionMetadata.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Storages/MergeTree/IPartMetadataManager.h>
#include <Storages/MergeTree/PrimaryIndexCache.h>


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

class IMergeTreeReader;
class MarkCache;
class UncompressedCache;
class MergeTreeTransaction;

struct MergeTreeReadTaskInfo;
using MergeTreeReadTaskInfoPtr = std::shared_ptr<const MergeTreeReadTaskInfo>;

enum class DataPartRemovalState : uint8_t
{
    NOT_ATTEMPTED,
    VISIBLE_TO_TRANSACTIONS,
    NON_UNIQUE_OWNERSHIP,
    NOT_REACHED_REMOVAL_TIME,
    HAS_SKIPPED_MUTATION_PARENT,
    EMPTY_PART_COVERS_OTHER_PARTS,
    REMOVED,
};

/// Description of the data part.
class IMergeTreeDataPart : public std::enable_shared_from_this<IMergeTreeDataPart>, public DataPartStorageHolder
{
public:
    static constexpr auto DATA_FILE_EXTENSION = ".bin";

    using Checksums = MergeTreeDataPartChecksums;
    using Checksum = MergeTreeDataPartChecksums::Checksum;
    using ValueSizeMap = std::map<std::string, double>;
    using VirtualFields = std::unordered_map<String, Field>;

    using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;

    using ColumnSizeByName = std::unordered_map<std::string, ColumnSize>;
    using NameToNumber = std::unordered_map<std::string, size_t>;

    using Index = Columns;
    using IndexPtr = std::shared_ptr<const Index>;
    using IndexSizeByName = std::unordered_map<std::string, ColumnSize>;

    using Type = MergeTreeDataPartType;

    using uint128 = IPartMetadataManager::uint128;

    IMergeTreeDataPart(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const MutableDataPartStoragePtr & data_part_storage_,
        Type part_type_,
        const IMergeTreeDataPart * parent_part_);

    virtual MergeTreeReaderPtr getReader(
        const NamesAndTypesList & columns_,
        const StorageSnapshotPtr & storage_snapshot,
        const MarkRanges & mark_ranges,
        const VirtualFields & virtual_fields,
        UncompressedCache * uncompressed_cache,
        MarkCache * mark_cache,
        const AlterConversionsPtr & alter_conversions,
        const MergeTreeReaderSettings & reader_settings_,
        const ValueSizeMap & avg_value_size_hints_,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_) const = 0;

    virtual bool isStoredOnDisk() const = 0;
    virtual bool isStoredOnReadonlyDisk() const = 0;
    virtual bool isStoredOnRemoteDisk() const = 0;
    virtual bool isStoredOnRemoteDiskWithZeroCopySupport() const = 0;


    /// NOTE: Returns zeros if column files are not found in checksums.
    /// Otherwise return information about column size on disk.
    ColumnSize getColumnSize(const String & column_name) const;

    virtual std::optional<time_t> getColumnModificationTime(const String & column_name) const = 0;

    /// NOTE: Returns zeros if secondary indexes are not found in checksums.
    /// Otherwise return information about secondary index size on disk.
    IndexSize getSecondaryIndexSize(const String & secondary_index_name) const;

    /// Returns true if there is materialized index with specified name in part.
    bool hasSecondaryIndex(const String & index_name) const;

    /// Return information about column size on disk for all columns in part
    ColumnSize getTotalColumnsSize() const { return total_columns_size; }

    /// Return information about secondary indexes size on disk for all indexes in part
    IndexSize getTotalSecondaryIndicesSize() const { return total_secondary_indices_size; }

    virtual std::optional<String> getFileNameForColumn(const NameAndTypePair & column) const = 0;

    virtual ~IMergeTreeDataPart();

    using ColumnToSize = std::map<std::string, UInt64>;
    /// Populates columns_to_size map (compressed size).
    void accumulateColumnSizes(ColumnToSize & /* column_to_size */) const;

    Type getType() const { return part_type; }
    MergeTreeDataPartFormat getFormat() const { return {part_type, getDataPartStorage().getType()}; }

    String getTypeName() const { return getType().toString(); }

    /// We could have separate method like setMetadata, but it's much more convenient to set it up with columns
    void setColumns(const NamesAndTypesList & new_columns, const SerializationInfoByName & new_infos, int32_t metadata_version_);

    /// Version of metadata for part (columns, pk and so on)
    int32_t getMetadataVersion() const { return metadata_version; }

    const NamesAndTypesList & getColumns() const { return columns; }
    const ColumnsDescription & getColumnsDescription() const { return columns_description; }
    const ColumnsDescription & getColumnsDescriptionWithCollectedNested() const { return columns_description_with_collected_nested; }

    NameAndTypePair getColumn(const String & name) const;
    std::optional<NameAndTypePair> tryGetColumn(const String & column_name) const;

    /// Get sample column from part. For ordinary columns it just creates column using it's type.
    /// For columns with dynamic structure it reads sample column with 0 rows from the part.
    ColumnPtr getColumnSample(const NameAndTypePair & column) const;

    const SerializationInfoByName & getSerializationInfos() const { return serialization_infos; }

    const SerializationByName & getSerializations() const { return serializations; }

    SerializationPtr getSerialization(const String & column_name) const;
    SerializationPtr tryGetSerialization(const String & column_name) const;

    /// Throws an exception if part is not stored in on-disk format.
    void assertOnDisk() const;

    void remove();

    ColumnsStatistics loadStatistics() const;

    /// Initialize columns (from columns.txt if exists, or create from column files if not).
    /// Load various metadata into memory: checksums from checksums.txt, index if required, etc.
    void loadColumnsChecksumsIndexes(bool require_columns_checksums, bool check_consistency);
    void appendFilesOfColumnsChecksumsIndexes(Strings & files, bool include_projection = false) const;

    void loadRowsCountFileForUnexpectedPart();

    /// Loads marks and saves them into mark cache for specified columns.
    virtual void loadMarksToCache(const Names & column_names, MarkCache * mark_cache) const = 0;

    /// Removes marks from cache for all columns in part.
    virtual void removeMarksFromCache(MarkCache * mark_cache) const = 0;

    /// Removes data related to data part from mark and primary index caches.
    void clearCaches();

    /// Returns true if data related to data part may be stored in mark and primary index caches.
    bool mayStoreDataInCaches() const;

    String getMarksFileExtension() const { return index_granularity_info.mark_type.getFileExtension(); }

    /// Generate the new name for this part according to `new_part_info` and min/max dates from the old name.
    /// This is useful when you want to change e.g. block numbers or the mutation version of the part.
    String getNewName(const MergeTreePartInfo & new_part_info) const;

    /// Returns column position in part structure or std::nullopt if it's missing in part.
    ///
    /// NOTE: Doesn't take column renames into account, if some column renames
    /// take place, you must take original name of column for this part from
    /// storage and pass it to this method.
    std::optional<size_t> getColumnPosition(const String & column_name) const;
    const NameToNumber & getColumnPositions() const { return column_name_to_position; }

    /// Returns the name of a column with minimum compressed size (as returned by getColumnSize()).
    /// If no checksums are present returns the name of the first physically existing column.
    /// We pass a list of available columns since the ones available in the current storage snapshot might be smaller
    /// than the one the table has (e.g a DROP COLUMN happened) and we don't want to get a column not in the snapshot
    String getColumnNameWithMinimumCompressedSize(const NamesAndTypesList & available_columns) const;

    bool contains(const IMergeTreeDataPart & other) const { return info.contains(other.info); }

    /// If the partition key includes date column (a common case), this function will return min and max values for that column.
    std::pair<DayNum, DayNum> getMinMaxDate() const;

    /// otherwise, if the partition key includes dateTime column (also a common case), this function will return min and max values for that column.
    std::pair<time_t, time_t> getMinMaxTime() const;

    bool isEmpty() const { return rows_count == 0; }

    /// Compute part block id for zero level part. Otherwise throws an exception.
    /// If token is not empty, block id is calculated based on it instead of block data
    UInt128 getPartBlockIDHash() const;
    String getZeroLevelPartBlockID(std::string_view token) const;

    void setName(const String & new_name);

    const MergeTreeData & storage;

    const String & name;    // const ref to private mutable_name
    MergeTreePartInfo info;

    /// Part unique identifier.
    /// The intention is to use it for identifying cases where the same part is
    /// processed by multiple shards.
    UUID uuid = UUIDHelpers::Nil;

    MergeTreeIndexGranularityInfo index_granularity_info;

    size_t rows_count = 0;

    /// Existing rows count (excluding lightweight deleted rows)
    std::optional<size_t> existing_rows_count;

    time_t modification_time = 0;
    /// When the part is removed from the working set. Changes once.
    mutable std::atomic<time_t> remove_time { std::numeric_limits<time_t>::max() };

    /// If true, the destructor will delete the directory with the part.
    /// FIXME Why do we need this flag? What's difference from Temporary and DeleteOnDestroy state? Can we get rid of this?
    bool is_temp = false;

    /// This type and the field remove_tmp_policy is used as a hint
    /// to help avoid communication with keeper when temporary part is deleting.
    /// The common procedure is to ask the keeper with unlock request to release a references to the blobs.
    /// And then follow the keeper answer decide remove or preserve the blobs in that part from s3.
    /// However in some special cases Clickhouse can make a decision without asking keeper.
    enum class BlobsRemovalPolicyForTemporaryParts : uint8_t
    {
        /// decision about removing blobs is determined by keeper, the common case
        ASK_KEEPER,
        /// is set when Clickhouse is sure that the blobs in the part are belong only to it, other replicas have not seen them yet
        REMOVE_BLOBS,
        /// is set when Clickhouse is sure that the blobs belong to other replica and current replica has not locked them on s3 yet
        PRESERVE_BLOBS,
        /// remove blobs even if the part is not temporary
        REMOVE_BLOBS_OF_NOT_TEMPORARY,
    };
    BlobsRemovalPolicyForTemporaryParts remove_tmp_policy = BlobsRemovalPolicyForTemporaryParts::ASK_KEEPER;

    /// If true it means that there are no ZooKeeper node for this part, so it should be deleted only from filesystem
    bool is_duplicate = false;

    /// Frozen by ALTER TABLE ... FREEZE ... It is used for information purposes in system.parts table.
    mutable std::atomic<bool> is_frozen {false};

    /// If it is a projection part, it can be broken sometimes.
    mutable std::atomic<bool> is_broken {false};
    mutable std::string exception;
    mutable int exception_code = 0;
    mutable std::mutex broken_reason_mutex;

    /// Indicates that the part was marked Outdated by PartCheckThread because the part was not committed to ZooKeeper
    mutable bool is_unexpected_local_part = false;

    /// Indicates that the part was detached and marked Outdated because it's broken
    mutable std::atomic_bool was_removed_as_broken = false;

    /// Flag for keep S3 data when zero-copy replication over S3 turned on.
    mutable bool force_keep_shared_data = false;

    /// Some old parts don't have metadata version, so we set it to the current table's version when loading the part
    bool old_part_with_no_metadata_version_on_disk = false;

    bool new_part_was_committed_to_zookeeper_after_rename_on_disk = false;

    using TTLInfo = MergeTreeDataPartTTLInfo;
    using TTLInfos = MergeTreeDataPartTTLInfos;

    mutable TTLInfos ttl_infos;

    /// Current state of the part. If the part is in working set already, it should be accessed via data_parts mutex
    void setState(MergeTreeDataPartState new_state) const;
    ALWAYS_INLINE MergeTreeDataPartState getState() const { return state; }

    static constexpr std::string_view stateString(MergeTreeDataPartState state) { return magic_enum::enum_name(state); }
    constexpr std::string_view stateString() const { return stateString(state); }

    String getNameWithState() const { return fmt::format("{} (state {})", name, stateString()); }

    /// Returns true if state of part is one of affordable_states
    bool checkState(const std::initializer_list<MergeTreeDataPartState> & affordable_states) const
    {
        for (auto affordable_state : affordable_states)
        {
            if (state == affordable_state)
                return true;
        }
        return false;
    }

    /// Throws an exception if state of the part is not in affordable_states
    void assertState(const std::initializer_list<MergeTreeDataPartState> & affordable_states) const;

    MergeTreePartition partition;

    /// Amount of rows between marks
    /// As index always loaded into memory
    MergeTreeIndexGranularityPtr index_granularity;

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

        [[nodiscard]] WrittenFiles store(const MergeTreeData & data, IDataPartStorage & part_storage, Checksums & checksums) const;
        [[nodiscard]] WrittenFiles store(const Names & column_names, const DataTypes & data_types, IDataPartStorage & part_storage, Checksums & checksums) const;

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

    /// Version of part metadata (columns, pk and so on). Managed properly only for replicated merge tree.
    int32_t metadata_version;

    IndexPtr getIndex() const;
    IndexPtr loadIndexToCache(PrimaryIndexCache & index_cache) const;
    void moveIndexToCache(PrimaryIndexCache & index_cache);
    void removeIndexFromCache(PrimaryIndexCache * index_cache) const;

    void setIndex(Columns index_columns);
    void unloadIndex();
    bool isIndexLoaded() const;

    /// For data in RAM ('index')
    UInt64 getIndexSizeInBytes() const;
    UInt64 getIndexSizeInAllocatedBytes() const;
    UInt64 getIndexGranularityBytes() const;
    UInt64 getIndexGranularityAllocatedBytes() const;
    UInt64 getMarksCount() const;
    UInt64 getIndexSizeFromFile() const;

    UInt64 getBytesOnDisk() const { return bytes_on_disk; }
    UInt64 getBytesUncompressedOnDisk() const { return bytes_uncompressed_on_disk; }
    void setBytesOnDisk(UInt64 bytes_on_disk_) { bytes_on_disk = bytes_on_disk_; }
    void setBytesUncompressedOnDisk(UInt64 bytes_uncompressed_on_disk_) { bytes_uncompressed_on_disk = bytes_uncompressed_on_disk_; }

    /// Returns estimated size of existing rows if setting exclude_deleted_rows_for_part_size_in_merge is true
    /// Otherwise returns bytes_on_disk
    UInt64 getExistingBytesOnDisk() const;

    size_t getFileSizeOrZero(const String & file_name) const;
    auto getFilesChecksums() const { return checksums.files; }

    /// Moves a part to detached/ directory and adds prefix to its name
    void renameToDetached(const String & prefix, bool ignore_error = false);

    /// Makes checks and move part to new directory
    /// Changes only relative_dir_name, you need to update other metadata (name, is_temp) explicitly
    virtual void renameTo(const String & new_relative_path, bool remove_new_dir_if_exists);

    /// Makes clone of a part in detached/ directory via hard links
    virtual DataPartStoragePtr makeCloneInDetached(const String & prefix, const StorageMetadataPtr & metadata_snapshot,
                                                   const DiskTransactionPtr & disk_transaction) const;

    /// Makes full clone of part in specified subdirectory (relative to storage data directory, e.g. "detached") on another disk
    MutableDataPartStoragePtr makeCloneOnDisk(
        const DiskPtr & disk,
        const String & directory_name,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        const std::function<void()> & cancellation_hook) const;

    /// Checks that .bin and .mrk files exist.
    ///
    /// NOTE: Doesn't take column renames into account, if some column renames
    /// take place, you must take original name of column for this part from
    /// storage and pass it to this method.
    virtual bool hasColumnFiles(const NameAndTypePair & /* column */) const { return false; }

    /// Returns true if this part shall participate in merges according to
    /// settings of given storage policy.
    bool shallParticipateInMerges(const StoragePolicyPtr & storage_policy) const;

    /// Calculate column and secondary indices sizes on disk.
    void calculateColumnsAndSecondaryIndicesSizesOnDisk(std::optional<Block> columns_sample = std::nullopt);

    std::optional<String> getRelativePathForPrefix(const String & prefix, bool detached = false, bool broken = false) const;

    /// This method ignores current tmp prefix of part and returns
    /// the name of part when it was or will be in Active state.
    String getRelativePathOfActivePart() const;

    bool isProjectionPart() const { return parent_part != nullptr; }

    /// Check if the part is in the `/moving` directory
    bool isMovingPart() const;

    const IMergeTreeDataPart * getParentPart() const { return parent_part; }
    String getParentPartName() const { return parent_part_name; }

    const std::map<String, std::shared_ptr<IMergeTreeDataPart>> & getProjectionParts() const { return projection_parts; }

    MergeTreeDataPartBuilder getProjectionPartBuilder(const String & projection_name, bool is_temp_projection = false);

    void addProjectionPart(const String & projection_name, std::shared_ptr<IMergeTreeDataPart> && projection_part);

    void markProjectionPartAsBroken(const String & projection_name, const String & message, int code) const;

    bool hasProjection(const String & projection_name) const { return projection_parts.contains(projection_name); }

    bool hasProjection() const { return !projection_parts.empty(); }

    bool hasBrokenProjection(const String & projection_name) const;

    /// Return true, if all projections were loaded successfully and none was marked as broken.
    void loadProjections(
        bool require_columns_checksums,
        bool check_consistency,
        bool & has_broken_projection,
        bool if_not_loaded = false,
        bool only_metadata = false);

    /// If checksums.txt exists, reads file's checksums (and sizes) from it
    void loadChecksums(bool require);

    void setBrokenReason(const String & message, int code) const;

    /// Return set of metadata file names without checksums. For example,
    /// columns.txt or checksums.txt itself.
    NameSet getFileNamesWithoutChecksums() const;

    /// File with compression codec name which was used to compress part columns
    /// by default. Some columns may have their own compression codecs, but
    /// default will be stored in this file.
    static constexpr auto DEFAULT_COMPRESSION_CODEC_FILE_NAME = "default_compression_codec.txt";

    /// "delete-on-destroy.txt" is deprecated. It is no longer being created, only is removed.
    static constexpr auto DELETE_ON_DESTROY_MARKER_FILE_NAME_DEPRECATED = "delete-on-destroy.txt";

    static constexpr auto UUID_FILE_NAME = "uuid.txt";

    /// File that contains information about kinds of serialization of columns
    /// and information that helps to choose kind of serialization later during merging
    /// (number of rows, number of rows with default values, etc).
    static constexpr auto SERIALIZATION_FILE_NAME = "serialization.json";

    /// Version used for transactions.
    static constexpr auto TXN_VERSION_METADATA_FILE_NAME = "txn_version.txt";


    static constexpr auto METADATA_VERSION_FILE_NAME = "metadata_version.txt";

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
    static constexpr auto FILE_FOR_REFERENCES_CHECK = "checksums.txt";

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
    void storeVersionMetadata(bool force = false) const;

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

    /// True if the part supports lightweight delete mutate.
    bool supportLightweightDeleteMutate() const;

    /// True if here is lightweight deleted mask file in part.
    bool hasLightweightDelete() const;

    /// Read existing rows count from _row_exists column
    UInt64 readExistingRowsCount();

    void writeChecksums(const MergeTreeDataPartChecksums & checksums_, const WriteSettings & settings);

    /// Checks the consistency of this data part.
    void checkConsistency(bool require_part_metadata) const;

    /// Checks the consistency of this data part, and check the consistency of its projections (if any) as well.
    void checkConsistencyWithProjections(bool require_part_metadata) const;

    /// "delete-on-destroy.txt" is deprecated. It is no longer being created, only is removed.
    /// TODO: remove this method after some time.
    void removeDeleteOnDestroyMarker();

    /// It may look like a stupid joke. but these two methods are absolutely unrelated.
    /// This one is about removing file with metadata about part version (for transactions)
    void removeVersionMetadata();
    /// This one is about removing file with version of part's metadata (columns, pk and so on)
    void removeMetadataVersion();

    static std::optional<String> getStreamNameOrHash(
        const String & name,
        const IMergeTreeDataPart::Checksums & checksums);

    static std::optional<String> getStreamNameOrHash(
        const String & name,
        const String & extension,
        const IDataPartStorage & storage_);

    static std::optional<String> getStreamNameForColumn(
        const String & column_name,
        const ISerialization::SubstreamPath & substream_path,
        const Checksums & checksums_);

    static std::optional<String> getStreamNameForColumn(
        const NameAndTypePair & column,
        const ISerialization::SubstreamPath & substream_path,
        const Checksums & checksums_);

    static std::optional<String> getStreamNameForColumn(
        const String & column_name,
        const ISerialization::SubstreamPath & substream_path,
        const String & extension,
        const IDataPartStorage & storage_);

    static std::optional<String> getStreamNameForColumn(
        const NameAndTypePair & column,
        const ISerialization::SubstreamPath & substream_path,
        const String & extension,
        const IDataPartStorage & storage_);

    mutable std::atomic<DataPartRemovalState> removal_state = DataPartRemovalState::NOT_ATTEMPTED;

    mutable std::atomic<time_t> last_removal_attempt_time = 0;

protected:
    /// Primary key (correspond to primary.idx file).
    /// Lazily loaded in RAM. Contains each index_granularity-th value of primary key tuple.
    /// Note that marks (also correspond to primary key) are not always in RAM, but cached. See MarkCache.h.
    mutable std::mutex index_mutex;
    mutable IndexPtr index;

    /// Total size of all columns, calculated once in calcuateColumnSizesOnDisk
    ColumnSize total_columns_size;

    /// Size for each column, calculated once in calcuateColumnSizesOnDisk
    ColumnSizeByName columns_sizes;

    ColumnSize total_secondary_indices_size;

    IndexSizeByName secondary_index_sizes;

    /// Total size on disk, not only columns. May not contain size of
    /// checksums.txt and columns.txt. 0 - if not counted;
    UInt64 bytes_on_disk{0};
    UInt64 bytes_uncompressed_on_disk{0};

    /// Columns description. Cannot be changed, after part initialization.
    NamesAndTypesList columns;

    const Type part_type;

    /// Not null when it's a projection part.
    const IMergeTreeDataPart * parent_part;
    String parent_part_name;

    mutable std::map<String, std::shared_ptr<IMergeTreeDataPart>> projection_parts;

    mutable PartMetadataManagerPtr metadata_manager;

    void removeIfNeeded() noexcept;

    /// Fill each_columns_size and total_size with sizes from columns files on
    /// disk using columns and checksums.
    virtual void calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size, std::optional<Block> columns_sample) const = 0;

    std::optional<String> getRelativePathForDetachedPart(const String & prefix, bool broken) const;

    /// Checks that part can be actually removed from disk.
    /// In ordinary scenario always returns true, but in case of
    /// zero-copy replication part can be hold by some other replicas.
    ///
    /// If method return false than only metadata of part from
    /// local storage can be removed, leaving data in remove FS untouched.
    ///
    /// If method return true, than files can be actually removed from remote
    /// storage storage, excluding files in the second returned argument.
    /// They can be hardlinks to some newer parts.
    std::pair<bool, NameSet> canRemovePart() const;

    void initializePartMetadataManager();

    void initializeIndexGranularityInfo();

    virtual void doCheckConsistency(bool require_part_metadata) const;

private:
    String mutable_name;
    mutable MergeTreeDataPartState state{MergeTreeDataPartState::Temporary};

    /// In compact parts order of columns is necessary
    NameToNumber column_name_to_position;

    /// Map from name of column to its serialization info.
    SerializationInfoByName serialization_infos;

    /// Serializations for every columns and subcolumns by their names.
    SerializationByName serializations;

    /// Columns description for more convenient access
    /// to columns by name and getting subcolumns.
    ColumnsDescription columns_description;

    /// The same as above but after call of Nested::collect().
    /// It is used while reading from wide parts.
    ColumnsDescription columns_description_with_collected_nested;

    /// Reads part unique identifier (if exists) from uuid.txt
    void loadUUID();

    static void appendFilesOfUUID(Strings & files);

    /// Reads columns names and types from columns.txt
    void loadColumns(bool require);

    static void appendFilesOfColumns(Strings & files);

    static void appendFilesOfChecksums(Strings & files);

    /// Loads marks index granularity into memory
    virtual void loadIndexGranularity();

    virtual void appendFilesOfIndexGranularity(Strings & files) const;

    /// Loads the index file.
    std::shared_ptr<Index> loadIndex() const;

    /// Optimize index. Drop useless columns from suffix of primary key.
    template <typename Columns>
    void optimizeIndexColumns(size_t marks_count, Columns & index_columns) const;

    void appendFilesOfIndex(Strings & files) const;

    /// Load rows count for this part from disk (for the newer storage format version).
    /// For the older format version calculates rows count from the size of a column with a fixed size.
    void loadRowsCount();

    /// Load existing rows count from _row_exists column
    /// if load_existing_rows_count_for_old_parts and exclude_deleted_rows_for_part_size_in_merge are both enabled.
    void loadExistingRowsCount();

    static void appendFilesOfRowsCount(Strings & files);

    /// Loads ttl infos in json format from file ttl.txt. If file doesn't exists assigns ttl infos with all zeros
    void loadTTLInfos();

    static void appendFilesOfTTLInfos(Strings & files);

    void loadPartitionAndMinMaxIndex();

    void calculateColumnsSizesOnDisk(std::optional<Block> columns_sample = std::nullopt);

    void calculateSecondaryIndicesSizesOnDisk();

    void appendFilesOfPartitionAndMinMaxIndex(Strings & files) const;

    /// Load default compression codec from file default_compression_codec.txt
    /// if it not exists tries to deduce codec from compressed column without
    /// any specifial compression.
    void loadDefaultCompressionCodec();

    void writeColumns(const NamesAndTypesList & columns_, const WriteSettings & settings);
    void writeVersionMetadata(const VersionMetadata & version_, bool fsync_part_dir) const;

    template <typename Writer>
    void writeMetadata(const String & filename, const WriteSettings & settings, Writer && writer);

    static void appendFilesOfDefaultCompressionCodec(Strings & files);

    static void appendFilesOfMetadataVersion(Strings & files);

    /// Found column without specific compression and return codec
    /// for this column with default parameters.
    CompressionCodecPtr detectDefaultCompressionCodec() const;

    void incrementStateMetric(MergeTreeDataPartState state) const;
    void decrementStateMetric(MergeTreeDataPartState state) const;

    void checkConsistencyBase() const;

    /// This ugly flag is needed for debug assertions only
    mutable bool part_is_probably_removed_from_disk = false;

    /// If it's true then data related to this part is cleared from mark and index caches.
    mutable std::atomic_bool cleared_data_in_caches = false;
};

using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using MergeTreeMutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;

bool isCompactPart(const MergeTreeDataPartPtr & data_part);
bool isWidePart(const MergeTreeDataPartPtr & data_part);

inline String getIndexExtension(bool is_compressed_primary_key) { return is_compressed_primary_key ? ".cidx" : ".idx"; }
bool isCompressedFromIndexExtension(const String & index_extension);

using MergeTreeDataPartsVector = std::vector<MergeTreeDataPartPtr>;

Strings getPartsNames(const MergeTreeDataPartsVector & parts);

}
