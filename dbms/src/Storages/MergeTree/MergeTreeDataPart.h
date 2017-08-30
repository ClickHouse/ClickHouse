#pragma once

#include <Core/Row.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
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


/// Index that for each part stores min and max values of a set of columns. This allows quickly excluding
/// parts based on conditions on these columns imposed by a query.
/// Currently this index is built using only columns required by partition expression, but in principle it
/// can be built using any set of columns.
struct MinMaxIndex
{
    void update(const Block & block, const Names & column_names);
    void merge(const MinMaxIndex & other);

    bool initialized = false;
    Row min_column_values;
    Row max_column_values;
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

    /// Returns checksum of column's binary file.
    const Checksum * tryGetBinChecksum(const String & name) const;

    /// Returns the size of .bin file for column `name` if found, zero otherwise
    size_t getColumnCompressedSize(const String & name) const;
    size_t getColumnUncompressedSize(const String & name) const;

    /// Returns the name of a column with minimum compressed size (as returned by getColumnSize()).
    /// If no checksums are present returns the name of the first physically existing column.
    String getColumnNameWithMinumumCompressedSize() const;

    /// If part has column with fixed size, will return exact size of part (in rows)
    size_t getExactSizeRows() const;

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

    Row partition;

    /// A directory path (realative to storage's path) where part data is actually stored
    /// Examples: 'detached/tmp_fetch_<name>', 'tmp_<name>', '<name>'
    mutable String relative_path;

    size_t size = 0;                        /// in number of marks.
    std::atomic<size_t> size_in_bytes {0};  /// size in bytes, 0 - if not counted;
                                            ///  is used from several threads without locks (it is changed with ALTER).
    time_t modification_time = 0;
    mutable time_t remove_time = std::numeric_limits<time_t>::max(); /// When the part is removed from the working set.

    /// If true, the destructor will delete the directory with the part.
    bool is_temp = false;

    /// For resharding.
    size_t shard_no = 0;

    /// Primary key (correspond to primary.idx file).
    /// Always loaded in RAM. Contains each index_granularity-th value of primary key tuple.
    /// Note that marks (also correspond to primary key) is not always in RAM, but cached. See MarkCache.h.
    using Index = Columns;
    Index index;

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
    static size_t calcTotalSize(const String & from);

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

private:
    /// Reads columns names and types from columns.txt
    void loadColumns(bool require);

    /// If checksums.txt exists, reads files' checksums (and sizes) from it
    void loadChecksums(bool require);

    /// Loads index file. Also calculates this->size if size=0
    void loadIndex();

    void loadPartitionAndMinMaxIndex();

    void checkConsistency(bool require_part_metadata);
};

}
