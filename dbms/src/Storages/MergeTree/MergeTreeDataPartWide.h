#pragma once

#include <Core/Row.h>
#include <Core/Block.h>
#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Columns/IColumn.h>

#include <Poco/Path.h>

#include <shared_mutex>


namespace DB
{

struct ColumnSize;
class MergeTreeData;


/// Description of the data part.
class MergeTreeDataPartWide : public IMergeTreeDataPart
{
public:
    using Checksums = MergeTreeDataPartChecksums;
    using Checksum = MergeTreeDataPartChecksums::Checksum;

   MergeTreeDataPartWide(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const DiskSpace::DiskPtr & disk,
        const std::optional<String> & relative_path = {});

    MergeTreeDataPartWide(
        MergeTreeData & storage_,
        const String & name_,
        const DiskSpace::DiskPtr & disk,
        const std::optional<String> & relative_path = {});

    MergeTreeReaderPtr getReader(
        const NamesAndTypesList & columns,
        const MarkRanges & mark_ranges,
        UncompressedCache * uncompressed_cache,
        MarkCache * mark_cache,
        const ReaderSettings & reader_settings_,
        const ValueSizeMap & avg_value_size_hints = ValueSizeMap{},
        const ReadBufferFromFileBase::ProfileCallback & profile_callback = ReadBufferFromFileBase::ProfileCallback{}) const override;

    bool isStoredOnDisk() const override { return true; }

    void remove() const override;

    bool supportsVerticalMerge() const override { return true; }

    /// NOTE: Returns zeros if column files are not found in checksums.
    /// NOTE: You must ensure that no ALTERs are in progress when calculating ColumnSizes.
    ///   (either by locking columns_lock, or by locking table structure).
    ColumnSize getColumnSize(const String & name, const IDataType & type) const override;

    /// Initialize columns (from columns.txt if exists, or create from column files if not).
    /// Load checksums from checksums.txt if exists. Load index if required.
    void loadColumnsChecksumsIndexes(bool require_columns_checksums, bool check_consistency) override;

    /// Returns the name of a column with minimum compressed size (as returned by getColumnSize()).
    /// If no checksums are present returns the name of the first physically existing column.
    String getColumnNameWithMinumumCompressedSize() const override;

    virtual Type getType() const override { return Type::WIDE; }

    ~MergeTreeDataPartWide() override;

    /// Calculate the total size of the entire directory with all the files
    static UInt64 calculateTotalSizeOnDisk(const String & from);


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

    ColumnSize getColumnSizeImpl(const String & name, const IDataType & type, std::unordered_set<String> * processed_substreams) const override;

    void checkConsistency(bool require_part_metadata);
};


// using MergeTreeDataPartState =IMergeTreeDataPart::State;

}
