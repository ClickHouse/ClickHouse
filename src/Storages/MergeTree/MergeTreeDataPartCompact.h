#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

/** In compact format all columns are stored in one file (`data.bin`).
  * Data is split in granules and columns are serialized sequentially in one granule.
  * Granules are written one by one in data file.
  * Marks are also stored in single file (`data.mrk3`).
  * In compact format one mark is an array of marks for every column and a number of rows in granule.
  * Format of other data part files is not changed.
  * It's considered to store only small parts in compact format (up to 10M).
  * NOTE: Compact parts aren't supported for tables with non-adaptive granularity.
  * NOTE: In compact part compressed and uncompressed size of single column is unknown.
  */
class MergeTreeDataPartCompact : public IMergeTreeDataPart
{
public:
    static constexpr auto DATA_FILE_NAME = "data";
    static constexpr auto DATA_FILE_NAME_WITH_EXTENSION = "data.bin";

    MergeTreeDataPartCompact(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const MutableDataPartStoragePtr & data_part_storage_,
        const IMergeTreeDataPart * parent_part_ = nullptr);

    MergeTreeReaderPtr getReader(
        const NamesAndTypesList & columns,
        const StorageSnapshotPtr & storage_snapshot,
        const MarkRanges & mark_ranges,
        const VirtualFields & virtual_fields,
        UncompressedCache * uncompressed_cache,
        MarkCache * mark_cache,
        const AlterConversionsPtr & alter_conversions,
        const MergeTreeReaderSettings & reader_settings_,
        const ValueSizeMap & avg_value_size_hints,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback) const override;

    bool isStoredOnDisk() const override { return true; }

    bool isStoredOnReadonlyDisk() const override;

    bool isStoredOnRemoteDisk() const override;

    bool isStoredOnRemoteDiskWithZeroCopySupport() const override;

    bool hasColumnFiles(const NameAndTypePair & column) const override;

    std::optional<time_t> getColumnModificationTime(const String & column_name) const override;

    std::optional<String> getFileNameForColumn(const NameAndTypePair & /* column */) const override { return DATA_FILE_NAME; }

    void loadMarksToCache(const Names & column_names, MarkCache * mark_cache) const override;

    ~MergeTreeDataPartCompact() override;

protected:
     static void loadIndexGranularityImpl(
         MergeTreeIndexGranularity & index_granularity_, const MergeTreeIndexGranularityInfo & index_granularity_info_,
         size_t columns_count, const IDataPartStorage & data_part_storage_);

     void doCheckConsistency(bool require_part_metadata) const override;

private:
     /// Loads marks index granularity into memory
     void loadIndexGranularity() override;

     /// Compact parts don't support per column size, only total size
     void calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const override;
};

}
