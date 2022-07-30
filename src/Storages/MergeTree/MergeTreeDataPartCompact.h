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
        const DataPartStoragePtr & data_part_storage_,
        const IMergeTreeDataPart * parent_part_ = nullptr);

    MergeTreeDataPartCompact(
        MergeTreeData & storage_,
        const String & name_,
        const DataPartStoragePtr & data_part_storage_,
        const IMergeTreeDataPart * parent_part_ = nullptr);

    MergeTreeReaderPtr getReader(
        const NamesAndTypesList & columns,
        const StorageMetadataPtr & metadata_snapshot,
        const MarkRanges & mark_ranges,
        UncompressedCache * uncompressed_cache,
        MarkCache * mark_cache,
        const MergeTreeReaderSettings & reader_settings_,
        const ValueSizeMap & avg_value_size_hints,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback) const override;

    MergeTreeWriterPtr getWriter(
        DataPartStorageBuilderPtr data_part_storage_builder,
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        const MergeTreeIndexGranularity & computed_index_granularity) const override;

    bool isStoredOnDisk() const override { return true; }

    bool isStoredOnRemoteDisk() const override;

    bool isStoredOnRemoteDiskWithZeroCopySupport() const override;

    bool hasColumnFiles(const NameAndTypePair & column) const override;

    String getFileNameForColumn(const NameAndTypePair & /* column */) const override { return DATA_FILE_NAME; }

    ~MergeTreeDataPartCompact() override;

private:
    void checkConsistency(bool require_part_metadata) const override;

    /// Loads marks index granularity into memory
    void loadIndexGranularity() override;

    /// Compact parts doesn't support per column size, only total size
    void calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const override;
};

}
