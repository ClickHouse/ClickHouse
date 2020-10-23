#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

/** In compact format all columns are stored in one file (`data.bin`).
  * Data is splitted in granules and columns are serialized sequentially in one granule.
  * Granules are written one by one in data file.
  * Marks are also stored in single file (`data.mrk3`).
  * In compact format one mark is an array of marks for every column and a number of rows in granule.
  * Format of other data part files is not changed.
  * It's considered to store only small parts in compact format (up to 10M).
  * NOTE: Compact parts aren't supported for tables with non-adaptive granularty.
  * NOTE: In compact part compressed and uncompressed size of single column is unknown.
  */
class MergeTreeDataPartCompact : public IMergeTreeDataPart
{
public:
    static constexpr auto DATA_FILE_NAME = "data";
    static constexpr auto DATA_FILE_EXTENSION = ".bin";
    static constexpr auto TEMP_FILE_SUFFIX = "_temp";
    static constexpr auto DATA_FILE_NAME_WITH_EXTENSION = "data.bin";

    MergeTreeDataPartCompact(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const DiskPtr & disk_,
        const std::optional<String> & relative_path_ = {});

    MergeTreeDataPartCompact(
        MergeTreeData & storage_,
        const String & name_,
        const DiskPtr & disk_,
        const std::optional<String> & relative_path_ = {});

    MergeTreeReaderPtr getReader(
        const NamesAndTypesList & columns,
        const MarkRanges & mark_ranges,
        UncompressedCache * uncompressed_cache,
        MarkCache * mark_cache,
        const MergeTreeReaderSettings & reader_settings_,
        const ValueSizeMap & avg_value_size_hints = ValueSizeMap{},
        const ReadBufferFromFileBase::ProfileCallback & profile_callback = ReadBufferFromFileBase::ProfileCallback{}) const override;

    MergeTreeWriterPtr getWriter(
        const NamesAndTypesList & columns_list,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        const MergeTreeIndexGranularity & computed_index_granularity = {}) const override;

    bool isStoredOnDisk() const override { return true; }

    ColumnSize getTotalColumnsSize() const override;

    bool hasColumnFiles(const String & column_name, const IDataType & type) const override;

    String getFileNameForColumn(const NameAndTypePair & /* column */) const override { return DATA_FILE_NAME; }

    NameToNameMap createRenameMapForAlter(
        AlterAnalysisResult & analysis_result,
        const NamesAndTypesList & old_columns) const override;

    ~MergeTreeDataPartCompact() override;

private:
    void checkConsistency(bool require_part_metadata) const override;

    /// Loads marks index granularity into memory
    void loadIndexGranularity() override;
};

}
