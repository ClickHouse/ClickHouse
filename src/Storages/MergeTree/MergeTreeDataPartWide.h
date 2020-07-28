#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

/** In wide format data of each column is stored in one or several (for complex types) files.
  * Every data file is followed by marks file.
  * Can be used in tables with both adaptive and non-adaptive granularity.
  * This is the regular format of parts for MergeTree and suitable for big parts, as it's the most efficient.
  * Data part would be created in wide format if it's uncompressed size in bytes or number of rows would exceed
  * thresholds `min_bytes_for_wide_part` and `min_rows_for_wide_part`.
  */
class MergeTreeDataPartWide : public IMergeTreeDataPart
{
public:
    MergeTreeDataPartWide(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const VolumePtr & volume,
        const std::optional<String> & relative_path = {});

    MergeTreeDataPartWide(
        MergeTreeData & storage_,
        const String & name_,
        const VolumePtr & volume,
        const std::optional<String> & relative_path = {});

    MergeTreeReaderPtr getReader(
        const NamesAndTypesList & columns,
        const MarkRanges & mark_ranges,
        UncompressedCache * uncompressed_cache,
        MarkCache * mark_cache,
        const MergeTreeReaderSettings & reader_settings_,
        const ValueSizeMap & avg_value_size_hints,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback) const override;

    MergeTreeWriterPtr getWriter(
        const NamesAndTypesList & columns_list,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        const MergeTreeIndexGranularity & computed_index_granularity) const override;

    bool isStoredOnDisk() const override { return true; }

    bool supportsVerticalMerge() const override { return true; }

    String getFileNameForColumn(const NameAndTypePair & column) const override;

    ~MergeTreeDataPartWide() override;

    bool hasColumnFiles(const String & column, const IDataType & type) const override;

private:
    void checkConsistency(bool require_part_metadata) const override;

    /// Loads marks index granularity into memory
    void loadIndexGranularity() override;

    ColumnSize getColumnSizeImpl(const String & name, const IDataType & type, std::unordered_set<String> * processed_substreams) const;

    void calculateEachColumnSizesOnDisk(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const override;
};

}
