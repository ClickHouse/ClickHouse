#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>

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
        const DiskPtr & disk,
        const std::optional<String> & relative_path = {});

    MergeTreeDataPartWide(
        MergeTreeData & storage_,
        const String & name_,
        const DiskPtr & disk,
        const std::optional<String> & relative_path = {});

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

    bool supportsVerticalMerge() const override { return true; }

    void accumulateColumnSizes(ColumnToSize & column_to_size) const override;

    String getFileNameForColumn(const NameAndTypePair & column) const override;

    ColumnSize getTotalColumnsSize() const override;

    ColumnSize getColumnSize(const String & column_name, const IDataType & type) const override;

    NameToNameMap createRenameMapForAlter(
        AlterAnalysisResult & analysis_result,
        const NamesAndTypesList & old_columns) const override;

    ~MergeTreeDataPartWide() override;

    bool hasColumnFiles(const String & column, const IDataType & type) const override;

private:
    void checkConsistency(bool require_part_metadata) const override;

    /// Loads marks index granularity into memory
    void loadIndexGranularity() override;

    ColumnSize getColumnSizeImpl(const String & name, const IDataType & type, std::unordered_set<String> * processed_substreams) const;
};

}
