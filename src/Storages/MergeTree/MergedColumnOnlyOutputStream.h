#pragma once

#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Storages/Statistics/Statistics.h>

namespace DB
{

class MergeTreeDataPartWriterWide;

/// Writes only those columns that are in `header`
class MergedColumnOnlyOutputStream final : public IMergedBlockOutputStream
{
public:
    /// Pass empty 'already_written_offset_columns' first time then and pass the same object to subsequent instances of MergedColumnOnlyOutputStream
    ///  if you want to serialize elements of Nested data structure in different instances of MergedColumnOnlyOutputStream.
    MergedColumnOnlyOutputStream(
        const MergeTreeMutableDataPartPtr & data_part,
        const StorageMetadataPtr & metadata_snapshot_,
        const NamesAndTypesList & columns_list_,
        CompressionCodecPtr default_codec_,
        const MergeTreeIndices & indices_to_recalc_,
        const ColumnsStatistics & stats_to_recalc_,
        WrittenOffsetColumns * offset_columns_ = nullptr,
        bool save_marks_in_cache = false,
        const MergeTreeIndexGranularity & index_granularity = {},
        const MergeTreeIndexGranularityInfo * index_granularity_info_ = nullptr);

    void write(const Block & block) override;

    MergeTreeData::DataPart::Checksums
    fillChecksums(MergeTreeData::MutableDataPartPtr & new_part, MergeTreeData::DataPart::Checksums & all_checksums);

    void finish(bool sync);
};

using MergedColumnOnlyOutputStreamPtr = std::shared_ptr<MergedColumnOnlyOutputStream>;


}
