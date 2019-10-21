#pragma once

#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

/// Writes only those columns that are in `header`
class MergedColumnOnlyOutputStream final : public IMergedBlockOutputStream
{
public:
    /// skip_offsets: used when ALTERing columns if we know that array offsets are not altered.
    /// Pass empty 'already_written_offset_columns' first time then and pass the same object to subsequent instances of MergedColumnOnlyOutputStream
    ///  if you want to serialize elements of Nested data structure in different instances of MergedColumnOnlyOutputStream.
    MergedColumnOnlyOutputStream(
        const MergeTreeDataPartPtr & data_part, const Block & header_, bool sync_,
        CompressionCodecPtr default_codec_, bool skip_offsets_,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc_,
        WrittenOffsetColumns & already_written_offset_columns_,
        const MergeTreeIndexGranularityInfo * index_granularity_info_ = nullptr);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;
    void writeSuffix() override;
    MergeTreeData::DataPart::Checksums writeSuffixAndGetChecksums();

private:
    Block header;

    bool sync;
    bool skip_offsets;

    /// To correctly write Nested elements column-by-column.
    WrittenOffsetColumns & already_written_offset_columns;
};


}
