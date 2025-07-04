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
        const MergeTreeIndices & indices_to_recalc,
        const ColumnsStatistics & stats_to_recalc,
        CompressionCodecPtr default_codec,
        MergeTreeIndexGranularityPtr index_granularity_ptr,
        size_t part_uncompressed_bytes,
        WrittenOffsetColumns * offset_columns = nullptr);

    void write(const Block & block) override;

    std::pair<MergeTreeData::DataPart::Checksums, NameSet>
    fillChecksums(MergeTreeData::MutableDataPartPtr & new_part, MergeTreeData::DataPart::Checksums & all_checksums);

    const Block & getColumnsSample() const { return writer->getColumnsSample(); }
    void finish(bool sync);
    void cancel() noexcept override;
};

using MergedColumnOnlyOutputStreamPtr = std::shared_ptr<MergedColumnOnlyOutputStream>;


}
