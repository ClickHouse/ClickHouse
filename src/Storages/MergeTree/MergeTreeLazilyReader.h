#pragma once
#include <Columns/IColumnLazyHelper.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

using AliasToName = std::unordered_map<std::string, std::string>;
using AliasToNamePtr = std::shared_ptr<AliasToName>;

struct RowOffsetWithIdx
{
    size_t row_offset;
    size_t row_idx;

    RowOffsetWithIdx(const size_t row_offset_, const size_t row_idx_)
        : row_offset(row_offset_), row_idx(row_idx_) {}
};

using RowOffsetsWithIdx = std::vector<RowOffsetWithIdx>;
using PartIndexToRowOffsets = std::map<size_t, RowOffsetsWithIdx>;

class MergeTreeLazilyReader : public IColumnLazyHelper
{
public:
    MergeTreeLazilyReader(
        const Block & header_,
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const LazilyReadInfoPtr & lazily_read_info_,
        const ContextPtr & context_,
        const AliasToNamePtr & alias_index_);

    void transformLazyColumns(
        const ColumnLazy & column_lazy,
        ColumnsWithTypeAndName & res_columns) override;

private:
    void readLazyColumns(
        const MergeTreeReaderSettings & reader_settings,
        const PartIndexToRowOffsets & part_to_row_offsets,
        MutableColumns & lazily_read_columns);

    const MergeTreeData & storage;
    DataPartsInfoPtr data_parts_info;
    StorageSnapshotPtr storage_snapshot;
    bool use_uncompressed_cache;
    Names requested_column_names;
    ColumnsWithTypeAndName lazy_columns;
};

}
