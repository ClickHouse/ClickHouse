#include <Storages/MergeTree/MergeTreeTransform.h>
#include "Storages/MergeTree/IMergeTreeReader.h"
#include "Columns/ColumnLazy.h"
// #include "Storages/MergeTree/IMergeTreeDataPart.h"
#include "Common/logger_useful.h"

namespace DB
{

Block MergeTreeTransform::transformHeader(Block header)
{
    for (auto & it : header)
    {
        if (isColumnLazy(*(it.column)))
        {
            it.column =  it.type->createColumn();
        }
    }
    return header;
}

MergeTreeTransform::MergeTreeTransform(
    const Block & header_,
    const MergeTreeData & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const LazilyReadInfoPtr & lazily_read_info_,
    const ContextPtr & context)
    : ISimpleTransform(
    header_,
    transformHeader(header_),
    true)
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , use_uncompressed_cache(context->getSettings().use_uncompressed_cache)
    , data_parts_info(std::move(lazily_read_info_->data_parts_info))
    , names_and_types_list()
{
    for (auto & it : header_)
    {
        if (isColumnLazy(*(it.column)))
        {
            names_and_types_list.emplace_back(it.name, it.type);
        }
    }
}

void MergeTreeTransform::transform(Chunk & chunk)
{
    using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;

    const size_t columns_size = names_and_types_list.size();
    const size_t rows_size = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    Block block = getInputPort().getHeader().cloneWithColumns(columns);
    auto * column_lazy = typeid_cast<const ColumnLazy *>(block.getByName(names_and_types_list.begin()->name).column.get());
    const ColumnUInt64 * part_num_column = typeid_cast<const ColumnUInt64 *>(&column_lazy->getPartNumsColumn());
    const ColumnUInt64 * row_num_column = typeid_cast<const ColumnUInt64 *>(&column_lazy->getRowNumsColumn());

    ReadSettings read_settings;
    read_settings.direct_io_threshold = 1;
    MergeTreeReaderSettings reader_settings =
    {
        .read_settings = read_settings,
        .save_marks_in_cache = false,
    };

    MutableColumns lazily_read_columns;
    lazily_read_columns.resize(columns_size);
    size_t column_idx = 0;
    size_t row_idx = 0;
    for (auto & iter : names_and_types_list)
    {
        lazily_read_columns[column_idx] = iter.type->createColumn();
        lazily_read_columns[column_idx]->reserve(rows_size);
        column_idx++;
    }

    for (; row_idx < rows_size; ++row_idx)
    {
        size_t row_offset = row_num_column->getUInt(row_idx);
        size_t part_index = part_num_column->getUInt(row_idx);
        MergeTreeData::DataPartPtr data_part = (*data_parts_info)[part_index].data_part;
        AlterConversionsPtr alter_conversions = (*data_parts_info)[part_index].alter_conversions;
        MarkRange mark_range = data_part->index_granularity.getMarkRangeForRowOffset(row_offset);
        MarkRanges mark_ranges{mark_range};
        MergeTreeReaderPtr reader = data_part->getReader(
            names_and_types_list, storage_snapshot, mark_ranges,
            use_uncompressed_cache ? storage.getContext()->getUncompressedCache().get() : nullptr,
            storage.getContext()->getMarkCache().get(), alter_conversions,
            reader_settings, {}, {});

        Columns columns_to_read;
        columns_to_read.resize(columns_size);
        size_t current_offset = row_offset - data_part->index_granularity.getMarkStartingRow(mark_range.begin);

        reader->readRows(mark_range.begin, mark_range.end, false, current_offset + 1, columns_to_read);

        for (size_t i = 0; i < columns_size; ++i)
            lazily_read_columns[i]->insert((*columns_to_read[i])[current_offset]);
    }

    column_idx = 0;
    for (auto & iter : names_and_types_list)
    {
        block.getByName(iter.name).column = std::move(lazily_read_columns[column_idx++]);
    }

    chunk.setColumns(block.getColumns(), rows_size);
}

}
