#include <cstddef>

#include <Common/typeid_cast.h>

#include <Storages/MergeTree/QueueModeColumns.h>
#include <Storages/MergeTree/Streaming/MergeTreePartitionSequentialSource.h>
#include <Storages/MergeTree/Streaming/ChunkSplitter.h>
#include <Storages/MergeTree/Streaming/CursorUtils.h>

namespace DB
{

ChunkSplitterTransform::ChunkSplitterTransform(
    Block header_, MergeTreeCursor cursor_, String storage_full_name_, std::optional<String> keeper_key_)
    : ISimpleTransform(header_, header_, true)
    , cursor(std::move(cursor_))
    , storage_full_name(std::move(storage_full_name_))
    , keeper_key(std::move(keeper_key_))
{
    block_number_column_index = getOutputPort().getHeader().getPositionByName(QueueBlockNumberColumn::name);
    block_offset_column_index = getOutputPort().getHeader().getPositionByName(QueueBlockOffsetColumn::name);
}

void ChunkSplitterTransform::transform(Chunk & chunk)
{
    if (chunk.empty())
        return;

    chassert(chunk.hasChunkInfo(PartitionIdChunkInfo::info_slot));
    const auto & info = chunk.getChunkInfo(PartitionIdChunkInfo::info_slot);
    const auto * partition_id_info = typeid_cast<const PartitionIdChunkInfo *>(info.get());
    const String & partition_id = partition_id_info->partition_id;

    PartitionCursor & current = cursor[partition_id];
    PartitionCursor left = getCursor(chunk, 0);
    PartitionCursor right = getCursor(chunk, chunk.getNumRows() - 1);

    if (current < left)
        // only new data
        ;
    else if (left <= current && current <= right)
        // intersects with cursor
        chunk = splitByCursor(std::move(chunk), current);
    else if (right < current)
        // only old data
        chunk = Chunk();

    auto cursor_info = buildMergeTreeCursorInfo(storage_full_name, partition_id, keeper_key, right.block_number, right.block_offset);
    chunk.setChunkInfo(std::move(cursor_info), CursorInfo::info_slot);

    if (current < right)
        current = right;
}

Chunk ChunkSplitterTransform::splitByCursor(Chunk && chunk, const PartitionCursor & current)
{
    size_t chunk_size = chunk.getNumRows();
    size_t l = 0, r = chunk.getNumRows();

    while (r - l > 1)
    {
        size_t mid = (l + r) / 2;
        PartitionCursor mid_cursor = getCursor(chunk, mid);

        if (mid_cursor <= current)
            l = mid;
        else
            r = mid;
    }

    size_t splitted_size = chunk_size - r;

    if (splitted_size == 0)
        return Chunk();

    Columns columns = chunk.detachColumns();

    for (auto & column : columns)
        column = column->cut(r, splitted_size);

    return Chunk(std::move(columns), splitted_size);
}

PartitionCursor ChunkSplitterTransform::getCursor(const Chunk & chunk, size_t index) const
{
    const auto & block_number_column = chunk.getColumns().at(block_number_column_index);
    const auto & block_offset_column = chunk.getColumns().at(block_offset_column_index);

    return PartitionCursor{
        .block_number = block_number_column->getInt(index),
        .block_offset = block_offset_column->getInt(index),
    };
}

}
