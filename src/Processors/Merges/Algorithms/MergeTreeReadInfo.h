#pragma once

#include <Processors/Chunk.h>
#include <Core/Block.h>

namespace DB
{

/// To carry part level and virtual row if chunk is produced by a merge tree source
class MergeTreeReadInfo : public ChunkInfoCloneable<MergeTreeReadInfo>
{
public:
    MergeTreeReadInfo() = delete;
    explicit MergeTreeReadInfo(size_t part_level) :
        origin_merge_tree_part_level(part_level) {}
    explicit MergeTreeReadInfo(size_t part_level, const Block & pk_block_) :
        origin_merge_tree_part_level(part_level), pk_block(pk_block_) {}
    MergeTreeReadInfo(const MergeTreeReadInfo & other) = default;

    size_t origin_merge_tree_part_level = 0;

    /// If is virtual_row, block should not be empty.
    Block pk_block;
};

inline size_t getPartLevelFromChunk(const Chunk & chunk)
{
    const auto read_info = chunk.getChunkInfos().get<MergeTreeReadInfo>();
    if (read_info)
        return read_info->origin_merge_tree_part_level;
    return 0;
}

inline bool isVirtualRow(const Chunk & chunk)
{
    const auto read_info = chunk.getChunkInfos().get<MergeTreeReadInfo>();
    if (read_info)
        return read_info->pk_block.columns() > 0;
    return false;
}

inline void setVirtualRow(Chunk & chunk, const Block & header)
{
    const auto read_info = chunk.getChunkInfos().get<MergeTreeReadInfo>();
    chassert(read_info);

    const Block & pk_block = read_info->pk_block;

    Columns ordered_columns;
    ordered_columns.reserve(header.columns());

    for (size_t i = 0; i < header.columns(); ++i)
    {
        const ColumnWithTypeAndName & type_and_name = header.getByPosition(i);
        ColumnPtr current_column = type_and_name.type->createColumn();

        size_t pos = type_and_name.name.find_last_of('.');
        String column_name = (pos == String::npos) ? type_and_name.name : type_and_name.name.substr(pos + 1);

        const ColumnWithTypeAndName * column = pk_block.findByName(column_name, true);
        ordered_columns.push_back(column ? column->column : current_column->cloneResized(1));
    }

    chunk.setColumns(ordered_columns, 1);
}

}
