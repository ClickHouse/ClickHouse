#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>

#include <DataTypes/IDataType.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


MergeTreeReadInfo::MergeTreeReadInfo(size_t part_level)
    : origin_merge_tree_part_level(part_level)
{
}

MergeTreeReadInfo::MergeTreeReadInfo(size_t part_level, const Block & pk_block_, ExpressionActionsPtr virtual_row_conversions_)
    : origin_merge_tree_part_level(part_level)
    , pk_block(pk_block_)
    , virtual_row_conversions(std::move(virtual_row_conversions_))
{
}

MergeTreeReadInfo::MergeTreeReadInfo(const MergeTreeReadInfo & other) = default;

size_t getPartLevelFromChunk(const Chunk & chunk)
{
    const auto read_info = chunk.getChunkInfos().get<MergeTreeReadInfo>();
    if (read_info)
        return read_info->origin_merge_tree_part_level;
    return 0;
}

bool isVirtualRow(const Chunk & chunk)
{
    const auto read_info = chunk.getChunkInfos().get<MergeTreeReadInfo>();
    if (read_info)
        return read_info->pk_block.columns() > 0;
    return false;
}

void setVirtualRow(Chunk & chunk, const Block & header, bool apply_virtual_row_conversions)
{
    auto read_info = chunk.getChunkInfos().get<MergeTreeReadInfo>();
    chassert(read_info);

    Block & pk_block = read_info->pk_block;

    // std::cerr << apply_virtual_row_conversions << std::endl;
    // std::cerr << read_info->virtual_row_conversions->dumpActions() << std::endl;

    if (apply_virtual_row_conversions)
        read_info->virtual_row_conversions->execute(pk_block);

    // std::cerr << "++++" << pk_block.dumpStructure() << std::endl;

    Columns ordered_columns;
    ordered_columns.reserve(pk_block.columns());

    for (size_t i = 0; i < header.columns(); ++i)
    {
        const ColumnWithTypeAndName & col = header.getByPosition(i);
        if (const auto * pk_col = pk_block.findByName(col.name))
        {
            if (!col.type->equals(*pk_col->type))
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Virtual row has different type for {}. Expected {}, got {}",
                    col.name, col.dumpStructure(), pk_col->dumpStructure());

            ordered_columns.push_back(pk_col->column);
        }
        else
            ordered_columns.push_back(col.type->createColumnConstWithDefaultValue(1));
    }

    chunk.setColumns(ordered_columns, 1);
}

}
