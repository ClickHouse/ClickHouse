#pragma once
#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{
class ProjectionPartTransform : public ISimpleTransform
{
public:
    ProjectionPartTransform(const Block & header, const Block & new_header, MergeTreeData::DataPartsVector && parent_parts_ = {})
        : ISimpleTransform(header, new_header, false), projection(new_header), parent_parts(parent_parts_)
    {
    }

    String getName() const override { return "ProjectionPartTransform"; }

protected:
    void transform(Chunk & chunk) override
    {
        auto info = std::make_shared<AggregatedChunkInfo>();
        info->bucket_num = -1;
        info->is_overflows = false;
        chunk.setChunkInfo(std::move(info));

        auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
        Block new_block;
        for (const auto & elem : projection)
        {
            ColumnWithTypeAndName column = block.getByName(elem.name);
            column.column = column.column->convertToFullColumnIfConst();
            new_block.insert(std::move(column));
        }
        block.swap(new_block);

        auto num_rows = block.rows();
        chunk.setColumns(block.getColumns(), num_rows);
    }

private:
    Block projection;
    MergeTreeData::DataPartsVector parent_parts;
};

}
