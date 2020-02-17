#include <Common/typeid_cast.h>
#include <Interpreters/JoinSwitcher.h>
#include <Interpreters/join_common.h>

namespace DB
{

static ColumnWithTypeAndName correctNullability(ColumnWithTypeAndName && column, bool nullable)
{
    if (nullable)
        JoinCommon::convertColumnToNullable(column);
    else
        JoinCommon::removeColumnNullability(column);

    return std::move(column);
}

bool JoinSwitcher::addJoinedBlock(const Block & block, bool)
{
    /// Trying to make MergeJoin without lock

    if (switched)
        return join->addJoinedBlock(block);

    std::lock_guard lock(switch_mutex);

    if (switched)
        return join->addJoinedBlock(block);

    /// HashJoin with external limits check

    join->addJoinedBlock(block, false);
    size_t rows = join->getTotalRowCount();
    size_t bytes = join->getTotalByteCount();

    auto & limits = table_join->sizeLimits();
    if (!limits.softCheck(rows, bytes))
        switchJoin();

    return true;
}

void JoinSwitcher::switchJoin()
{
    std::shared_ptr<Join::RightTableData> joined_data = static_cast<const Join &>(*join).getJoinedData();
    BlocksList right_blocks = std::move(joined_data->blocks);

    /// Destroy old join & create new one. Destroy first in case of memory saving.
    join = std::make_shared<MergeJoin>(table_join, right_sample_block);

    for (Block & block : right_blocks)
    {
        for (const auto & sample_column : right_sample_block)
        {
            auto & column = block.getByName(sample_column.name);
            block.insert(correctNullability(std::move(column), sample_column.type->isNullable()));
            join->addJoinedBlock(block);
        }
    }

    switched = true;
}

}
