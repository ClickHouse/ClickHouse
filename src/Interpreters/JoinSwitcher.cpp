#include <Common/typeid_cast.h>
#include <Interpreters/JoinSwitcher.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/join_common.h>

namespace DB
{

JoinSwitcher::JoinSwitcher(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_)
    : limits(table_join_->sizeLimits())
    , switched(false)
    , table_join(table_join_)
    , right_sample_block(right_sample_block_.cloneEmpty())
{
    join = std::make_shared<HashJoin>(table_join, right_sample_block);

    if (!limits.hasLimits())
        limits.max_bytes = table_join->defaultMaxBytes();
}

bool JoinSwitcher::addJoinedBlock(const Block & block, bool)
{
    std::lock_guard lock(switch_mutex);

    if (switched)
        return join->addJoinedBlock(block);

    /// HashJoin with external limits check

    join->addJoinedBlock(block, false);
    size_t rows = join->getTotalRowCount();
    size_t bytes = join->getTotalByteCount();

    if (!limits.softCheck(rows, bytes))
        switchJoin();

    return true;
}

void JoinSwitcher::switchJoin()
{
    HashJoin& hash_join = assert_cast<HashJoin &>(*join);
    BlocksList right_blocks = std::move(hash_join).releaseJoinedBlocks();

    /// Destroy old join & create new one.
    join = std::make_shared<MergeJoin>(table_join, right_sample_block);

    for (const Block & saved_block : right_blocks)
    {
        join->addJoinedBlock(saved_block);
    }

    switched = true;
}

}
