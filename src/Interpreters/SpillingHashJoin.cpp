#include <Interpreters/SpillingHashJoin.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Common/logger_useful.h>


namespace DB
{

SpillingHashJoin::SpillingHashJoin(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader left_sample_block_,
    SharedHeader right_sample_block_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t max_num_buckets_)
    : log(getLogger("SpillingHashJoin"))
    , table_join(std::move(table_join_))
    , left_sample_block(std::move(left_sample_block_))
    , right_sample_block(right_sample_block_->cloneEmpty())
    , tmp_data(std::move(tmp_data_))
    , max_num_buckets(max_num_buckets_)
    , limits(table_join->sizeLimits())
{
    /// Create a lightweight HashJoin for metadata operations (checkTypesOfKeys, initialize,
    /// header computation via joinBlock with empty block). This join is NOT used for data.
    hash_join = std::make_shared<HashJoin>(table_join, right_sample_block_);

    if (!limits.hasLimits())
        limits.max_bytes = table_join->defaultMaxBytes();
}

SpillingHashJoin::~SpillingHashJoin() = default;

bool SpillingHashJoin::addBlockToJoin(const Block & block, bool check_limits)
{
    if (state != State::COLLECTING)
        return inner_join->addBlockToJoin(block, check_limits);

    buffered_blocks.push_back(block);
    buffered_rows += block.rows();
    buffered_bytes += block.allocatedBytes();

    if (!limits.softCheck(buffered_rows, buffered_bytes))
    {
        LOG_DEBUG(log, "Memory limit exceeded ({} bytes, {} rows), switching to GraceHashJoin",
            buffered_bytes, buffered_rows);
        switchToGraceHashJoin();
    }

    return true;
}

void SpillingHashJoin::switchToGraceHashJoin()
{
    inner_join = std::make_shared<GraceHashJoin>(
        /*initial_num_buckets_=*/ 1,
        max_num_buckets,
        table_join,
        left_sample_block,
        std::make_shared<const Block>(right_sample_block),
        tmp_data);

    inner_join->initialize(*left_sample_block);

    /// Drain buffered blocks into GraceHashJoin one by one,
    /// freeing each after insertion to limit peak memory.
    while (!buffered_blocks.empty())
    {
        inner_join->addBlockToJoin(buffered_blocks.front(), /*check_limits=*/ false);
        buffered_blocks.pop_front();
    }
    buffered_rows = 0;
    buffered_bytes = 0;

    state = State::GRACE_HASH_JOIN;
}

void SpillingHashJoin::onBuildPhaseFinish()
{
    if (state == State::COLLECTING)
    {
        LOG_DEBUG(log, "All blocks fit in memory ({} bytes, {} rows), creating HashJoin",
            buffered_bytes, buffered_rows);

        inner_join = std::make_shared<HashJoin>(table_join, std::make_shared<const Block>(right_sample_block));

        while (!buffered_blocks.empty())
        {
            inner_join->addBlockToJoin(buffered_blocks.front(), /*check_limits=*/ false);
            buffered_blocks.pop_front();
        }
        buffered_rows = 0;
        buffered_bytes = 0;

        state = State::HASH_JOIN;
    }

    inner_join->onBuildPhaseFinish();
}

void SpillingHashJoin::checkTypesOfKeys(const Block & block) const
{
    hash_join->checkTypesOfKeys(block);
}

void SpillingHashJoin::initialize(const Block & sample_block)
{
    left_sample_block = std::make_shared<const Block>(sample_block.cloneEmpty());
    hash_join->initialize(sample_block);
}

JoinResultPtr SpillingHashJoin::joinBlock(Block block)
{
    /// During header computation (transformHeader), joinBlock is called with an empty block
    /// before any data is added. Delegate to hash_join in COLLECTING state.
    if (state == State::COLLECTING)
        return hash_join->joinBlock(std::move(block));
    return inner_join->joinBlock(std::move(block));
}

void SpillingHashJoin::setTotals(const Block & block)
{
    if (inner_join)
        inner_join->setTotals(block);
    else
        IJoin::setTotals(block);
}

const Block & SpillingHashJoin::getTotals() const
{
    if (inner_join)
        return inner_join->getTotals();
    return IJoin::getTotals();
}

size_t SpillingHashJoin::getTotalRowCount() const
{
    if (state == State::COLLECTING)
        return buffered_rows;
    return inner_join->getTotalRowCount();
}

size_t SpillingHashJoin::getTotalByteCount() const
{
    if (state == State::COLLECTING)
        return buffered_bytes;
    return inner_join->getTotalByteCount();
}

bool SpillingHashJoin::alwaysReturnsEmptySet() const
{
    if (state == State::COLLECTING)
        return hash_join->alwaysReturnsEmptySet();
    return inner_join->alwaysReturnsEmptySet();
}

IBlocksStreamPtr SpillingHashJoin::getNonJoinedBlocks(
    const Block & left_sample_block_,
    const Block & result_sample_block,
    UInt64 max_block_size) const
{
    chassert(inner_join);
    return inner_join->getNonJoinedBlocks(left_sample_block_, result_sample_block, max_block_size);
}

IBlocksStreamPtr SpillingHashJoin::getDelayedBlocks()
{
    chassert(inner_join);
    return inner_join->getDelayedBlocks();
}

}
