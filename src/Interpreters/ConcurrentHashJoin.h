#pragma once
#include <memory>
#include <Interpreters/IJoin.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/HashJoin.h>
#include "Context.h"
namespace DB
{

// If could not running shuffle  mode, this join behaviors as HashJoin.
// Not support parallel mode.
// Could support right join now, still no asof strictness.
class ConcurrentHashJoin : public IJoin, public boost::noncopyable
{
public:
    struct SharedContext
    {
        size_t clone_count;
        SizeLimits original_size_limit;
        SizeLimits size_limit_per_clone;
    };
    using SharedContextPtr = std::shared_ptr<SharedContext>;
    explicit ConcurrentHashJoin(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_);
    explicit ConcurrentHashJoin(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_, SharedContextPtr shared_context_);

    const TableJoin & getTableJoin() const override { return *table_join; }
    bool addJoinedBlock(const Block & block, bool check_limits) override;
    void checkTypesOfKeys(const Block & block) const override;
    void joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed) override;
    void setTotals(const Block & block) override;
    const Block & getTotals() const override;
    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;
    bool alwaysReturnsEmptySet() const override;
    bool supportShuffle() const override { return true; }
    JoinPtr clone() override;
    bool supportTotals() const override { return true; }
    IBlocksStreamPtr
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

    static bool isSupported(const std::shared_ptr<TableJoin> & table_join);
private:

    std::shared_ptr<TableJoin> table_join;
    Block right_sample_block;
    std::unique_ptr<HashJoin> inner_join;
    SharedContextPtr shared_context;
};
}
