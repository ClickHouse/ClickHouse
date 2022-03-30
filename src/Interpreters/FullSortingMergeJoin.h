#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Poco/Logger.h>

namespace DB
{

/// Dummy class, actual joining is done by MergeTransform
class FullSortingMergeJoin : public IJoin
{
public:
    explicit FullSortingMergeJoin(TableJoin & table_join_)
        : table_join(table_join_)
    {
        LOG_TRACE(&Poco::Logger::get("FullSortingMergeJoin"), "Will use full sorting merge join");
    }

    const TableJoin & getTableJoin() const override { return table_join; }

    bool addJoinedBlock(const Block & /* block */, bool /* check_limits */) override { __builtin_unreachable(); }

    void checkTypesOfKeys(const Block & /* block */) const override
    {
    }

    /// Used just to get result header
    void joinBlock(Block & block, std::shared_ptr<ExtraBlock> & /* not_processed */) override
    {
        UNUSED(block);
        /// ...
    }

    void setTotals(const Block & /* block */) override { __builtin_unreachable(); }
    const Block & getTotals() const override { __builtin_unreachable(); }

    size_t getTotalRowCount() const override { __builtin_unreachable(); }
    size_t getTotalByteCount() const override { __builtin_unreachable(); }
    bool alwaysReturnsEmptySet() const override { __builtin_unreachable(); }

    std::shared_ptr<NotJoinedBlocks>
    getNonJoinedBlocks(const Block & /* left_sample_block */, const Block & /* result_sample_block */, UInt64 /* max_block_size */) const override
    {
        __builtin_unreachable();
    }

    virtual JoinPipelineType pipelineType() const override { return JoinPipelineType::YShaped; }

private:
    TableJoin & table_join;
};

}
