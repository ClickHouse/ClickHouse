#pragma once
#include <Interpreters/IJoin.h>
#include <Interpreters/Context_fwd.h>
namespace DB
{
class PartitionHashJoin : public IJoin
{
public:
    using JoinGetter = std::function<JoinPtr()>; // for building a new inner join.
    explicit PartitionHashJoin(ContextPtr context_, std::shared_ptr<TableJoin> table_join_, JoinGetter getter);
    const TableJoin & getTableJoin() const override { return *table_join; }
    bool addJoinedBlock(const Block & block, bool check_limits) override;
    void checkTypesOfKeys(const Block & block) const override;
    void joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed) override;
    void setTotals(const Block & block) override;
    const Block & getTotals() const override;
    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;
    bool alwaysReturnsEmptySet() const override;
    JoinProperty getJoinProperty() const override;
    void setupInnerJoins(size_t n) override;
    JoinPtr getInnerJoin(size_t n) override;
    bool supportTotals() const override { return false; }
    IBlocksStreamPtr
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

private:
    ContextPtr context;
    std::shared_ptr<TableJoin> table_join;
    JoinGetter join_getter;
    JoinPtr init_join;
    std::vector<JoinPtr> inner_joins;
};
}
