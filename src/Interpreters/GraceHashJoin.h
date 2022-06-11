#include <Interpreters/IJoin.h>

#include <Core/Block.h>

namespace DB
{

class TableJoin;

class GraceHashJoin final : public IJoin
{
public:
    GraceHashJoin(std::shared_ptr<TableJoin> table_join, const Block & right_sample_block, bool any_take_last_row);

    const TableJoin & getTableJoin() const override { return *table_join; }

    bool addJoinedBlock(const Block & block, bool check_limits) override;
    void checkTypesOfKeys(const Block & block) const override;
    void joinBlock(Block& block, std::shared_ptr<ExtraBlock> & not_processed) override;

    void setTotals(const Block & block) override { totals = block; }
    const Block & getTotals() const override { return totals; }

    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;
    bool alwaysReturnsEmptySet() const override;

    std::shared_ptr<NotJoinedBlocks>
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

private:
    std::shared_ptr<TableJoin> table_join;
    Block totals;

    JoinPtr first_bucket;
    std::vector<std::string> file_buckets;
};

}
