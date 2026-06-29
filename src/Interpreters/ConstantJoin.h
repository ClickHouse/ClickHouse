#pragma once

#include <atomic>
#include <list>
#include <memory>
#include <optional>
#include <vector>

#include <Core/Block.h>
#include <Core/Field.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/RowRefs.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/logger_useful.h>

namespace DB
{

class TableJoin;

/** Implements joins with constant predicates, including `CROSS JOIN` and comma joins.
  * It stores right-side blocks without hash keys and emits cartesian or default rows according to the join kind,
  * strictness, and constant predicate value.
  * Right-side blocks may be compressed or spilled to a temporary block stream when join limits are exceeded.
  */
class ConstantJoin final : public IJoin
{
public:
    ConstantJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader right_sample_block_);

    std::string getName() const override { return "ConstantJoin"; }
    const TableJoin & getTableJoin() const override { return *table_join; }

    bool isCloneSupported() const override
    {
        return getTotals().empty() && total_rows_to_join == 0;
    }

    std::shared_ptr<IJoin> clone(
        const std::shared_ptr<TableJoin> & table_join_,
        SharedHeader,
        SharedHeader right_sample_block_) const override
    {
        return std::make_shared<ConstantJoin>(table_join_, right_sample_block_);
    }

    bool addBlockToJoin(const Block & source_block, bool check_limits) override;
    bool addBlockToJoin(const Block & source_block, size_t num_rows, bool check_limits) override;

    void checkTypesOfKeys(const Block &) const override {}

    JoinResultPtr joinBlock(Block block) override;

    size_t getTotalRowCount() const override { return in_memory_rows; }
    size_t getTotalByteCount() const override;

    bool alwaysReturnsEmptySet() const override;

    IBlocksStreamPtr getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

private:
    friend class ConstantJoinResult;
    friend class ConstantJoinNotJoinedRightFiller;

    struct StoredBlock
    {
        ColumnsInfo columns_info;
        size_t rows = 0;

        StoredBlock(ColumnsInfo columns_info_, size_t rows_)
            : columns_info(std::move(columns_info_))
            , rows(rows_)
        {
        }

        size_t allocatedBytes() const;
    };

    using StoredBlocks = std::list<StoredBlock>;

    enum class PredicateKind
    {
        True,
        False,
        CompareConstantKeys,
    };

    std::shared_ptr<TableJoin> table_join;

    Block right_sample_block;
    Block sample_block_with_columns_to_add;
    Block saved_block_sample;

    StoredBlocks right_blocks;

    TemporaryDataOnDiskScopePtr tmp_data;
    std::optional<TemporaryBlockStreamHolder> tmp_stream;

    size_t total_rows_to_join = 0;
    size_t in_memory_rows = 0;
    size_t allocated_size = 0;
    bool have_compressed = false;
    std::optional<ColumnsInfo> first_right_columns_info;

    PredicateKind predicate_kind = PredicateKind::True;
    std::optional<String> left_constant_key_name;
    std::optional<String> right_constant_key_name;
    std::optional<Field> right_constant_key_value;
    mutable std::atomic<Int32> constant_predicate_match = -1;
    mutable std::atomic_bool right_rows_matched = false;

    size_t max_joined_block_rows = 0;
    size_t max_joined_block_bytes = 0;

    bool shrink_blocks = false;
    Int64 memory_usage_before_adding_blocks = 0;

    LoggerPtr log;

    Block materializeColumnsFromRightBlock(Block block) const;
    bool constantPredicateMatches(const Block & left_block);
    void shrinkStoredBlocksToFit(size_t & total_bytes_in_join, bool force_optimize = false);
    void doDebugAsserts() const;
};

}
