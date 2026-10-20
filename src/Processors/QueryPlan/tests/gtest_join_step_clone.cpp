#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

/// `JoinStep::clone` is what lets `FutureSetFromSubquery::buildOrderedSetInplace` run the speculative
/// in-place build of an `IN` subquery against a copy of the source plan and keep the original for the
/// deferred build. The analyzer stores the source plan with a `JoinStepLogical`, which becomes a physical
/// `JoinStep` only during optimization, after the clone, so no SQL query reaches the physical clone; it
/// is exercised here on the step directly. As in the real path, the step is cloned before it is executed:
/// a filled `HashJoin` does not support cloning at all.

namespace
{

SharedHeader keyHeader(const String & name)
{
    return std::make_shared<const Block>(Block{ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), name)});
}

const SharedHeader left_header = keyHeader("lk");
const SharedHeader right_header = keyHeader("rk");

/// `INNER ALL JOIN ON lk = rk` with a `HashJoin`, the shape `convertLogicalJoinToPhysical` produces.
std::unique_ptr<JoinStep> makeJoinStep()
{
    Settings settings;
    auto table_join = std::make_shared<TableJoin>(settings, JoinAnalyzeMode::None, nullptr, nullptr);
    table_join->getTableJoin().kind = JoinKind::Inner;
    table_join->getTableJoin().strictness = JoinStrictness::All;
    table_join->addDisjunct();
    table_join->getClauses().back().addKey("lk", "rk", /*null_safe_comparison=*/false);
    table_join->setColumnsFromJoinedTable(right_header->getNamesAndTypesList(), {"lk"}, "", left_header->getNamesAndTypesList());
    table_join->setColumnsAddedByJoin(right_header->getNamesAndTypesList());

    auto join = std::make_shared<HashJoin>(table_join, right_header);
    return std::make_unique<JoinStep>(
        left_header,
        right_header,
        std::move(join),
        /*max_block_size_=*/DEFAULT_BLOCK_SIZE,
        /*min_block_size_rows_=*/0,
        /*min_block_size_bytes_=*/0,
        /*max_streams_=*/1,
        NameSet{"lk", "rk"},
        /*keep_left_read_in_order_=*/false,
        /*use_new_analyzer_=*/true);
}

/// What `optimizeJoinLegacy` does to swap the build side: a join over swapped sides, built with the
/// sample headers in reverse order, and `swap_streams` so that `updatePipeline` swaps the inputs.
void swapSides(JoinStep & step)
{
    const auto & join = step.getJoin();
    auto swapped_table_join = std::make_shared<TableJoin>(join->getTableJoin());
    swapped_table_join->swapSides();
    step.setJoin(join->clone(swapped_table_join, right_header, left_header), /*swap_streams_=*/true);
}

std::unique_ptr<QueryPipelineBuilder> keyPipeline(const SharedHeader & header, const std::vector<UInt64> & values)
{
    auto column = ColumnUInt64::create();
    for (auto value : values)
        column->insertValue(value);

    Chunks chunks;
    chunks.emplace_back(Chunk(Columns{std::move(column)}, values.size()));

    auto pipeline = std::make_unique<QueryPipelineBuilder>();
    pipeline->init(Pipe(std::make_shared<SourceFromChunks>(header, std::move(chunks))));
    return pipeline;
}

/// Executes the step over fresh inputs and returns the sorted `(lk, rk)` pairs.
std::vector<std::pair<UInt64, UInt64>> execute(IQueryPlanStep & step)
{
    /// `lk` in [0, 10), `rk` in [5, 1000): the join matches 5..9 once each.
    std::vector<UInt64> left_keys;
    for (UInt64 i = 0; i < 10; ++i)
        left_keys.push_back(i);
    std::vector<UInt64> right_keys;
    for (UInt64 i = 5; i < 1000; ++i)
        right_keys.push_back(i);

    QueryPipelineBuilders inputs;
    inputs.push_back(keyPipeline(left_header, left_keys));
    inputs.push_back(keyPipeline(right_header, right_keys));

    auto builder = step.updatePipeline(std::move(inputs), BuildQueryPipelineSettings(getContext().context));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    PullingPipelineExecutor executor(pipeline);

    std::vector<std::pair<UInt64, UInt64>> rows;
    Block block;
    while (executor.pull(block))
        for (size_t i = 0; i < block.rows(); ++i)
            rows.emplace_back(block.getByName("lk").column->getUInt(i), block.getByName("rk").column->getUInt(i));
    std::sort(rows.begin(), rows.end());
    return rows;
}

const std::vector<std::pair<UInt64, UInt64>> expected_rows{{5, 5}, {6, 6}, {7, 7}, {8, 8}, {9, 9}};

}

TEST(JoinStepClone, ClonedJoinHasIndependentState)
{
    auto step = makeJoinStep();
    auto cloned = step->clone();
    auto & cloned_step = typeid_cast<JoinStep &>(*cloned);

    /// A shallow copy of `JoinPtr` would make the in-place and the deferred build fill one `IJoin`.
    ASSERT_NE(cloned_step.getJoin().get(), step->getJoin().get());
    EXPECT_EQ(cloned_step.getOutputHeader()->getNames(), step->getOutputHeader()->getNames());

    /// The speculative build runs the clone; the original join stays empty, so the deferred build
    /// runs the original on its own and gets the same rows instead of a second copy of the right side.
    EXPECT_EQ(execute(cloned_step), expected_rows);
    EXPECT_EQ(typeid_cast<const HashJoin &>(*step->getJoin()).getTotalRowCount(), 0u);
    EXPECT_EQ(execute(*step), expected_rows);
}

TEST(JoinStepClone, CloneKeepsSwappedBuildSide)
{
    auto step = makeJoinStep();
    swapSides(*step);

    auto cloned = step->clone();
    auto & cloned_step = typeid_cast<JoinStep &>(*cloned);

    /// After the swap the left input is the build side, so the cloned join must be built with the left
    /// header as its right sample, not with `input_headers` in their original order.
    ASSERT_NE(cloned_step.getJoin().get(), step->getJoin().get());
    EXPECT_EQ(cloned_step.getOutputHeader()->getNames(), step->getOutputHeader()->getNames());

    EXPECT_EQ(execute(cloned_step), expected_rows);
    EXPECT_EQ(typeid_cast<const HashJoin &>(*step->getJoin()).getTotalRowCount(), 0u);
    EXPECT_EQ(execute(*step), expected_rows);
}
