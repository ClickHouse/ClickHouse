#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/TableJoin.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <QueryPipeline/QueryPipeline.h>

using namespace DB;

namespace
{

/// A totals port that exists but never delivers a chunk satisfies neither branch of
/// `FillingRightJoinSideTransform::prepare`, so `setTotals` is never called. `NullSource` has the
/// same empty-`Chunk` shape as `RemoteTotalsSource` when a remote sends no totals.

SharedHeader keyHeader(const String & name)
{
    Block header{ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), name)};
    return std::make_shared<const Block>(std::move(header));
}

std::shared_ptr<ISource> keySource(const String & name, const std::vector<UInt64> & values)
{
    auto column = ColumnUInt64::create();
    for (auto v : values)
        column->insertValue(v);

    Chunks chunks;
    chunks.emplace_back(Chunk(Columns{std::move(column)}, values.size()));
    return std::make_shared<SourceFromChunks>(keyHeader(name), std::move(chunks));
}

std::vector<std::pair<UInt64, UInt64>> runFullJoinWithStarvedTotals(
    const std::vector<UInt64> & left_keys, const std::vector<UInt64> & right_keys)
{
    auto left_header = keyHeader("lk");
    auto right_header = keyHeader("rk");

    Settings settings;
    auto table_join = std::make_shared<TableJoin>(settings, JoinAnalyzeMode::None, nullptr, nullptr);
    table_join->getTableJoin().kind = JoinKind::Full;
    table_join->getTableJoin().strictness = JoinStrictness::All;
    table_join->addDisjunct();
    table_join->getClauses().back().addKey("lk", "rk", /*null_safe_comparison=*/false);
    table_join->setColumnsFromJoinedTable(right_header->getNamesAndTypesList(), {"lk"}, "", left_header->getNamesAndTypesList());
    /// `rk` is a required right key, so the join emits it and the result can be compared per row.
    table_join->setColumnsAddedByJoin(right_header->getNamesAndTypesList());

    auto join = std::make_shared<MergeJoin>(table_join, right_header);

    auto shared_output_header
        = std::make_shared<const Block>(JoiningTransform::transformHeader(*left_header, join));

    /// Right side: real rows plus a totals port that finishes without pushing anything.
    auto right_source = keySource("rk", right_keys);
    auto totals_source = std::make_shared<NullSource>(right_header);
    auto filling = std::make_shared<FillingRightJoinSideTransform>(
        right_header, join, std::make_shared<FinishCounter>(1));
    auto * totals_port = filling->addTotalsPort();
    connect(right_source->getPort(), filling->getInputs().front());
    connect(totals_source->getPort(), *totals_port);

    auto left_source = keySource("lk", left_keys);
    auto joining = std::make_shared<JoiningTransform>(
        left_header, shared_output_header, join, /*max_block_size_=*/0,
        /*on_totals_=*/false, /*default_totals_=*/false, std::make_shared<FinishCounter>(1));
    connect(left_source->getPort(), joining->getInputs().front());
    connect(filling->getOutputs().front(), joining->getInputs().back());

    auto processors = std::make_shared<Processors>();
    processors->emplace_back(std::move(left_source));
    processors->emplace_back(std::move(right_source));
    processors->emplace_back(std::move(totals_source));
    processors->emplace_back(std::move(filling));
    processors->emplace_back(joining);

    QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, &joining->getOutputs().front());
    PullingPipelineExecutor executor(pipeline);

    std::vector<std::pair<UInt64, UInt64>> rows;
    Block block;
    while (executor.pull(block))
    {
        for (size_t i = 0; i < block.rows(); ++i)
            rows.emplace_back(block.getByName("lk").column->getUInt(i), block.getByName("rk").column->getUInt(i));
    }
    std::sort(rows.begin(), rows.end());
    return rows;
}

}

TEST(MergeJoinStarvedTotals, FullJoinKeepsRightRows)
{
    /// Keys 0 and 1 match; 2 is left only; 3 is right only. `join_use_nulls` defaults to false,
    /// so an unmatched side reads as 0.
    auto rows = runFullJoinWithStarvedTotals({0, 1, 2}, {0, 1, 3});

    const std::vector<std::pair<UInt64, UInt64>> expected{{0, 0}, {0, 3}, {1, 1}, {2, 0}};
    ASSERT_EQ(rows, expected);
}
