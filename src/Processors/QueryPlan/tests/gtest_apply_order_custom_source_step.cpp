#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/typeid_cast.h>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

using namespace DB;

/// `applyOrder` takes the sorting property of the data from any `ISourceStep`, not only from
/// `ReadFromMergeTree`. A storage with its own source step can advertise the order of the data it reads
/// by overriding `getSortDescription`, and the steps above have to reuse it.
namespace
{

/// A source step of a custom storage that reads data sorted by the given description.
class SortedCustomSourceStep final : public ISourceStep
{
public:
    SortedCustomSourceStep(SharedHeader header, SortDescription sort_description_)
        : ISourceStep(std::move(header))
        , sort_description(std::move(sort_description_))
    {
    }

    String getName() const override { return "SortedCustomSource"; }

    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The step is only used to test plan optimizations");
    }

    const SortDescription & getSortDescription() const override { return sort_description; }

private:
    SortDescription sort_description;
};

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({
        ColumnWithTypeAndName(type->createColumn(), type, "k"),
        ColumnWithTypeAndName(type->createColumn(), type, "v")}));
}

SortDescription sortedBy(const Names & columns)
{
    SortDescription description;
    for (const auto & column : columns)
        description.emplace_back(column);
    return description;
}

QueryPlanOptimizationSettings makeOptimizationSettings()
{
    QueryPlanOptimizationSettings settings(getContext().context);
    settings.optimize_sorting_by_input_stream_properties = true;
    settings.distinct_in_order = true;
    return settings;
}

/// Builds the plan `step <- source` and runs `applyOrder` over it.
template <typename Step>
Step & applyOrderAboveSource(QueryPlan::Nodes & nodes, SortDescription source_sort_description, QueryPlanStepPtr step)
{
    auto & source = nodes.emplace_back(QueryPlan::Node{
        .step = std::make_unique<SortedCustomSourceStep>(makeHeader(), std::move(source_sort_description))});
    auto & root = nodes.emplace_back(QueryPlan::Node{.step = std::move(step), .children = {&source}});

    QueryPlanOptimizations::applyOrder(makeOptimizationSettings(), root);
    return typeid_cast<Step &>(*root.step);
}

QueryPlanStepPtr makeFullSorting(const Names & columns)
{
    return std::make_unique<SortingStep>(makeHeader(), sortedBy(columns), /*limit_=*/ 0, SortingStep::Settings(DEFAULT_BLOCK_SIZE));
}

QueryPlanStepPtr makePreliminaryDistinct(const Names & columns)
{
    return std::make_unique<DistinctStep>(makeHeader(), DistinctStep::Settings{}, /*limit_hint_=*/ 0, columns, /*pre_distinct_=*/ true);
}

}

TEST(ApplyOrderCustomSourceStep, SortingByTheSourceOrderBecomesFinishSorting)
{
    QueryPlan::Nodes nodes;
    auto & sorting = applyOrderAboveSource<SortingStep>(nodes, sortedBy({"k"}), makeFullSorting({"k", "v"}));

    EXPECT_EQ(sorting.getType(), SortingStep::Type::FinishSorting);
}

TEST(ApplyOrderCustomSourceStep, SortingStaysFullWithoutTheSourceOrder)
{
    QueryPlan::Nodes nodes;
    auto & sorting = applyOrderAboveSource<SortingStep>(nodes, SortDescription{}, makeFullSorting({"k", "v"}));

    EXPECT_EQ(sorting.getType(), SortingStep::Type::Full);
}

TEST(ApplyOrderCustomSourceStep, SortingByAnotherColumnStaysFull)
{
    QueryPlan::Nodes nodes;
    auto & sorting = applyOrderAboveSource<SortingStep>(nodes, sortedBy({"k"}), makeFullSorting({"v"}));

    EXPECT_EQ(sorting.getType(), SortingStep::Type::Full);
}

TEST(ApplyOrderCustomSourceStep, PreliminaryDistinctReadsInTheSourceOrder)
{
    QueryPlan::Nodes nodes;
    auto & distinct = applyOrderAboveSource<DistinctStep>(nodes, sortedBy({"k"}), makePreliminaryDistinct({"k", "v"}));

    const auto & distinct_sort_description = distinct.getSortDescription();
    ASSERT_EQ(distinct_sort_description.size(), 1);
    EXPECT_EQ(distinct_sort_description.front().column_name, "k");
}

TEST(ApplyOrderCustomSourceStep, PreliminaryDistinctIsNotInOrderWithoutTheSourceOrder)
{
    QueryPlan::Nodes nodes;
    auto & distinct = applyOrderAboveSource<DistinctStep>(nodes, SortDescription{}, makePreliminaryDistinct({"k", "v"}));

    EXPECT_TRUE(distinct.getSortDescription().empty());
}
