#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/PlanStepIndex.h>
#include <Processors/QueryPlan/Profiling/Analysis/PlanAnalysisSnapshot.h>
#include <Processors/QueryPlan/Profiling/Capture/PlanWalk.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadNothingStep.h>

#include <memory>
#include <thread>

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    return std::make_shared<const Block>(Block{{ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x"}});
}

QueryPlanStepPtr makeLimit(const SharedHeader & header)
{
    return std::make_unique<LimitStep>(header, /*limit_=*/ 1, /*offset_=*/ 0);
}

/// Runs `build` on a thread that has a `ThreadStatus` but no thread group, so that the steps it
/// builds take `PlanStepIndex::NONE`. `with_group` runs a part of it inside one thread group, the
/// same for every call, as a query would, so that the steps built there take distinct indices.
template <typename Build>
void onThreadWithoutGroup(Build && build)
{
    std::thread([&]
    {
        ThreadStatus thread_status;
        const auto group = std::make_shared<ThreadGroup>(getContext().context, 0);
        const auto with_group = [&group](const auto & make)
        {
            CurrentThread::attachToGroupIfDetached(group);
            auto step = make();
            CurrentThread::detachFromGroupIfNotDetached();
            return step;
        };
        build(with_group);
    }).join();
}

}

/// Two connected steps built outside of any query both take `NONE`. Keyed by it, the child
/// resolved to the parent, and the post-order traversal of the snapshot never ended.
TEST(PlanWalk, StepsBuiltOutsideOfQueryAreLeftOut)
{
    QueryPlan plan;
    onThreadWithoutGroup([&](const auto &)
    {
        const auto header = makeHeader();
        plan.addStep(std::make_unique<ReadNothingStep>(header));
        plan.addStep(makeLimit(header));
    });

    ASSERT_EQ(plan.getRootNode()->step->getStepIndex(), PlanStepIndex::NONE);
    ASSERT_EQ(plan.getRootNode()->children.at(0)->step->getStepIndex(), PlanStepIndex::NONE);

    const PlanWalk walk = PlanWalk::of(plan, /*only_built_child_plans=*/ true);
    EXPECT_TRUE(walk.steps.empty());
    EXPECT_EQ(walk.plan, nullptr);

    const auto snapshot = PlanAnalysisSnapshot::capture(walk);
    EXPECT_TRUE(snapshot.stepsInPostOrder().empty());
}

/// A step without an identity between two that have one: the walk leaves it out and attaches the
/// step below it to the step above it.
TEST(PlanWalk, StepBuiltOutsideOfQueryIsBypassed)
{
    QueryPlan plan;
    onThreadWithoutGroup([&](const auto & with_group)
    {
        const auto header = makeHeader();
        plan.addStep(with_group([&] { return QueryPlanStepPtr(std::make_unique<ReadNothingStep>(header)); }));
        plan.addStep(makeLimit(header));
        plan.addStep(with_group([&] { return makeLimit(header); }));
    });

    const auto * top = plan.getRootNode();
    const auto * middle = top->children.at(0);
    const auto * bottom = middle->children.at(0);
    ASSERT_NE(top->step->getStepIndex(), PlanStepIndex::NONE);
    ASSERT_EQ(middle->step->getStepIndex(), PlanStepIndex::NONE);
    ASSERT_NE(bottom->step->getStepIndex(), PlanStepIndex::NONE);

    const PlanWalk walk = PlanWalk::of(plan, /*only_built_child_plans=*/ true);
    ASSERT_EQ(walk.steps.size(), 2);
    EXPECT_EQ(walk.root_id, top->step->getStepIndex());
    EXPECT_EQ(walk.steps[0].id, top->step->getStepIndex());
    EXPECT_EQ(walk.steps[0].children, VectorWithMemoryTracking<UInt64>{bottom->step->getStepIndex()});
    EXPECT_EQ(walk.steps[1].id, bottom->step->getStepIndex());
    EXPECT_TRUE(walk.steps[1].children.empty());

    const auto snapshot = PlanAnalysisSnapshot::capture(walk);
    EXPECT_EQ(snapshot.stepsInPostOrder().size(), 2);
}
