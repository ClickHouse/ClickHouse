#include <gtest/gtest.h>

#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadNothingStep.h>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

namespace
{

SharedHeader makeHeader(const Names & names)
{
    Block header;
    for (const auto & name : names)
    {
        auto type = std::make_shared<DataTypeUInt64>();
        header.insert({type->createColumn(), type, name});
    }
    return std::make_shared<const Block>(std::move(header));
}

/// A plan built bottom-up, each step over the one added before it.
struct Chain
{
    QueryPlan::Nodes nodes;

    QueryPlan::Node & top() { return nodes.back(); }

    void addSource(const Names & names)
    {
        nodes.emplace_back().step = std::make_unique<ReadNothingStep>(makeHeader(names));
    }

    /// An expression that outputs the columns `outputs` of the step below. Every column below is an input of its DAG,
    /// so nothing passes through, and a column not in `outputs` is read and not needed.
    void addProjection(const Names & outputs)
    {
        const auto input_header = top().step->getOutputHeader();

        ActionsDAG dag;
        std::unordered_map<String, const ActionsDAG::Node *> inputs;
        for (const auto & column : *input_header)
            inputs[column.name] = &dag.addInput(column.name, column.type);
        for (const auto & name : outputs)
            dag.getOutputs().push_back(inputs.at(name));

        add(std::make_unique<ExpressionStep>(input_header, std::move(dag)));
    }

    /// A step that cannot drop columns.
    void addOffset() { add(std::make_unique<OffsetStep>(top().step->getOutputHeader(), 1)); }

    /// The names of the output header of the step `depth` steps below the top.
    String outputAt(size_t depth)
    {
        auto it = nodes.rbegin();
        std::advance(it, depth);
        return it->step->getOutputHeader()->dumpNames();
    }

private:
    void add(QueryPlanStepPtr step)
    {
        auto * child = &top();
        auto & node = nodes.emplace_back();
        node.step = std::move(step);
        node.children = {child};
    }
};

}

/// Each step leaves the columns b and c of the step below unneeded, so the walk goes down to the read. The read cannot
/// drop columns, and the step above it consumes the ones it keeps.
TEST(RemoveUnusedColumnsModes, LocalWalksDownWhileSomethingIsUnneeded)
{
    Chain chain;
    chain.addSource({"a", "b", "c"});
    chain.addProjection({"a", "b", "c"});
    chain.addProjection({"a", "b", "c"});
    chain.addProjection({"a"});

    EXPECT_EQ(removeUnusedColumns(chain.top(), RemoveUnusedColumnsMode::Local), 3u);
    EXPECT_EQ(chain.outputAt(0), "a");
    EXPECT_EQ(chain.outputAt(1), "a");
    EXPECT_EQ(chain.outputAt(2), "a");
    EXPECT_EQ(chain.outputAt(3), "a, b, c");

    /// Nothing is left to remove.
    EXPECT_EQ(removeUnusedColumns(chain.top(), RemoveUnusedColumnsMode::Local), 0u);
    EXPECT_EQ(removeUnusedColumns(chain.top(), RemoveUnusedColumnsMode::Global), 0u);
}

/// The top step needs all of the step below it, so the local walk stops there, while the step below leaves columns of
/// its own child unread. That is the business of the local walk that starts at that step, or of the global one.
TEST(RemoveUnusedColumnsModes, LocalStopsWhereNothingIsUnneeded)
{
    {
        Chain chain;
        chain.addSource({"a", "b", "c"});
        chain.addProjection({"a", "b", "c"});
        chain.addProjection({"a"});
        chain.addProjection({"a"});

        EXPECT_EQ(removeUnusedColumns(chain.top(), RemoveUnusedColumnsMode::Local), 0u);
        EXPECT_EQ(chain.outputAt(2), "a, b, c");

        EXPECT_EQ(removeUnusedColumns(chain.top(), RemoveUnusedColumnsMode::Global), 3u);
        EXPECT_EQ(chain.outputAt(2), "a");
    }
    {
        Chain chain;
        chain.addSource({"a", "b", "c"});
        chain.addProjection({"a", "b", "c"});
        chain.addProjection({"a"});
        auto & middle = chain.top();
        chain.addProjection({"a"});

        /// Started at the step that reads only a, the local walk removes b and c below it.
        EXPECT_EQ(removeUnusedColumns(middle, RemoveUnusedColumnsMode::Local), 2u);
        EXPECT_EQ(chain.outputAt(2), "a");
    }
}

/// A step that cannot drop columns needs everything of its children, so a local walk from it has nothing to do. The
/// global walk goes on below it.
TEST(RemoveUnusedColumnsModes, LocalDoesNothingIfTheRootCannotPrune)
{
    Chain chain;
    chain.addSource({"a", "b", "c"});
    chain.addProjection({"a", "b", "c"});
    chain.addProjection({"a"});
    chain.addOffset();

    EXPECT_EQ(removeUnusedColumns(chain.top(), RemoveUnusedColumnsMode::Local), 0u);
    EXPECT_EQ(chain.outputAt(2), "a, b, c");

    EXPECT_EQ(removeUnusedColumns(chain.top(), RemoveUnusedColumnsMode::Global), 3u);
    EXPECT_EQ(chain.outputAt(2), "a");
}

/// The step below cannot drop the columns the top step does not need, so the top step keeps consuming them and does
/// not change.
TEST(RemoveUnusedColumnsModes, LocalStopsAtAChildThatCannotPrune)
{
    Chain chain;
    chain.addSource({"a", "b", "c"});
    chain.addProjection({"a", "b", "c"});
    chain.addOffset();
    chain.addProjection({"a"});

    EXPECT_EQ(removeUnusedColumns(chain.top(), RemoveUnusedColumnsMode::Local), 0u);
    EXPECT_EQ(chain.outputAt(0), "a");
    EXPECT_EQ(chain.outputAt(1), "a, b, c");
    EXPECT_EQ(chain.outputAt(2), "a, b, c");
}
