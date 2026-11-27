#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAGTraversal.h>
#include <Storages/VirtualColumnUtils.h>

using namespace DB;

namespace
{

const ActionsDAG::Node & addFunction(ActionsDAG & dag, const String & name, ActionsDAG::NodeRawConstPtrs children, const String & result_name)
{
    tryRegisterFunctions();
    return dag.addFunction(FunctionFactory::instance().get(name, getContext().context), std::move(children), result_name);
}

/// `a_{i+1} = plus(a_i, a_i)`: `levels + 1` nodes and 2^levels paths from the output to the input.
ActionsDAG makeChain(size_t levels)
{
    ActionsDAG dag;
    const auto * node = &dag.addInput("x", std::make_shared<DataTypeUInt64>());
    for (size_t i = 0; i < levels; ++i)
        node = &addFunction(dag, "plus", {node, node}, "a" + std::to_string(i));
    dag.getOutputs() = {node};
    return dag;
}

/// The node `steps` levels below the output of a chain.
const ActionsDAG::Node * chainNode(const ActionsDAG & dag, size_t steps)
{
    const auto * node = dag.getOutputs().front();
    for (size_t i = 0; i < steps; ++i)
        node = node->children.front();
    return node;
}

UInt64 countPaths(const ActionsDAG::Node * node, const std::unordered_map<const ActionsDAG::Node *, UInt64> & paths)
{
    if (node->children.empty())
        return 1;
    UInt64 sum = 0;
    for (const auto * child : node->children)
        sum += paths.at(child);
    return sum;
}

}

TEST(ActionsDAGTraversal, PostOrderVisitsEachNodeOnceChildrenFirst)
{
    const auto dag = makeChain(20);
    std::vector<const ActionsDAG::Node *> order;
    visitNodesPostOrder(dag.getOutputs(), [&](const ActionsDAG::Node * node) { order.push_back(node); });

    ASSERT_EQ(order.size(), 21);
    EXPECT_EQ(order.front()->type, ActionsDAG::ActionType::INPUT);
    EXPECT_EQ(order.back(), dag.getOutputs().front());
    for (size_t i = 1; i < order.size(); ++i)
        EXPECT_EQ(order[i]->children.front(), order[i - 1]);
}

TEST(ActionsDAGTraversal, PostOrderSkipsNode)
{
    const auto dag = makeChain(4);
    std::vector<const ActionsDAG::Node *> order;
    visitNodesPostOrder(
        dag.getOutputs(),
        [&](const ActionsDAG::Node * node) { order.push_back(node); },
        [&](const ActionsDAG::Node * node) { return node == chainNode(dag, 2); });

    ASSERT_EQ(order.size(), 2);
    EXPECT_EQ(order[0], chainNode(dag, 1));
    EXPECT_EQ(order[1], chainNode(dag, 0));
}

TEST(ActionsDAGTraversal, PostOrderSkipsRoot)
{
    const auto dag = makeChain(4);
    size_t visits = 0;
    visitNodesPostOrder(
        dag.getOutputs(),
        [&](const ActionsDAG::Node *) { ++visits; },
        [&](const ActionsDAG::Node * node) { return node == chainNode(dag, 0); });
    EXPECT_EQ(visits, 0);
}

TEST(ActionsDAGTraversal, ComputeEachNodeOnce)
{
    const auto dag = makeChain(20);
    size_t calls = 0;
    std::unordered_map<const ActionsDAG::Node *, UInt64> paths;
    computeForNodes(dag.getOutputs(), paths, [&](const ActionsDAG::Node * node) { ++calls; return countPaths(node, paths); });

    EXPECT_EQ(calls, 21);
    EXPECT_EQ(paths.at(dag.getOutputs().front()), UInt64{1} << 20);
}

TEST(ActionsDAGTraversal, ComputeResumesFromFilledValue)
{
    const auto dag = makeChain(4);
    std::unordered_map<const ActionsDAG::Node *, UInt64> paths{{chainNode(dag, 2), 100}};
    size_t calls = 0;
    computeForNodes(dag.getOutputs(), paths, [&](const ActionsDAG::Node * node) { ++calls; return countPaths(node, paths); });

    EXPECT_EQ(calls, 2);
    EXPECT_EQ(paths.at(chainNode(dag, 2)), 100);
    EXPECT_EQ(paths.at(chainNode(dag, 0)), 400);
    EXPECT_FALSE(paths.contains(chainNode(dag, 3)));
}

TEST(ActionsDAGTraversal, ReachableVisitsEachNodeOnce)
{
    const auto dag = makeChain(20);
    size_t visits = 0;
    EXPECT_TRUE(visitReachableNodes(dag.getOutputs(), [&](const ActionsDAG::Node *) { ++visits; return VisitResult::Continue; }));
    EXPECT_EQ(visits, 21);
}

TEST(ActionsDAGTraversal, ReachableStopsAtMiddleNode)
{
    const auto dag = makeChain(20);
    size_t visits = 0;
    EXPECT_FALSE(visitReachableNodes(dag.getOutputs(), [&](const ActionsDAG::Node * node)
    {
        ++visits;
        return node == chainNode(dag, 5) ? VisitResult::Stop : VisitResult::Continue;
    }));
    EXPECT_EQ(visits, 6);
}

TEST(ActionsDAGTraversal, SkipChildrenStillReachesThroughAnotherParent)
{
    ActionsDAG dag;
    const auto & x = dag.addInput("x", std::make_shared<DataTypeInt64>());
    const auto & y = dag.addInput("y", std::make_shared<DataTypeInt64>());
    const auto & a = addFunction(dag, "plus", {&x, &y}, "a");
    const auto & b = addFunction(dag, "abs", {&x}, "b");
    const auto & p = addFunction(dag, "plus", {&a, &b}, "p");

    /// `y` is reachable only through the skipped `a`, `x` also through `b`.
    std::unordered_set<const ActionsDAG::Node *> visited;
    EXPECT_TRUE(visitReachableNodes({&p}, [&](const ActionsDAG::Node * node)
    {
        visited.insert(node);
        return node == &a ? VisitResult::SkipChildren : VisitResult::Continue;
    }));
    EXPECT_EQ(visited, (std::unordered_set<const ActionsDAG::Node *>{&p, &a, &b, &x}));
}

TEST(ActionsDAGTraversal, FindReachableIncludesBarrierButNotItsChildren)
{
    ActionsDAG dag;
    const auto & x = dag.addInput("x", std::make_shared<DataTypeInt64>());
    const auto & y = dag.addInput("y", std::make_shared<DataTypeInt64>());
    const auto & a = addFunction(dag, "plus", {&x, &y}, "a");
    const auto & b = addFunction(dag, "abs", {&x}, "b");
    const auto & p = addFunction(dag, "plus", {&a, &b}, "p");

    const auto reachable = findReachableNodes({&p}, [&](const ActionsDAG::Node * node) { return node == &a; });
    EXPECT_EQ(reachable, (std::unordered_set<const ActionsDAG::Node *>{&p, &a, &b, &x}));
}

TEST(ActionsDAGTraversal, AnyReachableNode)
{
    const auto dag = makeChain(20);
    EXPECT_TRUE(anyReachableNode(dag.getOutputs(), [](const ActionsDAG::Node * node) { return node->type == ActionsDAG::ActionType::INPUT; }));
    EXPECT_FALSE(anyReachableNode(dag.getOutputs(), [](const ActionsDAG::Node * node) { return node->type == ActionsDAG::ActionType::COLUMN; }));
}

TEST(ActionsDAGTraversal, DeterminismOfSharedChain)
{
    const auto dag = makeChain(40);
    EXPECT_TRUE(VirtualColumnUtils::isDeterministic(dag.getOutputs().front()));
    EXPECT_TRUE(VirtualColumnUtils::isDeterministicAllowingTopKFilter(dag.getOutputs().front()));
}
