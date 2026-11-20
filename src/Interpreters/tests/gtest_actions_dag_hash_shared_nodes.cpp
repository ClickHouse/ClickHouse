#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>

using namespace DB;

namespace
{

const ActionsDAG::Node & addFunction(ActionsDAG & dag, const String & function, ActionsDAG::NodeRawConstPtrs children, const String & name)
{
    return dag.addFunction(FunctionFactory::instance().get(function, getContext().context), std::move(children), name);
}

ActionsDAG makeChain(size_t levels, const String & input = "x")
{
    ActionsDAG dag;
    const auto * node = &dag.addInput(input, std::make_shared<DataTypeUInt64>());
    for (size_t i = 0; i < levels; ++i)
        node = &addFunction(dag, "plus", {node, node}, "a" + std::to_string(i));
    dag.getOutputs() = {node};
    return dag;
}

}

TEST(ActionsDAGHash, SharedNodesHashedOnce)
{
    tryRegisterFunctions();
    /// 64 levels of `plus(prev, prev)` have 2^64 paths, so these return only if each node is hashed once.
    const auto first = makeChain(64);
    const auto second = makeChain(64);
    const auto & output = *first.getOutputs().front();
    EXPECT_EQ(output.getHash(), second.getOutputs().front()->getHash());
    EXPECT_NE(output.getHash(), output.children.front()->getHash());
    EXPECT_EQ(first.getHash(), second.getHash());
    EXPECT_EQ(first.getOutputIdentity(output.result_name), second.getOutputIdentity(output.result_name));

    /// Differs from `first` only in the input 64 levels down.
    const auto other = makeChain(64, "y");
    EXPECT_NE(output.getHash(), other.getOutputs().front()->getHash());
    EXPECT_NE(first.getHash(), other.getHash());
    EXPECT_NE(first.getOutputIdentity(output.result_name), other.getOutputIdentity(output.result_name));
}

TEST(ActionsDAGHash, StructureNotSharing)
{
    tryRegisterFunctions();
    const auto type = std::make_shared<DataTypeUInt64>();

    ActionsDAG shared;
    const auto & x = shared.addInput("x", type);
    const auto & n = addFunction(shared, "negate", {&x}, "n");
    const auto & shared_plus = addFunction(shared, "plus", {&n, &n}, "p");
    shared.getOutputs() = {&shared_plus};

    ActionsDAG separate;
    const auto & y = separate.addInput("x", type);
    const auto & separate_plus
        = addFunction(separate, "plus", {&addFunction(separate, "negate", {&y}, "n"), &addFunction(separate, "negate", {&y}, "n")}, "p");
    separate.getOutputs() = {&separate_plus};

    EXPECT_EQ(shared_plus.getHash(), separate_plus.getHash());
    EXPECT_EQ(shared.getHash(), separate.getHash());
    EXPECT_NE(addFunction(shared, "minus", {&n, &n}, "p").getHash(), shared_plus.getHash());
    EXPECT_NE(addFunction(shared, "plus", {&n, &x}, "q").getHash(), addFunction(shared, "plus", {&x, &n}, "q").getHash());

    /// Two outputs with an equal first one: the second one must count.
    shared.getOutputs() = {&shared_plus, &n};
    const auto with_n = shared.getHash();
    shared.getOutputs() = {&shared_plus, &x};
    EXPECT_NE(shared.getHash(), with_n);
}
