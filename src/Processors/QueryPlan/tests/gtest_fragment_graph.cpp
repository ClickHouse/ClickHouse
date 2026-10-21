#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Fragments/FragmentGraph.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/ShuffleReceiveStep.h>
#include <Processors/QueryPlan/ShuffleSendStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/Exception.h>

using namespace DB;

namespace
{

/// Without an explicit stage name, the fragment is named `stage_<id>`. The hand-built graphs of the algorithm tests
/// use the sink as fragment 0; real plans number the `main` stage last, which the dump tests reproduce.
QueryFragment makeFragment(UInt32 id, size_t tasks = 1, std::optional<String> stage_name = std::nullopt)
{
    QueryFragment fragment;
    fragment.id = id;
    fragment.stage_name = stage_name.value_or("stage_" + std::to_string(id));
    fragment.tasks = tasks;
    return fragment;
}

/// Adds an exchange `exchange_<producer>` from `producer` to `consumer`.
void connect(FragmentGraph & graph, UInt32 producer, UInt32 consumer)
{
    FragmentExchange exchange;
    exchange.id = producer;
    exchange.name = "exchange_" + std::to_string(producer);
    exchange.from = producer;
    exchange.to = consumer;
    exchange.kind = FragmentExchange::Kind::Gather;
    exchange.source_buckets = 1;
    exchange.destination_buckets = 1;
    graph.exchanges.push_back(exchange);
    std::sort(graph.exchanges.begin(), graph.exchanges.end(), [](const auto & l, const auto & r) { return l.id < r.id; });
}

SortDescription makeSortDescription(const String & column)
{
    SortDescription description;
    description.emplace_back(column);
    return description;
}

FragmentGraph makeGraph(std::initializer_list<UInt32> ids)
{
    FragmentGraph graph;
    for (UInt32 id : ids)
        graph.fragments.push_back(makeFragment(id));
    return graph;
}

#ifndef DEBUG_OR_SANITIZER_BUILD
/// Expects `validateFragmentGraph` to throw an exception whose message contains `expected_message`.
void expectInvalid(const FragmentGraph & graph, const String & expected_message)
{
    try
    {
        validateFragmentGraph(graph);
        FAIL() << "expected an exception containing: " << expected_message;
    }
    catch (const Exception & e)
    {
        EXPECT_NE(e.message().find(expected_message), String::npos) << e.message();
    }
}
#endif

}

TEST(FragmentGraph, LevelsOfDiamond)
{
    auto graph = makeGraph({0, 1, 2, 3});
    connect(graph, 1, 3);
    connect(graph, 2, 3);
    connect(graph, 3, 0);

    EXPECT_EQ(computeLevels(graph), (std::vector<std::vector<UInt32>>{{1, 2}, {3}, {0}}));
}

TEST(FragmentGraph, LevelsOfChain)
{
    auto graph = makeGraph({0, 1, 2, 3});
    connect(graph, 1, 2);
    connect(graph, 2, 3);
    connect(graph, 3, 0);

    EXPECT_EQ(computeLevels(graph), (std::vector<std::vector<UInt32>>{{1}, {2}, {3}, {0}}));
}

TEST(FragmentGraph, LevelsOfUnevenBranches)
{
    /// F0 <- F2 <- F1 and F0 <- F3: the level of F0 is determined by the longest branch.
    auto graph = makeGraph({0, 1, 2, 3});
    connect(graph, 1, 2);
    connect(graph, 2, 0);
    connect(graph, 3, 0);

    EXPECT_EQ(computeLevels(graph), (std::vector<std::vector<UInt32>>{{1, 3}, {2}, {0}}));
}

/// `LOGICAL_ERROR` aborts instead of throwing in debug and sanitizer builds.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(FragmentGraph, CycleThrows)
{
    auto graph = makeGraph({0, 1, 2});
    connect(graph, 1, 2);
    connect(graph, 2, 1);
    expectInvalid(graph, "The fragment graph has a cycle");
}
#endif

TEST(FragmentGraph, LevelsWithSetFragment)
{
    /// F0 (set source) -> F1 (fills the set on the initiator) -> F2 (reads the set) -> F3 (main).
    FragmentGraph graph;
    graph.fragments = {makeFragment(0, 8), makeFragment(1, 1), makeFragment(2, 8), makeFragment(3, 1, "main")};
    graph.fragments[1].execution_mode = QueryFragment::ExecutionMode::SingleNode;
    graph.fragments[1].description = "k IN (SELECT k FROM small)";
    connect(graph, 0, 1);
    connect(graph, 1, 2);
    connect(graph, 2, 3);
    graph.exchanges[1].kind = FragmentExchange::Kind::SetValues;

    EXPECT_EQ(computeLevels(graph), (std::vector<std::vector<UInt32>>{{0}, {1}, {2}, {3}}));

    EXPECT_EQ(dumpFragmentGraph(graph),
        "query fragments: 4\n"
        "  F0 stage_0  distributed  tasks=8\n"
        "  F1 stage_1  single node  tasks=1  needs F0 via exchange_0  description: k IN (SELECT k FROM small)\n"
        "  F2 stage_2  distributed  tasks=8  needs F1 via exchange_1\n"
        "  F3 main     distributed  tasks=1  needs F2 via exchange_2\n"
        "exchanges: 3\n"
        "  exchange_0  F0 -> F1  gather      streaming  1 -> 1\n"
        "  exchange_1  F1 -> F2  set values  streaming  1 -> 1\n"
        "  exchange_2  F2 -> F3  gather      streaming  1 -> 1\n"
        "levels: [F0] [F1] [F2] [F3]\n");
}

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(FragmentGraph, InconsistentExchangesThrow)
{
    {
        /// The exchange comes from a fragment that is not in the graph.
        auto graph = makeGraph({0});
        connect(graph, 7, 0);
        expectInvalid(graph, "Exchange exchange_7 comes from unknown fragment 7");
    }
    {
        /// The exchange goes to a fragment that is not in the graph.
        auto graph = makeGraph({0});
        connect(graph, 0, 7);
        expectInvalid(graph, "Exchange exchange_0 goes to unknown fragment 7");
    }
    {
        /// Two exchanges with the same id.
        auto graph = makeGraph({0, 1});
        connect(graph, 1, 0);
        connect(graph, 1, 0);
        expectInvalid(graph, "Exchanges exchange_1 (id 1) and exchange_1 (id 1) are not in strictly ascending order of ids");
    }
}
#endif

TEST(FragmentGraph, Dump)
{
    /// The numbering of a real plan: `stage_N` first, `main` last.
    FragmentGraph graph;
    graph.fragments = {makeFragment(0, 1), makeFragment(1, 8), makeFragment(2, 1, "main")};

    FragmentExchange broadcast;
    broadcast.id = 0;
    broadcast.name = "exchange_0";
    broadcast.from = 0;
    broadcast.to = 1;
    broadcast.kind = FragmentExchange::Kind::Broadcast;
    broadcast.source_buckets = 1;
    broadcast.destination_buckets = 8;

    FragmentExchange gather;
    gather.id = 1;
    gather.name = "exchange_1";
    gather.from = 1;
    gather.to = 2;
    gather.kind = FragmentExchange::Kind::Gather;
    gather.source_buckets = 8;
    gather.destination_buckets = 1;
    gather.statistics.estimated_rows = 5000;

    graph.exchanges = {broadcast, gather};

    EXPECT_EQ(dumpFragmentGraph(graph),
        "query fragments: 3\n"
        "  F0 stage_0  distributed  tasks=1\n"
        "  F1 stage_1  distributed  tasks=8  needs F0 via exchange_0\n"
        "  F2 main     distributed  tasks=1  needs F1 via exchange_1\n"
        "exchanges: 2\n"
        "  exchange_0  F0 -> F1  broadcast  streaming  1 -> 8\n"
        "  exchange_1  F1 -> F2  gather     streaming  8 -> 1  ~5000 rows\n"
        "levels: [F0] [F1] [F2]\n");
}

TEST(FragmentGraph, DumpExchangeKinds)
{
    FragmentGraph graph;
    graph.fragments = {makeFragment(0), makeFragment(1), makeFragment(2), makeFragment(3), makeFragment(4, 1, "main")};
    connect(graph, 0, 4);
    connect(graph, 1, 4);
    connect(graph, 2, 4);
    connect(graph, 3, 4);
    graph.exchanges[0].kind = FragmentExchange::Kind::Hash;
    graph.exchanges[0].keys = {"a", "b"};
    graph.exchanges[1].kind = FragmentExchange::Kind::Any;
    graph.exchanges[1].mode = ExchangeDescription::Kind::Persisted;
    graph.exchanges[2].merge_sort = makeSortDescription("k");
    graph.exchanges[3].kind = FragmentExchange::Kind::SetValues;

    EXPECT_EQ(dumpFragmentGraph(graph),
        "query fragments: 5\n"
        "  F0 stage_0  distributed  tasks=1\n"
        "  F1 stage_1  distributed  tasks=1\n"
        "  F2 stage_2  distributed  tasks=1\n"
        "  F3 stage_3  distributed  tasks=1\n"
        "  F4 main     distributed  tasks=1  needs F0 via exchange_0, F1 via exchange_1, "
        "F2 via exchange_2, F3 via exchange_3\n"
        "exchanges: 4\n"
        "  exchange_0  F0 -> F4  hash(a, b)            streaming  1 -> 1\n"
        "  exchange_1  F1 -> F4  any                   persisted  1 -> 1\n"
        "  exchange_2  F2 -> F4  gather(merge: k ASC)  streaming  1 -> 1\n"
        "  exchange_3  F3 -> F4  set values            streaming  1 -> 1\n"
        "levels: [F0, F1, F2, F3] [F4]\n");
}

TEST(FragmentGraph, BuildFromDistributedPlan)
{
    auto header = std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "k")});

    DistributedQueryPlan distributed_plan;

    /// stage_0: ReadNothing -> ShuffleSend(exchange_0)
    {
        auto & stage = distributed_plan.stages["stage_0"];
        stage.id = 0;
        stage.query_plan_fragment.addStep(std::make_unique<ReadNothingStep>(header));
        stage.query_plan_fragment.addStep(std::make_unique<ShuffleSendStep>(header, "exchange_0", Names{"k"}, 4));
        stage.tasks.resize(2);
    }

    /// main: ShuffleReceive(exchange_0) -> Expression -> Sorting(Full)
    {
        auto & stage = distributed_plan.stages["main"];
        stage.id = 1;
        stage.query_plan_fragment.addStep(std::make_unique<ShuffleReceiveStep>(header, "exchange_0", Strings{"0", "1"}));
        stage.query_plan_fragment.addStep(std::make_unique<ExpressionStep>(header, ActionsDAG(header->getColumnsWithTypeAndName())));
        stage.query_plan_fragment.addStep(std::make_unique<SortingStep>(
            header, makeSortDescription("k"), 0, SortingStep::Settings(65536)));
        stage.tasks.resize(1);
    }

    distributed_plan.stage_depends_on["stage_0"] = {};
    distributed_plan.stage_depends_on["main"] = {{"stage_0", "exchange_0"}};

    ExchangeDescription description;
    description.name = "exchange_0";
    description.id = 0;
    description.kind = ExchangeDescription::Kind::Streaming;
    description.source_bucket_count = 2;
    description.destination_bucket_count = 4;
    description.exchange_step_name = "ShuffleExchange";
    description.keys = {"k"};
    distributed_plan.exchange_descriptions[description.name] = description;

    auto graph = buildFragmentGraph(distributed_plan);

    ASSERT_EQ(graph.fragments.size(), 2u);
    const auto & main_fragment = graph.getFragment(1);
    EXPECT_EQ(main_fragment.stage_name, "main");
    EXPECT_EQ(main_fragment.execution_mode, QueryFragment::ExecutionMode::Distributed);
    EXPECT_FALSE(main_fragment.description.has_value());

    const auto & stage_0 = graph.getFragment(0);
    EXPECT_EQ(stage_0.stage_name, "stage_0");
    EXPECT_EQ(stage_0.execution_mode, QueryFragment::ExecutionMode::Distributed);
    EXPECT_FALSE(stage_0.description.has_value());
    EXPECT_EQ(stage_0.tasks, 2u);

    ASSERT_EQ(graph.exchanges.size(), 1u);
    const auto & exchange = graph.getExchange(0);
    EXPECT_EQ(exchange.from, 0u);
    EXPECT_EQ(exchange.to, 1u);
    EXPECT_EQ(exchange.kind, FragmentExchange::Kind::Hash);
    EXPECT_EQ(exchange.keys, (Names{"k"}));
    EXPECT_EQ(exchange.mode, ExchangeDescription::Kind::Streaming);
    EXPECT_EQ(exchange.source_buckets, 2u);
    EXPECT_EQ(exchange.destination_buckets, 4u);
    EXPECT_EQ(exchange.from, 0u);
    EXPECT_EQ(exchange.to, 1u);

    /// A persisted exchange is reported as persisted.
    distributed_plan.exchange_descriptions["exchange_0"].kind = ExchangeDescription::Kind::Persisted;
    auto persisted_graph = buildFragmentGraph(distributed_plan);
    EXPECT_EQ(persisted_graph.getExchange(0).mode, ExchangeDescription::Kind::Persisted);
}

TEST(FragmentGraph, BuildSingleFragment)
{
    auto header = std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "k")});
    QueryPlan plan;
    plan.addStep(std::make_unique<ReadNothingStep>(header));

    auto graph = buildSingleFragmentGraph(plan, "no exchanges");

    ASSERT_EQ(graph.fragments.size(), 1u);
    EXPECT_EQ(graph.fragments[0].plan, &plan);
    EXPECT_EQ(graph.fragments[0].execution_mode, QueryFragment::ExecutionMode::SingleNode);
    EXPECT_EQ(graph.fragments[0].fallback_reason, "no exchanges");
    EXPECT_EQ(graph.fragments[0].tasks, 1u);
    EXPECT_EQ(dumpFragmentGraph(graph),
        "query fragments: 1\n"
        "  F0 main  single node  tasks=1  not distributed: no exchanges\n"
        "exchanges: 0\n"
        "levels: [F0]\n");
}
