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

using Blocking = FragmentDependency::Type;

/// Without an explicit role, fragment 0 is the initiator: the hand-built graphs of the algorithm tests use
/// the sink as fragment 0. Real plans number the initiator last, which the dump tests reproduce.
QueryFragment makeFragment(UInt32 id, size_t tasks = 1, std::optional<QueryFragment::Role> role = std::nullopt)
{
    QueryFragment fragment;
    fragment.id = id;
    fragment.role = role.value_or(id == 0 ? QueryFragment::Role::Initiator : QueryFragment::Role::Worker);
    fragment.stage_name = fragment.role == QueryFragment::Role::Initiator ? "main" : "stage_" + std::to_string(id);
    fragment.tasks = tasks;
    return fragment;
}

/// Adds an exchange `exchange_<producer>` from `producer` to `consumer` and the matching dependency.
void connect(FragmentGraph & graph, UInt32 producer, UInt32 consumer, FragmentDependency::Type type)
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

    for (auto & fragment : graph.fragments)
    {
        if (fragment.id == consumer)
        {
            fragment.depends_on.push_back(FragmentDependency{.producer = producer, .exchange = producer, .type = type, .reason = {}});
            std::sort(fragment.depends_on.begin(), fragment.depends_on.end(),
                [](const auto & l, const auto & r) { return l.producer < r.producer; });
            return;
        }
    }
    FAIL() << "no fragment " << consumer;
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

}

TEST(FragmentGraph, LevelsOfDiamond)
{
    auto graph = makeGraph({0, 1, 2, 3});
    connect(graph, 1, 3, Blocking::Streaming);
    connect(graph, 2, 3, Blocking::Streaming);
    connect(graph, 3, 0, Blocking::Streaming);

    computeLevels(graph);

    EXPECT_EQ(graph.getFragment(0).level, 2u);
    EXPECT_EQ(graph.getFragment(1).level, 0u);
    EXPECT_EQ(graph.getFragment(2).level, 0u);
    EXPECT_EQ(graph.getFragment(3).level, 1u);
    EXPECT_EQ(graph.levels, (std::vector<std::vector<UInt32>>{{1, 2}, {3}, {0}}));
}

TEST(FragmentGraph, LevelsOfChain)
{
    auto graph = makeGraph({0, 1, 2, 3});
    connect(graph, 1, 2, Blocking::Streaming);
    connect(graph, 2, 3, Blocking::Blocking);
    connect(graph, 3, 0, Blocking::Streaming);

    computeLevels(graph);

    EXPECT_EQ(graph.levels, (std::vector<std::vector<UInt32>>{{1}, {2}, {3}, {0}}));
}

TEST(FragmentGraph, LevelsOfUnevenBranches)
{
    /// F0 <- F2 <- F1 and F0 <- F3: the level of F0 is determined by the longest branch.
    auto graph = makeGraph({0, 1, 2, 3});
    connect(graph, 1, 2, Blocking::Streaming);
    connect(graph, 2, 0, Blocking::Streaming);
    connect(graph, 3, 0, Blocking::Streaming);

    computeLevels(graph);

    EXPECT_EQ(graph.levels, (std::vector<std::vector<UInt32>>{{1, 3}, {2}, {0}}));
}

TEST(FragmentGraph, CycleThrows)
{
    auto graph = makeGraph({0, 1, 2});
    connect(graph, 1, 2, Blocking::Streaming);
    connect(graph, 2, 1, Blocking::Streaming);
    graph.fragments[0].depends_on.push_back(FragmentDependency{.producer = 2, .exchange = 2, .type = Blocking::Streaming, .reason = {}});

    EXPECT_THROW(computeLevels(graph), Exception);
}

TEST(FragmentGraph, MissingInitiatorThrows)
{
    auto graph = makeGraph({1});
    EXPECT_THROW(computeLevels(graph), Exception);
}

TEST(FragmentGraph, UnknownProducerThrows)
{
    auto graph = makeGraph({0});
    graph.fragments[0].depends_on.push_back(FragmentDependency{.producer = 7, .exchange = 7, .type = Blocking::Streaming, .reason = {}});
    EXPECT_THROW(computeLevels(graph), Exception);
}

TEST(FragmentGraph, ClassifyDependency)
{
    using Kind = ConsumerStepInfo::Kind;
    using Mode = FragmentExchange::Mode;

    struct Case
    {
        Kind kind;
        String step_name;
        bool build_side;
        SortingStep::Type sorting_type;
        Mode mode;
        FragmentDependency::Type expected;
        String expected_reason;
    };

    const std::vector<Case> cases = {
        {Kind::Other, "Aggregating", false, SortingStep::Type::Full, Mode::Pipelined, Blocking::Streaming, ""},
        {Kind::Other, "Aggregating", false, SortingStep::Type::Full, Mode::Materialised, Blocking::Blocking, "materialised exchange"},
        {Kind::Join, "JoinLogical", true, SortingStep::Type::Full, Mode::Pipelined, Blocking::Blocking, "build side of JoinLogical"},
        {Kind::Join, "Join", true, SortingStep::Type::Full, Mode::Pipelined, Blocking::Blocking, "build side of Join"},
        {Kind::Join, "JoinLogical", false, SortingStep::Type::Full, Mode::Pipelined, Blocking::Streaming, ""},
        {Kind::Join, "JoinLogical", true, SortingStep::Type::Full, Mode::Materialised, Blocking::Blocking, "materialised exchange"},
        {Kind::Join, "JoinLogical", false, SortingStep::Type::Full, Mode::Materialised, Blocking::Blocking, "materialised exchange"},
        {Kind::MergingAggregated, "MergingAggregated", false, SortingStep::Type::Full, Mode::Pipelined, Blocking::Blocking, "merge of aggregation states"},
        {Kind::MergingAggregated, "MergingAggregated", false, SortingStep::Type::Full, Mode::Materialised, Blocking::Blocking, "materialised exchange"},
        {Kind::Sorting, "Sorting", false, SortingStep::Type::Full, Mode::Pipelined, Blocking::Blocking, "full sort"},
        {Kind::Sorting, "Sorting", false, SortingStep::Type::FinishSorting, Mode::Pipelined, Blocking::Streaming, ""},
        {Kind::Sorting, "Sorting", false, SortingStep::Type::PartitionedFinishSorting, Mode::Pipelined, Blocking::Streaming, ""},
        {Kind::Sorting, "Sorting", false, SortingStep::Type::MergingSorted, Mode::Pipelined, Blocking::Streaming, ""},
        {Kind::Sorting, "Sorting", false, SortingStep::Type::Full, Mode::Materialised, Blocking::Blocking, "materialised exchange"},
        {Kind::Sorting, "Sorting", false, SortingStep::Type::MergingSorted, Mode::Materialised, Blocking::Blocking, "materialised exchange"},
    };

    for (size_t i = 0; i < cases.size(); ++i)
    {
        const auto & c = cases[i];
        ConsumerStepInfo consumer;
        consumer.kind = c.kind;
        consumer.step_name = c.step_name;
        consumer.receive_feeds_build_side = c.build_side;
        consumer.sorting_type = c.sorting_type;

        String reason = "stale";
        EXPECT_EQ(classifyDependency(consumer, c.mode, reason), c.expected) << "case " << i;
        EXPECT_EQ(reason, c.expected_reason) << "case " << i;
    }
}

TEST(FragmentGraph, Dump)
{
    /// The numbering of a real plan: worker stages first, the initiator last.
    FragmentGraph graph;
    graph.fragments = {
        makeFragment(0, 1, QueryFragment::Role::Worker),
        makeFragment(1, 8, QueryFragment::Role::Worker),
        makeFragment(2, 1, QueryFragment::Role::Initiator)};

    graph.fragments[1].depends_on.push_back(
        FragmentDependency{.producer = 0, .exchange = 0, .type = Blocking::Blocking, .reason = "build side of JoinLogical"});
    graph.fragments[2].depends_on.push_back(
        FragmentDependency{.producer = 1, .exchange = 1, .type = Blocking::Blocking, .reason = "merge of aggregation states"});

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
    gather.estimated_rows = 5000;

    graph.exchanges = {broadcast, gather};

    computeLevels(graph);

    EXPECT_EQ(dumpFragmentGraph(graph),
        "query fragments: 3\n"
        "  F0 stage_0  worker     tasks=1  level 0\n"
        "  F1 stage_1  worker     tasks=8  level 1  needs F0 via exchange_0 (blocking: build side of JoinLogical)\n"
        "  F2 main     initiator  tasks=1  level 2  needs F1 via exchange_1 (blocking: merge of aggregation states)\n"
        "exchanges: 2\n"
        "  exchange_0  F0 -> F1  broadcast  pipelined  1 -> 8\n"
        "  exchange_1  F1 -> F2  gather     pipelined  8 -> 1  ~5000 rows\n"
        "levels: [F0] [F1] [F2]\n");
}

TEST(FragmentGraph, DumpExchangeKinds)
{
    FragmentGraph graph;
    graph.fragments = {
        makeFragment(0, 1, QueryFragment::Role::Worker),
        makeFragment(1, 1, QueryFragment::Role::Worker),
        makeFragment(2, 1, QueryFragment::Role::Worker),
        makeFragment(3, 1, QueryFragment::Role::Initiator)};
    connect(graph, 0, 3, Blocking::Streaming);
    connect(graph, 1, 3, Blocking::Streaming);
    connect(graph, 2, 3, Blocking::Streaming);
    graph.exchanges[0].kind = FragmentExchange::Kind::Hash;
    graph.exchanges[0].keys = {"a", "b"};
    graph.exchanges[1].kind = FragmentExchange::Kind::Any;
    graph.exchanges[1].mode = FragmentExchange::Mode::Materialised;
    graph.exchanges[2].merge_sort = makeSortDescription("k");

    computeLevels(graph);

    EXPECT_EQ(dumpFragmentGraph(graph),
        "query fragments: 4\n"
        "  F0 stage_0  worker     tasks=1  level 0\n"
        "  F1 stage_1  worker     tasks=1  level 0\n"
        "  F2 stage_2  worker     tasks=1  level 0\n"
        "  F3 main     initiator  tasks=1  level 1  needs F0 via exchange_0 (streaming), F1 via exchange_1 (streaming), F2 via exchange_2 (streaming)\n"
        "exchanges: 3\n"
        "  exchange_0  F0 -> F3  hash(a, b)            pipelined     1 -> 1\n"
        "  exchange_1  F1 -> F3  any                   materialised  1 -> 1\n"
        "  exchange_2  F2 -> F3  gather(merge: k ASC)  pipelined     1 -> 1\n"
        "levels: [F0, F1, F2] [F3]\n");
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
    const auto & initiator = graph.getFragment(1);
    EXPECT_EQ(initiator.role, QueryFragment::Role::Initiator);
    EXPECT_EQ(initiator.summary, (std::vector<String>{"Sorting"}));
    ASSERT_EQ(initiator.depends_on.size(), 1u);
    EXPECT_EQ(initiator.depends_on[0].producer, 0u);
    EXPECT_EQ(initiator.depends_on[0].exchange, 0u);
    EXPECT_EQ(initiator.depends_on[0].type, Blocking::Blocking);
    EXPECT_EQ(initiator.depends_on[0].reason, "full sort");

    const auto & worker = graph.getFragment(0);
    EXPECT_EQ(worker.role, QueryFragment::Role::Worker);
    EXPECT_EQ(worker.tasks, 2u);
    EXPECT_EQ(worker.summary, (std::vector<String>{"ReadNothing"}));

    const auto & exchange = graph.getExchange(0);
    EXPECT_EQ(exchange.kind, FragmentExchange::Kind::Hash);
    EXPECT_EQ(exchange.keys, (Names{"k"}));
    EXPECT_EQ(exchange.mode, FragmentExchange::Mode::Pipelined);
    EXPECT_EQ(exchange.source_buckets, 2u);
    EXPECT_EQ(exchange.destination_buckets, 4u);


    /// A materialised exchange is blocking regardless of the consumer.
    distributed_plan.exchange_descriptions["exchange_0"].kind = ExchangeDescription::Kind::Persisted;
    auto materialised_graph = buildFragmentGraph(distributed_plan);
    EXPECT_EQ(materialised_graph.getFragment(1).depends_on[0].reason, "materialised exchange");

    /// A dependency without a matching receive step is an inconsistent plan.
    distributed_plan.stage_depends_on["main"] = {{"stage_0", "exchange_7"}};
    distributed_plan.exchange_descriptions["exchange_7"] = description;
    distributed_plan.exchange_descriptions["exchange_7"].name = "exchange_7";
    distributed_plan.exchange_descriptions["exchange_7"].id = 7;
    EXPECT_THROW(buildFragmentGraph(distributed_plan), Exception);
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
    EXPECT_EQ(dumpFragmentGraph(graph),
        "fallback: no exchanges\n"
        "query fragments: 1\n"
        "  F0 main  initiator  tasks=1  level 0\n"
        "exchanges: 0\n"
        "levels: [F0]\n");
}
