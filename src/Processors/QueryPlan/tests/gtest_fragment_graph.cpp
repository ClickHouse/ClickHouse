#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Fragments/FragmentGraph.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/ShuffleReceiveStep.h>
#include <Processors/QueryPlan/ShuffleSendStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Common/Exception.h>

using namespace DB;

namespace
{

using Blocking = FragmentExchange::Type;

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

/// Adds an exchange `exchange_<producer>` from `producer` to `consumer` and makes it a child of `consumer`.
void connect(FragmentGraph & graph, UInt32 producer, UInt32 consumer, FragmentExchange::Type type)
{
    FragmentExchange exchange;
    exchange.id = producer;
    exchange.name = "exchange_" + std::to_string(producer);
    exchange.from = producer;
    exchange.to = consumer;
    exchange.kind = FragmentExchange::Kind::Gather;
    exchange.source_buckets = 1;
    exchange.destination_buckets = 1;
    exchange.type = type;
    graph.exchanges.push_back(exchange);
    std::sort(graph.exchanges.begin(), graph.exchanges.end(), [](const auto & l, const auto & r) { return l.id < r.id; });

    for (auto & fragment : graph.fragments)
    {
        if (fragment.id == consumer)
        {
            fragment.children.push_back(producer);
            std::sort(fragment.children.begin(), fragment.children.end());
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
    connect(graph, 1, 3, Blocking::Streaming);
    connect(graph, 2, 3, Blocking::Streaming);
    connect(graph, 3, 0, Blocking::Streaming);

    EXPECT_EQ(computeLevels(graph), (std::vector<std::vector<UInt32>>{{1, 2}, {3}, {0}}));
}

TEST(FragmentGraph, LevelsOfChain)
{
    auto graph = makeGraph({0, 1, 2, 3});
    connect(graph, 1, 2, Blocking::Streaming);
    connect(graph, 2, 3, Blocking::Blocking);
    connect(graph, 3, 0, Blocking::Streaming);

    EXPECT_EQ(computeLevels(graph), (std::vector<std::vector<UInt32>>{{1}, {2}, {3}, {0}}));
}

TEST(FragmentGraph, LevelsOfUnevenBranches)
{
    /// F0 <- F2 <- F1 and F0 <- F3: the level of F0 is determined by the longest branch.
    auto graph = makeGraph({0, 1, 2, 3});
    connect(graph, 1, 2, Blocking::Streaming);
    connect(graph, 2, 0, Blocking::Streaming);
    connect(graph, 3, 0, Blocking::Streaming);

    EXPECT_EQ(computeLevels(graph), (std::vector<std::vector<UInt32>>{{1, 3}, {2}, {0}}));
}

/// `LOGICAL_ERROR` aborts instead of throwing in debug and sanitizer builds.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(FragmentGraph, CycleThrows)
{
    auto graph = makeGraph({0, 1, 2});
    connect(graph, 1, 2, Blocking::Streaming);
    connect(graph, 2, 1, Blocking::Streaming);
    expectInvalid(graph, "The fragment graph has a cycle");
}

TEST(FragmentGraph, TwoRootsThrows)
{
    /// F1 feeds F0, and F2 is consumed by nobody: two roots.
    auto graph = makeGraph({0, 1, 2});
    connect(graph, 1, 0, Blocking::Streaming);
    expectInvalid(graph, "The fragment graph has 2 root fragments, expected one");
}

TEST(FragmentGraph, WorkerRootThrows)
{
    FragmentGraph graph;
    graph.fragments = {makeFragment(0, 1, QueryFragment::Role::Worker), makeFragment(1, 1, QueryFragment::Role::Worker)};
    connect(graph, 0, 1, Blocking::Streaming);
    expectInvalid(graph, "The root fragment F1 must have the initiator role");
}
#endif

TEST(FragmentGraph, LevelsWithSetFragment)
{
    /// F0 (set source) -> F1 (fills the set on the initiator) -> F2 (reads the set) -> F3 (main).
    FragmentGraph graph;
    graph.fragments = {
        makeFragment(0, 8, QueryFragment::Role::Worker),
        makeFragment(1, 1, QueryFragment::Role::Initiator),
        makeFragment(2, 8, QueryFragment::Role::Worker),
        makeFragment(3, 1, QueryFragment::Role::Initiator)};
    graph.fragments[1].location = QueryFragment::ExecutionLocation::SingleNode;
    graph.fragments[1].description = "k IN (SELECT k FROM small)";
    connect(graph, 0, 1, Blocking::Streaming);
    connect(graph, 1, 2, Blocking::Blocking);
    connect(graph, 2, 3, Blocking::Streaming);
    graph.exchanges[1].kind = FragmentExchange::Kind::SetValues;
    graph.exchanges[1].reason = "set must be complete";

    EXPECT_EQ(computeLevels(graph), (std::vector<std::vector<UInt32>>{{0}, {1}, {2}, {3}}));

    EXPECT_EQ(dumpFragmentGraph(graph),
        "query fragments: 4\n"
        "  F0 stage_0  worker     distributed  tasks=8\n"
        "  F1 main     initiator  single node  tasks=1  needs F0 via exchange_0 (streaming)  description: k IN (SELECT k FROM small)\n"
        "  F2 stage_2  worker     distributed  tasks=8  needs F1 via exchange_1 (blocking: set must be complete)\n"
        "  F3 main     initiator  distributed  tasks=1  needs F2 via exchange_2 (streaming)\n"
        "exchanges: 3\n"
        "  exchange_0  F0 -> F1  gather      pipelined  1 -> 1\n"
        "  exchange_1  F1 -> F2  set values  pipelined  1 -> 1\n"
        "  exchange_2  F2 -> F3  gather      pipelined  1 -> 1\n"
        "levels: [F0] [F1] [F2] [F3]\n");
}

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(FragmentGraph, InconsistentExchangesThrow)
{
    {
        /// The exchange comes from a fragment that is not in the graph.
        auto graph = makeGraph({0, 7});
        connect(graph, 7, 0, Blocking::Streaming);
        graph.fragments.pop_back();
        expectInvalid(graph, "Fragment 0 reads exchange exchange_7 from unknown fragment 7");
    }
    {
        /// A child that is not an exchange of the graph.
        auto graph = makeGraph({0});
        graph.fragments[0].children = {7};
        expectInvalid(graph, "Fragment 0 reads unknown exchange 7");
    }
    {
        /// A child that goes to another fragment.
        auto graph = makeGraph({0, 1, 2});
        connect(graph, 1, 0, Blocking::Streaming);
        connect(graph, 2, 1, Blocking::Streaming);
        graph.fragments[0].children = {1, 2};
        graph.fragments[1].children = {};
        expectInvalid(graph, "Fragment 0 reads exchange exchange_2, which goes to fragment 1");
    }
    {
        /// An exchange that no fragment reads.
        auto graph = makeGraph({0, 1});
        connect(graph, 1, 0, Blocking::Streaming);
        graph.fragments[0].children = {};
        expectInvalid(graph, "The fragment graph has 1 exchanges, but its fragments read 0");
    }
    {
        /// An exchange read twice.
        auto graph = makeGraph({0, 1});
        connect(graph, 1, 0, Blocking::Streaming);
        graph.fragments[0].children = {1, 1};
        expectInvalid(graph, "Exchange exchange_1 is read more than once");
    }
}
#endif

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
        FragmentExchange::Type expected;
        String expected_reason;
    };

    const std::vector<Case> cases = {
        {Kind::Other, "Union", false, SortingStep::Type::Full, Mode::Pipelined, Blocking::Streaming, ""},
        {Kind::Other, "Union", false, SortingStep::Type::Full, Mode::Materialised, Blocking::Blocking, "materialised exchange"},
        {Kind::Aggregating, "Aggregating", false, SortingStep::Type::Full, Mode::Pipelined, Blocking::Blocking, "hash aggregation"},
        {Kind::Aggregating, "Aggregating", false, SortingStep::Type::Full, Mode::Materialised, Blocking::Blocking, "materialised exchange"},
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
        EXPECT_EQ(classifyDependency(consumer, FragmentExchange::Kind::Gather, c.mode, reason), c.expected) << "case " << i;
        EXPECT_EQ(reason, c.expected_reason) << "case " << i;
    }

    /// A `MergingAggregated` that merges bucket by bucket streams, unless the exchange is materialised.
    for (Mode mode : {Mode::Pipelined, Mode::Materialised})
    {
        ConsumerStepInfo consumer;
        consumer.kind = Kind::MergingAggregated;
        consumer.step_name = "MergingAggregated";
        consumer.merges_bucket_by_bucket = true;

        String reason = "stale";
        const bool materialised = mode == Mode::Materialised;
        EXPECT_EQ(classifyDependency(consumer, FragmentExchange::Kind::Gather, mode, reason),
            materialised ? Blocking::Blocking : Blocking::Streaming);
        EXPECT_EQ(reason, materialised ? "materialised exchange" : "");
    }

    /// An aggregation in order streams, unless the exchange is materialised.
    for (Mode mode : {Mode::Pipelined, Mode::Materialised})
    {
        ConsumerStepInfo consumer;
        consumer.kind = Kind::Aggregating;
        consumer.step_name = "Aggregating";
        consumer.aggregation_in_order = true;

        String reason = "stale";
        const bool materialised = mode == Mode::Materialised;
        EXPECT_EQ(classifyDependency(consumer, FragmentExchange::Kind::Gather, mode, reason),
            materialised ? Blocking::Blocking : Blocking::Streaming);
        EXPECT_EQ(reason, materialised ? "materialised exchange" : "");
    }

    /// A `SetValues` exchange is blocking regardless of the mode and of a streaming consumer.
    for (Mode mode : {Mode::Pipelined, Mode::Materialised})
    {
        ConsumerStepInfo consumer;
        consumer.kind = Kind::Other;
        consumer.step_name = "Filter";

        String reason = "stale";
        EXPECT_EQ(classifyDependency(consumer, FragmentExchange::Kind::SetValues, mode, reason), Blocking::Blocking);
        EXPECT_EQ(reason, "set must be complete");
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

    graph.fragments[1].children = {0};
    graph.fragments[2].children = {1};

    FragmentExchange broadcast;
    broadcast.id = 0;
    broadcast.name = "exchange_0";
    broadcast.from = 0;
    broadcast.to = 1;
    broadcast.kind = FragmentExchange::Kind::Broadcast;
    broadcast.source_buckets = 1;
    broadcast.destination_buckets = 8;
    broadcast.type = Blocking::Blocking;
    broadcast.reason = "build side of JoinLogical";

    FragmentExchange gather;
    gather.id = 1;
    gather.name = "exchange_1";
    gather.from = 1;
    gather.to = 2;
    gather.kind = FragmentExchange::Kind::Gather;
    gather.source_buckets = 8;
    gather.destination_buckets = 1;
    gather.type = Blocking::Blocking;
    gather.reason = "merge of aggregation states";
    gather.statistics.estimated_rows = 5000;

    graph.exchanges = {broadcast, gather};

    EXPECT_EQ(dumpFragmentGraph(graph),
        "query fragments: 3\n"
        "  F0 stage_0  worker     distributed  tasks=1\n"
        "  F1 stage_1  worker     distributed  tasks=8  needs F0 via exchange_0 (blocking: build side of JoinLogical)\n"
        "  F2 main     initiator  distributed  tasks=1  needs F1 via exchange_1 (blocking: merge of aggregation states)\n"
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
        makeFragment(3, 1, QueryFragment::Role::Worker),
        makeFragment(4, 1, QueryFragment::Role::Initiator)};
    connect(graph, 0, 4, Blocking::Streaming);
    connect(graph, 1, 4, Blocking::Streaming);
    connect(graph, 2, 4, Blocking::Streaming);
    connect(graph, 3, 4, Blocking::Blocking);
    graph.exchanges[0].kind = FragmentExchange::Kind::Hash;
    graph.exchanges[0].keys = {"a", "b"};
    graph.exchanges[1].kind = FragmentExchange::Kind::Any;
    graph.exchanges[1].mode = FragmentExchange::Mode::Materialised;
    graph.exchanges[2].merge_sort = makeSortDescription("k");
    graph.exchanges[3].kind = FragmentExchange::Kind::SetValues;
    graph.exchanges[3].reason = "set must be complete";

    EXPECT_EQ(dumpFragmentGraph(graph),
        "query fragments: 5\n"
        "  F0 stage_0  worker     distributed  tasks=1\n"
        "  F1 stage_1  worker     distributed  tasks=1\n"
        "  F2 stage_2  worker     distributed  tasks=1\n"
        "  F3 stage_3  worker     distributed  tasks=1\n"
        "  F4 main     initiator  distributed  tasks=1  needs F0 via exchange_0 (streaming), F1 via exchange_1 (streaming), "
        "F2 via exchange_2 (streaming), F3 via exchange_3 (blocking: set must be complete)\n"
        "exchanges: 4\n"
        "  exchange_0  F0 -> F4  hash(a, b)            pipelined     1 -> 1\n"
        "  exchange_1  F1 -> F4  any                   materialised  1 -> 1\n"
        "  exchange_2  F2 -> F4  gather(merge: k ASC)  pipelined     1 -> 1\n"
        "  exchange_3  F3 -> F4  set values            pipelined     1 -> 1\n"
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
    const auto & initiator = graph.getFragment(1);
    EXPECT_EQ(initiator.role, QueryFragment::Role::Initiator);
    EXPECT_EQ(initiator.location, QueryFragment::ExecutionLocation::Distributed);
    EXPECT_FALSE(initiator.description.has_value());
    EXPECT_EQ(initiator.summary, (std::vector<String>{"Sorting"}));
    EXPECT_EQ(initiator.children, (std::vector<UInt32>{0}));

    const auto & worker = graph.getFragment(0);
    EXPECT_EQ(worker.role, QueryFragment::Role::Worker);
    EXPECT_EQ(worker.location, QueryFragment::ExecutionLocation::Distributed);
    EXPECT_FALSE(worker.description.has_value());
    EXPECT_TRUE(worker.children.empty());
    EXPECT_EQ(worker.tasks, 2u);
    EXPECT_EQ(worker.summary, (std::vector<String>{"ReadNothing"}));

    const auto & exchange = graph.getExchange(0);
    EXPECT_EQ(exchange.kind, FragmentExchange::Kind::Hash);
    EXPECT_EQ(exchange.keys, (Names{"k"}));
    EXPECT_EQ(exchange.mode, FragmentExchange::Mode::Pipelined);
    EXPECT_EQ(exchange.source_buckets, 2u);
    EXPECT_EQ(exchange.destination_buckets, 4u);
    EXPECT_EQ(exchange.from, 0u);
    EXPECT_EQ(exchange.to, 1u);
    EXPECT_EQ(exchange.type, Blocking::Blocking);
    EXPECT_EQ(exchange.reason, "full sort");

    /// A materialised exchange is blocking regardless of the consumer.
    distributed_plan.exchange_descriptions["exchange_0"].kind = ExchangeDescription::Kind::Persisted;
    auto materialised_graph = buildFragmentGraph(distributed_plan);
    EXPECT_EQ(materialised_graph.getExchange(0).reason, "materialised exchange");

    /// A dependency without a matching receive step is an inconsistent plan.
    distributed_plan.stage_depends_on["main"] = {{"stage_0", "exchange_7"}};
    distributed_plan.exchange_descriptions["exchange_7"] = description;
    distributed_plan.exchange_descriptions["exchange_7"].name = "exchange_7";
    distributed_plan.exchange_descriptions["exchange_7"].id = 7;
#ifndef DEBUG_OR_SANITIZER_BUILD
    EXPECT_THROW(buildFragmentGraph(distributed_plan), Exception);
#endif
}

namespace
{

/// A worker stage `stage_0` shuffling into `main`, whose plan `build_main` makes from the receive step of the exchange.
DistributedQueryPlan makeTwoStagePlan(const SharedHeader & header, const std::function<void(QueryPlan &)> & build_main)
{
    DistributedQueryPlan distributed_plan;

    auto & worker = distributed_plan.stages["stage_0"];
    worker.id = 0;
    worker.query_plan_fragment.addStep(std::make_unique<ReadNothingStep>(header));
    worker.query_plan_fragment.addStep(std::make_unique<ShuffleSendStep>(header, "exchange_0", Names{"k"}, 2));
    worker.tasks.resize(2);

    auto & main = distributed_plan.stages["main"];
    main.id = 1;
    main.query_plan_fragment.addStep(std::make_unique<ShuffleReceiveStep>(header, "exchange_0", Strings{"0", "1"}));
    build_main(main.query_plan_fragment);
    main.tasks.resize(1);

    distributed_plan.stage_depends_on["stage_0"] = {};
    distributed_plan.stage_depends_on["main"] = {{"stage_0", "exchange_0"}};

    ExchangeDescription description;
    description.name = "exchange_0";
    description.id = 0;
    description.kind = ExchangeDescription::Kind::Streaming;
    description.source_bucket_count = 2;
    description.destination_bucket_count = 2;
    description.exchange_step_name = "ShuffleExchange";
    description.keys = {"k"};
    distributed_plan.exchange_descriptions[description.name] = description;

    return distributed_plan;
}

std::unique_ptr<AggregatingStep> makeAggregatingStep(const SharedHeader & header, SortDescription sort_description_for_merging)
{
    Aggregator::Params params(
        Names{"k"},
        AggregateDescriptions{},
        /*overflow_row=*/false,
        /*max_threads=*/1,
        /*max_block_size=*/65536,
        /*min_hit_rate_to_use_consecutive_keys_optimization=*/0.5f,
        /*serialize_string_with_zero_byte=*/false,
        /*enable_packed_string_keys=*/true);
    return std::make_unique<AggregatingStep>(
        header,
        std::move(params),
        GroupingSetsParamsList{},
        /*final=*/true,
        /*max_block_size=*/65536,
        /*aggregation_in_order_max_block_bytes=*/0,
        /*merge_threads=*/1,
        /*temporary_data_merge_threads=*/1,
        /*storage_has_evenly_distributed_read=*/false,
        /*group_by_use_nulls=*/false,
        std::move(sort_description_for_merging),
        /*group_by_sort_description=*/SortDescription{},
        /*should_produce_results_in_order_of_bucket_number=*/false,
        /*memory_bound_merging_of_aggregation_results_enabled=*/false,
        /*explicit_sorting_required_for_aggregation_in_order=*/false);
}

}

/// The consumer of a receive step is the first step on the way to the output of the fragment which needs all of
/// its input, not the first step above the receive step.
TEST(FragmentGraph, BuildFindsBlockingConsumer)
{
    auto header = std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "k")});

    auto dependency_of = [&](const std::function<void(QueryPlan &)> & build_main)
    {
        auto distributed_plan = makeTwoStagePlan(header, build_main);
        auto graph = buildFragmentGraph(distributed_plan);
        EXPECT_EQ(graph.getFragment(1).children, (std::vector<UInt32>{0}));
        const auto & exchange = graph.getExchange(0);
        return std::make_pair(exchange.type, exchange.reason);
    };

    auto union_with_another_input = [&](QueryPlan & plan)
    {
        std::vector<std::unique_ptr<QueryPlan>> inputs;
        inputs.push_back(std::make_unique<QueryPlan>(std::move(plan)));
        inputs.push_back(std::make_unique<QueryPlan>());
        inputs.back()->addStep(std::make_unique<ReadNothingStep>(header));
        plan = QueryPlan();
        plan.unitePlans(std::make_unique<UnionStep>(SharedHeaders{header, header}), std::move(inputs));
    };

    /// A hash aggregation emits rows only after all its input.
    EXPECT_EQ(dependency_of([&](QueryPlan & plan) { plan.addStep(makeAggregatingStep(header, {})); }),
        std::make_pair(Blocking::Blocking, String("hash aggregation")));

    /// An aggregation in order emits a key once it is complete.
    EXPECT_EQ(dependency_of([&](QueryPlan & plan) { plan.addStep(makeAggregatingStep(header, makeSortDescription("k"))); }),
        std::make_pair(Blocking::Streaming, String()));

    /// A `Union` streams, the full sort above it does not.
    EXPECT_EQ(dependency_of([&](QueryPlan & plan)
        {
            union_with_another_input(plan);
            plan.addStep(std::make_unique<SortingStep>(header, makeSortDescription("k"), 0, SortingStep::Settings(65536)));
        }),
        std::make_pair(Blocking::Blocking, String("full sort")));

    /// Nothing up to the output of the fragment needs the whole input.
    EXPECT_EQ(dependency_of([&](QueryPlan & plan)
        {
            union_with_another_input(plan);
            plan.addStep(std::make_unique<ExpressionStep>(header, ActionsDAG(header->getColumnsWithTypeAndName())));
        }),
        std::make_pair(Blocking::Streaming, String()));
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
    EXPECT_EQ(graph.fragments[0].location, QueryFragment::ExecutionLocation::SingleNode);
    EXPECT_EQ(graph.fragments[0].fallback_reason, "no exchanges");
    EXPECT_EQ(graph.fragments[0].tasks, 1u);
    EXPECT_EQ(dumpFragmentGraph(graph),
        "query fragments: 1\n"
        "  F0 main  initiator  single node  tasks=1  not distributed: no exchanges\n"
        "exchanges: 0\n"
        "levels: [F0]\n");
}
