#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Fragments/FragmentGraph.h>
#include <Processors/QueryPlan/GatherReceiveStep.h>
#include <Processors/QueryPlan/GatherSendStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergeRuntimeFiltersStep.h>
#include <Processors/QueryPlan/Optimizations/RuntimeFilterExchangeWiring.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>
#include <Common/CurrentThread.h>
#include <Common/QueryScope.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Common/typeid_cast.h>

#include <utility>

#include <gtest/gtest.h>

#include <algorithm>
#include <optional>
#include <set>
#include <vector>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

namespace
{

RuntimeFilterBuildOptions testBuildOptions()
{
    return RuntimeFilterBuildOptions{
        .geometry = RuntimeFilterGeometry{
            .exact_values_limit = 64,
            .exact_bytes_limit = 4096,
            .bloom_filter_bytes = 4096,
            .bloom_filter_hash_functions = 3,
            .pass_ratio_threshold_for_disabling = 1.0,
            .blocks_to_skip_before_reenabling = 0,
            .max_ratio_of_set_bits_in_bloom_filter = 1.0,
        },
        .polarity = RuntimeFilterPolarity::Contains,
        .track_key_range = false,
        .distinct_keys_hint = std::nullopt,
        .distinct_keys_hint_matches_filter_key = false,
    };
}

SharedHeader dataHeader()
{
    return std::make_shared<const Block>(Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x")});
}

void expectGeometryEq(const RuntimeFilterGeometry & left, const RuntimeFilterGeometry & right)
{
    EXPECT_EQ(left.exact_values_limit, right.exact_values_limit);
    EXPECT_EQ(left.exact_bytes_limit, right.exact_bytes_limit);
    EXPECT_EQ(left.bloom_filter_bytes, right.bloom_filter_bytes);
    EXPECT_EQ(left.bloom_filter_hash_functions, right.bloom_filter_hash_functions);
    EXPECT_DOUBLE_EQ(left.pass_ratio_threshold_for_disabling, right.pass_ratio_threshold_for_disabling);
    EXPECT_EQ(left.blocks_to_skip_before_reenabling, right.blocks_to_skip_before_reenabling);
    EXPECT_DOUBLE_EQ(left.max_ratio_of_set_bits_in_bloom_filter, right.max_ratio_of_set_bits_in_bloom_filter);
}

BuildRuntimeFilterStep * findBuildStep(QueryPlan & fragment)
{
    std::vector<QueryPlan::Node *> stack{fragment.getRootNode()};
    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();
        if (!node)
            continue;
        if (auto * build = typeid_cast<BuildRuntimeFilterStep *>(node->step.get()))
            return build;
        for (auto * child : node->children)
            stack.push_back(child);
    }
    return nullptr;
}

ActionsDAG makeApplyFilterDAG(const String & filter_key, const String & filter_name)
{
    tryRegisterFunctions();

    ActionsDAG dag(dataHeader()->getColumnsWithTypeAndName());
    const auto & key_input = dag.findInOutputs("x");

    auto string_type = std::make_shared<DataTypeString>();
    auto id_column = string_type->createColumnConst(0, filter_key);
    const auto & label = dag.addColumn(
        std::move(id_column),
        string_type,
        filter_name,
        /*is_deterministic_constant=*/false,
        /*is_masked_secret=*/false,
        /*is_runtime_filter_id=*/true);

    auto apply_filter = FunctionFactory::instance().get("__applyFilter", /*context*/ nullptr);
    const auto & application = dag.addFunction(apply_filter, {&label, &key_input}, {});

    auto & outputs = dag.getOutputs();
    outputs.clear();
    outputs.push_back(&key_input);
    outputs.push_back(&application);
    return dag;
}

String applyFilterResultName(const ActionsDAG & dag)
{
    for (const auto * node : dag.getOutputs())
    {
        if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base && node->function_base->getName() == "__applyFilter")
            return node->result_name;
    }
    return {};
}

DistributedQueryTask makeTask(const String & stage_name, size_t bucket)
{
    DistributedQueryTask task;
    task.task_id = stage_name + "_" + std::to_string(bucket);
    task.parameters.parameters["bucket_id"] = Field(std::to_string(bucket));
    return task;
}

void addBuildStage(DistributedQueryPlan & plan, const String & name, size_t num_tasks, const String & filter_key)
{
    DistributedQueryStage stage;
    QueryPlan fragment;
    fragment.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<NullSource>(dataHeader()))));
    fragment.addStep(
        std::make_unique<BuildRuntimeFilterStep>(
            dataHeader(), "x", std::make_shared<DataTypeUInt64>(), "f", filter_key, testBuildOptions()));
    stage.query_plan_fragment = std::move(fragment);
    for (size_t task = 0; task < num_tasks; ++task)
        stage.tasks.push_back(makeTask(name, task));
    plan.stages[name] = std::move(stage);
}

void addConsumerStage(DistributedQueryPlan & plan, const String & name, size_t num_tasks, const String & filter_key)
{
    DistributedQueryStage stage;
    QueryPlan fragment;
    fragment.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<NullSource>(dataHeader()))));
    auto dag = makeApplyFilterDAG(filter_key, "f");
    const String filter_column_name = applyFilterResultName(dag);
    fragment.addStep(std::make_unique<FilterStep>(dataHeader(), std::move(dag), filter_column_name, /*remove_filter_column_=*/true));
    stage.query_plan_fragment = std::move(fragment);
    for (size_t task = 0; task < num_tasks; ++task)
        stage.tasks.push_back(makeTask(name, task));
    plan.stages[name] = std::move(stage);
}

/// A Limit gives the site a row estimate; without it the site has none.
QueryPlanPtr makeApplyPlan(const String & filter_key, std::optional<size_t> limit)
{
    auto plan = std::make_unique<QueryPlan>();
    plan->addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<NullSource>(dataHeader()))));
    if (limit)
        plan->addStep(std::make_unique<LimitStep>(dataHeader(), *limit, /*offset=*/0));
    auto dag = makeApplyFilterDAG(filter_key, "f");
    const String filter_column_name = applyFilterResultName(dag);
    plan->addStep(std::make_unique<FilterStep>(dataHeader(), std::move(dag), filter_column_name, /*remove_filter_column_=*/true));
    return plan;
}

/// Two `__applyFilter` sites in one fragment, under a UNION with children
/// `[first_arm_limit, second_arm_limit]`. The wiring DFS visits the last child first.
void addTwoSiteConsumerStage(
    DistributedQueryPlan & plan,
    const String & name,
    const String & filter_key,
    std::optional<size_t> first_arm_limit,
    std::optional<size_t> second_arm_limit)
{
    SharedHeaders headers;
    headers.emplace_back(dataHeader());
    headers.emplace_back(dataHeader());
    std::vector<QueryPlanPtr> union_plans;
    union_plans.push_back(makeApplyPlan(filter_key, first_arm_limit));
    union_plans.push_back(makeApplyPlan(filter_key, second_arm_limit));

    DistributedQueryStage stage;
    QueryPlan fragment;
    fragment.unitePlans(std::make_unique<UnionStep>(std::move(headers)), std::move(union_plans));
    stage.query_plan_fragment = std::move(fragment);
    stage.tasks.push_back(makeTask(name, 0));
    plan.stages[name] = std::move(stage);
}

/// Total exchange streams in the plan. Also asserts that the outputs and inputs pair up exactly:
/// every stream written by some task is read by exactly one task and vice versa.
size_t countStreams(const DistributedQueryPlan & plan)
{
    std::multiset<String> outputs;
    std::multiset<String> inputs;
    for (const auto & [_, stage] : plan.stages)
    {
        for (const auto & task : stage.tasks)
        {
            for (const auto & stream : task.output_exchange_streams)
                outputs.insert(stream.toString());
            for (const auto & stream : task.input_exchange_streams)
                inputs.insert(stream.toString());
        }
    }
    EXPECT_EQ(outputs, inputs);
    EXPECT_EQ(outputs.size(), std::set<String>(outputs.begin(), outputs.end()).size()) << "duplicate stream ids";
    return outputs.size();
}

Strings mergeStageNames(const DistributedQueryPlan & plan)
{
    Strings names;
    for (const auto & [name, _] : plan.stages)
        if (name.starts_with("rf_merge_"))
            names.push_back(name);
    std::sort(names.begin(), names.end());
    return names;
}

bool dependsOn(const StageDependencies & dependencies, const String & stage)
{
    return std::ranges::any_of(dependencies, [&](const StageDependency & dependency) { return dependency.stage == stage; });
}

void expectConsumerDescriptors(const DistributedQueryPlan & plan, const String & stage_name, const BuildRuntimeFilterStep & build)
{
    for (const auto & task : plan.stages.at(stage_name).tasks)
    {
        ASSERT_EQ(task.runtime_filter_descriptors.size(), 1u);
        const auto & desc = task.runtime_filter_descriptors.front();
        EXPECT_EQ(desc.filter_key, build.getFilterKey());
        EXPECT_EQ(desc.filter_name, build.getFilterName());
        ASSERT_TRUE(desc.key_column_type);
        EXPECT_TRUE(desc.key_column_type->equals(*build.getFilterColumnType()));
        expectGeometryEq(desc.geometry, build.getGeometry());
        desc.geometry.validateTransported();

        ASSERT_EQ(task.input_exchange_streams.size(), 1u);
        EXPECT_EQ(task.input_exchange_streams.front().toString(), desc.stream.toString());
    }
}

void expectLocalBuild(DistributedQueryPlan & plan, const String & stage_name)
{
    auto * build = findBuildStep(plan.stages.at(stage_name).query_plan_fragment);
    ASSERT_NE(build, nullptr);
    EXPECT_FALSE(build->hasFilterExchanges());
    for (const auto & task : plan.stages.at(stage_name).tasks)
        EXPECT_TRUE(task.runtime_filter_descriptors.empty());
}

void expectWiredBuild(DistributedQueryPlan & plan, const String & stage_name)
{
    auto * build = findBuildStep(plan.stages.at(stage_name).query_plan_fragment);
    ASSERT_NE(build, nullptr);
    EXPECT_TRUE(build->hasFilterExchanges());
    /// Every transported filter is merged and broadcast by its own filter-only merge stage.
    EXPECT_FALSE(mergeStageNames(plan).empty());
    for (const auto & name : mergeStageNames(plan))
        EXPECT_TRUE(plan.stages.at(name).filter_only) << name;
}

/// Builds the symmetric case: one build stage of `num_build_tasks` and one receive stage of
/// `num_receive_tasks`, wires it, and returns the plan.
DistributedQueryPlan wireSymmetric(size_t num_build_tasks, size_t num_receive_tasks)
{
    DistributedQueryPlan plan;
    addBuildStage(plan, "build", num_build_tasks, "key");
    addConsumerStage(plan, "probe", num_receive_tasks, "key");
    size_t next_exchange_id = 100;
    wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Streaming);
    return plan;
}

}

/// Frees the `current_thread` slot for the duration of a fixture: another suite in the binary can
/// leave it set for the process lifetime (e.g. via `MainThreadStatus`), and constructing a
/// `ThreadStatus` over an occupied slot asserts. Declared before `thread_status`, so the slot is
/// cleared before it is constructed and the previous value is restored after it is destroyed.
struct CurrentThreadSlot
{
    /// Read the slot before clearing it: `current_thread` is a stateless accessor, so
    /// `std::exchange` would return the value it had just stored, not the previous one.
    ThreadStatus * previous = current_thread;
    CurrentThreadSlot() { current_thread = nullptr; }
    ~CurrentThreadSlot() { current_thread = previous; }
};

/// A `FilterStep` computes its output header by running its DAG on an empty block. With a non-empty
/// label, `__applyFilter` needs a query context to look the filter up in, and a lookup miss passes
/// all rows. So building a consumer fragment or calling `wireRuntimeFilterExchangeTopology` needs
/// the test thread attached to a query context, which production always has.
class RuntimeFilterExchangeWiring : public ::testing::Test
{
protected:
    RuntimeFilterExchangeWiring()
        : query_context(Context::createCopy(getContext().context))
    {
        query_context->makeQueryContext();
        chassert(&CurrentThread::get() == &thread_status);
        query_scope = QueryScope::create(query_context);
    }

    CurrentThreadSlot current_thread_slot;
    ThreadStatus thread_status;
    ContextMutablePtr query_context;
    QueryScope query_scope;
};

TEST_F(RuntimeFilterExchangeWiring, MultiLevelTree)
{
    /// With fan-in 16, 8 build tasks need only the root; 40 need ceil(40 / 16) = 3 tasks, then the
    /// root. Each build task writes one stream and each receive task reads one broadcast stream.
    static_assert(RUNTIME_FILTER_MERGE_FAN_IN == 16);
    struct Case
    {
        size_t build_tasks;
        size_t streams;
        std::vector<size_t> level_sizes;
    };
    for (const auto & test_case : {Case{8, 8 + 4, {1}}, Case{40, 40 + 3 + 4, {3, 1}}})
    {
        SCOPED_TRACE(test_case.build_tasks);
        auto plan = wireSymmetric(test_case.build_tasks, 4);

        EXPECT_EQ(countStreams(plan), test_case.streams);
        for (const auto & [_, exchange] : plan.exchange_descriptions)
            EXPECT_EQ(exchange.kind, ExchangeDescription::Kind::Streaming);

        /// Stage names carry increasing exchange ids, so sorted order is level order.
        auto merge_stages = mergeStageNames(plan);
        std::vector<size_t> level_sizes;
        for (const auto & name : merge_stages)
            level_sizes.push_back(plan.stages.at(name).tasks.size());
        EXPECT_EQ(level_sizes, test_case.level_sizes);

        /// Every merge task consumes at most fan-in inputs.
        for (const auto & name : merge_stages)
            for (const auto & task : plan.stages.at(name).tasks)
                EXPECT_LE(task.input_exchange_streams.size(), RUNTIME_FILTER_MERGE_FAN_IN);

        /// The scheduler chain: build -> each merge level -> probe.
        String previous = "build";
        for (const auto & name : merge_stages)
        {
            EXPECT_TRUE(dependsOn(plan.stage_depends_on.at(name), previous)) << name;
            previous = name;
        }
        EXPECT_TRUE(dependsOn(plan.stage_depends_on.at("probe"), previous));

        expectWiredBuild(plan, "build");
        auto * build = findBuildStep(plan.stages.at("build").query_plan_fragment);
        ASSERT_NE(build, nullptr);
        expectConsumerDescriptors(plan, "probe", *build);
    }
}

TEST_F(RuntimeFilterExchangeWiring, MultipleReceiveStages)
{
    DistributedQueryPlan plan;
    addBuildStage(plan, "build", 8, "key");
    addConsumerStage(plan, "probe_a", 4, "key");
    addConsumerStage(plan, "probe_b", 2, "key");
    size_t next_exchange_id = 100;
    wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Streaming);

    /// 8 streams into the root, then one broadcast stream per destination task of each stage.
    EXPECT_EQ(countStreams(plan), 8u + 4u + 2u);

    auto merge_stages = mergeStageNames(plan);
    ASSERT_EQ(merge_stages.size(), 1u);
    const auto & root_task = plan.stages.at(merge_stages.front()).tasks.front();
    EXPECT_EQ(root_task.output_exchange_streams.size(), 6u);

    /// Both receiving stages read the same root's output, over their own exchanges.
    std::set<String> exchanges_a;
    for (const auto & task : plan.stages.at("probe_a").tasks)
        for (const auto & stream : task.input_exchange_streams)
            exchanges_a.insert(stream.exchange_id);
    std::set<String> exchanges_b;
    for (const auto & task : plan.stages.at("probe_b").tasks)
        for (const auto & stream : task.input_exchange_streams)
            exchanges_b.insert(stream.exchange_id);
    EXPECT_EQ(exchanges_a.size(), 1u);
    EXPECT_EQ(exchanges_b.size(), 1u);
    EXPECT_NE(*exchanges_a.begin(), *exchanges_b.begin());

    expectWiredBuild(plan, "build");
    auto * build = findBuildStep(plan.stages.at("build").query_plan_fragment);
    ASSERT_NE(build, nullptr);
    expectConsumerDescriptors(plan, "probe_a", *build);
    expectConsumerDescriptors(plan, "probe_b", *build);
}

TEST_F(RuntimeFilterExchangeWiring, MixedLocalAndRemoteSkipsProducerStageExchange)
{
    /// Producer stage also applies the filter: skip that stage on the exchange, still wire the
    /// remote stage. Mixed consumers must not put a receive descriptor on the producer.
    DistributedQueryPlan plan;
    DistributedQueryStage both;
    QueryPlan both_fragment;
    both_fragment.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<NullSource>(dataHeader()))));
    both_fragment.addStep(
        std::make_unique<BuildRuntimeFilterStep>(dataHeader(), "x", std::make_shared<DataTypeUInt64>(), "f", "key", testBuildOptions()));
    auto dag = makeApplyFilterDAG("key", "f");
    const String filter_column_name = applyFilterResultName(dag);
    both_fragment.addStep(std::make_unique<FilterStep>(dataHeader(), std::move(dag), filter_column_name, /*remove_filter_column_=*/true));
    both.query_plan_fragment = std::move(both_fragment);
    both.tasks.push_back(makeTask("both", 0));
    plan.stages["both"] = std::move(both);
    addConsumerStage(plan, "probe", 2, "key");

    size_t next_exchange_id = 100;
    wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Streaming);

    EXPECT_EQ(countStreams(plan), 3u);
    auto merge_stages = mergeStageNames(plan);
    ASSERT_EQ(merge_stages.size(), 1u);
    expectWiredBuild(plan, "both");
    auto * build = findBuildStep(plan.stages.at("both").query_plan_fragment);
    ASSERT_NE(build, nullptr);
    expectConsumerDescriptors(plan, "probe", *build);
    for (const auto & task : plan.stages.at("both").tasks)
        EXPECT_TRUE(task.runtime_filter_descriptors.empty());
    EXPECT_TRUE(dependsOn(plan.stage_depends_on.at("probe"), merge_stages.front()));
}

TEST_F(RuntimeFilterExchangeWiring, SameStageAdmission)
{
    /// Two sites in one receive stage, the 10-row one visited first. The stage receives the filter
    /// once if any site admits it: a tiny site does not veto a large or estimate-less sibling. The
    /// budget is not upsized for the estimate-less site (100 * 8 < 4096), so it takes the
    /// no-estimate path.
    struct Case
    {
        std::optional<size_t> first_arm_limit;
        bool wired;
    };
    for (const auto & test_case : {Case{1'000'000, true}, Case{10, false}, Case{std::nullopt, true}})
    {
        SCOPED_TRACE(test_case.first_arm_limit ? std::to_string(*test_case.first_arm_limit) : "no estimate");
        DistributedQueryPlan plan;
        addBuildStage(plan, "build", 1, "key");
        auto * build = findBuildStep(plan.stages.at("build").query_plan_fragment);
        ASSERT_NE(build, nullptr);
        build->setEstimatedBuildRows(100);
        addTwoSiteConsumerStage(plan, "probe", "key", test_case.first_arm_limit, /*second_arm_limit=*/10);

        size_t next_exchange_id = 100;
        wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Streaming);

        if (test_case.wired)
        {
            EXPECT_EQ(countStreams(plan), 2u);
            expectWiredBuild(plan, "build");
            expectConsumerDescriptors(plan, "probe", *build);
        }
        else
        {
            EXPECT_EQ(countStreams(plan), 0u);
            EXPECT_TRUE(plan.exchange_descriptions.empty());
            expectLocalBuild(plan, "build");
        }
    }
}

namespace
{

/// The build stage (the producer) transitively depends on the probe stage through a data
/// exchange, the way a join stage depends on the probe scan stage it consumes - the common
/// runtime filter topology. Delivery is wired like any other; only the completion dependency
/// entry must be skipped, because it would create a cycle.
DistributedQueryPlan wireProducerDependingOnConsumer(size_t num_build_tasks, size_t num_receive_tasks, ExchangeDescription::Kind kind)
{
    DistributedQueryPlan plan;
    addBuildStage(plan, "build", num_build_tasks, "key");
    addConsumerStage(plan, "probe", num_receive_tasks, "key");

    ExchangeDescription data_exchange;
    data_exchange.name = "data_exchange_probe_to_build";
    data_exchange.kind = kind;
    data_exchange.source_bucket_count = num_receive_tasks;
    data_exchange.destination_bucket_count = num_build_tasks;
    plan.exchange_descriptions[data_exchange.name] = data_exchange;
    plan.stage_depends_on["build"].push_back({"probe", data_exchange.name});

    size_t next_exchange_id = 100;
    wireRuntimeFilterExchangeTopology(plan, next_exchange_id, kind);
    return plan;
}

}

TEST_F(RuntimeFilterExchangeWiring, NoCompletionEdgeWhenItWouldCycle)
{
    auto plan = wireProducerDependingOnConsumer(1, 4, ExchangeDescription::Kind::Streaming);

    /// Delivery is wired exactly as for any transported filter.
    auto merge_stages = mergeStageNames(plan);
    ASSERT_EQ(merge_stages.size(), 1u);
    const auto & merge_stage = plan.stages.at(merge_stages.front());
    EXPECT_TRUE(merge_stage.filter_only);
    EXPECT_EQ(merge_stage.tasks.size(), 1u);
    /// The filter name is part of the stage name.
    EXPECT_TRUE(merge_stages.front().ends_with("_f")) << merge_stages.front();

    /// One stream into the root, one broadcast stream per receive task.
    EXPECT_EQ(countStreams(plan), 1u + 4u);

    /// The merge stage depends on the build stage as usual, but the probe stage gets NO
    /// dependency entry on the merge stage: it would close a cycle with the data edge.
    EXPECT_TRUE(dependsOn(plan.stage_depends_on.at(merge_stages.front()), "build"));
    auto probe_dependencies = plan.stage_depends_on.find("probe");
    EXPECT_TRUE(probe_dependencies == plan.stage_depends_on.end()
        || !dependsOn(probe_dependencies->second, merge_stages.front()));

    expectWiredBuild(plan, "build");
    auto * build = findBuildStep(plan.stages.at("build").query_plan_fragment);
    ASSERT_NE(build, nullptr);
    expectConsumerDescriptors(plan, "probe", *build);
}

TEST_F(RuntimeFilterExchangeWiring, PersistedDeliveryRequiresCompletionEdge)
{
    /// The completion dependency is what orders a persisted read after the blob write. Where it
    /// cannot be added, a persisted plan does not wire the delivery: behavior identical to no
    /// transport.
    auto plan = wireProducerDependingOnConsumer(2, 2, ExchangeDescription::Kind::Persisted);

    EXPECT_TRUE(mergeStageNames(plan).empty());
    EXPECT_EQ(countStreams(plan), 0u);
    for (const auto & task : plan.stages.at("probe").tasks)
        EXPECT_TRUE(task.runtime_filter_descriptors.empty());
}

/// The merge stages of a filter are not part of the data flow, so the fragment graph leaves them out. `makeDistributedPlan`
/// wires the filters after the cut, with exchange ids continuing from the id of `main`, and the merge stages keep the
/// default stage id, so a builder which took them in would throw on duplicate ids.
TEST_F(RuntimeFilterExchangeWiring, FragmentGraphLeavesOutMergeStages)
{
    /// stage_0 builds the filter and gathers its rows into main, which applies the filter.
    DistributedQueryPlan plan;
    addBuildStage(plan, "stage_0", 40, "key");
    auto & build_stage = plan.stages.at("stage_0");
    build_stage.id = 0;
    build_stage.query_plan_fragment.addStep(std::make_unique<GatherSendStep>(dataHeader(), "exchange_0"));

    DistributedQueryStage main;
    main.id = 1;
    main.query_plan_fragment.addStep(std::make_unique<GatherReceiveStep>(dataHeader(), "exchange_0", 40));
    auto dag = makeApplyFilterDAG("key", "f");
    const String filter_column_name = applyFilterResultName(dag);
    main.query_plan_fragment.addStep(
        std::make_unique<FilterStep>(dataHeader(), std::move(dag), filter_column_name, /*remove_filter_column_=*/true));
    main.tasks.push_back(makeTask("main", 0));
    plan.stages["main"] = std::move(main);

    plan.stage_depends_on["stage_0"] = {};
    plan.stage_depends_on["main"] = {{"stage_0", "exchange_0"}};
    plan.exchange_descriptions["exchange_0"] = ExchangeDescription{
        .name = "exchange_0",
        .id = 0,
        .kind = ExchangeDescription::Kind::Streaming,
        .source_bucket_count = 40,
        .destination_bucket_count = 1,
        .exchange_step_name = "GatherExchange"};

    size_t next_exchange_id = 1;
    wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Streaming);

    /// 40 build tasks: a level of 3 merge tasks, then the root, which `main` depends on.
    auto merge_stages = mergeStageNames(plan);
    ASSERT_EQ(merge_stages.size(), 2u);
    EXPECT_TRUE(dependsOn(plan.stage_depends_on.at("main"), merge_stages.back()));

    auto graph = buildFragmentGraph(plan);
    ASSERT_EQ(graph.fragments.size(), 2u);
    EXPECT_EQ(graph.getFragment(0).stage_name, "stage_0");
    EXPECT_EQ(graph.getFragment(0).tasks, 40u);
    EXPECT_EQ(graph.getFragment(1).stage_name, "main");
    ASSERT_EQ(graph.exchanges.size(), 1u);
    const auto & exchange = graph.getExchange(0);
    EXPECT_EQ(exchange.from, 0u);
    EXPECT_EQ(exchange.to, 1u);
    EXPECT_EQ(exchange.kind, FragmentExchange::Kind::Gather);

    /// The steps that build and apply the filter stay in the plans of the data fragments.
    EXPECT_NE(findBuildStep(plan.stages.at("stage_0").query_plan_fragment), nullptr);
    EXPECT_EQ(graph.getFragment(0).plan, &plan.stages.at("stage_0").query_plan_fragment);
    EXPECT_EQ(graph.getFragment(1).plan, &plan.stages.at("main").query_plan_fragment);
}
