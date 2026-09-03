#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/FilterStep.h>
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
#include <set>
#include <vector>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

namespace
{

RuntimeFilterGeometry testGeometry()
{
    return RuntimeFilterGeometry{
        .exact_values_limit = 64,
        .exact_bytes_limit = 4096,
        .bloom_filter_bytes = 4096,
        .bloom_filter_hash_functions = 3,
        .pass_ratio_threshold_for_disabling = 1.0,
        .blocks_to_skip_before_reenabling = 0,
        .max_ratio_of_set_bits_in_bloom_filter = 1.0,
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
            dataHeader(),
            "x",
            std::make_shared<DataTypeUInt64>(),
            "f",
            filter_key,
            testGeometry(),
            /*allow_to_use_not_exact_filter_=*/true,
            /*track_key_range_=*/false));
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

QueryPlanPtr makeLimitedApplyPlan(const String & filter_key, size_t limit)
{
    auto plan = std::make_unique<QueryPlan>();
    plan->addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<NullSource>(dataHeader()))));
    plan->addStep(std::make_unique<LimitStep>(dataHeader(), limit, /*offset=*/0));
    auto dag = makeApplyFilterDAG(filter_key, "f");
    const String filter_column_name = applyFilterResultName(dag);
    plan->addStep(std::make_unique<FilterStep>(dataHeader(), std::move(dag), filter_column_name, /*remove_filter_column_=*/true));
    return plan;
}

void addUnionConsumerStage(DistributedQueryPlan & plan, const String & name, size_t num_tasks, QueryPlanPtr first, QueryPlanPtr second)
{
    SharedHeaders headers;
    headers.emplace_back(dataHeader());
    headers.emplace_back(dataHeader());
    std::vector<QueryPlanPtr> union_plans;
    union_plans.reserve(2);
    union_plans.push_back(std::move(first));
    union_plans.push_back(std::move(second));

    DistributedQueryStage stage;
    QueryPlan fragment;
    fragment.unitePlans(std::make_unique<UnionStep>(std::move(headers)), std::move(union_plans));
    stage.query_plan_fragment = std::move(fragment);
    for (size_t task = 0; task < num_tasks; ++task)
        stage.tasks.push_back(makeTask(name, task));
    plan.stages[name] = std::move(stage);
}

/// Two `__applyFilter` sites in one fragment, sized by Limit so admission can see them.
/// UNION children are `[first_arm_limit, second_arm_limit]`; the wiring DFS is last-child-first.
void addTwoSiteConsumerStage(
    DistributedQueryPlan & plan,
    const String & name,
    size_t num_tasks,
    const String & filter_key,
    size_t first_arm_limit,
    size_t second_arm_limit)
{
    addUnionConsumerStage(
        plan, name, num_tasks, makeLimitedApplyPlan(filter_key, first_arm_limit), makeLimitedApplyPlan(filter_key, second_arm_limit));
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

        std::multiset<String> descriptor_streams;
        for (const auto & stream : desc.streams)
            descriptor_streams.insert(stream.toString());
        std::multiset<String> input_streams;
        for (const auto & stream : task.input_exchange_streams)
            input_streams.insert(stream.toString());
        EXPECT_EQ(descriptor_streams, input_streams);
        EXPECT_FALSE(desc.streams.empty());
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

/// `__applyFilter` requires a query context whenever the label is non-empty (it looks the filter
/// up in that context's runtime-filter lookup, and fail-opens on a miss). Constructing a `FilterStep`
/// computes the output header by executing the DAG on an empty block, so every consumer-fragment
/// construction and `wireRuntimeFilterExchangeTopology` call must run with the test thread attached
/// to a query context. Production always has one.
/// Frees the `current_thread` slot for the duration of a fixture: another suite in the binary can
/// leave it set for the process lifetime (e.g. via `MainThreadStatus`), and constructing a
/// `ThreadStatus` over an occupied slot asserts. Declared before `thread_status`, so the slot is
/// cleared before it is constructed and the previous value is restored after it is destroyed.
struct CurrentThreadSlot
{
    ThreadStatus * previous = std::exchange(current_thread, nullptr);
    ~CurrentThreadSlot() { current_thread = previous; }
};

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

TEST_F(RuntimeFilterExchangeWiring, SymmetricTopologyIsLinear)
{
    /// With `S <= fan_in` build tasks and one receive stage of the same size, the tree is a single
    /// root: S streams into it and S broadcast streams out, i.e. exactly 2 * S, where all-to-all
    /// delivery would create S^2.
    for (size_t tasks : {2, 4, 8})
    {
        auto plan = wireSymmetric(tasks, tasks);

        EXPECT_EQ(countStreams(plan), 2 * tasks) << "for " << tasks << " tasks";

        auto merge_stages = mergeStageNames(plan);
        ASSERT_EQ(merge_stages.size(), 1u);
        EXPECT_EQ(plan.stages.at(merge_stages.front()).tasks.size(), 1u);

        for (const auto & task : plan.stages.at("build").tasks)
            EXPECT_EQ(task.output_exchange_streams.size(), 1u);
        for (const auto & task : plan.stages.at("probe").tasks)
            EXPECT_EQ(task.input_exchange_streams.size(), 1u);

        for (const auto & [_, exchange] : plan.exchange_descriptions)
            EXPECT_EQ(exchange.kind, ExchangeDescription::Kind::Streaming);

        /// The scheduler chain: probe depends on the root merge stage, which depends on build.
        EXPECT_TRUE(plan.stage_depends_on.at("probe").contains(merge_stages.front()));
        EXPECT_TRUE(plan.stage_depends_on.at(merge_stages.front()).contains("build"));

        expectWiredBuild(plan, "build");
        auto * build = findBuildStep(plan.stages.at("build").query_plan_fragment);
        ASSERT_NE(build, nullptr);
        expectConsumerDescriptors(plan, "probe", *build);
    }
}

TEST_F(RuntimeFilterExchangeWiring, SingleBuildTaskDeliversThroughMergeStage)
{
    auto plan = wireSymmetric(1, 4);

    /// One stream into the root merge task, one broadcast stream per receive task. The build
    /// (data) task itself never holds the broadcast: a broadcast sink may wait until query end
    /// for a receiver that finished early, and a data task must not carry that wait.
    EXPECT_EQ(countStreams(plan), 1u + 4u);
    auto merge_stages = mergeStageNames(plan);
    ASSERT_EQ(merge_stages.size(), 1u);
    EXPECT_EQ(plan.stages.at("build").tasks.front().output_exchange_streams.size(), 1u);
    EXPECT_TRUE(plan.stage_depends_on.at("probe").contains(merge_stages.front()));
    EXPECT_TRUE(plan.stage_depends_on.at(merge_stages.front()).contains("build"));

    expectWiredBuild(plan, "build");
    auto * build = findBuildStep(plan.stages.at("build").query_plan_fragment);
    ASSERT_NE(build, nullptr);
    expectConsumerDescriptors(plan, "probe", *build);
}

TEST_F(RuntimeFilterExchangeWiring, MultiLevelTree)
{
    /// 40 build tasks with fan-in 16 need two merge levels: ceil(40 / 16) = 3 tasks, then the
    /// root. Streams: 40 into level one, 3 into the root, 4 broadcast.
    static_assert(RUNTIME_FILTER_MERGE_FAN_IN == 16);
    auto plan = wireSymmetric(40, 4);

    EXPECT_EQ(countStreams(plan), 40u + 3u + 4u);

    auto merge_stages = mergeStageNames(plan);
    ASSERT_EQ(merge_stages.size(), 2u);
    std::multiset<size_t> level_sizes{plan.stages.at(merge_stages[0]).tasks.size(), plan.stages.at(merge_stages[1]).tasks.size()};
    EXPECT_EQ(level_sizes, (std::multiset<size_t>{3, 1}));

    /// Every merge task consumes at most fan-in inputs.
    for (const auto & name : merge_stages)
        for (const auto & task : plan.stages.at(name).tasks)
            EXPECT_LE(task.input_exchange_streams.size(), RUNTIME_FILTER_MERGE_FAN_IN);

    expectWiredBuild(plan, "build");
    auto * build = findBuildStep(plan.stages.at("build").query_plan_fragment);
    ASSERT_NE(build, nullptr);
    expectConsumerDescriptors(plan, "probe", *build);
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

TEST_F(RuntimeFilterExchangeWiring, ApplicationInBuildStageStaysLocal)
{
    DistributedQueryPlan plan;
    DistributedQueryStage stage;
    QueryPlan fragment;
    fragment.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<NullSource>(dataHeader()))));
    fragment.addStep(
        std::make_unique<BuildRuntimeFilterStep>(
            dataHeader(),
            "x",
            std::make_shared<DataTypeUInt64>(),
            "f",
            "key",
            testGeometry(),
            /*allow_to_use_not_exact_filter_=*/true,
            /*track_key_range_=*/false));
    auto dag = makeApplyFilterDAG("key", "f");
    const String filter_column_name = applyFilterResultName(dag);
    fragment.addStep(std::make_unique<FilterStep>(dataHeader(), std::move(dag), filter_column_name, /*remove_filter_column_=*/true));
    stage.query_plan_fragment = std::move(fragment);
    stage.tasks.push_back(makeTask("both", 0));
    plan.stages["both"] = std::move(stage);

    size_t next_exchange_id = 100;
    wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Streaming);

    EXPECT_EQ(countStreams(plan), 0u);
    EXPECT_TRUE(plan.exchange_descriptions.empty());
    EXPECT_EQ(next_exchange_id, 100u);
    expectLocalBuild(plan, "both");
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
        std::make_unique<BuildRuntimeFilterStep>(
            dataHeader(),
            "x",
            std::make_shared<DataTypeUInt64>(),
            "f",
            "key",
            testGeometry(),
            /*allow_to_use_not_exact_filter_=*/true,
            /*track_key_range_=*/false));
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
    EXPECT_TRUE(plan.stage_depends_on.at("probe").contains(merge_stages.front()));
}

TEST_F(RuntimeFilterExchangeWiring, RestoresRendezvousKeyFromSiblingApply)
{
    QueryPlan fragment;
    fragment.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<NullSource>(dataHeader()))));
    fragment.addStep(
        std::make_unique<BuildRuntimeFilterStep>(
            dataHeader(),
            "x",
            std::make_shared<DataTypeUInt64>(),
            "f",
            /*filter_key_=*/"",
            testGeometry(),
            /*allow_to_use_not_exact_filter_=*/true,
            /*track_key_range_=*/false));
    auto dag = makeApplyFilterDAG("secret", "f");
    const String filter_column_name = applyFilterResultName(dag);
    fragment.addStep(std::make_unique<FilterStep>(dataHeader(), std::move(dag), filter_column_name, /*remove_filter_column_=*/true));

    auto * build = findBuildStep(fragment);
    ASSERT_NE(build, nullptr);
    EXPECT_TRUE(build->getFilterKey().empty());

    restoreRuntimeFilterRendezvousKeys(fragment);
    EXPECT_EQ(build->getFilterKey(), "secret");
}

TEST_F(RuntimeFilterExchangeWiring, RestoreLeavesKeyEmptyWithoutSiblingApply)
{
    QueryPlan fragment;
    fragment.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<NullSource>(dataHeader()))));
    fragment.addStep(
        std::make_unique<BuildRuntimeFilterStep>(
            dataHeader(),
            "x",
            std::make_shared<DataTypeUInt64>(),
            "f",
            /*filter_key_=*/"",
            testGeometry(),
            /*allow_to_use_not_exact_filter_=*/true,
            /*track_key_range_=*/false));

    restoreRuntimeFilterRendezvousKeys(fragment);
    auto * build = findBuildStep(fragment);
    ASSERT_NE(build, nullptr);
    EXPECT_TRUE(build->getFilterKey().empty());
}

TEST_F(RuntimeFilterExchangeWiring, PersistedDataEdgeMakesChainPersisted)
{
    for (size_t build_tasks : {1, 8})
    {
        DistributedQueryPlan plan;
        addBuildStage(plan, "build", build_tasks, "key");
        addConsumerStage(plan, "probe", 4, "key");

        /// A pre-existing Persisted data edge between the same two stages: the scheduler will run
        /// the build stage to completion before the probe stage starts, so the whole filter chain
        /// must be persisted too.
        ExchangeDescription data_exchange;
        data_exchange.name = "exchange_0";
        data_exchange.kind = ExchangeDescription::Kind::Persisted;
        data_exchange.source_bucket_count = build_tasks;
        data_exchange.destination_bucket_count = 4;
        plan.exchange_descriptions[data_exchange.name] = data_exchange;
        plan.stage_depends_on["probe"]["build"] = data_exchange.name;

        size_t next_exchange_id = 100;
        wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Streaming);

        size_t filter_exchanges = 0;
        for (const auto & [name, exchange] : plan.exchange_descriptions)
        {
            if (name == "exchange_0")
                continue;
            ++filter_exchanges;
            EXPECT_EQ(exchange.kind, ExchangeDescription::Kind::Persisted) << name;
        }
        EXPECT_GT(filter_exchanges, 0u);

        expectWiredBuild(plan, "build");
        auto * build = findBuildStep(plan.stages.at("build").query_plan_fragment);
        ASSERT_NE(build, nullptr);
        expectConsumerDescriptors(plan, "probe", *build);
    }
}

TEST_F(RuntimeFilterExchangeWiring, PersistedPlanKindAppliesToSiblingStages)
{
    /// The realistic shape: the probe producer stage that receives the filter is a sibling of the
    /// build stage, so there is no data edge between them whose kind could be copied. A Persisted
    /// plan (forced, or auto-selected because no streaming listener is configured) must still get
    /// persisted filter exchanges, otherwise they would use a transport that is never started.
    for (size_t build_tasks : {1, 8})
    {
        DistributedQueryPlan plan;
        addBuildStage(plan, "build", build_tasks, "key");
        addConsumerStage(plan, "probe", 4, "key");
        ASSERT_TRUE(plan.stage_depends_on.empty()) << "the sibling shape must have no data edge";

        size_t next_exchange_id = 100;
        wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Persisted);

        EXPECT_FALSE(plan.exchange_descriptions.empty()) << "for " << build_tasks << " build tasks";
        for (const auto & [name, exchange] : plan.exchange_descriptions)
            EXPECT_EQ(exchange.kind, ExchangeDescription::Kind::Persisted) << name;

        expectWiredBuild(plan, "build");
        auto * build = findBuildStep(plan.stages.at("build").query_plan_fragment);
        ASSERT_NE(build, nullptr);
        expectConsumerDescriptors(plan, "probe", *build);
    }
}

TEST_F(RuntimeFilterExchangeWiring, StreamingPlanKindStaysStreaming)
{
    for (size_t build_tasks : {1, 8})
    {
        DistributedQueryPlan plan;
        addBuildStage(plan, "build", build_tasks, "key");
        addConsumerStage(plan, "probe", 4, "key");

        size_t next_exchange_id = 100;
        wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Streaming);

        EXPECT_FALSE(plan.exchange_descriptions.empty()) << "for " << build_tasks << " build tasks";
        for (const auto & [name, exchange] : plan.exchange_descriptions)
            EXPECT_EQ(exchange.kind, ExchangeDescription::Kind::Streaming) << name;

        expectWiredBuild(plan, "build");
        auto * build = findBuildStep(plan.stages.at("build").query_plan_fragment);
        ASSERT_NE(build, nullptr);
        expectConsumerDescriptors(plan, "probe", *build);
    }
}

TEST_F(RuntimeFilterExchangeWiring, BuildWithoutConsumersStaysLocal)
{
    DistributedQueryPlan plan;
    addBuildStage(plan, "build", 4, "key");

    size_t next_exchange_id = 100;
    wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Streaming);

    EXPECT_EQ(countStreams(plan), 0u);
    EXPECT_TRUE(plan.exchange_descriptions.empty());
    EXPECT_EQ(next_exchange_id, 100u);
    expectLocalBuild(plan, "build");
}

TEST_F(RuntimeFilterExchangeWiring, SameStageTinySiteDoesNotVetoLargeSibling)
{
    /// UNION children [1e6, 10]: DFS visits the 10-row site first. The 1e6-row sibling still admits,
    /// so the stage ships once.
    DistributedQueryPlan plan;
    addBuildStage(plan, "build", 1, "key");
    auto * build = findBuildStep(plan.stages.at("build").query_plan_fragment);
    ASSERT_NE(build, nullptr);
    build->setEstimatedBuildRows(100);
    addTwoSiteConsumerStage(plan, "probe", 1, "key", /*first_arm_limit=*/1'000'000, /*second_arm_limit=*/10);

    size_t next_exchange_id = 100;
    wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Streaming);

    EXPECT_EQ(countStreams(plan), 2u);
    expectWiredBuild(plan, "build");
    expectConsumerDescriptors(plan, "probe", *build);
}

TEST_F(RuntimeFilterExchangeWiring, SameStageHugeVisitedFirstStillWires)
{
    /// Reverse child order. Still one delivery.
    DistributedQueryPlan plan;
    addBuildStage(plan, "build", 1, "key");
    auto * build = findBuildStep(plan.stages.at("build").query_plan_fragment);
    ASSERT_NE(build, nullptr);
    build->setEstimatedBuildRows(100);
    addTwoSiteConsumerStage(plan, "probe", 1, "key", /*first_arm_limit=*/10, /*second_arm_limit=*/1'000'000);

    size_t next_exchange_id = 100;
    wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Streaming);

    EXPECT_EQ(countStreams(plan), 2u);
    expectWiredBuild(plan, "build");
    expectConsumerDescriptors(plan, "probe", *build);
}

TEST_F(RuntimeFilterExchangeWiring, SameStageBothTinyStaysLocal)
{
    DistributedQueryPlan plan;
    addBuildStage(plan, "build", 1, "key");
    auto * build = findBuildStep(plan.stages.at("build").query_plan_fragment);
    ASSERT_NE(build, nullptr);
    build->setEstimatedBuildRows(100);
    addTwoSiteConsumerStage(plan, "probe", 1, "key", /*first_arm_limit=*/10, /*second_arm_limit=*/10);

    size_t next_exchange_id = 100;
    wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Streaming);

    EXPECT_EQ(countStreams(plan), 0u);
    EXPECT_TRUE(plan.exchange_descriptions.empty());
    expectLocalBuild(plan, "build");
}

TEST_F(RuntimeFilterExchangeWiring, SameStageStatsLessSiblingStillAdmits)
{
    /// Tiny numbered site visited first, sibling has no row estimate. Budget is not upsized
    /// (100 * 8 < 4096), so the no-estimate path admits.
    DistributedQueryPlan plan;
    addBuildStage(plan, "build", 1, "key");
    auto * build = findBuildStep(plan.stages.at("build").query_plan_fragment);
    ASSERT_NE(build, nullptr);
    build->setEstimatedBuildRows(100);

    auto unknown = std::make_unique<QueryPlan>();
    unknown->addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<NullSource>(dataHeader()))));
    auto dag = makeApplyFilterDAG("key", "f");
    const String filter_column_name = applyFilterResultName(dag);
    unknown->addStep(std::make_unique<FilterStep>(dataHeader(), std::move(dag), filter_column_name, /*remove_filter_column_=*/true));

    addUnionConsumerStage(plan, "probe", 1, std::move(unknown), makeLimitedApplyPlan("key", 10));

    size_t next_exchange_id = 100;
    wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Streaming);

    EXPECT_EQ(countStreams(plan), 2u);
    expectWiredBuild(plan, "build");
    expectConsumerDescriptors(plan, "probe", *build);
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
    plan.stage_depends_on["build"]["probe"] = data_exchange.name;

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
    EXPECT_TRUE(plan.stage_depends_on.at(merge_stages.front()).contains("build"));
    auto probe_dependencies = plan.stage_depends_on.find("probe");
    EXPECT_TRUE(probe_dependencies == plan.stage_depends_on.end()
        || !probe_dependencies->second.contains(merge_stages.front()));

    expectWiredBuild(plan, "build");
    auto * build = findBuildStep(plan.stages.at("build").query_plan_fragment);
    ASSERT_NE(build, nullptr);
    expectConsumerDescriptors(plan, "probe", *build);
}

TEST_F(RuntimeFilterExchangeWiring, NoCompletionEdgeWhenItWouldCycleTree)
{
    auto plan = wireProducerDependingOnConsumer(4, 4, ExchangeDescription::Kind::Streaming);

    auto merge_stages = mergeStageNames(plan);
    ASSERT_EQ(merge_stages.size(), 1u);
    EXPECT_TRUE(plan.stages.at(merge_stages.front()).filter_only);

    /// Four partials into the root, four broadcast streams out.
    EXPECT_EQ(countStreams(plan), 4u + 4u);

    auto probe_dependencies = plan.stage_depends_on.find("probe");
    EXPECT_TRUE(probe_dependencies == plan.stage_depends_on.end()
        || !probe_dependencies->second.contains(merge_stages.front()));

    auto * build = findBuildStep(plan.stages.at("build").query_plan_fragment);
    ASSERT_NE(build, nullptr);
    expectConsumerDescriptors(plan, "probe", *build);
}

TEST_F(RuntimeFilterExchangeWiring, PersistedDeliveryRequiresCompletionEdge)
{
    /// The completion dependency is what orders a persisted read after the blob write. Where it
    /// cannot be added, a persisted plan does not wire the delivery: behavior identical to no
    /// transport.
    for (size_t build_tasks : {1, 2})
    {
        auto plan = wireProducerDependingOnConsumer(build_tasks, 2, ExchangeDescription::Kind::Persisted);

        EXPECT_TRUE(mergeStageNames(plan).empty());
        EXPECT_EQ(countStreams(plan), 0u);
        for (const auto & task : plan.stages.at("probe").tasks)
            EXPECT_TRUE(task.runtime_filter_descriptors.empty());
    }
}
