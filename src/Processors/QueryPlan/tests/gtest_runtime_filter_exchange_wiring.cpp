#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/MergeRuntimeFiltersStep.h>
#include <Processors/QueryPlan/Optimizations/RuntimeFilterExchangeWiring.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ReceiveRuntimeFilterStep.h>
#include <Processors/QueryPlan/SendRuntimeFilterStep.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <set>

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
    return std::make_shared<Block>(Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x")});
}

DistributedQueryTask makeTask(const String & stage_name, size_t bucket)
{
    DistributedQueryTask task;
    task.task_id = stage_name + "_" + std::to_string(bucket);
    task.parameters.parameters["bucket_id"] = Field(std::to_string(bucket));
    return task;
}

void addSendStage(DistributedQueryPlan & plan, const String & name, size_t num_tasks, const String & filter_key)
{
    DistributedQueryStage stage;
    QueryPlan fragment;
    fragment.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<NullSource>(dataHeader()))));
    fragment.addStep(
        std::make_unique<SendRuntimeFilterStep>(dataHeader(), "x", std::make_shared<DataTypeUInt64>(), "f", filter_key, testGeometry()));
    stage.query_plan_fragment = std::move(fragment);
    for (size_t task = 0; task < num_tasks; ++task)
        stage.tasks.push_back(makeTask(name, task));
    plan.stages[name] = std::move(stage);
}

void addReceiveStage(DistributedQueryPlan & plan, const String & name, size_t num_tasks, const String & filter_key)
{
    DistributedQueryStage stage;
    QueryPlan fragment;
    fragment.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<NullSource>(dataHeader()))));
    fragment.addStep(
        std::make_unique<ReceiveRuntimeFilterStep>(dataHeader(), "f", filter_key, std::make_shared<DataTypeUInt64>(), testGeometry()));
    stage.query_plan_fragment = std::move(fragment);
    for (size_t task = 0; task < num_tasks; ++task)
        stage.tasks.push_back(makeTask(name, task));
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

/// Builds the symmetric case: one build stage of `num_build_tasks` and one receive stage of
/// `num_receive_tasks`, wires it, and returns the plan.
DistributedQueryPlan wireSymmetric(size_t num_build_tasks, size_t num_receive_tasks)
{
    DistributedQueryPlan plan;
    addSendStage(plan, "build", num_build_tasks, "key");
    addReceiveStage(plan, "probe", num_receive_tasks, "key");
    size_t next_exchange_id = 100;
    wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Streaming);
    return plan;
}

}

TEST(RuntimeFilterExchangeWiring, SymmetricTopologyIsLinear)
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
    }
}

TEST(RuntimeFilterExchangeWiring, SingleBuildTaskBroadcastsDirectly)
{
    auto plan = wireSymmetric(1, 4);

    EXPECT_EQ(countStreams(plan), 4u);
    EXPECT_TRUE(mergeStageNames(plan).empty());
    EXPECT_EQ(plan.stages.at("build").tasks.front().output_exchange_streams.size(), 4u);
    EXPECT_TRUE(plan.stage_depends_on.at("probe").contains("build"));
}

TEST(RuntimeFilterExchangeWiring, MultiLevelTree)
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
}

TEST(RuntimeFilterExchangeWiring, MultipleReceiveStages)
{
    DistributedQueryPlan plan;
    addSendStage(plan, "build", 8, "key");
    addReceiveStage(plan, "probe_a", 4, "key");
    addReceiveStage(plan, "probe_b", 2, "key");
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
}

TEST(RuntimeFilterExchangeWiring, ReceiveInSendStageStaysLocal)
{
    DistributedQueryPlan plan;
    DistributedQueryStage stage;
    QueryPlan fragment;
    fragment.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<NullSource>(dataHeader()))));
    fragment.addStep(
        std::make_unique<SendRuntimeFilterStep>(dataHeader(), "x", std::make_shared<DataTypeUInt64>(), "f", "key", testGeometry()));
    fragment.addStep(
        std::make_unique<ReceiveRuntimeFilterStep>(dataHeader(), "f", "key", std::make_shared<DataTypeUInt64>(), testGeometry()));
    stage.query_plan_fragment = std::move(fragment);
    stage.tasks.push_back(makeTask("both", 0));
    plan.stages["both"] = std::move(stage);

    size_t next_exchange_id = 100;
    wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Streaming);

    EXPECT_EQ(countStreams(plan), 0u);
    EXPECT_TRUE(plan.exchange_descriptions.empty());
    EXPECT_EQ(next_exchange_id, 100u);
}

TEST(RuntimeFilterExchangeWiring, PersistedDataEdgeMakesChainPersisted)
{
    for (size_t build_tasks : {1, 8})
    {
        DistributedQueryPlan plan;
        addSendStage(plan, "build", build_tasks, "key");
        addReceiveStage(plan, "probe", 4, "key");

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
    }
}

TEST(RuntimeFilterExchangeWiring, PersistedPlanKindAppliesToSiblingStages)
{
    /// The realistic shape: the probe producer stage that receives the filter is a sibling of the
    /// build stage, so there is no data edge between them whose kind could be copied. A Persisted
    /// plan (forced, or auto-selected because no streaming listener is configured) must still get
    /// persisted filter exchanges, otherwise they would use a transport that is never started.
    for (size_t build_tasks : {1, 8})
    {
        DistributedQueryPlan plan;
        addSendStage(plan, "build", build_tasks, "key");
        addReceiveStage(plan, "probe", 4, "key");
        ASSERT_TRUE(plan.stage_depends_on.empty()) << "the sibling shape must have no data edge";

        size_t next_exchange_id = 100;
        wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Persisted);

        EXPECT_FALSE(plan.exchange_descriptions.empty()) << "for " << build_tasks << " build tasks";
        for (const auto & [name, exchange] : plan.exchange_descriptions)
            EXPECT_EQ(exchange.kind, ExchangeDescription::Kind::Persisted) << name;
    }
}

TEST(RuntimeFilterExchangeWiring, StreamingPlanKindStaysStreaming)
{
    for (size_t build_tasks : {1, 8})
    {
        DistributedQueryPlan plan;
        addSendStage(plan, "build", build_tasks, "key");
        addReceiveStage(plan, "probe", 4, "key");

        size_t next_exchange_id = 100;
        wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Streaming);

        EXPECT_FALSE(plan.exchange_descriptions.empty()) << "for " << build_tasks << " build tasks";
        for (const auto & [name, exchange] : plan.exchange_descriptions)
            EXPECT_EQ(exchange.kind, ExchangeDescription::Kind::Streaming) << name;
    }
}

TEST(RuntimeFilterExchangeWiring, SendWithoutRemoteReceiveStaysPassthrough)
{
    DistributedQueryPlan plan;
    addSendStage(plan, "build", 4, "key");

    size_t next_exchange_id = 100;
    wireRuntimeFilterExchangeTopology(plan, next_exchange_id, ExchangeDescription::Kind::Streaming);

    EXPECT_EQ(countStreams(plan), 0u);
    EXPECT_TRUE(plan.exchange_descriptions.empty());
}
