#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Common/typeid_cast.h>

namespace DB
{
void registerBuildRuntimeFilterStep(QueryPlanStepRegistry & registry);
}

using namespace DB;

/// Covers local rendezvous-key transport and receiver-side pairing.

namespace
{

constexpr auto step_name = "BuildRuntimeFilter";
constexpr UInt64 current_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
constexpr UInt64 pre_key_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_RUNTIME_FILTER_EXCHANGES - 1;
constexpr UInt64 pre_local_key_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_LOCAL_RUNTIME_FILTER_KEY - 1;

/// Isolate the registry from other tests in this binary.
QueryPlanStepRegistry & stepRegistry()
{
    static QueryPlanStepRegistry registry;
    static const bool registered = []
    {
        registerBuildRuntimeFilterStep(registry);
        return true;
    }();
    (void)registered;
    return registry;
}

SharedHeader makeHeader(const String & column_name)
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, column_name)}));
}

std::unique_ptr<BuildRuntimeFilterStep> makeBuildStep(const String & key)
{
    return std::make_unique<BuildRuntimeFilterStep>(
        makeHeader("r"),
        "r",
        std::make_shared<DataTypeUInt64>(),
        "_runtime_filter_1",
        key,
        RuntimeFilterBuildOptions{
            .geometry = RuntimeFilterGeometry{.exact_values_limit = 1000},
            .polarity = RuntimeFilterPolarity::Contains,
            .track_key_range = false,
            .distinct_keys_hint = std::nullopt,
            .distinct_keys_hint_matches_filter_key = false});
}

/// Use the registry-selected step version, as `QueryPlan::serialize` does.
String serializeStep(const IQueryPlanStep & step, UInt64 version, bool for_cache_key = false)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    registry.for_cache_key = for_cache_key;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = version;
    ctx.step_version = stepRegistry().versionToWrite(step_name, version);
    ctx.for_cache_key = for_cache_key;
    step.serialize(ctx);
    return out.str();
}

std::unique_ptr<BuildRuntimeFilterStep> deserializeStep(const String & bytes, UInt64 version)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    auto header = makeHeader("r");
    SharedHeaders input_headers{header};
    QueryPlanSerializationSettings settings;

    const UInt64 step_version = stepRegistry().versionToWrite(step_name, version);
    stepRegistry().checkVersionReadable(step_name, step_version);
    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, getContext().context, input_headers, header, settings, 0, version, step_version, false};

    auto step = BuildRuntimeFilterStep::deserialize(ctx);
    /// The buffer holds one step, so a byte left unread here would be taken for the next node of a real plan.
    EXPECT_TRUE(in.eof());
    return std::unique_ptr<BuildRuntimeFilterStep>(typeid_cast<BuildRuntimeFilterStep *>(step.release()));
}

std::unique_ptr<BuildRuntimeFilterStep> makeShippedBuildStep(const String & key)
{
    return deserializeStep(serializeStep(*makeBuildStep(key), current_version), current_version);
}

std::unique_ptr<ExpressionStep> makeProbeStep(const String & key)
{
    ActionsDAG dag;
    const auto & input = dag.addInput("l", std::make_shared<DataTypeUInt64>());
    auto string_type = std::make_shared<DataTypeString>();
    const auto & label = dag.addColumn(
        string_type->createColumnConst(0, key), string_type, "_runtime_filter_1",
        /*is_deterministic_constant=*/false, /*is_masked_secret=*/false, /*is_runtime_filter_id=*/true);
    dag.addFunction(FunctionFactory::instance().get("__applyFilter", nullptr), {&label, &input}, {});
    /// Keep the filter out of outputs so header computation does not require a query context.
    dag.getOutputs() = {&input};
    return std::make_unique<ExpressionStep>(makeHeader("l"), std::move(dag));
}

std::unique_ptr<JoinStepLogical> makeJoinStep(bool optimized)
{
    auto left_header = makeHeader("l");
    auto right_header = makeHeader("r");
    QueryPlanSerializationSettings settings;

    JoinExpressionActions expression_actions(*left_header, *right_header);
    auto actions_dag = expression_actions.getActionsDAG();
    for (const auto * input : actions_dag->getInputs())
        actions_dag->getOutputs().push_back(input);

    auto join_step = std::make_unique<JoinStepLogical>(
        left_header,
        right_header,
        JoinOperator{},
        std::move(expression_actions),
        ActionsDAG::NodeRawConstPtrs{},
        JoinSettings(settings, current_version),
        SortingStep::Settings(settings));
    if (optimized)
        join_step->setOptimized();
    return join_step;
}

struct JoinWithFilter
{
    QueryPlan::Node * join = nullptr;
    BuildRuntimeFilterStep * build_step = nullptr;
};

JoinWithFilter addJoin(
    QueryPlan::Nodes & nodes, const String & probe_key, std::unique_ptr<BuildRuntimeFilterStep> build_step, bool optimized = true)
{
    auto * probe_read = &nodes.emplace_back(QueryPlan::Node{.step = std::make_unique<ReadNothingStep>(makeHeader("l"))});
    auto * probe = &nodes.emplace_back(QueryPlan::Node{.step = makeProbeStep(probe_key), .children = {probe_read}});

    auto * build_read = &nodes.emplace_back(QueryPlan::Node{.step = std::make_unique<ReadNothingStep>(makeHeader("r"))});
    auto * build_step_ptr = build_step.get();
    auto * build = &nodes.emplace_back(QueryPlan::Node{.step = std::move(build_step), .children = {build_read}});

    auto * join = &nodes.emplace_back(QueryPlan::Node{.step = makeJoinStep(optimized), .children = {probe, build}});
    return {join, build_step_ptr};
}

QueryPlan::Node * addUnion(QueryPlan::Nodes & nodes, VectorWithMemoryTracking<QueryPlan::Node *> children)
{
    SharedHeaders headers;
    for (const auto * child : children)
        headers.push_back(child->step->getOutputHeader());
    return &nodes.emplace_back(QueryPlan::Node{.step = std::make_unique<UnionStep>(std::move(headers)), .children = std::move(children)});
}

const VectorWithMemoryTracking<SharedRuntimeFilterDescriptor> & sharedDescriptors(const QueryPlan::Node & join)
{
    return typeid_cast<const JoinStepLogical &>(*join.step).getJoinOperator().shared_runtime_filter_descriptors;
}

}

TEST(BuildRuntimeFilterStepKey, KeyRoundTripsAtCurrentVersion)
{
    auto step = makeBuildStep("key-a");
    const String bytes = serializeStep(*step, current_version);

    /// Without this the assertions below hold for a serializer that does not write the key.
    EXPECT_NE(bytes, serializeStep(*makeBuildStep("key-b"), current_version));

    auto restored = deserializeStep(bytes, current_version);
    EXPECT_EQ(restored->getFilterKey(), "key-a");
    EXPECT_TRUE(restored->needsPairing());
    EXPECT_EQ(bytes, serializeStep(*restored, current_version));
}

/// Older peers receive inert local builds without keys.
TEST(BuildRuntimeFilterStepKey, OlderPeerGetsInertStep)
{
    ASSERT_EQ(stepRegistry().versionToWrite(step_name, pre_key_version), 0);
    ASSERT_EQ(stepRegistry().versionToWrite(step_name, pre_local_key_version), 1);
    for (const UInt64 version : {pre_key_version, pre_local_key_version})
    {
        SCOPED_TRACE(version);
        const String bytes = serializeStep(*makeBuildStep("key-a"), version);
        EXPECT_EQ(bytes, serializeStep(*makeBuildStep("key-b"), version));

        auto restored = deserializeStep(bytes, version);
        EXPECT_TRUE(restored->getFilterKey().empty());
        EXPECT_FALSE(restored->needsPairing());
    }
}

/// The key is random per plan build, so two builds of one plan must hash alike.
TEST(BuildRuntimeFilterStepKey, CacheKeyLeavesKeyOut)
{
    EXPECT_EQ(
        serializeStep(*makeBuildStep("key-a"), current_version, /*for_cache_key=*/true),
        serializeStep(*makeBuildStep("key-b"), current_version, /*for_cache_key=*/true));
}

TEST(PairJoinRuntimeFilters, KeepsKeyOfPairedStep)
{
    tryRegisterFunctions();

    QueryPlan::Nodes nodes;
    auto [join, build_step] = addJoin(nodes, "key-a", makeShippedBuildStep("key-a"));

    EXPECT_TRUE(QueryPlanOptimizations::pairJoinRuntimeFilters(*join, /*allow_pairing=*/true, /*track_key_range=*/false));
    EXPECT_EQ(build_step->getFilterKey(), "key-a");
    EXPECT_FALSE(build_step->needsPairing());

    /// Pairing restores the non-serialized shared `FixedHashMap` descriptor.
    ASSERT_EQ(sharedDescriptors(*join).size(), 1u);
    EXPECT_EQ(sharedDescriptors(*join).front().filter_key, "key-a");
    EXPECT_EQ(sharedDescriptors(*join).front().build_key_name, "r");

    /// A later optimization of the same plan leaves the paired step alone.
    EXPECT_FALSE(QueryPlanOptimizations::pairJoinRuntimeFilters(*join, /*allow_pairing=*/true, /*track_key_range=*/false));
    EXPECT_EQ(build_step->getFilterKey(), "key-a");
    EXPECT_EQ(sharedDescriptors(*join).size(), 1u);
}

TEST(PairJoinRuntimeFilters, MakesStepWithoutConsumerInert)
{
    tryRegisterFunctions();

    QueryPlan::Nodes nodes;
    auto [join, build_step] = addJoin(nodes, "key-other", makeShippedBuildStep("key-a"));

    EXPECT_FALSE(QueryPlanOptimizations::pairJoinRuntimeFilters(*join, /*allow_pairing=*/true, /*track_key_range=*/false));
    EXPECT_TRUE(build_step->getFilterKey().empty());
    EXPECT_TRUE(sharedDescriptors(*join).empty());
}

TEST(PairJoinRuntimeFilters, MakesStepsWithDuplicateKeyInert)
{
    tryRegisterFunctions();

    QueryPlan::Nodes nodes;
    auto first = addJoin(nodes, "key-a", makeShippedBuildStep("key-a"));
    auto second = addJoin(nodes, "key-a", makeShippedBuildStep("key-a"));
    auto * root = addUnion(nodes, {first.join, second.join});

    EXPECT_FALSE(QueryPlanOptimizations::pairJoinRuntimeFilters(*root, /*allow_pairing=*/true, /*track_key_range=*/false));
    EXPECT_TRUE(first.build_step->getFilterKey().empty());
    EXPECT_TRUE(second.build_step->getFilterKey().empty());
    EXPECT_FALSE(first.build_step->needsPairing());
    EXPECT_FALSE(second.build_step->needsPairing());
    EXPECT_TRUE(sharedDescriptors(*first.join).empty());
    EXPECT_TRUE(sharedDescriptors(*second.join).empty());
}

TEST(PairJoinRuntimeFilters, MakesStepOfUnoptimizedJoinInert)
{
    tryRegisterFunctions();

    QueryPlan::Nodes nodes;
    auto [join, build_step] = addJoin(nodes, "key-a", makeShippedBuildStep("key-a"), /*optimized=*/false);

    EXPECT_FALSE(QueryPlanOptimizations::pairJoinRuntimeFilters(*join, /*allow_pairing=*/true, /*track_key_range=*/false));
    EXPECT_TRUE(build_step->getFilterKey().empty());
    EXPECT_TRUE(sharedDescriptors(*join).empty());
}

TEST(PairJoinRuntimeFilters, MakesAllStepsInertWithoutPairing)
{
    tryRegisterFunctions();

    QueryPlan::Nodes nodes;
    auto [join, build_step] = addJoin(nodes, "key-a", makeShippedBuildStep("key-a"));

    EXPECT_FALSE(QueryPlanOptimizations::pairJoinRuntimeFilters(*join, /*allow_pairing=*/false, /*track_key_range=*/false));
    EXPECT_TRUE(build_step->getFilterKey().empty());
    EXPECT_FALSE(build_step->needsPairing());
    EXPECT_TRUE(sharedDescriptors(*join).empty());
}
