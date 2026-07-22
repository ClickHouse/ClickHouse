#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/QueryPlanEnvelope.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace
{

/// A minimal serializable source: fixed header, empty payload. Lets the harness round-trip
/// transforming steps without depending on any storage.
class TestSourceStep : public ISourceStep
{
public:
    explicit TestSourceStep(SharedHeader header_) : ISourceStep(std::move(header_)) { }

    String getName() const override { return "TestSource"; }

    void initializePipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings & /*settings*/) override { }

    bool isSerializable() const override { return true; }

    void serialize(Serialization & /*ctx*/) const override { }

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx)
    {
        return std::make_unique<TestSourceStep>(ctx.output_header);
    }
};

void registerStepsOnce()
{
    static std::once_flag flag;
    std::call_once(flag, []
    {
        /// Another suite in this binary may have registered a subset already.
        if (!QueryPlanStepRegistry::instance().hasStep("Expression"))
            QueryPlanStepRegistry::registerPlanSteps();
        QueryPlanStepRegistry::instance().registerStep("TestSource", TestSourceStep::deserialize);
    });
}

SharedHeader makeTestHeader()
{
    ColumnsWithTypeAndName columns;
    columns.emplace_back(DataTypeUInt64().createColumn(), std::make_shared<DataTypeUInt64>(), "x");
    columns.emplace_back(DataTypeUInt8().createColumn(), std::make_shared<DataTypeUInt8>(), "f");
    return std::make_shared<const Block>(Block(columns));
}

QueryPlan makeSourcePlan()
{
    QueryPlan plan;
    plan.addStep(std::make_unique<TestSourceStep>(makeTestHeader()));
    return plan;
}

std::string serializePlan(const QueryPlan & plan, size_t version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION)
{
    WriteBufferFromOwnString out;
    plan.serialize(out, version);
    out.finalize();
    return out.str();
}

QueryPlan deserializePlan(const std::string & bytes)
{
    ReadBufferFromString in(bytes);
    auto plan_and_sets = QueryPlan::deserialize(in, getContext().context, /*max_type_complexity=*/0);
    EXPECT_TRUE(in.eof());
    return QueryPlan::makeSets(std::move(plan_and_sets), getContext().context);
}

/// For every supported version: serialize -> deserialize -> serialize must reproduce identical
/// bytes, and the reconstructed plans must explain identically across versions. This pins both
/// the legacy and the v4 skeleton formats and per-step determinism.
void checkRoundTrip(QueryPlan plan)
{
    registerStepsOnce();

    std::string explain_reference;
    for (size_t version : {size_t(3), size_t(DBMS_QUERY_PLAN_SERIALIZATION_VERSION)})
    {
        std::string first_bytes = serializePlan(plan, version);
        auto restored_plan = deserializePlan(first_bytes);

        std::string second_bytes = serializePlan(restored_plan, version);
        EXPECT_EQ(first_bytes, second_bytes) << "at version " << version;

        auto explain = debugExplainPlan(restored_plan);
        if (explain_reference.empty())
            explain_reference = explain;
        else
            EXPECT_EQ(explain, explain_reference) << "plans diverge between versions";
    }
}

}

TEST(QueryPlanSerialization, SourceRoundTrip)
{
    checkRoundTrip(makeSourcePlan());
}

TEST(QueryPlanSerialization, ExpressionRoundTrip)
{
    auto plan = makeSourcePlan();

    ActionsDAG dag(plan.getCurrentHeader()->getColumnsWithTypeAndName());
    const auto & alias = dag.addAlias(*dag.getInputs().front(), "x_alias");
    dag.getOutputs().push_back(&alias);

    plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(dag)));
    checkRoundTrip(std::move(plan));
}

TEST(QueryPlanSerialization, FilterRoundTrip)
{
    auto plan = makeSourcePlan();

    ActionsDAG dag(plan.getCurrentHeader()->getColumnsWithTypeAndName());
    plan.addStep(std::make_unique<FilterStep>(plan.getCurrentHeader(), std::move(dag), "f", false));
    checkRoundTrip(std::move(plan));
}

TEST(QueryPlanSerialization, LimitOffsetRoundTrip)
{
    auto plan = makeSourcePlan();
    plan.addStep(std::make_unique<LimitStep>(plan.getCurrentHeader(), 10, 1));
    plan.addStep(std::make_unique<OffsetStep>(plan.getCurrentHeader(), 5));
    checkRoundTrip(std::move(plan));
}

TEST(QueryPlanSerialization, DistinctRoundTrip)
{
    for (bool pre_distinct : {false, true})
    {
        auto plan = makeSourcePlan();
        plan.addStep(std::make_unique<DistinctStep>(
            plan.getCurrentHeader(), SizeLimits{}, 0, Names{"x"}, pre_distinct));
        checkRoundTrip(std::move(plan));
    }
}

TEST(QueryPlanSerialization, UnionRoundTrip)
{
    std::vector<QueryPlanPtr> plans;
    plans.push_back(std::make_unique<QueryPlan>(makeSourcePlan()));
    plans.push_back(std::make_unique<QueryPlan>(makeSourcePlan()));

    SharedHeaders input_headers{plans[0]->getCurrentHeader(), plans[1]->getCurrentHeader()};

    QueryPlan plan;
    plan.unitePlans(std::make_unique<UnionStep>(std::move(input_headers)), std::move(plans));
    checkRoundTrip(std::move(plan));
}

TEST(QueryPlanSerialization, PerVersionSerializedPlanCache)
{
    registerStepsOnce();
    auto plan = makeSourcePlan();

    ASSERT_GE(DBMS_QUERY_PLAN_SERIALIZATION_VERSION, 2);
    const size_t current = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
    const size_t older = current - 1;

    EXPECT_FALSE(plan.isSerialized(current));
    EXPECT_FALSE(plan.isSerialized(older));

    plan.ensureSerialized(current);
    EXPECT_TRUE(plan.isSerialized(current));
    /// Bytes for one version must never be served for another: each advertised peer version gets
    /// its own cache entry.
    EXPECT_FALSE(plan.isSerialized(older));

    plan.ensureSerialized(older);
    EXPECT_TRUE(plan.isSerialized(older));

    auto current_bytes = plan.getSerializedData(current);
    auto older_bytes = plan.getSerializedData(older);

    /// The stream starts with the version varint, so the leading byte must differ.
    ASSERT_FALSE(current_bytes.empty());
    ASSERT_FALSE(older_bytes.empty());
    EXPECT_EQ(static_cast<UInt8>(current_bytes[0]), current);
    EXPECT_EQ(static_cast<UInt8>(older_bytes[0]), older);

    /// Requests above the supported version clamp to the current one.
    plan.ensureSerialized(current + 100);
    EXPECT_EQ(plan.getSerializedData(current + 100), current_bytes);
}

namespace
{

/// A two-node skeleton: root "Expression" with one "TestSource" child.
PlanSkeleton makeTestSkeleton()
{
    PlanSkeleton skeleton;

    PlanSkeleton::Node root;
    root.child_count = 1;
    root.step_name = "Expression";
    root.step_format_version = 1;
    root.has_output_header = true;
    root.header = makeTestHeader();
    skeleton.nodes.push_back(std::move(root));

    PlanSkeleton::Node leaf;
    leaf.child_count = 0;
    leaf.step_name = "TestSource";
    leaf.step_format_version = 1;
    leaf.has_output_header = true;
    leaf.header = makeTestHeader();
    leaf.payload_size = 0;
    skeleton.nodes.push_back(std::move(leaf));

    return skeleton;
}

std::string writeSkeletonToString(const PlanSkeleton & skeleton)
{
    WriteBufferFromOwnString out;
    writeQueryPlanSkeleton(skeleton, out);
    out.finalize();
    return out.str();
}

}

TEST(QueryPlanSkeleton, WriteReadRoundTrip)
{
    registerStepsOnce();
    auto skeleton = makeTestSkeleton();
    skeleton.nodes[0].step_description = "test description";
    skeleton.nodes[1].settings.push_back({.name = "max_block_size", .flags = 0, .value = "\x01"});
    skeleton.nodes[1].payload_size = 42;

    auto bytes = writeSkeletonToString(skeleton);

    ReadBufferFromString in(bytes);
    auto restored = readQueryPlanSkeleton(in, /*max_type_complexity=*/0);
    EXPECT_TRUE(in.eof());

    ASSERT_EQ(restored.nodes.size(), 2u);
    EXPECT_EQ(restored.nodes[0].step_name, "Expression");
    EXPECT_EQ(restored.nodes[0].child_count, 1u);
    EXPECT_EQ(restored.nodes[0].step_description, "test description");
    ASSERT_TRUE(restored.nodes[0].has_output_header);
    EXPECT_EQ(restored.nodes[0].header->columns(), 2u);
    EXPECT_EQ(restored.nodes[1].payload_size, 42u);
    ASSERT_EQ(restored.nodes[1].settings.size(), 1u);
    EXPECT_EQ(restored.nodes[1].settings[0].name, "max_block_size");

    /// Re-write must reproduce identical bytes.
    EXPECT_EQ(writeSkeletonToString(restored), bytes);
}

TEST(QueryPlanSkeleton, ValidationCollectsAllIssues)
{
    registerStepsOnce();
    auto skeleton = makeTestSkeleton();

    skeleton.nodes[1].step_name = "NoSuchStep";
    skeleton.nodes[1].has_output_header = false;
    skeleton.nodes[1].header = nullptr;
    skeleton.nodes[0].settings.push_back({.name = "no_such_setting", .flags = 0, .value = ""});

    auto result = validateQueryPlanSkeleton(skeleton, /*plan_version=*/4);
    ASSERT_FALSE(result.ok());
    /// All three problems reported at once, not just the first.
    EXPECT_EQ(result.issues.size(), 3u) << result.describe();
    EXPECT_NE(result.describe().find("NoSuchStep"), std::string::npos);
    EXPECT_NE(result.describe().find("no output header"), std::string::npos);
    EXPECT_NE(result.describe().find("no_such_setting"), std::string::npos);
}

TEST(QueryPlanSkeleton, ValidationAcceptsIgnorableUnknownSetting)
{
    registerStepsOnce();
    auto skeleton = makeTestSkeleton();
    skeleton.nodes[0].settings.push_back(
        {.name = "future_setting", .flags = PlanSkeleton::SettingEntry::FLAG_IGNORABLE, .value = ""});

    EXPECT_TRUE(validateQueryPlanSkeleton(skeleton, 4).ok());
}

TEST(QueryPlanSkeleton, ValidationChecksStepVersionAgainstManifest)
{
    registerStepsOnce();

    static std::once_flag flag;
    std::call_once(flag, []
    {
        QueryPlanStepRegistry::StepSerializationInfo info;
        info.max_step_format_version = 2;
        info.min_plan_version_for_step_version[2] = 5;
        QueryPlanStepRegistry::instance().registerStep(
            "TestGatedStep", TestSourceStep::deserialize, std::move(info));
    });

    auto skeleton = makeTestSkeleton();
    skeleton.nodes[1].step_name = "TestGatedStep";
    skeleton.nodes[1].step_format_version = 2;

    /// Format version 2 of this step requires plan version 5.
    EXPECT_FALSE(validateQueryPlanSkeleton(skeleton, 4).ok());
    EXPECT_TRUE(validateQueryPlanSkeleton(skeleton, 5).ok());

    /// A version above the known maximum is an ignorable extension when no mapping forbids it.
    skeleton.nodes[1].step_format_version = 3;
    EXPECT_TRUE(validateQueryPlanSkeleton(skeleton, 5).ok());
}

TEST(QueryPlanSkeleton, ValidationChecksTreeStructureAndSetOrder)
{
    registerStepsOnce();
    auto skeleton = makeTestSkeleton();

    skeleton.nodes[0].child_count = 2;  /// Declares two children, only one node follows.
    EXPECT_FALSE(validateQueryPlanSkeleton(skeleton, 4).ok());
    skeleton.nodes[0].child_count = 1;

    PlanSkeleton::SetEntry set1;
    set1.hash.low64 = 10;
    set1.hash.high64 = 1;
    set1.kind = 2;
    PlanSkeleton::SetEntry set2;
    set2.hash.low64 = 5;
    set2.hash.high64 = 0;
    set2.kind = 200;  /// Unknown kind.
    skeleton.sets = {set1, set2};  /// Also out of canonical order.

    auto result = validateQueryPlanSkeleton(skeleton, 4);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.issues.size(), 2u) << result.describe();
}

TEST(QueryPlanSkeleton, TruncationAndTrailingBytesFailClosed)
{
    registerStepsOnce();
    auto bytes = writeSkeletonToString(makeTestSkeleton());

    /// Truncation at every byte boundary must throw, never crash or misread.
    for (size_t cut = 0; cut + 1 < bytes.size(); ++cut)
    {
        std::string truncated = bytes.substr(0, cut);
        ReadBufferFromString in(truncated);
        EXPECT_ANY_THROW(readQueryPlanSkeleton(in, 0)) << "no exception at cut " << cut;
    }
}

TEST(QueryPlanSkeleton, NodeExtraIsSkippedForFutureLayouts)
{
    registerStepsOnce();
    auto skeleton = makeTestSkeleton();
    skeleton.nodes[0].node_extra = "future skeleton fields";

    auto bytes = writeSkeletonToString(skeleton);
    ReadBufferFromString in(bytes);
    auto restored = readQueryPlanSkeleton(in, 0);

    /// A reader that does not understand the extra bytes still reconstructs the shape.
    EXPECT_EQ(restored.nodes[0].node_extra, "future skeleton fields");
    EXPECT_TRUE(validateQueryPlanSkeleton(restored, 4).ok());
    EXPECT_FALSE(formatQueryPlanSkeleton(restored).empty());
}

TEST(QueryPlanSkeleton, FormatShowsShapeWithUnknownSteps)
{
    registerStepsOnce();
    auto skeleton = makeTestSkeleton();
    skeleton.nodes[1].step_name = "StepFromTheFuture";
    skeleton.nodes[1].payload_size = 128;

    auto text = formatQueryPlanSkeleton(skeleton);
    EXPECT_NE(text.find("Expression"), std::string::npos);
    EXPECT_NE(text.find("StepFromTheFuture"), std::string::npos);
    EXPECT_NE(text.find("unknown step, 128 payload bytes"), std::string::npos);
    /// The child is indented under the root.
    EXPECT_NE(text.find("\n  StepFromTheFuture"), std::string::npos);
}

TEST(QueryPlanSerialization, SetsRegistryCanonicalOrder)
{
    SerializedSetsRegistry registry;

    /// Adversarial insertion order; values are irrelevant for ordering.
    for (UInt64 value : {7, 1, 5, 3, 6, 2, 4})
    {
        FutureSet::Hash hash;
        hash.low64 = value * 1000003;
        hash.high64 = value % 3;
        registry.sets.emplace(hash, nullptr);
    }

    auto ordered = registry.orderedEntries();
    ASSERT_EQ(ordered.size(), 7u);
    for (size_t i = 1; i < ordered.size(); ++i)
    {
        const auto & prev = ordered[i - 1].first;
        const auto & next = ordered[i].first;
        EXPECT_TRUE(std::tie(prev.high64, prev.low64) < std::tie(next.high64, next.low64));
    }
}
