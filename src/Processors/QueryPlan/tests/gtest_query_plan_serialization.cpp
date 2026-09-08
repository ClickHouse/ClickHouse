#include <gtest/gtest.h>

#include <Columns/ColumnSet.h>
#include <Columns/ColumnsNumber.h>
#include <Common/scope_guard_safe.h>
#include <Core/ProtocolDefines.h>
#include <Core/ServerSettings.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/QueryPlanOutline.h>
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

#include <atomic>
#include <thread>

using namespace DB;

namespace DB::ServerSetting
{
    extern const ServerSettingsUInt64 max_query_plan_serialization_version;
    extern const ServerSettingsUInt64 max_serialized_query_plan_size;
}

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}

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

/// Writes payload bytes its own reader never consumes, so every stream carrying it has a step
/// payload tail. Each step name owns one payload layout, so a tail is always a corrupt stream.
class TestTailStep : public ISourceStep
{
public:
    explicit TestTailStep(SharedHeader header_) : ISourceStep(std::move(header_)) { }

    String getName() const override { return "TestTailStep"; }

    void initializePipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings & /*settings*/) override { }

    bool isSerializable() const override { return true; }

    /// Writes a payload its deserializer never reads, which is the tail the reader has to judge.
    void serialize(Serialization & ctx) const override
    {
        writeVarUInt(UInt64(12345), ctx.out);
    }

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx)
    {
        return std::make_unique<TestTailStep>(ctx.output_header);
    }
};

/// Its deserializer throws, so any reader that actually builds the plan fails on it.
class TestUnreadableStep : public ISourceStep
{
public:
    explicit TestUnreadableStep(SharedHeader header_) : ISourceStep(std::move(header_)) { }

    String getName() const override { return "TestUnreadableStep"; }

    void initializePipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings & /*settings*/) override { }

    bool isSerializable() const override { return true; }

    void serialize(Serialization & /*ctx*/) const override { }

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & /*ctx*/)
    {
        /// Not LOGICAL_ERROR: debug and sanitizer builds turn that into an abort, and this throw
        /// is the point of the step rather than a bug.
        throw Exception(ErrorCodes::INCORRECT_DATA, "TestUnreadableStep must not be built");
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

        /// Writes a payload byte its own reader never consumes, so a stream carrying it has a step
        /// payload tail the reader must reject.
        QueryPlanStepRegistry::instance().registerStep("TestTailStep", TestTailStep::deserialize);

        QueryPlanStepRegistry::instance().registerStep("TestUnreadableStep", TestUnreadableStep::deserialize);
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

/// A chain of alias steps. The payload is big enough that serializing it is not instantaneous,
/// which is what gives a concurrent sender the chance to observe a half-filled cache entry.
QueryPlan makeChainPlan(size_t step_count)
{
    QueryPlan plan = makeSourcePlan();
    for (size_t i = 0; i < step_count; ++i)
    {
        ActionsDAG dag(plan.getCurrentHeader()->getColumnsWithTypeAndName());
        const auto & alias = dag.addAlias(*dag.getInputs().front(), "x_" + std::to_string(i));
        dag.getOutputs().push_back(&alias);
        plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(dag)));
    }
    return plan;
}

std::string serializePlan(const QueryPlan & plan, size_t version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION)
{
    WriteBufferFromOwnString out;
    plan.serialize(out, version);
    out.finalize();
    return out.str();
}

/// The cached bytes for a version, as they would go on the wire.
std::string cachedPlanBytes(const QueryPlan & plan, size_t version)
{
    WriteBufferFromOwnString out;
    plan.writeSerializedTo(out, version);
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

/// For the legacy v3 stream and the current stream: serialize -> deserialize -> serialize must reproduce identical
/// bytes, and the reconstructed plans must explain identically across versions. This pins both
/// the older and the framed formats and per-step determinism.
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

TEST(QueryPlanSerialization, PlanIsSerializedOnceAndOffVersionPeersAreRefused)
{
    registerStepsOnce();
    auto plan = makeSourcePlan();

    ASSERT_GE(DBMS_QUERY_PLAN_SERIALIZATION_VERSION, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE);
    const size_t current = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
    const size_t pre_framed = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE - 1;

    /// The writer serializes once, at its own version. The stream starts with the version varint.
    plan.ensureSerialized(current);
    auto current_bytes = cachedPlanBytes(plan, current);
    ASSERT_FALSE(current_bytes.empty());
    EXPECT_EQ(static_cast<UInt8>(current_bytes[0]), current);

    /// A framed peer newer than the writer reads the very same bytes: one serialization serves every
    /// framed peer, which accepts it by its own reader-version check.
    EXPECT_EQ(cachedPlanBytes(plan, current + 100), current_bytes);

    /// A peer too old for the framed format cannot read those bytes, and the plan is not
    /// re-serialized at an older version for it: it is refused cleanly.
    EXPECT_ANY_THROW(plan.ensureSerialized(pre_framed));
    EXPECT_ANY_THROW(cachedPlanBytes(plan, pre_framed));
}

TEST(QueryPlanSerialization, ChildOrderSurvivesTheRoundTrip)
{
    registerStepsOnce();

    /// Nodes are written children-first, and the two orders that satisfy that (post-order and
    /// reverse pre-order) differ in how siblings come out. Children are positional for binary
    /// steps, so pin that the left child stays the left child. The two children are given
    /// different shapes, since identical ones would hide a swap.
    std::vector<std::unique_ptr<QueryPlan>> plans;
    SharedHeaders input_headers;

    plans.push_back(std::make_unique<QueryPlan>(makeSourcePlan()));

    auto deeper_child = std::make_unique<QueryPlan>(makeSourcePlan());
    deeper_child->addStep(std::make_unique<LimitStep>(deeper_child->getCurrentHeader(), 10, 1));
    plans.push_back(std::move(deeper_child));

    for (const auto & child : plans)
        input_headers.push_back(child->getCurrentHeader());

    QueryPlan plan;
    plan.unitePlans(std::make_unique<UnionStep>(std::move(input_headers)), std::move(plans));

    auto restored = deserializePlan(serializePlan(plan));
    const auto explain = debugExplainPlan(restored);

    /// The plain source is the left child, so it must appear before the `Limit` subtree.
    const auto source = explain.find("TestSource");
    const auto limit = explain.find("Limit");
    ASSERT_NE(source, std::string::npos) << explain;
    ASSERT_NE(limit, std::string::npos) << explain;
    EXPECT_LT(source, limit) << "children came back swapped:\n" << explain;
}

TEST(QueryPlanSerialization, SkippingDrainsThePlanWithoutBuildingIt)
{
    registerStepsOnce();

    QueryPlan plan;
    plan.addStep(std::make_unique<TestUnreadableStep>(makeTestHeader()));
    const std::string bytes = serializePlan(plan) + "sentinel";

    /// Building the plan reaches the step and fails.
    {
        ReadBufferFromString in(bytes);
        EXPECT_THROW(QueryPlan::deserialize(in, getContext().context, /*max_type_complexity=*/0), Exception);
    }

    /// Draining takes the framed plan off the stream without constructing anything, and stops exactly
    /// at its end, so whatever the protocol put after it is still readable.
    ReadBufferFromString in(bytes);
    EXPECT_NO_THROW(QueryPlan::deserialize(in, getContext().context, /*max_type_complexity=*/0, /*skip_data=*/true));

    String rest;
    readStringUntilEOF(rest, in);
    EXPECT_EQ(rest, "sentinel");
}

TEST(QueryPlanSerialization, PlanArrivingInPiecesIsRead)
{
    registerStepsOnce();
    const std::string bytes = serializePlan(makeChainPlan(4));

    /// Two pieces, so no frame is ever fully available when the reader starts on it. That is what
    /// a socket delivers for a plan bigger than what has arrived so far.
    const size_t split = bytes.size() / 2;
    ConcatReadBuffer::Buffers pieces;
    pieces.emplace_back(std::make_unique<ReadBufferFromMemory>(bytes.data(), split));
    pieces.emplace_back(std::make_unique<ReadBufferFromMemory>(bytes.data() + split, bytes.size() - split));
    ConcatReadBuffer in(std::move(pieces));

    auto plan_and_sets = QueryPlan::deserialize(in, getContext().context, /*max_type_complexity=*/0);
    auto restored = QueryPlan::makeSets(std::move(plan_and_sets), getContext().context);
    EXPECT_EQ(serializePlan(restored), bytes);
}

TEST(QueryPlanSerialization, QueryPicksTheWriterVersion)
{
    registerStepsOnce();
    auto plan = makeSourcePlan();

    /// Not asking writes the server default.
    EXPECT_EQ(static_cast<UInt8>(serializePlan(plan)[0]), DBMS_DEFAULT_QUERY_PLAN_SERIALIZATION_VERSION);

    /// Asking for an older version writes that one, whatever the peer could read.
    WriteBufferFromOwnString asked;
    plan.serialize(asked, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, /*requested_version=*/3);
    asked.finalize();
    EXPECT_EQ(static_cast<UInt8>(asked.str()[0]), 3);
    EXPECT_EQ(debugExplainPlan(deserializePlan(asked.str())), debugExplainPlan(plan));

    /// Asking for one this server cannot write is an error, not a silently older plan.
    WriteBufferFromOwnString too_new;
    EXPECT_THROW(
        plan.serialize(too_new, DBMS_QUERY_PLAN_SERIALIZATION_VERSION,
            /*requested_version=*/DBMS_QUERY_PLAN_SERIALIZATION_VERSION + 1),
        Exception);
}

TEST(QueryPlanSerialization, WriterVersionCanBeHeldBack)
{
    registerStepsOnce();
    auto plan = makeSourcePlan();

    /// Server settings live in the shared context, so the old value has to come back.
    const UInt64 old_clamp = getContext().context->getServerSettings()[ServerSetting::max_query_plan_serialization_version];
    SCOPE_EXIT({ getContext().context->setServerSetting("max_query_plan_serialization_version", old_clamp); });

    /// Held at 3, the writer emits the legacy stream even though it can write the current version,
    /// and even for a peer that could read the newer one.
    getContext().context->setServerSetting("max_query_plan_serialization_version", UInt64(3));
    const std::string held = serializePlan(plan, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    ASSERT_FALSE(held.empty());
    EXPECT_EQ(static_cast<UInt8>(held[0]), 3);
    EXPECT_EQ(debugExplainPlan(deserializePlan(held)), debugExplainPlan(plan));

    /// The kept bytes are the version actually written: held at 3, `ensureSerialized` serializes
    /// and serves version 3, not the current one.
    plan.ensureSerialized(DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    EXPECT_EQ(static_cast<UInt8>(cachedPlanBytes(plan, DBMS_QUERY_PLAN_SERIALIZATION_VERSION)[0]), 3);

    getContext().context->setServerSetting("max_query_plan_serialization_version", UInt64(0));
    const std::string current = serializePlan(plan, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    EXPECT_EQ(static_cast<UInt8>(current[0]), DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
}

TEST(QueryPlanSerialization, FramedPlanSizeLimitComesFromTheServerSetting)
{
    registerStepsOnce();
    const std::string bytes = serializePlan(makeChainPlan(4));

    /// The plan passes at the default limit.
    ReadBufferFromString in(bytes);
    EXPECT_NO_THROW(QueryPlan::deserialize(in, getContext().context, /*max_type_complexity=*/0));

    /// Server settings live in the shared context, so the old value has to come back for the
    /// other tests in this binary.
    const UInt64 old_limit = getContext().context->getServerSettings()[ServerSetting::max_serialized_query_plan_size];
    getContext().context->setServerSetting("max_serialized_query_plan_size", UInt64(8));
    SCOPE_EXIT({ getContext().context->setServerSetting("max_serialized_query_plan_size", old_limit); });

    /// A limit below the plan size rejects it instead of buffering the plan.
    ReadBufferFromString limited_in(bytes);
    EXPECT_THROW(QueryPlan::deserialize(limited_in, getContext().context, /*max_type_complexity=*/0), Exception);
}

TEST(QueryPlanSerialization, PayloadTailIsRejectedAtAKnownStepFormat)
{
    registerStepsOnce();

    QueryPlan plan;
    plan.addStep(std::make_unique<TestTailStep>(makeTestHeader()));

    /// Format 1 is a format this build knows in full, so bytes the step did not read mean the
    /// stream is corrupt rather than newer.
    EXPECT_THROW(deserializePlan(serializePlan(plan)), Exception);
}

TEST(QueryPlanSerialization, ConcurrentSendersGetTheWholePlan)
{
    registerStepsOnce();

    const size_t thread_count = 8;
    const size_t iterations = 100;

    for (size_t iteration = 0; iteration < iterations; ++iteration)
    {
        /// Every iteration needs a cold cache: the race is between the sender that fills the entry
        /// and the ones arriving while it is still writing. All the replicas of a query share one
        /// plan and send it from their own threads, so they all land here at the same time.
        auto plan = makeChainPlan(16);
        const std::string reference = serializePlan(plan);

        std::atomic<size_t> ready = 0;
        std::atomic<bool> start = false;
        std::vector<std::string> sent(thread_count);
        std::vector<std::thread> senders;

        for (size_t i = 0; i < thread_count; ++i)
        {
            senders.emplace_back([&, i]
            {
                ready.fetch_add(1);
                while (!start.load())
                    std::this_thread::yield();

                plan.ensureSerialized(DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
                sent[i] = cachedPlanBytes(plan, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
            });
        }

        while (ready.load() < thread_count)
            std::this_thread::yield();
        start.store(true);

        for (auto & sender : senders)
            sender.join();

        for (size_t i = 0; i < thread_count; ++i)
            ASSERT_EQ(sent[i], reference) << "sender " << i << " on iteration " << iteration;
    }
}

namespace
{

/// A two-node outline: root "Expression" with one "TestSource" child.
/// Children-first, so the leaf comes first and the root is the last node.
PlanOutline makeTestOutline()
{
    PlanOutline outline;

    PlanOutline::Node leaf;
    leaf.step_name = "TestSource";
    leaf.step_format_version = 1;
    leaf.header = makeTestHeader();
    outline.nodes.push_back(std::move(leaf));

    PlanOutline::Node root;
    root.children = {0};
    root.step_name = "Expression";
    root.step_format_version = 1;
    root.header = makeTestHeader();
    outline.nodes.push_back(std::move(root));

    return outline;
}

std::string writeOutlineToString(const PlanOutline & outline)
{
    WriteBufferFromOwnString out;
    writeQueryPlanOutline(outline, out);
    out.finalize();
    return out.str();
}

/// A whole stream built by hand, for the streams a writer of this build cannot produce.
std::string writeFramedPlanToString(const PlanOutline & outline, const std::vector<std::string> & payloads)
{
    const std::string outline_bytes = writeOutlineToString(outline);

    WriteBufferFromOwnString out;
    writeVarUInt(UInt64(DBMS_QUERY_PLAN_SERIALIZATION_VERSION), out);
    writeVarUInt(UInt64(DBMS_QUERY_PLAN_FORMAT_KIND_OUTLINE), out);
    out.write(outline_bytes.data(), outline_bytes.size());
    /// Each payload carries its size just before its bytes.
    for (const auto & payload : payloads)
    {
        writeVarUInt(payload.size(), out);
        out.write(payload.data(), payload.size());
    }
    out.finalize();
    return out.str();
}

}

TEST(QueryPlanOutline, StepInputCountIsValidated)
{
    registerStepsOnce();

    auto make_node = [](const char * name, std::vector<UInt64> children)
    {
        PlanOutline::Node outline_node;
        outline_node.children = std::move(children);
        outline_node.step_name = name;
        outline_node.step_format_version = 1;
        outline_node.header = makeTestHeader();
        return outline_node;
    };

    /// A single transforming step with no child. The tree shape is valid, but the step reads one
    /// input, so building it would dereference a header that is not there.
    {
        PlanOutline outline;
        outline.nodes.push_back(make_node("Expression", {}));
        auto result = validateQueryPlanOutline(outline);
        ASSERT_FALSE(result.ok());
        EXPECT_NE(result.describe().find("has 0 inputs but reads 1"), std::string::npos) << result.describe();
    }

    /// A source step given a child is rejected the same way.
    {
        PlanOutline outline;
        outline.nodes.push_back(make_node("TestSource", {}));
        outline.nodes.push_back(make_node("ReadNothing", {0}));
        auto result = validateQueryPlanOutline(outline);
        ASSERT_FALSE(result.ok());
        EXPECT_NE(result.describe().find("has 1 inputs but reads 0"), std::string::npos) << result.describe();
    }

    /// The matching shape passes the arity check.
    {
        PlanOutline outline;
        outline.nodes.push_back(make_node("TestSource", {}));
        outline.nodes.push_back(make_node("Expression", {0}));
        EXPECT_TRUE(validateQueryPlanOutline(outline).ok());
    }
}

TEST(QueryPlanOutline, WriteReadRoundTrip)
{
    registerStepsOnce();
    auto outline = makeTestOutline();
    /// Descriptions travel only when the writer opts in; this round trip checks that path.
    outline.include_step_descriptions = true;
    outline.nodes[1].step_description = "test description";
    outline.nodes[0].settings.push_back({.name = "max_block_size", .flags = 0, .value = "\x01"});

    auto bytes = writeOutlineToString(outline);

    ReadBufferFromString in(bytes);
    auto restored = readQueryPlanOutline(in, /*max_type_complexity=*/0, /*max_frame_bytes=*/bytes.size());
    EXPECT_TRUE(in.eof());

    ASSERT_EQ(restored.nodes.size(), 2u);
    /// The root is the last node, its child the first.
    EXPECT_EQ(restored.nodes[1].step_name, "Expression");
    EXPECT_EQ(restored.nodes[1].children.size(), 1u);
    EXPECT_EQ(restored.nodes[1].step_description, "test description");
    ASSERT_TRUE(restored.nodes[1].header);
    EXPECT_EQ(restored.nodes[1].header->columns(), 2u);
    ASSERT_EQ(restored.nodes[0].settings.size(), 1u);
    EXPECT_EQ(restored.nodes[0].settings[0].name, "max_block_size");

    /// Re-write must reproduce identical bytes.
    EXPECT_EQ(writeOutlineToString(restored), bytes);
}

TEST(QueryPlanOutline, ValidationCollectsAllIssues)
{
    registerStepsOnce();
    auto outline = makeTestOutline();

    outline.nodes[0].step_name = "NoSuchStep";
    outline.nodes[0].header = nullptr;
    outline.nodes[1].settings.push_back({.name = "no_such_setting", .flags = 0, .value = ""});

    auto result = validateQueryPlanOutline(outline);
    ASSERT_FALSE(result.ok());
    /// All three problems reported at once, not just the first.
    EXPECT_EQ(result.issues.size(), 3u) << result.describe();
    EXPECT_NE(result.describe().find("NoSuchStep"), std::string::npos);
    EXPECT_NE(result.describe().find("no output header"), std::string::npos);
    EXPECT_NE(result.describe().find("no_such_setting"), std::string::npos);
}

TEST(QueryPlanOutline, ValidationAcceptsIgnorableUnknownSetting)
{
    registerStepsOnce();
    auto outline = makeTestOutline();
    outline.nodes[0].settings.push_back(
        {.name = "future_setting", .flags = PlanOutline::SettingEntry::FLAG_IGNORABLE, .value = ""});

    EXPECT_TRUE(validateQueryPlanOutline(outline).ok());
}

TEST(QueryPlanOutline, ValidationRejectsUnknownStepPayloadFormat)
{
    registerStepsOnce();

    /// Each step name owns one payload layout, numbered 1. Any other value is a payload this
    /// server cannot read, so the plan is refused on the outline before a step is built.
    auto outline = makeTestOutline();
    outline.nodes[1].step_format_version = 2;
    auto result = validateQueryPlanOutline(outline);
    ASSERT_FALSE(result.ok());
    EXPECT_NE(result.describe().find("unknown payload format version 2"), std::string::npos) << result.describe();
}

TEST(QueryPlanSerialization, TooNewVersionAndUnknownKindAreRefused)
{
    registerStepsOnce();
    auto plan = makeSourcePlan();

    std::string bytes = serializePlan(plan);
    /// Head layout: [plan_version][format_kind]... - both single-byte varints for a plan this small.
    ASSERT_GE(bytes.size(), 2u);
    ASSERT_EQ(static_cast<UInt8>(bytes[0]), DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    ASSERT_EQ(static_cast<UInt8>(bytes[1]), DBMS_QUERY_PLAN_FORMAT_KIND_OUTLINE);

    /// The version is a coarse gate: a version above the one this server supports is refused. The
    /// body of a known kind is still walkable, so the connection would be kept (checked elsewhere).
    std::string too_new = bytes;
    too_new[0] = static_cast<char>(DBMS_QUERY_PLAN_SERIALIZATION_VERSION + 1);
    ReadBufferFromString in(too_new);
    EXPECT_THROW(QueryPlan::deserialize(in, getContext().context, 0), Exception);

    /// A body layout this server does not know is refused on the kind alone.
    std::string unknown_kind = bytes;
    unknown_kind[1] = static_cast<char>(DBMS_QUERY_PLAN_FORMAT_KIND_OUTLINE + 1);
    ReadBufferFromString unknown_in(unknown_kind);
    EXPECT_THROW(QueryPlan::deserialize(unknown_in, getContext().context, 0), Exception);
}

TEST(QueryPlanOutline, ValidationChecksTreeStructureAndSetOrder)
{
    registerStepsOnce();
    auto outline = makeTestOutline();

    outline.nodes[1].children = {0, 1};  /// A child index that is the root itself, so it does not precede it.
    EXPECT_FALSE(validateQueryPlanOutline(outline).ok());
    outline.nodes[1].children = {0};

    PlanOutline::SetEntry set1;
    set1.hash.low64 = 10;
    set1.hash.high64 = 1;
    set1.kind = 2;
    PlanOutline::SetEntry set2;
    set2.hash.low64 = 5;
    set2.hash.high64 = 0;
    set2.kind = 200;  /// Unknown kind.
    outline.sets = {set1, set2};  /// Also not sorted by hash.

    auto result = validateQueryPlanOutline(outline);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.issues.size(), 2u) << result.describe();
}

TEST(QueryPlanSerialization, RejectedPlansAreStillTakenOffTheStream)
{
    registerStepsOnce();
    const std::string plan_bytes = serializePlan(makeSourcePlan());

    /// What follows the plan on a real connection is the next protocol packet. A plan this server
    /// turns down still has to be walked to its end, or that packet is read as plan bytes. This holds
    /// only for a body layout the reader can walk; an unknown layout has no known end and closes the
    /// connection instead, which is checked elsewhere.
    const std::string sentinel = "the next protocol packet";

    /// A version above the one this server supports, on a body layout it can still walk.
    {
        std::string too_new = plan_bytes;
        too_new[0] = static_cast<char>(DBMS_QUERY_PLAN_SERIALIZATION_VERSION + 1);
        /// `ReadBufferFromString` does not own its bytes, so the stream has to outlive the reader.
        const std::string stream = too_new + sentinel;
        ReadBufferFromString in(stream);
        EXPECT_THROW(QueryPlan::deserialize(in, getContext().context, /*max_type_complexity=*/0), Exception);

        String rest;
        readStringUntilEOF(rest, in);
        EXPECT_EQ(rest, sentinel) << "a too-new plan left bytes on the stream";
    }

    /// A plan refused on its outline: the payload frames behind it are still plan bytes and must be
    /// skipped. The two nodes need two payload frames; their contents do not matter, since the plan
    /// is turned down before any step is built.
    {
        auto outline = makeTestOutline();
        outline.nodes[0].step_name = "NoSuchStep";
        const std::string stream = writeFramedPlanToString(outline, {"", ""}) + sentinel;

        ReadBufferFromString in(stream);
        EXPECT_THROW(QueryPlan::deserialize(in, getContext().context, /*max_type_complexity=*/0), Exception);

        String rest;
        readStringUntilEOF(rest, in);
        EXPECT_EQ(rest, sentinel) << "a plan rejected on its outline left bytes on the stream";
    }
}

TEST(QueryPlanSerialization, AStepThatRefusesItsPayloadLeavesTheStreamAtTheNextPacket)
{
    registerStepsOnce();

    /// The unreadable step is the child, so it throws while its parent's payload is still on the
    /// stream. A failure in the middle of the body has to take the rest of the body with it.
    QueryPlan plan;
    plan.addStep(std::make_unique<TestUnreadableStep>(makeTestHeader()));
    ActionsDAG dag(plan.getCurrentHeader()->getColumnsWithTypeAndName());
    const auto & alias = dag.addAlias(*dag.getInputs().front(), "y");
    dag.getOutputs().push_back(&alias);
    plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(dag)));

    const std::string sentinel = "the next protocol packet";
    const std::string stream = serializePlan(plan) + sentinel;

    ReadBufferFromString in(stream);
    EXPECT_THROW(QueryPlan::deserialize(in, getContext().context, /*max_type_complexity=*/0), Exception);

    String rest;
    readStringUntilEOF(rest, in);
    EXPECT_EQ(rest, sentinel) << "a step that threw left plan bytes on the stream";
}

TEST(QueryPlanSerialization, OutlineFrameCannotEscapeThePlanSizeLimit)
{
    registerStepsOnce();

    /// A head followed by an outline frame that claims far more bytes than the plan-size limit
    /// allows. The frame must be rejected on its declared size, before anything is allocated and
    /// before the reader takes the bytes that belong to whatever follows the plan on the connection.
    WriteBufferFromOwnString head;
    writeVarUInt(UInt64(DBMS_QUERY_PLAN_SERIALIZATION_VERSION), head);
    writeVarUInt(UInt64(DBMS_QUERY_PLAN_FORMAT_KIND_OUTLINE), head);
    writeVarUInt(UInt64(32) << 20, head);                       /// the outline frame claims 32 MiB
    head.finalize();

    const std::string bytes = head.str() + "bytes that belong to the protocol";

    const UInt64 old_limit = getContext().context->getServerSettings()[ServerSetting::max_serialized_query_plan_size];
    getContext().context->setServerSetting("max_serialized_query_plan_size", UInt64(8));
    SCOPE_EXIT({ getContext().context->setServerSetting("max_serialized_query_plan_size", old_limit); });

    ReadBufferFromString in(bytes);
    EXPECT_THROW(QueryPlan::deserialize(in, getContext().context, /*max_type_complexity=*/0), Exception);
    EXPECT_LE(in.count(), head.str().size()) << "the reader consumed past the head";
}

TEST(QueryPlanSerialization, SetRowCountCannotExceedItsFrame)
{
    registerStepsOnce();

    /// A set frame holding only two varints, whose row count claims far more rows than a frame
    /// that size could ever carry.
    WriteBufferFromOwnString set_frame;
    writeVarUInt(UInt64(1), set_frame);          /// one column
    writeVarUInt(UInt64(1) << 40, set_frame);    /// ... with a trillion rows
    set_frame.finalize();

    PlanOutline outline;
    PlanOutline::SetEntry entry;
    entry.hash.low64 = 1;
    entry.hash.high64 = 0;
    entry.kind = UInt8(SetSerializationKind::TupleValues);
    outline.sets.push_back(entry);

    auto column_set = ColumnSet::create(1, nullptr);
    DeserializedSetsRegistry registry;
    registry.sets[entry.hash].push_back(column_set.get());

    QueryPlan::SerializationFlags flags;
    flags.version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;

    /// The set travels as its own sized frame: its size, then its bytes.
    WriteBufferFromOwnString stream_buf;
    writeVarUInt(set_frame.str().size(), stream_buf);
    stream_buf.write(set_frame.str().data(), set_frame.str().size());
    stream_buf.finalize();

    ReadBufferFromString in(stream_buf.str());
    UInt64 frames_consumed = 0;
    try
    {
        deserializeFramedSets(
            QueryPlan{}, registry, outline, in, flags, getContext().context, /*max_type_complexity=*/0,
            /*max_plan_bytes=*/UInt64(1) << 20, /*body_start=*/0, frames_consumed);
        FAIL() << "the row count should have been rejected";
    }
    catch (const Exception & e)
    {
        /// Checking the message, not just that it threw: reading on past the counts hits the end
        /// of the frame and throws anyway, which would pass with the row count never looked at.
        EXPECT_NE(e.message().find("rows but only"), std::string::npos) << e.message();
    }
}

TEST(QueryPlanOutline, ValidationRejectsMalformedChildCounts)
{
    registerStepsOnce();

    /// Nothing to be the root.
    EXPECT_FALSE(validateQueryPlanOutline(PlanOutline{}).ok());

    /// The first node cannot have children: nothing precedes it.
    auto under_run = makeTestOutline();
    under_run.nodes[0].children = {0};
    EXPECT_FALSE(validateQueryPlanOutline(under_run).ok());

    /// A leaf that no step takes as input leaves the plan without a single root.
    auto two_roots = makeTestOutline();
    two_roots.nodes[1].children = {};
    auto result = validateQueryPlanOutline(two_roots);
    EXPECT_FALSE(result.ok());
    EXPECT_NE(result.describe().find("not an input of any step"), std::string::npos) << result.describe();
}

TEST(QueryPlanOutline, ReservedFlagBitsAreRejected)
{
    registerStepsOnce();

    /// The spare bits of both flag bytes mean nothing yet. A writer that sets one is asking for
    /// behavior this reader does not have, so it cannot pretend the bit was not there.
    {
        auto bytes = writeOutlineToString(makeTestOutline());

        /// Everything before the first node's flag byte is one byte except the step name: outline
        /// size, the two plan-level limits, the descriptions flag, node count, set count, the node
        /// record size, child count, name length, name and format version.
        const std::string first_step_name = "TestSource";
        const size_t flags_at = 10 + first_step_name.size();
        ASSERT_EQ(bytes[flags_at], char(1)) << "the node flag byte is not where this test expects it";
        bytes[flags_at] = char(1 | 2);

        ReadBufferFromString in(bytes);
        EXPECT_THROW(readQueryPlanOutline(in, /*max_type_complexity=*/0, /*max_frame_bytes=*/bytes.size()), Exception);
    }

    {
        auto outline = makeTestOutline();
        outline.nodes[0].settings.push_back({.name = "future_setting", .flags = 0x80, .value = ""});
        auto bytes = writeOutlineToString(outline);

        ReadBufferFromString in(bytes);
        EXPECT_THROW(readQueryPlanOutline(in, /*max_type_complexity=*/0, /*max_frame_bytes=*/bytes.size()), Exception);
    }
}

TEST(QueryPlanOutline, TruncatedOutlineIsRejected)
{
    registerStepsOnce();
    auto bytes = writeOutlineToString(makeTestOutline());

    /// Truncation at every byte boundary must throw, never crash or misread.
    for (size_t cut = 0; cut < bytes.size(); ++cut)
    {
        std::string truncated = bytes.substr(0, cut);
        ReadBufferFromString in(truncated);
        EXPECT_ANY_THROW(readQueryPlanOutline(in, 0, bytes.size())) << "no exception at cut " << cut;
    }
}

TEST(QueryPlanOutline, TrailingBytesInsideFrameAreRejected)
{
    registerStepsOnce();
    auto bytes = writeOutlineToString(makeTestOutline());

    /// Grow the declared frame size by one and append a byte: the outline content then ends
    /// before its frame does, which must be rejected, not silently accepted.
    ReadBufferFromString size_in(bytes);
    UInt64 outline_size = 0;
    readVarUInt(outline_size, size_in);
    size_t size_prefix_bytes = bytes.size() - size_in.available();

    WriteBufferFromOwnString patched;
    writeVarUInt(outline_size + 1, patched);
    patched.write(bytes.data() + size_prefix_bytes, bytes.size() - size_prefix_bytes);
    writeChar('\0', patched);
    patched.finalize();

    ReadBufferFromString in(patched.str());
    EXPECT_ANY_THROW(readQueryPlanOutline(in, 0, bytes.size()));
}

TEST(QueryPlanOutline, FormatShowsShapeWithUnknownSteps)
{
    registerStepsOnce();
    auto outline = makeTestOutline();
    outline.nodes[0].step_name = "StepFromTheFuture";

    auto text = formatQueryPlanOutline(outline);
    EXPECT_NE(text.find("Expression"), std::string::npos);
    EXPECT_NE(text.find("StepFromTheFuture"), std::string::npos);
    EXPECT_NE(text.find("unknown step"), std::string::npos);
    /// The child is indented under the root.
    EXPECT_NE(text.find("\n  StepFromTheFuture"), std::string::npos);
}

TEST(QueryPlanSerialization, SetsRegistryEntriesAreSortedByHash)
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

    auto ordered = registry.entriesSortedByHash();
    ASSERT_EQ(ordered.size(), 7u);
    for (size_t i = 1; i < ordered.size(); ++i)
    {
        const auto & prev = ordered[i - 1].first;
        const auto & next = ordered[i].first;
        EXPECT_TRUE(std::tie(prev.high64, prev.low64) < std::tie(next.high64, next.low64));
    }
}
