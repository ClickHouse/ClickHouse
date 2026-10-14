#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

/// The version tests never build a step, so the create function is a placeholder.
QueryPlanStepPtr nullCreate(IQueryPlanStep::Deserialization &)
{
    return nullptr;
}

}

/// The writer serializes at the plan version both peers support and must send the newest step version
/// that peer's release knows, so an older peer gets the older bytes.
TEST(QueryPlanStepRegistryVersions, WriteVersionFollowsPlanVersion)
{
    QueryPlanStepRegistry registry;
    /// Declared out of order on purpose: registration orders the entries by the release that introduced them.
    registry.registerStep("Test", nullCreate, {{5, 18}, {3, 15}, {4, 16}});

    EXPECT_EQ(registry.versionToWrite("Test", 15), 3u);
    EXPECT_EQ(registry.versionToWrite("Test", 16), 4u);
    EXPECT_EQ(registry.versionToWrite("Test", 17), 4u);
    EXPECT_EQ(registry.versionToWrite("Test", 18), 5u);
    EXPECT_EQ(registry.versionToWrite("Test", 100), 5u);
}

/// A step version this binary does not know is refused up front, so wrong-code bytes are never
/// misparsed. This is the protection the per-step version buys on master.
TEST(QueryPlanStepRegistryVersions, UnknownReadVersionIsRefused)
{
    QueryPlanStepRegistry registry;
    registry.registerStep("Test", nullCreate, {{3, 15}, {4, 16}});

    EXPECT_NO_THROW(registry.checkVersionReadable("Test", 3));
    EXPECT_NO_THROW(registry.checkVersionReadable("Test", 4));

    try
    {
        registry.checkVersionReadable("Test", 5);
        FAIL() << "expected INCORRECT_DATA for an unknown step version";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA);
    }
}

/// A step whose bytes have never changed stays at version 0 and needs no version declaration.
TEST(QueryPlanStepRegistryVersions, DefaultStepStaysAtVersionZero)
{
    QueryPlanStepRegistry registry;
    registry.registerStep("Plain", nullCreate);

    EXPECT_EQ(registry.versionToWrite("Plain", DBMS_QUERY_PLAN_SERIALIZATION_VERSION), 0u);
    EXPECT_NO_THROW(registry.checkVersionReadable("Plain", 0));
    EXPECT_ANY_THROW(registry.checkVersionReadable("Plain", 1));
}

/// The plan version moves for many reasons unrelated to this step. None of them changes what the step
/// writes or requires touching its entry: the entry changes only when the step's own bytes change.
TEST(QueryPlanStepRegistryVersions, UnrelatedPlanVersionBumpsDoNotTouchTheStep)
{
    QueryPlanStepRegistry registry;
    registry.registerStep("Test", nullCreate, {{1, 16}});

    for (UInt64 plan_version : {UInt64(16), UInt64(17), UInt64(25), UInt64(100)})
        EXPECT_EQ(registry.versionToWrite("Test", plan_version), 1u) << "plan version " << plan_version;
}

/// A critical fix backported into a released line gets a new step version anchored at that release's
/// plan version. Peers of that release then receive the fixed bytes, and the old version stays readable
/// from peers that do not have the fix yet.
TEST(QueryPlanStepRegistryVersions, BackportedFixIsWrittenAtItsReleaseVersion)
{
    QueryPlanStepRegistry registry;
    registry.registerStep("Test", nullCreate, {{1, 16}, {2, 16}});

    EXPECT_EQ(registry.versionToWrite("Test", 16), 2u);
    EXPECT_EQ(registry.versionToWrite("Test", 17), 2u);
    EXPECT_NO_THROW(registry.checkVersionReadable("Test", 1));
    EXPECT_NO_THROW(registry.checkVersionReadable("Test", 2));
}

namespace
{

/// A source step that exists only here. Its payload is written from step version 1 on, so a full
/// round trip that recovers the payload proves the per-step version reaches both `serialize` and
/// `deserialize` through `QueryPlan::serialize` / `QueryPlan::deserialize`, not only the registry
/// helper checked above.
class VersionedTestStep : public ISourceStep
{
public:
    VersionedTestStep(SharedHeader header, UInt64 payload_)
        : ISourceStep(std::move(header))
        , payload(payload_)
    {
    }

    String getName() const override { return "VersionedTest"; }
    QueryPlanStepPtr clone() const override { return std::make_unique<VersionedTestStep>(getOutputHeader(), payload); }
    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override { }
    bool isSerializable() const override { return true; }

    UInt64 getPayload() const { return payload; }

    void serialize(Serialization & ctx) const override
    {
        if (ctx.step_version >= 1)
            writeVarUInt(payload, ctx.out);
    }

    static QueryPlanStepPtr deserialize(Deserialization & ctx)
    {
        UInt64 payload = 0;
        if (ctx.step_version >= 1)
            readVarUInt(payload, ctx.in);
        return std::make_unique<VersionedTestStep>(ctx.output_header, payload);
    }

private:
    UInt64 payload = 0;
};

void tryRegisterVersionedTestStep()
{
    /// The registry is a process-wide singleton and rejects duplicate names, so register once.
    static struct Register
    {
        Register()
        {
            QueryPlanStepRegistry::instance().registerStep(
                "VersionedTest",
                &VersionedTestStep::deserialize,
                {{0, 0}, {1, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_STEP_VERSIONS}});
        }
    } registered;
}

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "k")}));
}

/// Round-trips a one-node plan through `QueryPlan::serialize` / `QueryPlan::deserialize` at `plan_version`.
QueryPlan roundTrip(UInt64 payload, UInt64 plan_version)
{
    tryRegisterVersionedTestStep();

    QueryPlan plan;
    plan.addStep(std::make_unique<VersionedTestStep>(makeHeader(), payload));

    WriteBufferFromOwnString out;
    plan.serialize(out, plan_version);

    ReadBufferFromString in(out.str());
    auto deserialized = QueryPlan::deserialize(in, getContext().context, /*max_type_complexity=*/0);
    return QueryPlan::makeSets(std::move(deserialized), getContext().context);
}

const VersionedTestStep * rootStep(const QueryPlan & plan)
{
    return dynamic_cast<const VersionedTestStep *>(plan.getRootNode()->step.get());
}

}

/// At a plan version that carries step versions, the step is written at version 1 and its
/// version-gated payload comes back intact: the per-step version reaches both `serialize` and
/// `deserialize` through the real packet path, not only the registry helper.
TEST(QueryPlanStepVersionRoundTrip, NonZeroStepVersionCarriesThePayload)
{
    auto restored = roundTrip(0x9abcdef1u, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    const auto * step = rootStep(restored);
    ASSERT_NE(step, nullptr);
    EXPECT_EQ(step->getPayload(), 0x9abcdef1u);
}

/// Below the version that introduced step versions, no per-step version is written and the step is
/// read at version 0, so the payload the step gained at version 1 is not carried. This also checks
/// that the older wire layout is unchanged: an extra varint would corrupt the fields after it.
TEST(QueryPlanStepVersionRoundTrip, VersionZeroStreamDoesNotCarryThePayload)
{
    auto restored = roundTrip(0x9abcdef1u, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_STEP_VERSIONS - 1);
    const auto * step = rootStep(restored);
    ASSERT_NE(step, nullptr);
    EXPECT_EQ(step->getPayload(), 0u);
}
