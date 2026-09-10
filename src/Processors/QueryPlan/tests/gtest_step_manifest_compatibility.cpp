#include <gtest/gtest.h>

#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/StepManifest.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
}

/// Two builds of the server exchange plans in one process. The older build speaks the current plan
/// version and knows the step `Versioned`. The newer build speaks the next plan version, ships a
/// result-changing fix to `Versioned` under the new name `VersionedFixed`, and has a whole step
/// `Future` the older build has never heard of. Each build is a registry; a scope makes it the
/// current one for a thread.
namespace
{

constexpr UInt64 old_build_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
constexpr UInt64 new_build_version = old_build_version + 1;

constexpr auto L = WireFieldClass::Logical;

struct VersionedWire
{
    UInt64 value = 0;

    bool operator==(const VersionedWire &) const = default;
};

struct FutureWire
{
};

class VersionedStepBase : public ISourceStep
{
public:
    VersionedStepBase(SharedHeader header_, VersionedWire wire_) : ISourceStep(std::move(header_)), wire(std::move(wire_)) { }

    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override { }
    bool isSerializable() const override { return true; }

    VersionedWire wire;
};

class VersionedStep : public VersionedStepBase
{
public:
    using VersionedStepBase::VersionedStepBase;
    String getName() const override { return "Versioned"; }
    void serialize(Serialization & ctx) const override;
    static QueryPlanStepPtr deserialize(Deserialization & ctx);
};

/// The result-changing fix to `Versioned`, carried by a new name. It is available from the framed
/// baseline, so it does not move the plan version: an older build passes the version check but does
/// not have the name, so it refuses by the name rather than running the wrong step.
class FixedVersionedStep : public VersionedStepBase
{
public:
    using VersionedStepBase::VersionedStepBase;
    String getName() const override { return "VersionedFixed"; }
    void serialize(Serialization & ctx) const override;
    static QueryPlanStepPtr deserialize(Deserialization & ctx);
};

/// A step that exists only in the newer build.
class FutureStep : public ISourceStep
{
public:
    explicit FutureStep(SharedHeader header_) : ISourceStep(std::move(header_)) { }

    String getName() const override { return "Future"; }
    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override { }
    bool isSerializable() const override { return true; }
    void serialize(Serialization & ctx) const override;
    static QueryPlanStepPtr deserialize(Deserialization & ctx);
};

constexpr auto VERSIONED_MANIFEST = StepManifest<VersionedStep, VersionedWire>("Versioned")
    .nameIntroducedIn(1)
    .baseFormat(field("value", L, &VersionedWire::value));

constexpr auto FIXED_VERSIONED_MANIFEST = StepManifest<FixedVersionedStep, VersionedWire>("VersionedFixed")
    .nameIntroducedIn(1)
    .baseFormat(field("value", L, &VersionedWire::value));

constexpr auto FUTURE_MANIFEST = StepManifest<FutureStep, FutureWire>("Future")
    .nameIntroducedIn(new_build_version)
    .baseFormat();

void VersionedStep::serialize(Serialization & ctx) const
{
    writeManifestPayload(VERSIONED_MANIFEST, wire, ctx);
}

QueryPlanStepPtr VersionedStep::deserialize(Deserialization & ctx)
{
    return std::make_unique<VersionedStep>(ctx.output_header, readManifestPayload(VERSIONED_MANIFEST, ctx));
}

void FixedVersionedStep::serialize(Serialization & ctx) const
{
    writeManifestPayload(FIXED_VERSIONED_MANIFEST, wire, ctx);
}

QueryPlanStepPtr FixedVersionedStep::deserialize(Deserialization & ctx)
{
    return std::make_unique<FixedVersionedStep>(ctx.output_header, readManifestPayload(FIXED_VERSIONED_MANIFEST, ctx));
}

void FutureStep::serialize(Serialization & ctx) const
{
    writeManifestPayload(FUTURE_MANIFEST, FutureWire{}, ctx);
}

QueryPlanStepPtr FutureStep::deserialize(Deserialization & ctx)
{
    readManifestPayload(FUTURE_MANIFEST, ctx);
    return std::make_unique<FutureStep>(ctx.output_header);
}

/// A build of the server: the plan version it speaks and the steps it knows.
struct Build
{
    QueryPlanStepRegistry registry;

    Build(UInt64 version, void (*register_steps)(QueryPlanStepRegistry &))
    {
        registry.setSupportedVersion(version);
        register_steps(registry);
    }
};

Build & oldBuild()
{
    static Build build(old_build_version, [](QueryPlanStepRegistry & registry)
    {
        registerManifest<VERSIONED_MANIFEST>(registry, VersionedStep::deserialize);
    });
    return build;
}

Build & newBuild()
{
    static Build build(new_build_version, [](QueryPlanStepRegistry & registry)
    {
        registerManifest<VERSIONED_MANIFEST>(registry, VersionedStep::deserialize);
        registerManifest<FIXED_VERSIONED_MANIFEST>(registry, FixedVersionedStep::deserialize);
        registerManifest<FUTURE_MANIFEST>(registry, FutureStep::deserialize);
    });
    return build;
}

SharedHeader makeHeader()
{
    ColumnsWithTypeAndName columns;
    columns.emplace_back(DataTypeUInt64().createColumn(), std::make_shared<DataTypeUInt64>(), "x");
    return std::make_shared<const Block>(Block(columns));
}

QueryPlan planOf(QueryPlanStepPtr step)
{
    QueryPlan plan;
    plan.addStep(std::move(step));
    return plan;
}

/// What `build` sends to a peer that reads up to `peer_version`, at the version the query asked for.
String serializeWith(Build & build, const QueryPlan & plan, UInt64 peer_version, UInt64 requested_version)
{
    QueryPlanStepRegistry::ScopedInstance scope(build.registry);
    WriteBufferFromOwnString out;
    plan.serialize(out, peer_version, requested_version);
    out.finalize();
    return out.str();
}

/// What `build` makes of a stream. The whole stream must be consumed, accepted or not.
QueryPlan deserializeWith(Build & build, const String & bytes)
{
    QueryPlanStepRegistry::ScopedInstance scope(build.registry);
    ReadBufferFromString in(bytes);
    auto plan_and_sets = QueryPlan::deserialize(in, getContext().context, /*max_type_complexity=*/0);
    EXPECT_TRUE(in.eof());
    return QueryPlan::makeSets(std::move(plan_and_sets), getContext().context);
}

/// The refusal of a stream: the error code, and the stream left fully consumed so the connection survives.
int refusalOf(Build & build, const String & bytes)
{
    QueryPlanStepRegistry::ScopedInstance scope(build.registry);
    ReadBufferFromString in(bytes);
    try
    {
        QueryPlan::deserialize(in, getContext().context, /*max_type_complexity=*/0);
    }
    catch (const Exception & e)
    {
        EXPECT_TRUE(in.eof()) << "a refused plan must be taken off the stream";
        return e.code();
    }
    ADD_FAILURE() << "the plan was accepted";
    return 0;
}

template <typename Step>
const Step & rootAs(const QueryPlan & plan)
{
    const auto * step = dynamic_cast<const Step *>(plan.getRootNode()->step.get());
    if (!step)
        throw std::runtime_error("the root step is not of the expected class");
    return *step;
}

}

TEST(StepManifestCompatibility, TheOldBuildRoundTrips)
{
    auto bytes = serializeWith(oldBuild(), planOf(std::make_unique<VersionedStep>(makeHeader(), VersionedWire{7})), old_build_version, old_build_version);
    auto plan = deserializeWith(oldBuild(), bytes);
    EXPECT_EQ(rootAs<VersionedStep>(plan).wire, (VersionedWire{7}));
}

TEST(StepManifestCompatibility, TheNewBuildReadsAnOldStream)
{
    /// The newer build speaks a higher plan version but reads a stream the older build wrote.
    auto bytes = serializeWith(oldBuild(), planOf(std::make_unique<VersionedStep>(makeHeader(), VersionedWire{7})), old_build_version, old_build_version);
    auto plan = deserializeWith(newBuild(), bytes);
    EXPECT_EQ(rootAs<VersionedStep>(plan).wire, (VersionedWire{7}));
}

TEST(StepManifestCompatibility, AStepTheOldBuildDoesNotKnowIsRefusedBeforeItIsBuilt)
{
    /// The name is what decides. Sent at the newer version, the older reader refuses on the version
    /// at the head; sent at the older version, it passes the version check but does not have the name,
    /// so it refuses on the unknown step from the outline, before the step is built.
    auto new_version_bytes = serializeWith(newBuild(), planOf(std::make_unique<FutureStep>(makeHeader())), new_build_version, new_build_version);
    EXPECT_EQ(refusalOf(oldBuild(), new_version_bytes), ErrorCodes::NOT_IMPLEMENTED);
    EXPECT_NO_THROW(deserializeWith(newBuild(), new_version_bytes));

    auto old_version_bytes = serializeWith(newBuild(), planOf(std::make_unique<FutureStep>(makeHeader())), old_build_version, old_build_version);
    EXPECT_EQ(refusalOf(oldBuild(), old_version_bytes), ErrorCodes::INCORRECT_DATA);
}

TEST(StepManifestCompatibility, TheRegistriesDescribeWhatEachBuildKnows)
{
    const auto * old_info = oldBuild().registry.getStepSerializationInfo("Versioned");
    const auto * new_info = newBuild().registry.getStepSerializationInfo("Versioned");
    ASSERT_TRUE(old_info && new_info);
    EXPECT_TRUE(new_info->has_wire_struct);

    /// The newer build knows names the older one does not.
    EXPECT_EQ(newBuild().registry.getStepSerializationInfo("Future")->introduced_in_plan_version, new_build_version);
    EXPECT_EQ(oldBuild().registry.getStepSerializationInfo("Future"), nullptr);
    EXPECT_NE(newBuild().registry.getStepSerializationInfo("VersionedFixed"), nullptr);
    EXPECT_EQ(oldBuild().registry.getStepSerializationInfo("VersionedFixed"), nullptr);

    /// Both builds declare `Versioned` the same way.
    String old_dump = oldBuild().registry.dumpManifests();
    String new_dump = newBuild().registry.dumpManifests();
    for (const auto & line : {"format 1 introduced_in " + std::to_string(old_build_version) + "\n", String("  field value Logical UInt64\n")})
    {
        EXPECT_NE(old_dump.find(line), String::npos) << line;
        EXPECT_NE(new_dump.find(line), String::npos) << line;
    }
}

TEST(StepManifestCompatibility, AResultChangingFixCarriedByANewNameIsRefusedByTheName)
{
    /// The fix ships under the new name `VersionedFixed`, available from the framed baseline, so the
    /// plan's reader requirement stays at the older build's version. The older build passes the
    /// version check but does not have the name, so it refuses by the name and leaves the stream
    /// clean. A plan that keeps the plain `Versioned` name still flows to it, as the tests above show.
    auto bytes = serializeWith(newBuild(), planOf(std::make_unique<FixedVersionedStep>(makeHeader(), VersionedWire{7})), old_build_version, old_build_version);
    EXPECT_EQ(refusalOf(oldBuild(), bytes), ErrorCodes::INCORRECT_DATA);

    /// The newer build has the name and runs it.
    auto plan = deserializeWith(newBuild(), bytes);
    EXPECT_EQ(rootAs<FixedVersionedStep>(plan).wire, (VersionedWire{7}));
}
