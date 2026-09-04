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
    extern const int SUPPORT_IS_DISABLED;
}

/// Two builds of the server exchange plans in one process. The older build speaks the current plan
/// version and knows the base format of the step `Versioned`. The newer build speaks the next plan
/// version, appended a second format to `Versioned` at that version, and has a step the older build
/// has never heard of. Each build is a registry; a scope makes it the current one for a thread.
namespace
{

constexpr UInt64 old_build_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
constexpr UInt64 new_build_version = old_build_version + 1;

constexpr auto L = WireFieldClass::Logical;

/// The step as the older build knows it.
struct OldVersionedWire
{
    UInt64 value = 0;

    bool operator==(const OldVersionedWire &) const = default;
};

/// The step as the newer build knows it: the same base, then an appended format.
struct NewVersionedWire
{
    UInt64 value = 0;
    String note;
    std::optional<UInt64> extra;

    bool operator==(const NewVersionedWire &) const = default;
};

struct FutureWire
{
};

template <typename Wire>
class VersionedStepBase : public ISourceStep
{
public:
    VersionedStepBase(SharedHeader header_, Wire wire_) : ISourceStep(std::move(header_)), wire(std::move(wire_)) { }

    String getName() const override { return "Versioned"; }
    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override { }
    bool isSerializable() const override { return true; }

    Wire wire;
};

class OldVersionedStep : public VersionedStepBase<OldVersionedWire>
{
public:
    using VersionedStepBase::VersionedStepBase;
    void serialize(Serialization & ctx) const override;
    static QueryPlanStepPtr deserialize(Deserialization & ctx);
};

class NewVersionedStep : public VersionedStepBase<NewVersionedWire>
{
public:
    using VersionedStepBase::VersionedStepBase;
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

constexpr auto OLD_VERSIONED_MANIFEST = StepManifest<OldVersionedStep, OldVersionedWire>("Versioned")
    .nameIntroducedIn(1)
    .baseFormat(field("value", L, &OldVersionedWire::value));

constexpr auto NEW_VERSIONED_MANIFEST = StepManifest<NewVersionedStep, NewVersionedWire>("Versioned")
    .nameIntroducedIn(1)
    .baseFormat(field("value", L, &NewVersionedWire::value))
    .appendFormat(IntroducedIn{new_build_version},
        field("note", L, &NewVersionedWire::note),
        field("extra", L, &NewVersionedWire::extra));

constexpr auto FUTURE_MANIFEST = StepManifest<FutureStep, FutureWire>("Future")
    .nameIntroducedIn(new_build_version)
    .baseFormat();

void OldVersionedStep::serialize(Serialization & ctx) const
{
    writeManifestPayload(OLD_VERSIONED_MANIFEST, wire, ctx);
}

QueryPlanStepPtr OldVersionedStep::deserialize(Deserialization & ctx)
{
    return std::make_unique<OldVersionedStep>(ctx.output_header, readManifestPayload(OLD_VERSIONED_MANIFEST, ctx));
}

void NewVersionedStep::serialize(Serialization & ctx) const
{
    writeManifestPayload(NEW_VERSIONED_MANIFEST, wire, ctx);
}

QueryPlanStepPtr NewVersionedStep::deserialize(Deserialization & ctx)
{
    return std::make_unique<NewVersionedStep>(ctx.output_header, readManifestPayload(NEW_VERSIONED_MANIFEST, ctx));
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
        registerManifest<OLD_VERSIONED_MANIFEST>(registry, OldVersionedStep::deserialize);
    });
    return build;
}

Build & newBuild()
{
    static Build build(new_build_version, [](QueryPlanStepRegistry & registry)
    {
        registerManifest<NEW_VERSIONED_MANIFEST>(registry, NewVersionedStep::deserialize);
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
    auto bytes = serializeWith(oldBuild(), planOf(std::make_unique<OldVersionedStep>(makeHeader(), OldVersionedWire{7})), old_build_version, old_build_version);
    auto plan = deserializeWith(oldBuild(), bytes);
    EXPECT_EQ(rootAs<OldVersionedStep>(plan).wire, (OldVersionedWire{7}));
}

TEST(StepManifestCompatibility, TheNewBuildReadsAnOldStream)
{
    /// An older writer knows nothing about the appended format; the newer reader reconstructs its
    /// initializers.
    auto bytes = serializeWith(oldBuild(), planOf(std::make_unique<OldVersionedStep>(makeHeader(), OldVersionedWire{7})), old_build_version, old_build_version);
    auto plan = deserializeWith(newBuild(), bytes);
    EXPECT_EQ(rootAs<NewVersionedStep>(plan).wire, (NewVersionedWire{.value = 7, .note = "", .extra = std::nullopt}));
}

TEST(StepManifestCompatibility, TheNewBuildRoundTripsAtItsOwnVersion)
{
    NewVersionedWire wire{.value = 7, .note = "hello", .extra = 42};
    auto bytes = serializeWith(newBuild(), planOf(std::make_unique<NewVersionedStep>(makeHeader(), wire)), new_build_version, new_build_version);
    auto plan = deserializeWith(newBuild(), bytes);
    EXPECT_EQ(rootAs<NewVersionedStep>(plan).wire, wire);
}

TEST(StepManifestCompatibility, TheOldBuildReadsANewStreamWhenTheAppendIsAtItsInitializers)
{
    /// The newer writer writes both formats at its own version. The older reader knows the base
    /// format only, so the frame skips the appended one by the payload size, and nothing the older
    /// reader needed was in it.
    NewVersionedWire wire{.value = 7, .note = "", .extra = std::nullopt};
    auto bytes = serializeWith(newBuild(), planOf(std::make_unique<NewVersionedStep>(makeHeader(), wire)), new_build_version, new_build_version);
    auto plan = deserializeWith(oldBuild(), bytes);
    EXPECT_EQ(rootAs<OldVersionedStep>(plan).wire, (OldVersionedWire{7}));
}

TEST(StepManifestCompatibility, TheOldBuildRefusesANewStreamWithAValueItWouldLose)
{
    /// A value that differs from its initializer raises the plan's requirement to the appended
    /// format's version. The older reader refuses at the head, before it builds anything, and
    /// leaves the stream clean.
    NewVersionedWire wire{.value = 7, .note = "hello", .extra = std::nullopt};
    auto bytes = serializeWith(newBuild(), planOf(std::make_unique<NewVersionedStep>(makeHeader(), wire)), new_build_version, new_build_version);
    EXPECT_EQ(refusalOf(oldBuild(), bytes), ErrorCodes::NOT_IMPLEMENTED);

    NewVersionedWire only_extra{.value = 7, .note = "", .extra = 1};
    bytes = serializeWith(newBuild(), planOf(std::make_unique<NewVersionedStep>(makeHeader(), only_extra)), new_build_version, new_build_version);
    EXPECT_EQ(refusalOf(oldBuild(), bytes), ErrorCodes::NOT_IMPLEMENTED);
}

TEST(StepManifestCompatibility, ANewWriterHeldAtTheOldVersionWritesTheOldLayout)
{
    /// Held at the older version, by a query setting or by the server ceiling, the newer writer
    /// lowers the format and produces the bytes the older build would have produced itself.
    auto from_new = serializeWith(newBuild(), planOf(std::make_unique<NewVersionedStep>(makeHeader(), NewVersionedWire{.value = 7, .note = "", .extra = std::nullopt})), old_build_version, old_build_version);
    auto from_old = serializeWith(oldBuild(), planOf(std::make_unique<OldVersionedStep>(makeHeader(), OldVersionedWire{7})), old_build_version, old_build_version);
    EXPECT_EQ(from_new, from_old);

    auto plan = deserializeWith(oldBuild(), from_new);
    EXPECT_EQ(rootAs<OldVersionedStep>(plan).wire, (OldVersionedWire{7}));
}

TEST(StepManifestCompatibility, ANewWriterHeldAtTheOldVersionRefusesAValueTheOldLayoutCannotCarry)
{
    /// The same writer, held at the older version, with a value only the appended format can carry:
    /// it refuses before the first byte, because the stream is written for a reader that would run
    /// the plan without the value.
    QueryPlanStepRegistry::ScopedInstance scope(newBuild().registry);
    auto plan = planOf(std::make_unique<NewVersionedStep>(makeHeader(), NewVersionedWire{.value = 7, .note = "hello", .extra = std::nullopt}));
    WriteBufferFromOwnString out;
    try
    {
        plan.serialize(out, old_build_version, old_build_version);
        FAIL() << "the writer must refuse";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::SUPPORT_IS_DISABLED);
    }
    out.finalize();
    EXPECT_TRUE(out.str().empty()) << "nothing may reach the stream before the refusal";
}

TEST(StepManifestCompatibility, AStepTheOldBuildDoesNotKnowIsRefusedBeforeItIsBuilt)
{
    /// The name's own version is a requirement. Sent at the newer version, the older reader refuses
    /// at the head; held at the older version, the writer refuses before the first byte.
    auto bytes = serializeWith(newBuild(), planOf(std::make_unique<FutureStep>(makeHeader())), new_build_version, new_build_version);
    EXPECT_EQ(refusalOf(oldBuild(), bytes), ErrorCodes::NOT_IMPLEMENTED);
    EXPECT_NO_THROW(deserializeWith(newBuild(), bytes));

    QueryPlanStepRegistry::ScopedInstance scope(newBuild().registry);
    WriteBufferFromOwnString out;
    EXPECT_THROW(planOf(std::make_unique<FutureStep>(makeHeader())).serialize(out, old_build_version, old_build_version), Exception);
}

TEST(StepManifestCompatibility, TheRegistriesDescribeWhatEachBuildKnows)
{
    const auto * old_info = oldBuild().registry.getStepSerializationInfo("Versioned");
    const auto * new_info = newBuild().registry.getStepSerializationInfo("Versioned");
    ASSERT_TRUE(old_info && new_info);

    EXPECT_EQ(old_info->maxFormatVersion(), 1u);
    EXPECT_EQ(new_info->maxFormatVersion(), 2u);
    /// The appended format keeps the base readable in front, and needs nothing from a reader that
    /// skips it: the requirement is raised per value, while writing.
    EXPECT_EQ(new_info->prefixReadableFrom(2), 1u);
    EXPECT_EQ(new_info->minPlanVersionForFormat(2), 1u);
    EXPECT_TRUE(new_info->has_wire_struct);

    EXPECT_EQ(newBuild().registry.getStepSerializationInfo("Future")->introduced_in_plan_version, new_build_version);
    EXPECT_EQ(oldBuild().registry.getStepSerializationInfo("Future"), nullptr);

    /// Append-only: every line the older build declares is in the newer build's declaration too.
    String old_dump = oldBuild().registry.dumpManifests();
    String new_dump = newBuild().registry.dumpManifests();
    for (const auto & line : {"format 1 introduced_in " + std::to_string(old_build_version) + "\n", String("  field value Logical UInt64\n")})
    {
        EXPECT_NE(old_dump.find(line), String::npos) << line;
        EXPECT_NE(new_dump.find(line), String::npos) << line;
    }
    EXPECT_NE(new_dump.find("format 2 introduced_in " + std::to_string(new_build_version) + "\n"), String::npos);
}
