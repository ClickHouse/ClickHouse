#include <gtest/gtest.h>

#include <Common/SipHash.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/StepManifest.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace DB::QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsUInt64 max_rows_in_distinct;
}

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

/// The wire struct of a step that exists only here, to exercise the framework without a real step.
struct TestWire
{
    UInt64 count = 0;
    bool flag = false;
    String tail;
    std::optional<UInt64> maybe;
};

/// A tag type: the framework tests exercise a manifest without ever building the step.
struct TestStep {};

constexpr UInt64 base_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE;

/// A manifest that binds every member of its wire struct, with both digest classes.
constexpr auto ONE_FORMAT = StepManifest<TestStep, TestWire>("TestManifest")
    .nameIntroducedIn(base_version)
    .baseFormat(
        field("count", WireFieldClass::Logical, &TestWire::count),
        field("flag", WireFieldClass::Physical, &TestWire::flag),
        field("tail", WireFieldClass::Physical, &TestWire::tail),
        field("maybe", WireFieldClass::Physical, &TestWire::maybe));

/// A manifest that leaves two members unbound, so it fails the coverage rule.
constexpr auto INCOMPLETE = StepManifest<TestStep, TestWire>("TestManifest")
    .nameIntroducedIn(base_version)
    .baseFormat(
        field("count", WireFieldClass::Logical, &TestWire::count),
        field("flag", WireFieldClass::Physical, &TestWire::flag));

static_assert(manifestCoversWire(ONE_FORMAT));
static_assert(!manifestCoversWire(INCOMPLETE), "a manifest that leaves members unbound is incomplete");

SortDescription sortByX()
{
    SortDescription description;
    description.emplace_back("x", 1, 1);
    return description;
}

SharedHeader makeHeader()
{
    ColumnsWithTypeAndName columns;
    columns.emplace_back(DataTypeUInt64().createColumn(), std::make_shared<DataTypeUInt64>(), "x");
    return std::make_shared<const Block>(Block(columns));
}

/// Everything a `Deserialization` context refers to, kept alive together.
struct Reader
{
    ReadBufferFromString in;
    /// The payload is read inside a frame, exactly as the framed plan reader wraps each step payload.
    LimitReadBuffer frame;
    DeserializedSetsRegistry registry;
    ContextPtr context = getContext().context;
    SharedHeaders input_headers{makeHeader()};
    SharedHeader output_header{makeHeader()};
    QueryPlanSerializationSettings settings;
    IQueryPlanStep::Deserialization ctx;

    Reader(const String & bytes, UInt64 stream_version, UInt64 step_format_version)
        : in(bytes)
        , frame(in, {.read_no_more = bytes.size()})
        , ctx{frame, registry, {}, context, input_headers, output_header, settings, 0, stream_version, false, step_format_version}
    {
    }
};

template <typename Manifest>
typename Manifest::Wire read(const Manifest & manifest, Reader & reader)
{
    return readManifestPayload(manifest, reader.ctx);
}

void registerStepsOnce()
{
    static std::once_flag flag;
    std::call_once(flag, []
    {
        if (!QueryPlanStepRegistry::instance().hasStep("Expression"))
            QueryPlanStepRegistry::registerPlanSteps();
    });
}

}

TEST(StepManifest, MalformedBytesAreRefused)
{
    {
        /// count = 1, then a bool byte of 2.
        String bytes = "\x01\x02";
        Reader reader(bytes, base_version, 1);
        EXPECT_THROW(read(ONE_FORMAT, reader), Exception);
    }
    {
        /// count = 1, flag = 0, then a string whose length runs past the payload.
        String bytes("\x01\x00\x7f", 3);
        Reader reader(bytes, base_version, 1);
        try
        {
            read(ONE_FORMAT, reader);
            FAIL() << "a string longer than the payload must be refused before allocation";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA);
        }
    }
}

TEST(StepManifest, RegistryEntryIsDerived)
{
    auto info = manifestRegistryInfo(ONE_FORMAT);
    EXPECT_EQ(info.introduced_in_plan_version, base_version);
    EXPECT_EQ(info.max_format_version, 1u);
    EXPECT_TRUE(info.has_wire_struct);
}

TEST(StepManifest, LogicalProjectionIgnoresPhysicalEntries)
{
    auto project = [](const TestWire & wire, bool logical_only)
    {
        SipHash hash;
        forEachWireEntry(ONE_FORMAT, wire, [&](const String & name, WireFieldClass field_class, const auto & value)
        {
            if (logical_only && field_class != WireFieldClass::Logical)
                return;
            hash.update(name);
            WriteBufferFromOwnString out;
            SerializedSetsRegistry registry;
            IQueryPlanStep::Serialization ctx{out, registry};
            WireEncoding::write(value, ctx);
            out.finalize();
            hash.update(out.str());
        });
        return hash.get64();
    };

    TestWire a{.count = 1, .flag = false, .tail = "t", .maybe = std::nullopt};
    TestWire b{.count = 1, .flag = true, .tail = "t", .maybe = std::nullopt};
    TestWire c{.count = 2, .flag = false, .tail = "t", .maybe = std::nullopt};
    EXPECT_EQ(project(a, true), project(b, true)) << "flag is Physical";
    EXPECT_NE(project(a, false), project(b, false));
    EXPECT_NE(project(a, true), project(c, true));
}

TEST(StepManifest, DescriptionNamesEveryNode)
{
    String description = describeManifest(ONE_FORMAT);
    EXPECT_NE(description.find("name TestManifest introduced_in " + std::to_string(base_version)), String::npos);
    EXPECT_NE(description.find("format 1 introduced_in " + std::to_string(base_version)), String::npos);
    EXPECT_NE(description.find("  field count Logical UInt64\n"), String::npos);
    EXPECT_NE(description.find("  field flag Physical bool\n"), String::npos);
    EXPECT_NE(description.find("  field tail Physical String\n"), String::npos);
    EXPECT_NE(description.find("  field maybe Physical optional<UInt64>\n"), String::npos);
    /// count 0, flag 0, empty tail, absent maybe.
    EXPECT_NE(description.find("  initializers 00000000\n"), String::npos);
}

TEST(StepManifest, DistinctSettingsAreWrittenOnlyWhenChanged)
{
    registerStepsOnce();
    auto header = makeHeader();

    auto changed_names = [](const QueryPlanSerializationSettings & settings)
    {
        std::vector<String> names;
        for (const auto & entry : settings.getChangedEntries())
            names.push_back(entry.name);
        std::sort(names.begin(), names.end());
        return names;
    };

    /// A step at its defaults writes nothing; only the settings whose value differs are on the wire,
    /// so a setting a receiver does not know stays off the wire while it sits at its default.
    DistinctStep at_defaults(header, SizeLimits{}, 0, Names{"x"}, false);
    QueryPlanSerializationSettings settings;
    at_defaults.serializeSettings(settings, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    EXPECT_TRUE(changed_names(settings).empty());

    DistinctStep limited(header, SizeLimits{5, 0, OverflowMode::BREAK}, 0, Names{"x"}, false);
    QueryPlanSerializationSettings changed;
    limited.serializeSettings(changed, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    EXPECT_EQ(changed_names(changed), (std::vector<String>{"distinct_overflow_mode", "max_rows_in_distinct"}));

    /// The wire struct's initializers are the registered defaults of the settings it binds.
    QueryPlanSerializationSettings defaults;
    DistinctWire initializers{};
    EXPECT_EQ(initializers.max_rows, static_cast<UInt64>(defaults[QueryPlanSerializationSetting::max_rows_in_distinct]));
}

TEST(StepManifest, WireStructsSurviveTheFramedRoundTrip)
{
    registerStepsOnce();
    auto header = makeHeader();

    {
        LimitStep original(header, 10, 3, /*always_read_till_end=*/true, /*with_ties=*/true, sortByX());
        original.markAsShardLimit();

        WriteBufferFromOwnString out;
        SerializedSetsRegistry registry;
        IQueryPlanStep::Serialization ctx{out, registry};
        ctx.version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
        original.serialize(ctx);
        out.finalize();

        Reader reader(out.str(), DBMS_QUERY_PLAN_SERIALIZATION_VERSION, ctx.step_format_version);
        auto restored = LimitStep::deserialize(reader.ctx);
        EXPECT_TRUE(reader.frame.eof());
        EXPECT_EQ(dynamic_cast<LimitStep &>(*restored).toWire(), original.toWire());
        EXPECT_TRUE(original.toWire().is_shard_limit);
    }
    {
        OffsetStep original(header, 12);
        WriteBufferFromOwnString out;
        SerializedSetsRegistry registry;
        IQueryPlanStep::Serialization ctx{out, registry};
        ctx.version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
        original.serialize(ctx);
        out.finalize();

        Reader reader(out.str(), DBMS_QUERY_PLAN_SERIALIZATION_VERSION, ctx.step_format_version);
        auto restored = OffsetStep::deserialize(reader.ctx);
        EXPECT_TRUE(reader.frame.eof());
        EXPECT_EQ(dynamic_cast<OffsetStep &>(*restored).toWire(), original.toWire());
    }
    for (bool pre_distinct : {false, true})
    {
        DistinctStep original(header, SizeLimits{5, 6, OverflowMode::BREAK}, 9, Names{"x"}, pre_distinct);
        original.applyOrder(sortByX());
        original.skipStreamMerging();

        QueryPlanSerializationSettings settings;
        original.serializeSettings(settings, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);

        WriteBufferFromOwnString out;
        SerializedSetsRegistry registry;
        IQueryPlanStep::Serialization ctx{out, registry};
        ctx.version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
        original.serialize(ctx);
        out.finalize();

        Reader reader(out.str(), DBMS_QUERY_PLAN_SERIALIZATION_VERSION, ctx.step_format_version);
        reader.settings.applyEntries(settings.getChangedEntries());
        auto restored = pre_distinct ? DistinctStep::deserializePre(reader.ctx) : DistinctStep::deserializeNormal(reader.ctx);
        EXPECT_TRUE(reader.frame.eof());
        auto & restored_distinct = dynamic_cast<DistinctStep &>(*restored);
        EXPECT_EQ(restored_distinct.toWire(), original.toWire());
        EXPECT_EQ(restored_distinct.isPreliminary(), pre_distinct);
        EXPECT_EQ(restored_distinct.getLimitHint(), 9u);
    }
}

TEST(StepManifest, LegacyStreamsKeepTheHandWrittenLayout)
{
    registerStepsOnce();
    auto header = makeHeader();
    LimitStep step(header, 10, 3, /*always_read_till_end=*/true, /*with_ties=*/false);

    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = 3;
    step.serialize(ctx);
    out.finalize();
    /// flags byte (bit 1 = always_read_till_end), then limit and offset as varints.
    EXPECT_EQ(out.str(), String("\x01\x0a\x03", 3));
}
