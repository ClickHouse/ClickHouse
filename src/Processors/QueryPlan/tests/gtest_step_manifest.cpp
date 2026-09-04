#include <gtest/gtest.h>

#include <Common/SipHash.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
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
    extern const int CANNOT_PARSE_QUERY_PLAN;
}

namespace
{

/// The wire struct of a step that exists only here: two base fields and an appended format.
struct TestWire
{
    UInt64 count = 0;
    bool flag = false;
    String tail = "";
    std::optional<UInt64> maybe;

    bool operator==(const TestWire &) const = default;
};

struct TestStep;

constexpr UInt64 base_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE;
constexpr UInt64 append_version = base_version + 1;

/// The manifest of a binary that knows both formats.
constexpr auto TWO_FORMATS = StepManifest<TestStep, TestWire>("TestManifest")
    .nameIntroducedIn(base_version)
    .baseFormat(
        field("count", WireFieldClass::Logical, &TestWire::count),
        field("flag", WireFieldClass::Physical, &TestWire::flag))
    .appendFormat(IntroducedIn{append_version},
        field("tail", WireFieldClass::Logical, &TestWire::tail),
        field("maybe", WireFieldClass::Logical, &TestWire::maybe));

/// The manifest of an older binary that knows the base format only. It reads what the two-format
/// writer wrote and leaves the tail to the frame.
constexpr auto BASE_ONLY = StepManifest<TestStep, TestWire>("TestManifest")
    .nameIntroducedIn(base_version)
    .baseFormat(
        field("count", WireFieldClass::Logical, &TestWire::count),
        field("flag", WireFieldClass::Physical, &TestWire::flag));

static_assert(manifestCoversWire(TWO_FORMATS));
static_assert(!manifestCoversWire(BASE_ONLY), "a manifest that leaves members unbound is incomplete");

struct Written
{
    String bytes;
    UInt64 step_format_version = 0;
    UInt64 min_reader_version = 0;
};

template <typename Manifest>
Written write(const Manifest & manifest, const typename Manifest::Wire & wire, UInt64 stream_version)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = stream_version;
    writeManifestPayload(manifest, wire, ctx);
    out.finalize();
    return {out.str(), ctx.step_format_version, ctx.min_reader_version};
}

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
    DeserializedSetsRegistry registry;
    ContextPtr context = getContext().context;
    SharedHeaders input_headers{makeHeader()};
    SharedHeader output_header{makeHeader()};
    QueryPlanSerializationSettings settings;
    IQueryPlanStep::Deserialization ctx;

    Reader(const String & bytes, UInt64 stream_version, UInt64 step_format_version)
        : in(bytes)
        , ctx{in, registry, {}, context, input_headers, output_header, settings, 0, stream_version, false, step_format_version}
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

TEST(StepManifest, RoundTripAtTheNewestFormat)
{
    TestWire wire{.count = 7, .flag = true, .tail = "abc", .maybe = 42};
    auto written = write(TWO_FORMATS, wire, append_version);
    EXPECT_EQ(written.step_format_version, 2u);
    EXPECT_EQ(written.min_reader_version, append_version);

    Reader reader(written.bytes, append_version, written.step_format_version);
    EXPECT_EQ(read(TWO_FORMATS, reader), wire);
    EXPECT_TRUE(reader.in.eof());
}

TEST(StepManifest, AnAppendAtItsInitializersNeedsNoReaderVersion)
{
    TestWire wire{.count = 7, .flag = true, .tail = "", .maybe = std::nullopt};
    auto written = write(TWO_FORMATS, wire, append_version);
    EXPECT_EQ(written.step_format_version, 2u);
    EXPECT_EQ(written.min_reader_version, 0u);

    /// An older reader reads the base format and leaves the tail to the frame.
    Reader reader(written.bytes, append_version, written.step_format_version);
    auto old_view = read(BASE_ONLY, reader);
    EXPECT_EQ(old_view.count, 7u);
    EXPECT_TRUE(old_view.flag);
    EXPECT_FALSE(reader.in.eof()) << "the appended format is the tail the frame skips";
}

TEST(StepManifest, AWriterBelowTheAppendLowersTheFormat)
{
    TestWire at_initializers{.count = 7, .flag = false, .tail = "", .maybe = std::nullopt};
    auto written = write(TWO_FORMATS, at_initializers, base_version);
    EXPECT_EQ(written.step_format_version, 1u);
    EXPECT_EQ(written.min_reader_version, 0u);
    EXPECT_EQ(written.bytes, write(BASE_ONLY, at_initializers, base_version).bytes);

    /// A value an old reader would reconstruct wrongly raises the requirement above the stream
    /// version, which is what makes the frame refuse the plan.
    TestWire with_tail{.count = 7, .flag = false, .tail = "x", .maybe = std::nullopt};
    auto refused = write(TWO_FORMATS, with_tail, base_version);
    EXPECT_EQ(refused.step_format_version, 1u);
    EXPECT_EQ(refused.min_reader_version, append_version);
}

TEST(StepManifest, MalformedBytesAreRefused)
{
    {
        /// count = 1, then a bool byte of 2.
        String bytes = "\x01\x02";
        Reader reader(bytes, base_version, 1);
        EXPECT_THROW(read(BASE_ONLY, reader), Exception);
    }
    {
        /// count = 1, flag = 0, then a string whose length runs past the payload.
        String bytes("\x01\x00\x7f", 3);
        Reader reader(bytes, append_version, 2);
        try
        {
            read(TWO_FORMATS, reader);
            FAIL() << "a string longer than the payload must be refused before allocation";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::CANNOT_PARSE_QUERY_PLAN);
        }
    }
}

TEST(StepManifest, RegistryEntryIsDerived)
{
    auto info = manifestRegistryInfo(TWO_FORMATS);
    EXPECT_EQ(info.introduced_in_plan_version, base_version);
    ASSERT_EQ(info.payload_formats.size(), 1u);
    EXPECT_EQ(info.payload_formats.at(2).change, QueryPlanStepRegistry::PayloadChange::Append);
    EXPECT_EQ(info.payload_formats.at(2).min_plan_version, 0u);
    EXPECT_EQ(info.maxFormatVersion(), 2u);
    EXPECT_EQ(info.prefixReadableFrom(2), 1u);

    auto base_only = manifestRegistryInfo(BASE_ONLY);
    EXPECT_TRUE(base_only.payload_formats.empty());
}

TEST(StepManifest, LogicalProjectionIgnoresPhysicalEntries)
{
    auto project = [](const TestWire & wire, bool logical_only)
    {
        SipHash hash;
        forEachWireEntry(TWO_FORMATS, wire, [&](const String & name, WireFieldClass field_class, const auto & value)
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
    String description = describeManifest(TWO_FORMATS);
    EXPECT_NE(description.find("name TestManifest introduced_in " + std::to_string(base_version)), String::npos);
    EXPECT_NE(description.find("format 1 introduced_in " + std::to_string(base_version)), String::npos);
    EXPECT_NE(description.find("format 2 introduced_in " + std::to_string(append_version)), String::npos);
    EXPECT_NE(description.find("  field count Logical UInt64\n"), String::npos);
    EXPECT_NE(description.find("  field flag Physical bool\n"), String::npos);
    EXPECT_NE(description.find("  field tail Logical String\n"), String::npos);
    EXPECT_NE(description.find("  field maybe Logical optional<UInt64>\n"), String::npos);
    /// count 0, flag 0, empty tail, absent maybe.
    EXPECT_NE(description.find("  initializers 00000000\n"), String::npos);
}

TEST(StepManifest, DistinctSettingsAreWrittenOnlyWhenChanged)
{
    registerStepsOnce();
    auto header = makeHeader();

    DistinctStep at_defaults(header, SizeLimits{}, 0, Names{"x"}, false);
    QueryPlanSerializationSettings settings;
    at_defaults.serializeSettings(settings, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    EXPECT_TRUE(settings.getChangedEntries().empty());

    DistinctStep limited(header, SizeLimits{5, 0, OverflowMode::BREAK}, 0, Names{"x"}, false);
    QueryPlanSerializationSettings changed;
    limited.serializeSettings(changed, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    auto entries = changed.getChangedEntries();
    ASSERT_EQ(entries.size(), 2u);
    std::vector<String> names;
    for (const auto & entry : entries)
        names.push_back(entry.name);
    std::sort(names.begin(), names.end());
    EXPECT_EQ(names, (std::vector<String>{"distinct_overflow_mode", "max_rows_in_distinct"}));

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
        EXPECT_TRUE(reader.in.eof());
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
        EXPECT_TRUE(reader.in.eof());
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
        EXPECT_TRUE(reader.in.eof());
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
