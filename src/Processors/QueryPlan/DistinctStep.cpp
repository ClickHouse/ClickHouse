#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/StepManifest.h>
#include <Processors/Transforms/DistinctSortedStreamTransform.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Core/SortDescription.h>

namespace DB
{

namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsOverflowMode distinct_overflow_mode;
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_in_distinct;
    extern const QueryPlanSerializationSettingsUInt64 max_rows_in_distinct;
}

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

bool preliminaryDistinctIsUseful(size_t max_threads)
{
    return max_threads > 1;
}

static ITransformingStep::Traits getTraits(bool pre_distinct)
{
    const bool preserves_number_of_streams = pre_distinct;
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = !pre_distinct,
            .preserves_number_of_streams = preserves_number_of_streams,
            .preserves_sorting = preserves_number_of_streams,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

DistinctStep::DistinctStep(
    const SharedHeader & input_header_,
    const SizeLimits & set_size_limits_,
    UInt64 limit_hint_,
    const Names & columns_,
    bool pre_distinct_)
    : ITransformingStep(
            input_header_,
            input_header_,
            getTraits(pre_distinct_))
    , set_size_limits(set_size_limits_)
    , limit_hint(limit_hint_)
    , columns(columns_)
    , pre_distinct(pre_distinct_)
{
}

void DistinctStep::updateLimitHint(UInt64 hint)
{
    if (hint && limit_hint)
        /// Both limits are set - take the min
        limit_hint = std::min(hint, limit_hint);
    else
        /// Some limit is not set - take the other one
        limit_hint = std::max(hint, limit_hint);
}

void DistinctStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    /// The final distinct deduplicates across the whole input, so it needs all data in a single
    /// stream; the pre-distinct only reduces the data, deduplicating each stream independently.
    /// However, when the input streams carry disjoint sets of the DISTINCT key values, each stream
    /// can be deduplicated independently, so we keep the streams and skip merging them into one.
    if (!pre_distinct && !skip_stream_merging)
        pipeline.resize(1);

    /// The preliminary deduplication is best-effort (a deduplicating consumer follows), so on
    /// mostly-unique input the transform may abandon it and free its hash table - unless a limit
    /// hint is set: an abandoned transform cannot count the distinct rows to stop the input early.
    const bool allow_abandoning = pre_distinct && settings.allow_preliminary_distinct_abandoning && limit_hint == 0;

    pipeline.addSimpleTransform(
        [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;

            /// When the stream is sorted by a prefix of the distinct columns, deduplicate by
            /// ranges of equal prefix values, hashing only the remaining columns within a range
            /// (and with no remaining columns, keeping one row per range without hashing at all).
            if (!distinct_sort_desc.empty())
                return std::make_shared<DistinctSortedStreamTransform>(header, set_size_limits, limit_hint, distinct_sort_desc, columns);

            return std::make_shared<DistinctTransform>(header, set_size_limits, limit_hint, columns, allow_abandoning);
        });
}

void DistinctStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
    settings.out << prefix << "Columns: ";

    if (columns.empty())
        settings.out << "none";
    else
    {
        bool first = true;
        for (const auto & column : columns)
        {
            if (!first)
                settings.out << ", ";
            first = false;

            settings.out << (settings.pretty ? QueryPlanFormat::formatColumnPretty(column, settings.pretty_names) : column);
        }
    }

    settings.out << '\n';

    if (skip_stream_merging)
        settings.out << prefix << "Skip stream merging: 1\n";
}

void DistinctStep::describeActions(JSONBuilder::JSONMap & map) const
{
    auto columns_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : columns)
        columns_array->add(column);

    map.add("Columns", std::move(columns_array));
    if (skip_stream_merging)
        map.add("Skip stream merging", true);
}

void DistinctStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

namespace
{

/// A step with disjoint input streams and no merge is not the same relation as one that merges,
/// so it has no logical digest; the full digest still has it.
constexpr bool mergesStreams(const DistinctWire & wire)
{
    return !wire.skip_stream_merging;
}

/// `Distinct` and `PreDistinct` are two serialization names over the same wire struct.
constexpr auto makeDistinctManifest(const char * name)
{
    return StepManifest<DistinctStep, DistinctWire>(name)
        .nameIntroducedIn(1)
        .baseFormat(
            field("columns", WireFieldClass::Logical, &DistinctWire::columns),
            field("limit_hint", WireFieldClass::Logical, &DistinctWire::limit_hint),
            field("distinct_sort_desc", WireFieldClass::Logical, &DistinctWire::distinct_sort_desc),
            field("skip_stream_merging", WireFieldClass::Physical, &DistinctWire::skip_stream_merging))
        .settings(
            setting(QueryPlanSerializationSetting::max_rows_in_distinct, WireFieldClass::Logical, &DistinctWire::max_rows),
            setting(QueryPlanSerializationSetting::max_bytes_in_distinct, WireFieldClass::Logical, &DistinctWire::max_bytes),
            setting(QueryPlanSerializationSetting::distinct_overflow_mode, WireFieldClass::Logical, &DistinctWire::overflow_mode))
        .logicalDigest(mergesStreams);
}

constexpr auto DISTINCT_MANIFEST = makeDistinctManifest("Distinct");
constexpr auto PRE_DISTINCT_MANIFEST = makeDistinctManifest("PreDistinct");

const auto & distinctManifest(bool pre_distinct)
{
    return pre_distinct ? PRE_DISTINCT_MANIFEST : DISTINCT_MANIFEST;
}

}

DistinctWire DistinctStep::toWire() const
{
    return DistinctWire{
        columns, limit_hint, distinct_sort_desc, skip_stream_merging,
        set_size_limits.max_rows, set_size_limits.max_bytes, set_size_limits.overflow_mode};
}

QueryPlanStepPtr DistinctStep::fromWire(DistinctWire wire, Deserialization & ctx, bool pre_distinct_)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "DistinctStep must have one input stream");

    auto step = std::make_unique<DistinctStep>(
        ctx.input_headers.front(),
        SizeLimits(wire.max_rows, wire.max_bytes, wire.overflow_mode),
        wire.limit_hint,
        std::move(wire.columns),
        pre_distinct_);
    if (!wire.distinct_sort_desc.empty())
        step->applyOrder(std::move(wire.distinct_sort_desc));
    if (wire.skip_stream_merging)
        step->skipStreamMerging();
    return step;
}

void DistinctStep::serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const
{
    if (usesManifest(version))
        writeManifestSettings(distinctManifest(pre_distinct), toWire(), settings);
    else
        serializeSettingsLegacy(settings);
}

void DistinctStep::serialize(Serialization & ctx) const
{
    if (usesManifest(ctx.version))
        writeManifestPayload(distinctManifest(pre_distinct), toWire(), ctx);
    else
        serializeLegacy(ctx);
}

QueryPlanStepPtr DistinctStep::deserialize(Deserialization & ctx, bool pre_distinct_)
{
    if (usesManifest(ctx.version))
        return fromWire(readManifestPayload(distinctManifest(pre_distinct_), ctx), ctx, pre_distinct_);
    return deserializeLegacy(ctx, pre_distinct_);
}

void DistinctStep::serializeSettingsLegacy(QueryPlanSerializationSettings & settings) const
{
    settings[QueryPlanSerializationSetting::max_rows_in_distinct] = set_size_limits.max_rows;
    settings[QueryPlanSerializationSetting::max_bytes_in_distinct] = set_size_limits.max_bytes;
    settings[QueryPlanSerializationSetting::distinct_overflow_mode] = set_size_limits.overflow_mode;
}

void DistinctStep::serializeLegacy(Serialization & ctx) const
{
    /// The older stream does not carry `limit_hint`, the sort description or the stream merging flag.
    writeVarUInt(columns.size(), ctx.out);
    for (const auto & column : columns)
        writeStringBinary(column, ctx.out);
}

QueryPlanStepPtr DistinctStep::deserializeLegacy(Deserialization & ctx, bool pre_distinct_)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "DistinctStep must have one input stream");

    size_t columns_size = 0;
    readVarUInt(columns_size, ctx.in);
    Names column_names(columns_size);
    for (size_t i = 0; i < columns_size; ++i)
        readStringBinary(column_names[i], ctx.in);

    SizeLimits size_limits;
    size_limits.max_rows = ctx.settings[QueryPlanSerializationSetting::max_rows_in_distinct];
    size_limits.max_bytes = ctx.settings[QueryPlanSerializationSetting::max_bytes_in_distinct];
    size_limits.overflow_mode = ctx.settings[QueryPlanSerializationSetting::distinct_overflow_mode];

    return std::make_unique<DistinctStep>(
        ctx.input_headers.front(), size_limits, 0, column_names, pre_distinct_);
}

QueryPlanStepPtr DistinctStep::deserializeNormal(Deserialization & ctx)
{
    return DistinctStep::deserialize(ctx, false);
}
QueryPlanStepPtr DistinctStep::deserializePre(Deserialization & ctx)
{
    return DistinctStep::deserialize(ctx, true);
}

QueryPlanStepPtr DistinctStep::clone() const
{
    return std::make_unique<DistinctStep>(*this);
}

void registerDistinctStep(QueryPlanStepRegistry & registry);
void registerDistinctStep(QueryPlanStepRegistry & registry)
{
    /// Preliminary distinct probably can be a query plan optimization.
    /// It's easier to serialize it using different names, so that pre-distinct can be potentially removed later.
    registerManifest<DISTINCT_MANIFEST>(registry, DistinctStep::deserializeNormal);
    registerManifest<PRE_DISTINCT_MANIFEST>(registry, DistinctStep::deserializePre);
}

}
