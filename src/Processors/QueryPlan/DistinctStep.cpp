#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/DistinctSortedStreamTransform.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/scatterByPartition.h>
#include <IO/Operators.h>
#include <Columns/IColumn.h>
#include <Common/JSONBuilder.h>
#include <Core/ColumnNumbers.h>
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
    bool pre_distinct_,
    bool has_order_sensitive_post_distinct_limit_)
    : ITransformingStep(
            input_header_,
            input_header_,
            getTraits(pre_distinct_))
    , set_size_limits(set_size_limits_)
    , limit_hint(limit_hint_)
    , columns(columns_)
    , pre_distinct(pre_distinct_)
    , has_order_sensitive_post_distinct_limit(has_order_sensitive_post_distinct_limit_)
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

/// The columns the DISTINCT is computed on, derived exactly as `DistinctTransform` derives them:
/// an empty list of columns means every column of the header, and constant columns are not part of the key.
static ColumnNumbers getKeyColumnPositions(const Block & header, const Names & columns)
{
    const size_t num_columns = columns.empty() ? header.columns() : columns.size();

    ColumnNumbers key_column_positions;
    key_column_positions.reserve(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        const size_t position = columns.empty() ? i : header.getPositionByName(columns[i]);
        const auto & column = header.getByPosition(position).column;
        if (column && !isColumnConst(*column))
            key_column_positions.push_back(position);
    }

    return key_column_positions;
}

bool DistinctStep::scatterStreamsByHash(QueryPipelineBuilder & pipeline) const
{
    if (!parallel_distinct)
        return false;

    /// The order-sensitivity guard of this step did not survive serialization, so it is not known
    /// whether reordering the output is allowed.
    if (!order_guard_state_is_known)
        return false;

    /// With a sorted input the transform below deduplicates range by range of equal values, holding one
    /// range at a time instead of a hash table of everything. Scattering the rows would destroy the order
    /// it relies on, and each stream would need a hash table of its own again.
    if (!distinct_sort_desc.empty())
        return false;

    /// With a limit the transform stops as soon as it has enough values, so the single stream is not the
    /// bottleneck it is otherwise. Keeping it also keeps the values that a `LIMIT` without `ORDER BY`
    /// returns: the first ones in the order the input arrives, rather than an arbitrary subset.
    if (limit_hint != 0 || has_order_sensitive_post_distinct_limit)
        return false;

    /// Every input chunk is split across all partitions, so the work the scatter adds grows with their
    /// number: measured against the un-scattered pipeline on `SELECT DISTINCT number FROM
    /// numbers_mt(4e7)`, the total CPU time grows by 8% at 4 partitions, 20% at 16 and 96% at 96.
    /// Past a point that outweighs deduplicating in more threads, so do not follow `max_threads` up.
    static constexpr size_t max_partitions = 16;
    static constexpr size_t max_scatter_streams = 16;

    /// Repartitioning wires `num_streams * num_partitions` connections. Narrow the input just as
    /// `ShuffleSendStep` does, so a wide pipeline cannot create an excessive scatter mesh.
    if (pipeline.getNumStreams() > max_scatter_streams)
        pipeline.resize(max_scatter_streams);

    const size_t num_streams = pipeline.getNumStreams();
    const size_t num_partitions = std::min(pipeline.getNumThreads(), max_partitions);
    if (num_streams <= 1 || num_partitions <= 1)
        return false;

    auto key_column_positions = getKeyColumnPositions(*pipeline.getSharedHeader(), columns);
    if (key_column_positions.empty())
        return false;

    scatterByPartition(pipeline, num_partitions, key_column_positions);
    return true;
}

void DistinctStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    /// The final distinct deduplicates across the whole input, so it needs all data in a single
    /// stream; the pre-distinct only reduces the data, deduplicating each stream independently.
    /// However, when the input streams carry disjoint sets of the DISTINCT key values, each stream
    /// can be deduplicated independently, so we keep the streams and skip merging them into one.
    bool scattered = false;
    if (!pre_distinct && (!skip_stream_merging || limit_hint != 0 || has_order_sensitive_post_distinct_limit))
    {
        /// The streams may also be made disjoint on the spot: repartitioning them by the hash of the
        /// DISTINCT columns routes equal key values into the same stream, which is all the deduplication
        /// below needs to run in parallel.
        scattered = scatterStreamsByHash(pipeline);
        if (!scattered)
            pipeline.resize(1);
    }

    /// The scattered streams hold disjoint parts of one DISTINCT set, and `max_rows_in_distinct` and
    /// `max_bytes_in_distinct` limit the size of the whole of it, so the transforms below add up their
    /// sizes here and check the limits against the total.
    DistinctSharedSetSizePtr shared_set_size;
    if (scattered && (set_size_limits.max_rows != 0 || set_size_limits.max_bytes != 0))
        shared_set_size = std::make_shared<DistinctSharedSetSize>();

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

            return std::make_shared<DistinctTransform>(header, set_size_limits, limit_hint, columns, shared_set_size);
        });

    /// The step is declared to return a single stream. Collecting the scattered streams back costs
    /// nothing - they are already deduplicated, and no two of them hold the same key value.
    if (scattered)
        pipeline.resize(1);
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

void DistinctStep::serializeSettings(QueryPlanSerializationSettings & settings, UInt64 /*version*/) const
{
    settings[QueryPlanSerializationSetting::max_rows_in_distinct] = set_size_limits.max_rows;
    settings[QueryPlanSerializationSetting::max_bytes_in_distinct] = set_size_limits.max_bytes;
    settings[QueryPlanSerializationSetting::distinct_overflow_mode] = set_size_limits.overflow_mode;
}

void DistinctStep::serialize(Serialization & ctx) const
{
    /// Let's not serialize limit_hint.
    /// Ideally, we can get if from a query plan optimization on the follower.
    /// The same holds for `has_order_sensitive_post_distinct_limit`; because neither is restored,
    /// `deserialize` disables the hash scatter of the final `DISTINCT` on the follower.

    writeVarUInt(columns.size(), ctx.out);
    for (const auto & column : columns)
        writeStringBinary(column, ctx.out);
}

QueryPlanStepPtr DistinctStep::deserialize(Deserialization & ctx, bool pre_distinct_)
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

    auto step = std::make_unique<DistinctStep>(
        ctx.input_headers.front(), size_limits, 0, column_names, pre_distinct_);
    /// Neither `limit_hint` nor `has_order_sensitive_post_distinct_limit` is serialized, so the guard
    /// against reordering the output of the final `DISTINCT` cannot be reconstructed here.
    step->forgetOrderGuardState();
    return step;
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
    registry.registerStep("Distinct", DistinctStep::deserializeNormal);
    registry.registerStep("PreDistinct", DistinctStep::deserializePre);
}

}
