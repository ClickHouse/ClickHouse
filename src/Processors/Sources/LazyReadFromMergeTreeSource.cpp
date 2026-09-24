#include <Processors/Sources/LazyReadFromMergeTreeSource.h>
#include <Processors/QueryPlan/LazilyReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeReadPoolInOrder.h>
#include <Processors/Transforms/LazyMaterializingTransform.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/MergeTreeSelectAlgorithms.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeSource.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Processors/Sources/MergeTreePointReadSource.h>
#include <Compression/CompressionCodecQuantized.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

#include <algorithm>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 preferred_block_size_bytes;
    extern const SettingsUInt64 preferred_max_column_in_block_size_bytes;
    extern const SettingsBool merge_tree_use_const_size_tasks_for_remote_reading;
    extern const SettingsBool use_uncompressed_cache;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// The vector column a point read can serve, with the dimension count its `Quantized(...)` codec declares.
struct PointReadVectorColumn
{
    NameAndTypePair column;
    size_t dimensions;
};

/// The single vector column the point read may serve, if the lazy header holds exactly one. The step is not told which
/// column the search ranks by, so with several we could point-read payload and leave the rescored one on the granule read.
std::optional<PointReadVectorColumn> findPointReadVectorColumn(const Block & lazy_header, const ColumnsDescription & columns_desc)
{
    std::optional<PointReadVectorColumn> found;

    for (const auto & col : lazy_header)
    {
        if (!columns_desc.has(col.name))
            continue;

        if (auto params = tryExtractQuantizedCodecParams(columns_desc.get(col.name).codec))
        {
            if (found)
                return {};
            found = PointReadVectorColumn{NameAndTypePair(col.name, col.type), params->dimensions};
        }
    }

    return found;
}

/// Whether `part` may be point-read: it addresses base `.bin` files directly and applies none of the read-time machinery
/// of `MergeTreeReadTask`, so require a part needing no conversion at all (not a stale list) that stores every lazy column.
bool canPointReadPart(
    const RangesInDataPart & part,
    const PointReadVectorColumn & vector_column,
    const Block & lazy_header,
    const MergeTreeData::MutationsSnapshotPtr & mutations_snapshot,
    const ContextPtr & context)
{
    auto alter_conversions = MergeTreeData::getAlterConversionsForPart(part.data_part, mutations_snapshot, context);
    if (alter_conversions->hasAnyConversions() || part.data_part->hasLightweightDelete())
        return false;

    for (const auto & col : lazy_header)
        if (col.name != vector_column.column.name && !part.data_part->hasColumnFiles(NameAndTypePair(col.name, col.type)))
            return false;

    /// Finally the layout itself: the vector column must be stored one vector per compressed block.
    return MergeTreePointReadSource::isEligible(part, vector_column.column, vector_column.dimensions);
}

}


LazyReadFromMergeTreeSource::LazyReadFromMergeTreeSource(
    SharedHeader header,
    size_t max_block_size_,
    size_t max_threads_,
    size_t min_marks_for_concurrent_read_,
    ExpressionActionsSettings actions_settings_,
    MergeTreeReaderSettings reader_settings_,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
    StorageSnapshotPtr storage_snapshot_,
    ContextPtr context_,
    const std::string & log_name_,
    LazyMaterializingRowsPtr lazy_materializing_rows_,
    RuntimeDataflowStatisticsCacheUpdaterPtr updater_)
    : IProcessor({}, {std::move(header)})
    , max_block_size(max_block_size_)
    , max_threads(max_threads_)
    , min_marks_for_concurrent_read(min_marks_for_concurrent_read_)
    , actions_settings(actions_settings_)
    , reader_settings(reader_settings_)
    , mutations_snapshot(std::move(mutations_snapshot_))
    , storage_snapshot(std::move(storage_snapshot_))
    , context(std::move(context_))
    , log_name(log_name_)
    , lazy_materializing_rows(std::move(lazy_materializing_rows_))
    , updater(std::move(updater_))
{
}

LazyReadFromMergeTreeSource::~LazyReadFromMergeTreeSource() = default;

RangesInDataParts LazyReadFromMergeTreeSource::splitRanges(RangesInDataParts parts_with_ranges, size_t total_marks) const
{
    /// Split ranges to read more concurrently.
    /// We need to keep an order of parts and ranges, but we can read the same part from multiple readers.
    const size_t marks_per_stream = total_marks / std::max<size_t>(max_threads, 1) + 1;

    RangesInDataParts split_parts_and_ranges;

    for (auto & part : parts_with_ranges)
    {
        size_t marks = part.getMarksCount();
        while (!part.ranges.empty())
        {
            if (marks <= marks_per_stream)
            {
                split_parts_and_ranges.emplace_back(
                    part.data_part,
                    part.parent_part,
                    part.part_index_in_query,
                    part.part_starting_offset_in_query,
                    std::move(part.ranges),
                    part.read_hints);

                break;
            }

            MarkRanges ranges;
            size_t added_marks = 0;
            while (added_marks < marks_per_stream && !part.ranges.empty())
            {
                size_t range_marks = part.ranges.front().getNumberOfMarks();

                /// It's not clear if we should respect min_marks_for_concurrent_read here.

                bool split_range =
                    range_marks + added_marks > marks_per_stream  /// range overflows limit
                    && range_marks >= 2 * min_marks_for_concurrent_read /// range is big enough for 2 concurrent reads
                    && (range_marks + added_marks - marks_per_stream) > min_marks_for_concurrent_read; /// what's rest is big enough for concurrent read

                if (split_range)
                {
                    size_t num_marks_to_split = marks_per_stream - added_marks;
                    num_marks_to_split = std::max(num_marks_to_split, min_marks_for_concurrent_read);
                    num_marks_to_split = std::min(num_marks_to_split, range_marks - min_marks_for_concurrent_read);

                    size_t split = part.ranges.front().begin + num_marks_to_split;
                    ranges.emplace_back(part.ranges.front().begin, split);
                    part.ranges.front().begin = split;
                }
                else
                {
                    ranges.emplace_back(std::move(part.ranges.front()));
                    part.ranges.pop_front();
                }

                auto added = ranges.back().getNumberOfMarks();
                added_marks += added;
                marks -= added;
            }

            split_parts_and_ranges.emplace_back(
                part.data_part,
                part.parent_part,
                part.part_index_in_query,
                part.part_starting_offset_in_query,
                std::move(ranges),
                part.read_hints);
        }
    }

    return split_parts_and_ranges;
}

IProcessor::Status LazyReadFromMergeTreeSource::prepare(const UpdatedInputPorts & updated_input_ports, const UpdatedOutputPorts & /*updated_output_ports*/)
{
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    if (lazy_materializing_rows)
        return Status::UpdatePipeline;

    /// Here we reading inputs as long as they are ready, to parallelize reading.
    /// But the chunks should be processed in the order of parts and ranges.
    /// So we keep the chunks in the list to keep the order.

    if (next_input_to_process != inputs.end())
    {
        for (const auto * input_port : updated_input_ports)
        {
            const auto input_num = input_port_to_index.at(input_port);
            auto & input = *const_cast<InputPort *>(input_port);
            if (!input.isFinished() && input.hasData())
            {
                auto chunk = input.pull();
                chunks[input_num].emplace_back(std::move(chunk));
            }
        }
    }

    while (next_input_to_process != inputs.end())
    {
        auto & input = *next_input_to_process;
        auto & lst = chunks[next_chunk_to_process];

        if (!lst.empty())
        {
            output.push(std::move(lst.front()));
            lst.pop_front();
            return Status::PortFull;
        }

        if (input.isFinished())
        {
            next_input_to_process++;
            next_chunk_to_process++;
            continue;
        }

        if (!input.hasData())
            return Status::NeedData;

        auto chunk = input.pull();
        lst.emplace_back(std::move(chunk));
        output.push(std::move(lst.front()));
        lst.pop_front();
        return Status::PortFull;
    }

    output.finish();
    return Status::Finished;
}

IProcessor::PipelineUpdate LazyReadFromMergeTreeSource::updatePipeline()
{
    if (!lazy_materializing_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LazyReadFromMergeTreeSource: No lazy materializing rows");
    auto processors = buildReaders();
    lazy_materializing_rows.reset();
    for (auto & processor : processors)
    {
        auto & output = processor->getOutputs().front();
        inputs.emplace_back(output.getHeader(), this);
        connect(output, inputs.back());
        inputs.back().setNeeded();
        input_port_to_index[&inputs.back()] = input_port_to_index.size();
    }

    next_input_to_process = inputs.begin();
    chunks.resize(processors.size());
    return PipelineUpdate{.to_add = std::move(processors), .to_remove = {}};
}

void LazyReadFromMergeTreeSource::takePointReadSources(RangesInDataParts & parts, SourcesByPart & sources)
{
    const Block & lazy_header = outputs.front().getHeader();

    auto vector_column = findPointReadVectorColumn(lazy_header, storage_snapshot->metadata->getColumns());
    if (!vector_column)
        return;

    /// The remaining lazy columns (in header order, minus the vector column) are read with a standard reader.
    NamesAndTypesList other_columns;
    for (const auto & col : lazy_header)
        if (col.name != vector_column->column.name)
            other_columns.emplace_back(col.name, col.type);

    auto mark_cache = context->getMarkCache();
    auto lazy_header_ptr = std::make_shared<const Block>(lazy_header);

    RangesInDataParts not_taken;
    not_taken.reserve(parts.size());

    for (auto & part_with_ranges : parts)
    {
        /// Each part is judged on its own: a part the point read cannot serve - a Compact one from a recent insert,
        /// a pending mutation, an unaligned layout - costs only its own granule read, not the whole fast path.
        if (!canPointReadPart(part_with_ranges, *vector_column, lazy_header, mutations_snapshot, context))
        {
            not_taken.push_back(std::move(part_with_ranges));
            continue;
        }

        auto & offsets = lazy_materializing_rows->rows_in_parts[part_with_ranges.part_index_in_query];
        const size_t total_rows = offsets.size();
        const UInt64 part_starting_offset = part_with_ranges.part_starting_offset_in_query;
        auto source = std::make_shared<MergeTreePointReadSource>(
            lazy_header_ptr,
            part_with_ranges,
            std::move(offsets),
            vector_column->column,
            vector_column->dimensions,
            other_columns,
            storage_snapshot,
            reader_settings,
            mark_cache,
            max_block_size);
        source->addTotalRowsApprox(total_rows);
        sources.emplace(part_starting_offset, std::move(source));
    }

    /// The runtime-statistics sample is collected by `MergeTreeReadTask`, which this path does not go through, so
    /// every part served here is missing from it. An entry short of the rescoring read is worse than none, because
    /// it prices later parallel-replica decisions.
    if (updater && !sources.empty())
        updater->markUnsupportedCase();

    parts = std::move(not_taken);
}

Processors LazyReadFromMergeTreeSource::buildReaders()
{
    const auto & ctx_settings = context->getSettingsRef();

    auto & parts = lazy_materializing_rows->ranges_in_data_parts;
    SourcesByPart sources;
    takePointReadSources(parts, sources);

    auto collect = [&sources]
    {
        Processors processors;
        for (auto & source : sources)
            processors.push_back(std::move(source.second));
        return processors;
    };

    if (parts.empty())
        return collect();

    size_t sum_marks = parts.getMarksCountAllParts();
    size_t sum_rows = parts.getRowsCountAllParts();

    MergeTreeReadPoolBase::PoolSettings pool_settings{
        .threads = max_threads,
        .sum_marks = sum_marks,
        .min_marks_for_concurrent_read = min_marks_for_concurrent_read,
        .preferred_block_size_bytes = ctx_settings[Setting::preferred_block_size_bytes],
        .use_uncompressed_cache = ctx_settings[Setting::use_uncompressed_cache],
        .use_const_size_tasks_for_remote_reading = ctx_settings[Setting::merge_tree_use_const_size_tasks_for_remote_reading],
        .total_query_nodes = 1,
    };

    MergeTreeReadTask::BlockSizeParams block_size{
        .max_block_size_rows = max_block_size,
        .preferred_block_size_bytes = ctx_settings[Setting::preferred_block_size_bytes],
        .preferred_max_column_in_block_size_bytes = ctx_settings[Setting::preferred_max_column_in_block_size_bytes]};

    auto ranges_in_data_parts = splitRanges(std::move(parts), sum_marks);
    /// Why this is needed?
    VirtualFields shared_virtual_fields;
    shared_virtual_fields.emplace("_sample_factor", 1.0);

    /// Lazy materialization reads a precomputed set of rows — no filter is applied here,
    /// so `sum_rows` is a hard upper bound. Treat it as a hard limit.
    bool has_hard_limit_below_one_block = sum_rows < block_size.max_block_size_rows;

    auto pool = std::make_shared<MergeTreeReadPoolInOrder>(
        has_hard_limit_below_one_block,
        /* has_soft_limit_below_one_block */ false,
        MergeTreeReadType::InOrder,
        ranges_in_data_parts,
        mutations_snapshot,
        shared_virtual_fields,
        /*index_read_tasks*/ IndexReadTasks{},
        storage_snapshot,
        /* row_level_filter */ nullptr,
        /* prewhere_info */ nullptr,
        actions_settings,
        reader_settings,
        outputs.front().getHeader().getNames(),
        pool_settings,
        block_size,
        context,
        updater);

    for (size_t i = 0; i < ranges_in_data_parts.size(); ++i)
    {
        const auto & part_with_ranges = ranges_in_data_parts[i];
        UInt64 total_rows = part_with_ranges.getRowsCount();

        MergeTreeSelectAlgorithmPtr algorithm = std::make_unique<MergeTreeInOrderSelectAlgorithm>(i);

        auto processor = std::make_unique<MergeTreeSelectProcessor>(
            pool,
            std::move(algorithm),
            nullptr,
            nullptr,
            /*index_read_tasks*/ IndexReadTasks{},
            actions_settings,
            reader_settings,
            /*index_build_context*/ nullptr,
            lazy_materializing_rows);

        auto source = std::make_shared<MergeTreeSource>(std::move(processor), log_name);
        source->addTotalRowsApprox(total_rows);

        sources.emplace(part_with_ranges.part_starting_offset_in_query, std::move(source));
    }
    return collect();
}

}
