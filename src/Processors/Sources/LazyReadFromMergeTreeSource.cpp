#include <Processors/Sources/LazyReadFromMergeTreeSource.h>
#include <Processors/QueryPlan/LazilyReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeReadPoolInOrder.h>
#include <Processors/Transforms/LazyMaterializingTransform.h>
#include <Interpreters/Context.h>
#include <Interpreters/Squashing.h>
#include <Core/Settings.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeSelectAlgorithms.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeSource.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/StorageSnapshot.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <algorithm>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 preferred_block_size_bytes;
    extern const SettingsUInt64 preferred_max_column_in_block_size_bytes;
    extern const SettingsBool merge_tree_use_const_size_tasks_for_remote_reading;
    extern const SettingsBool use_uncompressed_cache;
    extern const SettingsBool use_columns_cache;
    extern const SettingsUInt64 query_plan_max_column_groups_for_lazy_materialization;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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

    if (num_column_groups > 1)
        return prepareColumnGroups(updated_input_ports);

    /// Here we reading inputs as long as they are ready, to parallelize reading.
    /// But the chunks should be processed in the order of parts and ranges.
    /// So we keep the chunks in the list to keep the order.
    /// It is done before checking the output: an updated port is reported only once, and a reader whose chunk
    /// is left in its port does not read further until its turn in the loop below.
    if (!lazy_materializing_rows && next_input_to_process != inputs.end())
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

    if (!output.canPush())
        return Status::PortFull;

    if (lazy_materializing_rows)
        return Status::UpdatePipeline;

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

IProcessor::Status LazyReadFromMergeTreeSource::prepareColumnGroups(const UpdatedInputPorts & updated_input_ports)
{
    auto & output = outputs.front();

    /// All readers run at once: take their chunks as soon as they are ready (see `prepare`).
    for (const auto * input_port : updated_input_ports)
    {
        const auto input_num = input_port_to_index.at(input_port);
        auto & input = *const_cast<InputPort *>(input_port);
        if (!input.isFinished() && input.hasData())
            chunks[input_num].emplace_back(input.pull());
    }

    if (!output.canPush())
        return Status::PortFull;

    while (next_range_to_process < num_ranges)
    {
        bool all_finished = true;
        for (size_t column_group = 0; column_group < num_column_groups; ++column_group)
        {
            const size_t input_num = next_range_to_process * num_column_groups + column_group;
            auto & input = *input_ports[input_num];
            if (input.isFinished())
                continue;

            all_finished = false;
            if (input.hasData())
                chunks[input_num].emplace_back(input.pull());
        }

        if (!all_finished)
            return Status::NeedData;

        auto chunk = joinColumnGroups(next_range_to_process);
        ++next_range_to_process;
        if (chunk.getNumRows())
        {
            output.push(std::move(chunk));
            return Status::PortFull;
        }
    }

    output.finish();
    return Status::Finished;
}

Chunk LazyReadFromMergeTreeSource::joinColumnGroups(size_t range)
{
    std::optional<size_t> num_rows;
    VectorWithMemoryTracking<Columns> column_group_columns(num_column_groups);
    for (size_t column_group = 0; column_group < num_column_groups; ++column_group)
    {
        auto & column_group_chunks = chunks[range * num_column_groups + column_group];
        Chunks non_empty;
        for (auto & chunk : column_group_chunks)
            if (chunk.getNumRows())
                non_empty.push_back(std::move(chunk));
        column_group_chunks.clear();

        size_t column_group_rows = 0;
        if (non_empty.size() == 1)
        {
            column_group_rows = non_empty.front().getNumRows();
            column_group_columns[column_group] = non_empty.front().detachColumns();
        }
        else if (!non_empty.empty())
        {
            auto squashed = Squashing::squashWithoutChunkInfo(std::move(non_empty));
            column_group_rows = squashed.getNumRows();
            column_group_columns[column_group] = squashed.detachColumns();
        }

        /// The readers of the column groups of a range read the same rows.
        if (num_rows && *num_rows != column_group_rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "LazyReadFromMergeTreeSource: the readers of the column groups of a range read {} and {} rows",
                *num_rows, column_group_rows);
        num_rows = column_group_rows;
    }

    if (!num_rows || *num_rows == 0)
        return {};

    Columns columns;
    columns.reserve(output_column_sources.size());
    for (const auto & [column_group, position] : output_column_sources)
        columns.push_back(column_group_columns[column_group].at(position));
    return Chunk(std::move(columns), *num_rows);
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
        input_ports.push_back(&inputs.back());
    }

    if (num_column_groups > 1)
    {
        num_ranges = processors.size() / num_column_groups;

        /// Every column of the output is read by exactly one column group.
        const auto & header = outputs.front().getHeader();
        output_column_sources.clear();
        output_column_sources.reserve(header.columns());
        for (const auto & column : header)
        {
            std::optional<std::pair<size_t, size_t>> source;
            for (size_t column_group = 0; column_group < num_column_groups && !source; ++column_group)
                if (auto position = input_ports[column_group]->getHeader().findPositionByName(column.name))
                    source.emplace(column_group, *position);

            if (!source)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "LazyReadFromMergeTreeSource: no column group reads the column {}", column.name);
            output_column_sources.push_back(*source);
        }
    }

    next_input_to_process = inputs.begin();
    chunks.resize(processors.size());
    return PipelineUpdate{.to_add = std::move(processors), .to_remove = {}};
}

VectorWithMemoryTracking<Names> LazyReadFromMergeTreeSource::splitColumnsIntoGroups(const RangesInDataParts & parts_with_ranges) const
{
    const auto & header = outputs.front().getHeader();
    VectorWithMemoryTracking<Names> column_groups(1, header.getNames());

    /// A query whose main branch reads with one thread has no other thread for the readers of the column groups.
    const size_t max_column_groups = context->getSettingsRef()[Setting::query_plan_max_column_groups_for_lazy_materialization];
    const size_t num_streams = parts_with_ranges.size();
    const size_t threads = std::min(max_threads, lazy_materializing_rows->num_threads);
    if (max_column_groups <= 1 || threads <= 1 || num_streams == 0)
        return column_groups;

    /// Each reader would compute these values on its own instead of reading them.
    if ((mutations_snapshot && mutations_snapshot->hasDataMutations())
        || MergeTreeDataSelectExecutor::hasPendingAlterOrMetadataMutations(parts_with_ranges, mutations_snapshot)
        || parts_with_ranges.front().data_part->storage.hasEnabledMaskingPolicies(context))
        return column_groups;

    /// The columns of a compact part are read from one file, and each reader fills a column a part lacks from its default.
    const auto & columns_description = storage_snapshot->metadata->getColumns();
    for (const auto & part : parts_with_ranges)
    {
        if (!isWidePart(part.data_part))
            return column_groups;

        for (const auto & column : header)
            if (columns_description.hasPhysical(column.name) && !part.data_part->getColumnPosition(column.name))
                return column_groups;
    }

    /// Only the physical columns are balanced, the other ones (virtual columns, subcolumns) are in the first column group.
    struct Column
    {
        UInt64 bytes = 0;
        size_t position = 0;
    };
    VectorWithMemoryTracking<Column> columns_to_split;
    VectorWithMemoryTracking<size_t> other_columns;
    NameSet names;
    for (size_t position = 0; position < header.columns(); ++position)
    {
        const auto & name = header.getByPosition(position).name;
        if (!names.insert(name).second)
            return column_groups;

        if (!columns_description.hasPhysical(name))
        {
            other_columns.push_back(position);
            continue;
        }

        Column column{.bytes = 0, .position = position};
        for (const auto & part : parts_with_ranges)
            column.bytes += part.data_part->getColumnSize(name).data_uncompressed;
        columns_to_split.push_back(column);
    }

    /// About four readers per thread, at most one column group per eight physical columns: every reader also finds the rows.
    static constexpr size_t readers_per_thread = 4;
    static constexpr size_t min_columns_in_column_group = 8;
    const size_t column_group_count = std::min(
        {max_column_groups,
         (readers_per_thread * threads + num_streams - 1) / num_streams,
         columns_to_split.size() / min_columns_in_column_group});
    if (column_group_count <= 1)
        return column_groups;

    /// The largest columns first, each to the column group with the fewest bytes so far.
    std::ranges::stable_sort(columns_to_split, std::ranges::greater{}, &Column::bytes);
    VectorWithMemoryTracking<UInt64> column_group_bytes(column_group_count, 0);
    VectorWithMemoryTracking<VectorWithMemoryTracking<size_t>> column_group_positions(column_group_count);
    column_group_positions[0] = other_columns;
    for (const auto & column : columns_to_split)
    {
        const size_t column_group = std::ranges::min_element(column_group_bytes) - column_group_bytes.begin();
        column_group_bytes[column_group] += column.bytes;
        column_group_positions[column_group].push_back(column.position);
    }

    column_groups.assign(column_group_count, {});
    for (size_t column_group = 0; column_group < column_group_count; ++column_group)
    {
        std::ranges::sort(column_group_positions[column_group]);
        for (size_t position : column_group_positions[column_group])
            column_groups[column_group].push_back(header.getByPosition(position).name);
    }
    return column_groups;
}

Processors LazyReadFromMergeTreeSource::buildReaders()
{
    const auto & ctx_settings = context->getSettingsRef();
    size_t sum_marks = lazy_materializing_rows->ranges_in_data_parts.getMarksCountAllParts();

    MergeTreeReadTask::BlockSizeParams block_size{
        .max_block_size_rows = max_block_size,
        .preferred_block_size_bytes = ctx_settings[Setting::preferred_block_size_bytes],
        .preferred_max_column_in_block_size_bytes = ctx_settings[Setting::preferred_max_column_in_block_size_bytes]};

    auto ranges_in_data_parts = splitRanges(std::move(lazy_materializing_rows->ranges_in_data_parts), sum_marks);
    const auto column_groups = splitColumnsIntoGroups(ranges_in_data_parts);
    num_column_groups = column_groups.size();
    /// Why this is needed?
    VirtualFields shared_virtual_fields;
    shared_virtual_fields.emplace("_sample_factor", 1.0);

    /// The parts of a projection, which the main read was served some of its parts from, are read with the snapshot of that
    /// read, which has the metadata of the projection, so each such snapshot has a pool of its own, and the table has one. The
    /// readers still follow the order of the parts, which is the order of the rows.
    struct Group
    {
        StorageSnapshotPtr snapshot;
        RangesInDataParts parts;
        /// One pool per column group.
        VectorWithMemoryTracking<std::shared_ptr<MergeTreeReadPoolInOrder>> pools;
    };
    VectorWithMemoryTracking<Group> groups;
    UnorderedMapWithMemoryTracking<const StorageSnapshot *, size_t> group_of_snapshot;
    VectorWithMemoryTracking<std::pair<size_t, size_t>> group_and_index_of_part;
    for (auto & part : ranges_in_data_parts)
    {
        StorageSnapshotPtr part_snapshot = storage_snapshot;
        if (part.data_part->isProjectionPart())
        {
            const auto it = lazy_materializing_rows->snapshots_of_projection_parts.find(part.part_index_in_query);
            if (it == lazy_materializing_rows->snapshots_of_projection_parts.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "The projection part {} of the part {} is read lazily without its snapshot",
                    part.data_part->name, part.parent_part ? part.parent_part->name : "");
            part_snapshot = it->second;
        }

        const auto [it, inserted] = group_of_snapshot.try_emplace(part_snapshot.get(), groups.size());
        if (inserted)
            groups.push_back({.snapshot = part_snapshot, .parts = {}, .pools = {}});

        auto & group = groups[it->second];
        group_and_index_of_part.emplace_back(it->second, group.parts.size());
        group.parts.push_back(std::move(part));
    }

    for (auto & group : groups)
    {
        /// As the projection was read in the main branch, without the mutations of the table.
        auto group_mutations_snapshot = group.snapshot == storage_snapshot ? mutations_snapshot : mutations_snapshot->cloneEmpty();

        const size_t group_marks = group.parts.getMarksCountAllParts();
        MergeTreeReadPoolBase::PoolSettings pool_settings{
            .threads = max_threads,
            .sum_marks = group_marks,
            .min_marks_for_concurrent_read = min_marks_for_concurrent_read,
            .preferred_block_size_bytes = ctx_settings[Setting::preferred_block_size_bytes],
            .use_uncompressed_cache = ctx_settings[Setting::use_uncompressed_cache],
            .use_columns_cache = ctx_settings[Setting::use_columns_cache],
            .use_const_size_tasks_for_remote_reading = ctx_settings[Setting::merge_tree_use_const_size_tasks_for_remote_reading],
            .total_query_nodes = 1,
        };

        /// Lazy materialization reads a precomputed set of rows — no filter is applied here,
        /// so the number of rows is a hard upper bound. Treat it as a hard limit.
        const bool has_hard_limit_below_one_block = group.parts.getRowsCountAllParts() < block_size.max_block_size_rows;

        for (const auto & column_names : column_groups)
        {
            group.pools.push_back(std::make_shared<MergeTreeReadPoolInOrder>(
                has_hard_limit_below_one_block,
                /* has_soft_limit_below_one_block */ false,
                MergeTreeReadType::InOrder,
                group.parts,
                group_mutations_snapshot,
                shared_virtual_fields,
                /*index_read_tasks*/ IndexReadTasks{},
                group.snapshot,
                /* row_level_filter */ nullptr,
                /* prewhere_info */ nullptr,
                actions_settings,
                reader_settings,
                column_names,
                pool_settings,
                block_size,
                context,
                updater));
        }
    }

    Processors processors;
    for (const auto & [group_index, index_in_group] : group_and_index_of_part)
    {
        const auto & group = groups[group_index];
        UInt64 total_rows = group.parts[index_in_group].getRowsCount();

        for (size_t column_group = 0; column_group < num_column_groups; ++column_group)
        {
            MergeTreeSelectAlgorithmPtr algorithm = std::make_unique<MergeTreeInOrderSelectAlgorithm>(index_in_group);

            auto processor = std::make_unique<MergeTreeSelectProcessor>(
                group.pools[column_group],
                std::move(algorithm),
                nullptr,
                nullptr,
                /*index_read_tasks*/ IndexReadTasks{},
                actions_settings,
                reader_settings,
                /*index_build_context*/ nullptr,
                lazy_materializing_rows);

            auto source = std::make_shared<MergeTreeSource>(std::move(processor), log_name);
            source->keepReadingOnPartialResult();

            /// The readers of the other column groups read the same rows again.
            if (column_group == 0)
                source->addTotalRowsApprox(total_rows);
            else
                source->doNotCountReadRows();

            processors.emplace_back(std::move(source));
        }
    }
    return processors;
}

}
