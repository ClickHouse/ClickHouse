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
                    std::move(part.ranges));

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
                std::move(ranges));
        }
    }

    return split_parts_and_ranges;
}

IProcessor::Status LazyReadFromMergeTreeSource::prepare(const PortNumbers & updated_input_ports, const PortNumbers & /*updated_output_ports*/)
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
        return Status::ExpandPipeline;

    /// Here we reading inputs as long as they are ready, to parallelize reading.
    /// But the chunks should be processed in the order of parts and ranges.
    /// So we keep the chunks in the list to keep the order.

    if (next_input_to_process != inputs.end())
    {
        for (auto input_num : updated_input_ports)
        {
            auto it = inputs.begin();
            std::advance(it, input_num);
            auto & input = *it;
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

Processors LazyReadFromMergeTreeSource::expandPipeline()
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
    }

    next_input_to_process = inputs.begin();
    chunks.resize(processors.size());
    return processors;
}

Processors LazyReadFromMergeTreeSource::buildReaders()
{
    const auto & ctx_settings = context->getSettingsRef();
    size_t sum_marks = lazy_materializing_rows->ranges_in_data_parts.getMarksCountAllParts();
    size_t sum_rows = lazy_materializing_rows->ranges_in_data_parts.getRowsCountAllParts();

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

    auto ranges_in_data_parts = splitRanges(std::move(lazy_materializing_rows->ranges_in_data_parts), sum_marks);
    /// Why this is needed?
    VirtualFields shared_virtual_fields;
    shared_virtual_fields.emplace("_sample_factor", 1.0);

    bool has_limit_below_one_block = sum_rows < block_size.max_block_size_rows;

    auto pool = std::make_shared<MergeTreeReadPoolInOrder>(
        has_limit_below_one_block,
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

    Processors processors;
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

        processors.emplace_back(std::move(source));
    }
    return processors;
}

}
