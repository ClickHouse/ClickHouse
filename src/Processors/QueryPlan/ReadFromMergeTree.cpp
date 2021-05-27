#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPipeline.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/Transforms/ReverseTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/AddingSelectorTransform.h>
#include <Processors/Transforms/CopyTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/CollapsingSortedTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Merges/ReplacingSortedTransform.h>
#include <Processors/Merges/SummingSortedTransform.h>
#include <Processors/Merges/VersionedCollapsingTransform.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeReverseSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeThreadSelectBlockInputProcessor.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/VirtualColumnUtils.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <common/logger_useful.h>
#include <Common/JSONBuilder.h>

namespace ProfileEvents
{
    extern const Event SelectedParts;
    extern const Event SelectedRanges;
    extern const Event SelectedMarks;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INDEX_NOT_USED;
}

namespace
{

/// Marks are placed whenever threshold on rows or bytes is met.
/// So we have to return the number of marks on whatever estimate is higher - by rows or by bytes.
size_t roundRowsOrBytesToMarks(
    size_t rows_setting,
    size_t bytes_setting,
    size_t rows_granularity,
    size_t bytes_granularity)
{
    size_t res = (rows_setting + rows_granularity - 1) / rows_granularity;

    if (bytes_granularity == 0)
        return res;
    else
        return std::max(res, (bytes_setting + bytes_granularity - 1) / bytes_granularity);
}
/// Same as roundRowsOrBytesToMarks() but do not return more then max_marks
size_t minMarksForConcurrentRead(
    size_t rows_setting,
    size_t bytes_setting,
    size_t rows_granularity,
    size_t bytes_granularity,
    size_t max_marks)
{
    size_t marks = 1;

    if (rows_setting + rows_granularity <= rows_setting) /// overflow
        marks = max_marks;
    else if (rows_setting)
        marks = (rows_setting + rows_granularity - 1) / rows_granularity;

    if (bytes_granularity == 0)
        return marks;
    else
    {
        /// Overflow
        if (bytes_setting + bytes_granularity <= bytes_setting) /// overflow
            return max_marks;
        if (bytes_setting)
            return std::max(marks, (bytes_setting + bytes_granularity - 1) / bytes_granularity);
        else
            return marks;
    }
}

}

struct ReadFromMergeTree::AnalysisResult
{
    RangesInDataParts parts_with_ranges;
    MergeTreeDataSelectSamplingData sampling;
    bool sample_factor_column_queried = false;
    String query_id;
    IndexStats index_stats;
    Names column_names_to_read;
    ReadFromMergeTree::ReadType read_type = ReadFromMergeTree::ReadType::Default;
};

ReadFromMergeTree::ReadFromMergeTree(
    const SelectQueryInfo & query_info_,
    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read_,
    ContextPtr context_,
    const MergeTreeData & data_,
    StorageMetadataPtr metadata_snapshot_,
    StorageMetadataPtr metadata_snapshot_base_,
    Names real_column_names_,
    MergeTreeData::DataPartsVector parts_,
    PrewhereInfoPtr prewhere_info_,
    Names virt_column_names_,
    Settings settings_,
    Poco::Logger * log_)
    : ISourceStep(DataStream{.header = MergeTreeBaseSelectProcessor::transformHeader(
        metadata_snapshot_->getSampleBlockForColumns(real_column_names_, data_.getVirtuals(), data_.getStorageID()),
        prewhere_info_,
        data_.getPartitionValueType(),
        virt_column_names_)})
    , query_info(std::move(query_info_))
    , max_block_numbers_to_read(std::move(max_block_numbers_to_read_))
    , context(std::move(context_))
    , data(data_)
    , metadata_snapshot(std::move(metadata_snapshot_))
    , metadata_snapshot_base(std::move(metadata_snapshot_base_))
    , real_column_names(std::move(real_column_names_))
    , prepared_parts(std::move(parts_))
    , prewhere_info(std::move(prewhere_info_))
    , virt_column_names(std::move(virt_column_names_))
    , settings(std::move(settings_))
    , log(log_)
{
}

Pipe ReadFromMergeTree::readFromPool(
    RangesInDataParts parts_with_range, Names required_columns,
    size_t used_max_streams, size_t min_marks_for_concurrent_read, bool use_uncompressed_cache)
{
    Pipes pipes;
    size_t sum_marks = 0;
    size_t total_rows = 0;

    for (const auto & part : parts_with_range)
    {
        sum_marks += part.getMarksCount();
        total_rows += part.getRowsCount();
    }

    auto pool = std::make_shared<MergeTreeReadPool>(
        used_max_streams,
        sum_marks,
        min_marks_for_concurrent_read,
        std::move(parts_with_range),
        data,
        metadata_snapshot,
        prewhere_info,
        true,
        required_columns,
        settings.backoff_settings,
        settings.preferred_block_size_bytes,
        false);

    auto * logger = &Poco::Logger::get(data.getLogName() + " (SelectExecutor)");
    LOG_DEBUG(logger, "Reading approx. {} rows with {} streams", total_rows, used_max_streams);

    for (size_t i = 0; i < used_max_streams; ++i)
    {
        auto source = std::make_shared<MergeTreeThreadSelectBlockInputProcessor>(
            i, pool, min_marks_for_concurrent_read, settings.max_block_size,
            settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes,
            data, metadata_snapshot, use_uncompressed_cache,
            prewhere_info, settings.reader_settings, virt_column_names);

        if (i == 0)
        {
            /// Set the approximate number of rows for the first source only
            source->addTotalRowsApprox(total_rows);
        }

        pipes.emplace_back(std::move(source));
    }

    return Pipe::unitePipes(std::move(pipes));
}

template<typename TSource>
ProcessorPtr ReadFromMergeTree::createSource(const RangesInDataPart & part, const Names & required_columns, bool use_uncompressed_cache)
{
    return std::make_shared<TSource>(
            data, metadata_snapshot, part.data_part, settings.max_block_size, settings.preferred_block_size_bytes,
            settings.preferred_max_column_in_block_size_bytes, required_columns, part.ranges, use_uncompressed_cache,
            prewhere_info, true, settings.reader_settings, virt_column_names, part.part_index_in_query);
}

Pipe ReadFromMergeTree::readInOrder(RangesInDataParts parts_with_range, Names required_columns, ReadType read_type, bool use_uncompressed_cache)
{
    Pipes pipes;
    for (const auto & part : parts_with_range)
    {
        auto source = read_type == ReadType::InReverseOrder
                    ? createSource<MergeTreeReverseSelectProcessor>(part, required_columns, use_uncompressed_cache)
                    : createSource<MergeTreeSelectProcessor>(part, required_columns, use_uncompressed_cache);

        pipes.emplace_back(std::move(source));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    if (read_type == ReadType::InReverseOrder)
    {
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ReverseTransform>(header);
        });
    }

    return pipe;
}

Pipe ReadFromMergeTree::read(
    RangesInDataParts parts_with_range, Names required_columns, ReadType read_type,
    size_t used_max_streams, size_t min_marks_for_concurrent_read, bool use_uncompressed_cache)
{
    if (read_type == ReadType::Default && used_max_streams > 1)
        return readFromPool(parts_with_range, required_columns, used_max_streams, min_marks_for_concurrent_read, use_uncompressed_cache);

    auto pipe = readInOrder(parts_with_range, required_columns, read_type, use_uncompressed_cache);

    /// Use ConcatProcessor to concat sources together.
    /// It is needed to read in parts order (and so in PK order) if single thread is used.
    if (read_type == ReadType::Default && pipe.numOutputPorts() > 1)
        pipe.addTransform(std::make_shared<ConcatProcessor>(pipe.getHeader(), pipe.numOutputPorts()));

    return pipe;
}

Pipe ReadFromMergeTree::spreadMarkRangesAmongStreams(
    RangesInDataParts && parts_with_ranges,
    const Names & column_names)
{
    const auto & q_settings = context->getSettingsRef();

    /// Count marks for each part.
    std::vector<size_t> sum_marks_in_parts(parts_with_ranges.size());
    size_t sum_marks = 0;
    size_t total_rows = 0;

    const auto data_settings = data.getSettings();
    size_t adaptive_parts = 0;
    for (size_t i = 0; i < parts_with_ranges.size(); ++i)
    {
        total_rows += parts_with_ranges[i].getRowsCount();
        sum_marks_in_parts[i] = parts_with_ranges[i].getMarksCount();
        sum_marks += sum_marks_in_parts[i];

        if (parts_with_ranges[i].data_part->index_granularity_info.is_adaptive)
            ++adaptive_parts;
    }

    size_t index_granularity_bytes = 0;
    if (adaptive_parts > parts_with_ranges.size() / 2)
        index_granularity_bytes = data_settings->index_granularity_bytes;

    const size_t max_marks_to_use_cache = roundRowsOrBytesToMarks(
        q_settings.merge_tree_max_rows_to_use_cache,
        q_settings.merge_tree_max_bytes_to_use_cache,
        data_settings->index_granularity,
        index_granularity_bytes);

    const size_t min_marks_for_concurrent_read = minMarksForConcurrentRead(
        q_settings.merge_tree_min_rows_for_concurrent_read,
        q_settings.merge_tree_min_bytes_for_concurrent_read,
        data_settings->index_granularity,
        index_granularity_bytes,
        sum_marks);

    bool use_uncompressed_cache = q_settings.use_uncompressed_cache;
    if (sum_marks > max_marks_to_use_cache)
        use_uncompressed_cache = false;

    if (0 == sum_marks)
        return {};

    size_t used_num_streams = settings.num_streams;
    if (used_num_streams > 1)
    {
        /// Reduce the number of num_streams if the data is small.
        if (sum_marks < used_num_streams * min_marks_for_concurrent_read && parts_with_ranges.size() < used_num_streams)
            used_num_streams = std::max((sum_marks + min_marks_for_concurrent_read - 1) / min_marks_for_concurrent_read, parts_with_ranges.size());
    }

    return read(std::move(parts_with_ranges), column_names, ReadType::Default,
                used_num_streams, min_marks_for_concurrent_read, use_uncompressed_cache);
}

static ActionsDAGPtr createProjection(const Block & header)
{
    auto projection = std::make_shared<ActionsDAG>(header.getNamesAndTypesList());
    projection->removeUnusedActions(header.getNames());
    projection->projectInput();
    return projection;
}

Pipe ReadFromMergeTree::spreadMarkRangesAmongStreamsWithOrder(
    RangesInDataParts && parts_with_ranges,
    const Names & column_names,
    const ActionsDAGPtr & sorting_key_prefix_expr,
    ActionsDAGPtr & out_projection,
    const InputOrderInfoPtr & input_order_info)
{
    const auto & q_settings = context->getSettingsRef();
    size_t sum_marks = 0;
    size_t adaptive_parts = 0;
    std::vector<size_t> sum_marks_in_parts(parts_with_ranges.size());
    const auto data_settings = data.getSettings();

    for (size_t i = 0; i < parts_with_ranges.size(); ++i)
    {
        sum_marks_in_parts[i] = parts_with_ranges[i].getMarksCount();
        sum_marks += sum_marks_in_parts[i];

        if (parts_with_ranges[i].data_part->index_granularity_info.is_adaptive)
            ++adaptive_parts;
    }

    size_t index_granularity_bytes = 0;
    if (adaptive_parts > parts_with_ranges.size() / 2)
        index_granularity_bytes = data_settings->index_granularity_bytes;

    const size_t max_marks_to_use_cache = roundRowsOrBytesToMarks(
        q_settings.merge_tree_max_rows_to_use_cache,
        q_settings.merge_tree_max_bytes_to_use_cache,
        data_settings->index_granularity,
        index_granularity_bytes);

    const size_t min_marks_for_concurrent_read = minMarksForConcurrentRead(
        q_settings.merge_tree_min_rows_for_concurrent_read,
        q_settings.merge_tree_min_bytes_for_concurrent_read,
        data_settings->index_granularity,
        index_granularity_bytes,
        sum_marks);

    bool use_uncompressed_cache = settings.use_uncompressed_cache;
    if (sum_marks > max_marks_to_use_cache)
        use_uncompressed_cache = false;

    Pipes res;

    if (sum_marks == 0)
        return {};

    /// Let's split ranges to avoid reading much data.
    auto split_ranges = [rows_granularity = data_settings->index_granularity, max_block_size = settings.max_block_size]
        (const auto & ranges, int direction)
    {
        MarkRanges new_ranges;
        const size_t max_marks_in_range = (max_block_size + rows_granularity - 1) / rows_granularity;
        size_t marks_in_range = 1;

        if (direction == 1)
        {
            /// Split first few ranges to avoid reading much data.
            bool split = false;
            for (auto range : ranges)
            {
                while (!split && range.begin + marks_in_range < range.end)
                {
                    new_ranges.emplace_back(range.begin, range.begin + marks_in_range);
                    range.begin += marks_in_range;
                    marks_in_range *= 2;

                    if (marks_in_range > max_marks_in_range)
                        split = true;
                }
                new_ranges.emplace_back(range.begin, range.end);
            }
        }
        else
        {
            /// Split all ranges to avoid reading much data, because we have to
            ///  store whole range in memory to reverse it.
            for (auto it = ranges.rbegin(); it != ranges.rend(); ++it)
            {
                auto range = *it;
                while (range.begin + marks_in_range < range.end)
                {
                    new_ranges.emplace_front(range.end - marks_in_range, range.end);
                    range.end -= marks_in_range;
                    marks_in_range = std::min(marks_in_range * 2, max_marks_in_range);
                }
                new_ranges.emplace_front(range.begin, range.end);
            }
        }

        return new_ranges;
    };

    const size_t min_marks_per_stream = (sum_marks - 1) / settings.num_streams + 1;
    bool need_preliminary_merge = (parts_with_ranges.size() > q_settings.read_in_order_two_level_merge_threshold);

    Pipes pipes;

    for (size_t i = 0; i < settings.num_streams && !parts_with_ranges.empty(); ++i)
    {
        size_t need_marks = min_marks_per_stream;
        RangesInDataParts new_parts;

        /// Loop over parts.
        /// We will iteratively take part or some subrange of a part from the back
        ///  and assign a stream to read from it.
        while (need_marks > 0 && !parts_with_ranges.empty())
        {
            RangesInDataPart part = parts_with_ranges.back();
            parts_with_ranges.pop_back();

            size_t & marks_in_part = sum_marks_in_parts.back();

            /// We will not take too few rows from a part.
            if (marks_in_part >= min_marks_for_concurrent_read &&
                need_marks < min_marks_for_concurrent_read)
                need_marks = min_marks_for_concurrent_read;

            /// Do not leave too few rows in the part.
            if (marks_in_part > need_marks &&
                marks_in_part - need_marks < min_marks_for_concurrent_read)
                need_marks = marks_in_part;

            MarkRanges ranges_to_get_from_part;

            /// We take the whole part if it is small enough.
            if (marks_in_part <= need_marks)
            {
                ranges_to_get_from_part = part.ranges;

                need_marks -= marks_in_part;
                sum_marks_in_parts.pop_back();
            }
            else
            {
                /// Loop through ranges in part. Take enough ranges to cover "need_marks".
                while (need_marks > 0)
                {
                    if (part.ranges.empty())
                        throw Exception("Unexpected end of ranges while spreading marks among streams", ErrorCodes::LOGICAL_ERROR);

                    MarkRange & range = part.ranges.front();

                    const size_t marks_in_range = range.end - range.begin;
                    const size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);

                    ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
                    range.begin += marks_to_get_from_range;
                    marks_in_part -= marks_to_get_from_range;
                    need_marks -= marks_to_get_from_range;
                    if (range.begin == range.end)
                        part.ranges.pop_front();
                }
                parts_with_ranges.emplace_back(part);
            }
            ranges_to_get_from_part = split_ranges(ranges_to_get_from_part, input_order_info->direction);
            new_parts.emplace_back(part.data_part, part.part_index_in_query, std::move(ranges_to_get_from_part));
        }

        auto read_type = input_order_info->direction == 1
                       ? ReadFromMergeTree::ReadType::InOrder
                       : ReadFromMergeTree::ReadType::InReverseOrder;

        pipes.emplace_back(read(std::move(new_parts), column_names, read_type,
                           settings.num_streams, min_marks_for_concurrent_read, use_uncompressed_cache));
    }

    if (need_preliminary_merge)
    {
        SortDescription sort_description;
        for (size_t j = 0; j < input_order_info->order_key_prefix_descr.size(); ++j)
            sort_description.emplace_back(metadata_snapshot->getSortingKey().column_names[j],
                                          input_order_info->direction, 1);

        auto sorting_key_expr = std::make_shared<ExpressionActions>(sorting_key_prefix_expr);

        for (auto & pipe : pipes)
        {
            /// Drop temporary columns, added by 'sorting_key_prefix_expr'
            out_projection = createProjection(pipe.getHeader());

            pipe.addSimpleTransform([sorting_key_expr](const Block & header)
            {
                return std::make_shared<ExpressionTransform>(header, sorting_key_expr);
            });

            if (pipe.numOutputPorts() > 1)
            {

                auto transform = std::make_shared<MergingSortedTransform>(
                        pipe.getHeader(),
                        pipe.numOutputPorts(),
                        sort_description,
                        settings.max_block_size);

                pipe.addTransform(std::move(transform));
            }
        }
    }

    return Pipe::unitePipes(std::move(pipes));
}

static void addMergingFinal(
    Pipe & pipe,
    size_t num_output_streams,
    const SortDescription & sort_description,
    MergeTreeData::MergingParams merging_params,
    Names partition_key_columns,
    size_t max_block_size)
{
    const auto & header = pipe.getHeader();
    size_t num_outputs = pipe.numOutputPorts();

    auto get_merging_processor = [&]() -> MergingTransformPtr
    {
        switch (merging_params.mode)
        {
            case MergeTreeData::MergingParams::Ordinary:
            {
                return std::make_shared<MergingSortedTransform>(header, num_outputs,
                           sort_description, max_block_size);
            }

            case MergeTreeData::MergingParams::Collapsing:
                return std::make_shared<CollapsingSortedTransform>(header, num_outputs,
                           sort_description, merging_params.sign_column, true, max_block_size);

            case MergeTreeData::MergingParams::Summing:
                return std::make_shared<SummingSortedTransform>(header, num_outputs,
                           sort_description, merging_params.columns_to_sum, partition_key_columns, max_block_size);

            case MergeTreeData::MergingParams::Aggregating:
                return std::make_shared<AggregatingSortedTransform>(header, num_outputs,
                           sort_description, max_block_size);

            case MergeTreeData::MergingParams::Replacing:
                return std::make_shared<ReplacingSortedTransform>(header, num_outputs,
                           sort_description, merging_params.version_column, max_block_size);

            case MergeTreeData::MergingParams::VersionedCollapsing:
                return std::make_shared<VersionedCollapsingTransform>(header, num_outputs,
                           sort_description, merging_params.sign_column, max_block_size);

            case MergeTreeData::MergingParams::Graphite:
                throw Exception("GraphiteMergeTree doesn't support FINAL", ErrorCodes::LOGICAL_ERROR);
        }

        __builtin_unreachable();
    };

    if (num_output_streams <= 1 || sort_description.empty())
    {
        pipe.addTransform(get_merging_processor());
        return;
    }

    ColumnNumbers key_columns;
    key_columns.reserve(sort_description.size());

    for (const auto & desc : sort_description)
    {
        if (!desc.column_name.empty())
            key_columns.push_back(header.getPositionByName(desc.column_name));
        else
            key_columns.emplace_back(desc.column_number);
    }

    pipe.addSimpleTransform([&](const Block & stream_header)
    {
        return std::make_shared<AddingSelectorTransform>(stream_header, num_output_streams, key_columns);
    });

    pipe.transform([&](OutputPortRawPtrs ports)
    {
        Processors transforms;
        std::vector<OutputPorts::iterator> output_ports;
        transforms.reserve(ports.size() + num_output_streams);
        output_ports.reserve(ports.size());

        for (auto & port : ports)
        {
            auto copier = std::make_shared<CopyTransform>(header, num_output_streams);
            connect(*port, copier->getInputPort());
            output_ports.emplace_back(copier->getOutputs().begin());
            transforms.emplace_back(std::move(copier));
        }

        for (size_t i = 0; i < num_output_streams; ++i)
        {
            auto merge = get_merging_processor();
            merge->setSelectorPosition(i);
            auto input = merge->getInputs().begin();

            /// Connect i-th merge with i-th input port of every copier.
            for (size_t j = 0; j < ports.size(); ++j)
            {
                connect(*output_ports[j], *input);
                ++output_ports[j];
                ++input;
            }

            transforms.emplace_back(std::move(merge));
        }

        return transforms;
    });
}


Pipe ReadFromMergeTree::spreadMarkRangesAmongStreamsFinal(
    RangesInDataParts && parts_with_range,
    const Names & column_names,
    ActionsDAGPtr & out_projection)
{
    const auto & q_settings = context->getSettingsRef();
    const auto data_settings = data.getSettings();
    size_t sum_marks = 0;
    size_t adaptive_parts = 0;
    for (const auto & part : parts_with_range)
    {
        for (const auto & range : part.ranges)
            sum_marks += range.end - range.begin;

        if (part.data_part->index_granularity_info.is_adaptive)
            ++adaptive_parts;
    }

    size_t index_granularity_bytes = 0;
    if (adaptive_parts >= parts_with_range.size() / 2)
        index_granularity_bytes = data_settings->index_granularity_bytes;

    const size_t max_marks_to_use_cache = roundRowsOrBytesToMarks(
        q_settings.merge_tree_max_rows_to_use_cache,
        q_settings.merge_tree_max_bytes_to_use_cache,
        data_settings->index_granularity,
        index_granularity_bytes);

    bool use_uncompressed_cache = settings.use_uncompressed_cache;
    if (sum_marks > max_marks_to_use_cache)
        use_uncompressed_cache = false;

    size_t used_num_streams = settings.num_streams;
    if (used_num_streams > q_settings.max_final_threads)
        used_num_streams = q_settings.max_final_threads;

    /// If setting do_not_merge_across_partitions_select_final is true than we won't merge parts from different partitions.
    /// We have all parts in parts vector, where parts with same partition are nearby.
    /// So we will store iterators pointed to the beginning of each partition range (and parts.end()),
    /// then we will create a pipe for each partition that will run selecting processor and merging processor
    /// for the parts with this partition. In the end we will unite all the pipes.
    std::vector<RangesInDataParts::iterator> parts_to_merge_ranges;
    auto it = parts_with_range.begin();
    parts_to_merge_ranges.push_back(it);

    if (q_settings.do_not_merge_across_partitions_select_final)
    {
        while (it != parts_with_range.end())
        {
            it = std::find_if(
                it, parts_with_range.end(), [&it](auto & part) { return it->data_part->info.partition_id != part.data_part->info.partition_id; });
            parts_to_merge_ranges.push_back(it);
        }
        /// We divide threads for each partition equally. But we will create at least the number of partitions threads.
        /// (So, the total number of threads could be more than initial num_streams.
        used_num_streams /= (parts_to_merge_ranges.size() - 1);
    }
    else
    {
        /// If do_not_merge_across_partitions_select_final is false we just merge all the parts.
        parts_to_merge_ranges.push_back(parts_with_range.end());
    }

    Pipes partition_pipes;

    /// If do_not_merge_across_partitions_select_final is true and num_streams > 1
    /// we will store lonely parts with level > 0 to use parallel select on them.
    std::vector<RangesInDataPart> lonely_parts;
    size_t total_rows_in_lonely_parts = 0;
    size_t sum_marks_in_lonely_parts = 0;

    for (size_t range_index = 0; range_index < parts_to_merge_ranges.size() - 1; ++range_index)
    {
        Pipe pipe;

        {
            RangesInDataParts new_parts;

            /// If do_not_merge_across_partitions_select_final is true and there is only one part in partition
            /// with level > 0 then we won't postprocess this part and if num_streams > 1 we
            /// can use parallel select on such parts. We save such parts in one vector and then use
            /// MergeTreeReadPool and MergeTreeThreadSelectBlockInputProcessor for parallel select.
            if (used_num_streams > 1 && q_settings.do_not_merge_across_partitions_select_final &&
                std::distance(parts_to_merge_ranges[range_index], parts_to_merge_ranges[range_index + 1]) == 1 &&
                parts_to_merge_ranges[range_index]->data_part->info.level > 0)
            {
                total_rows_in_lonely_parts += parts_to_merge_ranges[range_index]->getRowsCount();
                sum_marks_in_lonely_parts += parts_to_merge_ranges[range_index]->getMarksCount();
                lonely_parts.push_back(std::move(*parts_to_merge_ranges[range_index]));
                continue;
            }
            else
            {
                for (auto part_it = parts_to_merge_ranges[range_index]; part_it != parts_to_merge_ranges[range_index + 1]; ++part_it)
                {
                    new_parts.emplace_back(part_it->data_part, part_it->part_index_in_query, part_it->ranges);
                }
            }

            if (new_parts.empty())
                continue;

            // ReadFromMergeTree::Settings step_settings
            // {
            //     .max_block_size = max_block_size,
            //     .preferred_block_size_bytes = settings.preferred_block_size_bytes,
            //     .preferred_max_column_in_block_size_bytes = settings.preferred_max_column_in_block_size_bytes,
            //     .min_marks_for_concurrent_read = 0, /// this setting is not used for reading in order
            //     .use_uncompressed_cache = use_uncompressed_cache,
            //     .reader_settings = reader_settings,
            //     .backoff_settings = MergeTreeReadPool::BackoffSettings(settings),
            // };

            pipe = read(std::move(new_parts), column_names, ReadFromMergeTree::ReadType::InOrder,
                used_num_streams, 0, use_uncompressed_cache);

            /// Drop temporary columns, added by 'sorting_key_expr'
            if (!out_projection)
                out_projection = createProjection(pipe.getHeader());
        }

        auto sorting_expr = std::make_shared<ExpressionActions>(
            metadata_snapshot->getSortingKey().expression->getActionsDAG().clone());

        pipe.addSimpleTransform([sorting_expr](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, sorting_expr);
        });

        /// If do_not_merge_across_partitions_select_final is true and there is only one part in partition
        /// with level > 0 then we won't postprocess this part
        if (q_settings.do_not_merge_across_partitions_select_final &&
            std::distance(parts_to_merge_ranges[range_index], parts_to_merge_ranges[range_index + 1]) == 1 &&
            parts_to_merge_ranges[range_index]->data_part->info.level > 0)
        {
            partition_pipes.emplace_back(std::move(pipe));
            continue;
        }

        Names sort_columns = metadata_snapshot->getSortingKeyColumns();
        SortDescription sort_description;
        size_t sort_columns_size = sort_columns.size();
        sort_description.reserve(sort_columns_size);

        Names partition_key_columns = metadata_snapshot->getPartitionKey().column_names;

        const auto & header = pipe.getHeader();
        for (size_t i = 0; i < sort_columns_size; ++i)
            sort_description.emplace_back(header.getPositionByName(sort_columns[i]), 1, 1);

        addMergingFinal(
            pipe,
            std::min<size_t>(used_num_streams, q_settings.max_final_threads),
            sort_description, data.merging_params, partition_key_columns, settings.max_block_size);

        // auto final_step = std::make_unique<MergingFinal>(
        //     plan->getCurrentDataStream(),
        //     std::min<size_t>(used_num_streams, settings.max_final_threads),
        //     sort_description,
        //     data.merging_params,
        //     partition_key_columns,
        //     max_block_size);

        // final_step->setStepDescription("Merge rows for FINAL");
        // plan->addStep(std::move(final_step));

        partition_pipes.emplace_back(std::move(pipe));
    }

    if (!lonely_parts.empty())
    {
        RangesInDataParts new_parts;

        size_t num_streams_for_lonely_parts = used_num_streams * lonely_parts.size();

        const size_t min_marks_for_concurrent_read = minMarksForConcurrentRead(
            q_settings.merge_tree_min_rows_for_concurrent_read,
            q_settings.merge_tree_min_bytes_for_concurrent_read,
            data_settings->index_granularity,
            index_granularity_bytes,
            sum_marks_in_lonely_parts);

        /// Reduce the number of num_streams_for_lonely_parts if the data is small.
        if (sum_marks_in_lonely_parts < num_streams_for_lonely_parts * min_marks_for_concurrent_read && lonely_parts.size() < num_streams_for_lonely_parts)
            num_streams_for_lonely_parts = std::max((sum_marks_in_lonely_parts + min_marks_for_concurrent_read - 1) / min_marks_for_concurrent_read, lonely_parts.size());

        // ReadFromMergeTree::Settings step_settings
        // {
        //     .max_block_size = max_block_size,
        //     .preferred_block_size_bytes = settings.preferred_block_size_bytes,
        //     .preferred_max_column_in_block_size_bytes = settings.preferred_max_column_in_block_size_bytes,
        //     .min_marks_for_concurrent_read = min_marks_for_concurrent_read,
        //     .use_uncompressed_cache = use_uncompressed_cache,
        //     .reader_settings = reader_settings,
        //     .backoff_settings = MergeTreeReadPool::BackoffSettings(settings),
        // };

        // auto plan = std::make_unique<QueryPlan>();
        // auto step = std::make_unique<ReadFromMergeTree>(
        //     data,
        //     metadata_snapshot,
        //     query_id,
        //     column_names,
        //     std::move(lonely_parts),
        //     // std::move(index_stats),
        //     query_info.projection ? query_info.projection->prewhere_info : query_info.prewhere_info,
        //     virt_columns,
        //     step_settings,
        //     num_streams_for_lonely_parts,
        //     ReadFromMergeTree::ReadType::Default);

        // plan->addStep(std::move(step));

        auto pipe = read(std::move(lonely_parts), column_names, ReadFromMergeTree::ReadType::Default,
                num_streams_for_lonely_parts, min_marks_for_concurrent_read, use_uncompressed_cache);

        /// Drop temporary columns, added by 'sorting_key_expr'
        if (!out_projection)
            out_projection = createProjection(pipe.getHeader());

        auto sorting_expr = std::make_shared<ExpressionActions>(
            metadata_snapshot->getSortingKey().expression->getActionsDAG().clone());

        pipe.addSimpleTransform([sorting_expr](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, sorting_expr);
        });

        // auto expression_step = std::make_unique<ExpressionStep>(
        //     plan->getCurrentDataStream(),
        //     metadata_snapshot->getSortingKey().expression->getActionsDAG().clone());

        // expression_step->setStepDescription("Calculate sorting key expression");
        // plan->addStep(std::move(expression_step));

        partition_pipes.emplace_back(std::move(pipe));
    }

    return Pipe::unitePipes(std::move(partition_pipes));
}

ReadFromMergeTree::AnalysisResult ReadFromMergeTree::selectRangesToRead(MergeTreeData::DataPartsVector parts) const
{
    AnalysisResult result;

    size_t total_parts = parts.size();

    auto part_values = MergeTreeDataSelectExecutor::filterPartsByVirtualColumns(data, parts, query_info.query, context);
    if (part_values && part_values->empty())
        return result;

    result.column_names_to_read = real_column_names;

    /// If there are only virtual columns in the query, you must request at least one non-virtual one.
    if (result.column_names_to_read.empty())
    {
        NamesAndTypesList available_real_columns = metadata_snapshot->getColumns().getAllPhysical();
        result.column_names_to_read.push_back(ExpressionActions::getSmallestColumn(available_real_columns));
    }

    metadata_snapshot->check(result.column_names_to_read, data.getVirtuals(), data.getStorageID());

    // Build and check if primary key is used when necessary
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    Names primary_key_columns = primary_key.column_names;
    KeyCondition key_condition(query_info, context, primary_key_columns, primary_key.expression);

    if (settings.force_primary_key && key_condition.alwaysUnknownOrTrue())
    {
        throw Exception(
            ErrorCodes::INDEX_NOT_USED,
            "Primary key ({}) is not used and setting 'force_primary_key' is set.",
            fmt::join(primary_key_columns, ", "));
    }
    LOG_DEBUG(log, "Key condition: {}", key_condition.toString());

    const auto & select = query_info.query->as<ASTSelectQuery &>();
    auto query_context = context->hasQueryContext() ? context->getQueryContext() : context;

    MergeTreeDataSelectExecutor::filterPartsByPartition(
        metadata_snapshot_base, data, query_info, context, query_context, parts, part_values, max_block_numbers_to_read.get(), log, result.index_stats);

    for (const auto & col : virt_column_names)
        if (col == "_sample_factor")
            result.sample_factor_column_queried = true;

    result.sampling = MergeTreeDataSelectExecutor::getSampling(
        select, parts, metadata_snapshot, key_condition,
        data, log, result.sample_factor_column_queried, metadata_snapshot->getColumns().getAllPhysical(), context);

    if (result.sampling.read_nothing)
        return result;

    size_t total_marks_pk = 0;
    for (const auto & part : parts)
        total_marks_pk += part->index_granularity.getMarksCountWithoutFinal();

    size_t parts_before_pk = parts.size();

    result.parts_with_ranges = MergeTreeDataSelectExecutor::filterPartsByPrimaryKeyAndSkipIndexes(
        std::move(parts),
        metadata_snapshot,
        query_info,
        context,
        key_condition,
        settings.reader_settings,
        log,
        settings.num_streams,
        result.index_stats,
        true);

    size_t sum_marks_pk = total_marks_pk;
    for (const auto & stat : result.index_stats)
        if (stat.type == IndexType::PrimaryKey)
            sum_marks_pk = stat.num_granules_after;

    size_t sum_marks = 0;
    size_t sum_ranges = 0;

    for (const auto & part : result.parts_with_ranges)
    {
        sum_ranges += part.ranges.size();
        sum_marks += part.getMarksCount();
    }

    LOG_DEBUG(
        log,
        "Selected {}/{} parts by partition key, {} parts by primary key, {}/{} marks by primary key, {} marks to read from {} ranges",
        parts_before_pk,
        total_parts,
        result.parts_with_ranges.size(),
        sum_marks_pk,
        total_marks_pk,
        sum_marks,
        sum_ranges);

    result.query_id = MergeTreeDataSelectExecutor::checkLimits(data, result.parts_with_ranges, context);

    ProfileEvents::increment(ProfileEvents::SelectedParts, result.parts_with_ranges.size());
    ProfileEvents::increment(ProfileEvents::SelectedRanges, sum_ranges);
    ProfileEvents::increment(ProfileEvents::SelectedMarks, sum_marks);

    const auto & input_order_info = query_info.input_order_info
        ? query_info.input_order_info
        : (query_info.projection ? query_info.projection->input_order_info : nullptr);

    const auto & q_settings = context->getSettingsRef();
    if ((q_settings.optimize_read_in_order || q_settings.optimize_aggregation_in_order) && input_order_info)
        result.read_type = (input_order_info->direction > 0) ? ReadType::InOrder
                                                             : ReadType::InReverseOrder;

    return result;
}

void ReadFromMergeTree::initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    auto result = selectRangesToRead(prepared_parts);
    if (result.parts_with_ranges.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
        return;
    }

    /// Projection, that needed to drop columns, which have appeared by execution
    /// of some extra expressions, and to allow execute the same expressions later.
    /// NOTE: It may lead to double computation of expressions.
    ActionsDAGPtr result_projection;

    Names column_names_to_read = std::move(result.column_names_to_read);
    const auto & select = query_info.query->as<ASTSelectQuery &>();
    if (!select.final() && result.sampling.use_sampling)
    {
        /// Add columns needed for `sample_by_ast` to `column_names_to_read`.
        /// Skip this if final was used, because such columns were already added from PK.
        std::vector<String> add_columns = result.sampling.filter_expression->getRequiredColumns().getNames();
        column_names_to_read.insert(column_names_to_read.end(), add_columns.begin(), add_columns.end());
        std::sort(column_names_to_read.begin(), column_names_to_read.end());
        column_names_to_read.erase(std::unique(column_names_to_read.begin(), column_names_to_read.end()),
                                   column_names_to_read.end());
    }

    const auto & input_order_info = query_info.input_order_info
        ? query_info.input_order_info
        : (query_info.projection ? query_info.projection->input_order_info : nullptr);

    Pipe pipe;

    const auto & q_settings = context->getSettingsRef();

    if (select.final())
    {
        /// Add columns needed to calculate the sorting expression and the sign.
        std::vector<String> add_columns = metadata_snapshot->getColumnsRequiredForSortingKey();
        column_names_to_read.insert(column_names_to_read.end(), add_columns.begin(), add_columns.end());

        if (!data.merging_params.sign_column.empty())
            column_names_to_read.push_back(data.merging_params.sign_column);
        if (!data.merging_params.version_column.empty())
            column_names_to_read.push_back(data.merging_params.version_column);

        std::sort(column_names_to_read.begin(), column_names_to_read.end());
        column_names_to_read.erase(std::unique(column_names_to_read.begin(), column_names_to_read.end()), column_names_to_read.end());

        pipe = spreadMarkRangesAmongStreamsFinal(
            std::move(result.parts_with_ranges),
            column_names_to_read,
            result_projection);
    }
    else if ((q_settings.optimize_read_in_order || q_settings.optimize_aggregation_in_order) && input_order_info)
    {
        size_t prefix_size = input_order_info->order_key_prefix_descr.size();
        auto order_key_prefix_ast = metadata_snapshot->getSortingKey().expression_list_ast->clone();
        order_key_prefix_ast->children.resize(prefix_size);

        auto syntax_result = TreeRewriter(context).analyze(order_key_prefix_ast, metadata_snapshot->getColumns().getAllPhysical());
        auto sorting_key_prefix_expr = ExpressionAnalyzer(order_key_prefix_ast, syntax_result, context).getActionsDAG(false);

        pipe = spreadMarkRangesAmongStreamsWithOrder(
            std::move(result.parts_with_ranges),
            column_names_to_read,
            sorting_key_prefix_expr,
            result_projection,
            input_order_info);
    }
    else
    {
        pipe = spreadMarkRangesAmongStreams(
            std::move(result.parts_with_ranges),
            column_names_to_read);
    }

    if (pipe.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
        return;
    }

    if (result.sampling.use_sampling)
    {
        auto sampling_actions = std::make_shared<ExpressionActions>(result.sampling.filter_expression);
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<FilterTransform>(
                header,
                sampling_actions,
                result.sampling.filter_function->getColumnName(),
                false);
        });
    }

    if (result_projection)
    {
        auto projection_actions = std::make_shared<ExpressionActions>(result_projection);
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, projection_actions);
        });
    }

    /// By the way, if a distributed query or query to a Merge table is made, then the `_sample_factor` column can have different values.
    if (result.sample_factor_column_queried)
    {
        ColumnWithTypeAndName column;
        column.name = "_sample_factor";
        column.type = std::make_shared<DataTypeFloat64>();
        column.column = column.type->createColumnConst(0, Field(result.sampling.used_sample_factor));

        auto adding_column_dag = ActionsDAG::makeAddingColumnActions(std::move(column));
        auto adding_column_action = std::make_shared<ExpressionActions>(adding_column_dag);

        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, adding_column_action);
        });
    }

    // TODO There seems to be no place initializing remove_columns_actions
    if (query_info.prewhere_info && query_info.prewhere_info->remove_columns_actions)
    {
        auto remove_columns_action = std::make_shared<ExpressionActions>(
            query_info.prewhere_info->remove_columns_actions->getActionsDAG().clone());

        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, remove_columns_action);
        });
    }

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    // Attach QueryIdHolder if needed
    if (!result.query_id.empty())
        pipe.addQueryIdHolder(std::make_shared<QueryIdHolder>(result.query_id, data));

    pipeline.init(std::move(pipe));
}

static const char * indexTypeToString(ReadFromMergeTree::IndexType type)
{
    switch (type)
    {
        case ReadFromMergeTree::IndexType::None:
            return "None";
        case ReadFromMergeTree::IndexType::MinMax:
            return "MinMax";
        case ReadFromMergeTree::IndexType::Partition:
            return "Partition";
        case ReadFromMergeTree::IndexType::PrimaryKey:
            return "PrimaryKey";
        case ReadFromMergeTree::IndexType::Skip:
            return "Skip";
    }

    __builtin_unreachable();
}

static const char * readTypeToString(ReadFromMergeTree::ReadType type)
{
    switch (type)
    {
        case ReadFromMergeTree::ReadType::Default:
            return "Default";
        case ReadFromMergeTree::ReadType::InOrder:
            return "InOrder";
        case ReadFromMergeTree::ReadType::InReverseOrder:
            return "InReverseOrder";
    }

    __builtin_unreachable();
}

void ReadFromMergeTree::describeActions(FormatSettings & format_settings) const
{
    auto result = selectRangesToRead(prepared_parts);
    std::string prefix(format_settings.offset, format_settings.indent_char);
    format_settings.out << prefix << "ReadType: " << readTypeToString(result.read_type) << '\n';

    if (!result.index_stats.empty())
    {
        format_settings.out << prefix << "Parts: " << result.index_stats.back().num_parts_after << '\n';
        format_settings.out << prefix << "Granules: " << result.index_stats.back().num_granules_after << '\n';
    }
}

void ReadFromMergeTree::describeActions(JSONBuilder::JSONMap & map) const
{
    auto result = selectRangesToRead(prepared_parts);
    map.add("Read Type", readTypeToString(result.read_type));
    if (!result.index_stats.empty())
    {
        map.add("Parts", result.index_stats.back().num_parts_after);
        map.add("Granules", result.index_stats.back().num_granules_after);
    }
}

void ReadFromMergeTree::describeIndexes(FormatSettings & format_settings) const
{
    auto result = selectRangesToRead(prepared_parts);
    auto index_stats = std::move(result.index_stats);

    std::string prefix(format_settings.offset, format_settings.indent_char);
    if (!index_stats.empty())
    {
        /// Do not print anything if no indexes is applied.
        if (index_stats.size() == 1 && index_stats.front().type == IndexType::None)
            return;

        std::string indent(format_settings.indent, format_settings.indent_char);
        format_settings.out << prefix << "Indexes:\n";

        for (size_t i = 0; i < index_stats.size(); ++i)
        {
            const auto & stat = index_stats[i];
            if (stat.type == IndexType::None)
                continue;

            format_settings.out << prefix << indent << indexTypeToString(stat.type) << '\n';

            if (!stat.name.empty())
                format_settings.out << prefix << indent << indent << "Name: " << stat.name << '\n';

            if (!stat.description.empty())
                format_settings.out << prefix << indent << indent << "Description: " << stat.description << '\n';

            if (!stat.used_keys.empty())
            {
                format_settings.out << prefix << indent << indent << "Keys: " << stat.name << '\n';
                for (const auto & used_key : stat.used_keys)
                    format_settings.out << prefix << indent << indent << indent << used_key << '\n';
            }

            if (!stat.condition.empty())
                format_settings.out << prefix << indent << indent << "Condition: " << stat.condition << '\n';

            format_settings.out << prefix << indent << indent << "Parts: " << stat.num_parts_after;
            if (i)
                format_settings.out << '/' << index_stats[i - 1].num_parts_after;
            format_settings.out << '\n';

            format_settings.out << prefix << indent << indent << "Granules: " << stat.num_granules_after;
            if (i)
                format_settings.out << '/' << index_stats[i - 1].num_granules_after;
            format_settings.out << '\n';
        }
    }
}

void ReadFromMergeTree::describeIndexes(JSONBuilder::JSONMap & map) const
{
    auto result = selectRangesToRead(prepared_parts);
    auto index_stats = std::move(result.index_stats);

    if (!index_stats.empty())
    {
        /// Do not print anything if no indexes is applied.
        if (index_stats.size() == 1 && index_stats.front().type == IndexType::None)
            return;

        auto indexes_array = std::make_unique<JSONBuilder::JSONArray>();

        for (size_t i = 0; i < index_stats.size(); ++i)
        {
            const auto & stat = index_stats[i];
            if (stat.type == IndexType::None)
                continue;

            auto index_map = std::make_unique<JSONBuilder::JSONMap>();

            index_map->add("Type", indexTypeToString(stat.type));

            if (!stat.name.empty())
                index_map->add("Name", stat.name);

            if (!stat.description.empty())
                index_map->add("Description", stat.description);

            if (!stat.used_keys.empty())
            {
                auto keys_array = std::make_unique<JSONBuilder::JSONArray>();

                for (const auto & used_key : stat.used_keys)
                    keys_array->add(used_key);

                index_map->add("Keys", std::move(keys_array));
            }

            if (!stat.condition.empty())
                index_map->add("Condition", stat.condition);

            if (i)
                index_map->add("Initial Parts", index_stats[i - 1].num_parts_after);
            index_map->add("Selected Parts", stat.num_parts_after);

            if (i)
                index_map->add("Initial Granules", index_stats[i - 1].num_granules_after);
            index_map->add("Selected Granules", stat.num_granules_after);

            indexes_array->add(std::move(index_map));
        }

        map.add("Indexes", std::move(indexes_array));
    }
}

}
