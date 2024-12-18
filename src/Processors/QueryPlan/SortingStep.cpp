#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/Transforms/FinishSortingTransform.h>
#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/QueryPlan/BufferChunksTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>
#include <Core/Settings.h>

#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/ScatterByPartitionTransform.h>

#include <memory>

namespace CurrentMetrics
{
    extern const Metric TemporaryFilesForSort;
}

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_block_size;
    extern const SettingsUInt64 max_bytes_before_external_sort;
    extern const SettingsUInt64 max_bytes_before_remerge_sort;
    extern const SettingsUInt64 max_bytes_to_sort;
    extern const SettingsUInt64 max_rows_to_sort;
    extern const SettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const SettingsUInt64 prefer_external_sort_block_bytes;
    extern const SettingsBool read_in_order_use_buffering;
    extern const SettingsFloat remerge_sort_lowered_memory_bytes_ratio;
    extern const SettingsOverflowMode sort_overflow_mode;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

SortingStep::Settings::Settings(const Context & context)
{
    const auto & settings = context.getSettingsRef();
    max_block_size = settings[Setting::max_block_size];
    size_limits = SizeLimits(settings[Setting::max_rows_to_sort], settings[Setting::max_bytes_to_sort], settings[Setting::sort_overflow_mode]);
    max_bytes_before_remerge = settings[Setting::max_bytes_before_remerge_sort];
    remerge_lowered_memory_bytes_ratio = settings[Setting::remerge_sort_lowered_memory_bytes_ratio];
    max_bytes_before_external_sort = settings[Setting::max_bytes_before_external_sort];
    tmp_data = context.getTempDataOnDisk();
    min_free_disk_space = settings[Setting::min_free_disk_space_for_temporary_data];
    max_block_bytes = settings[Setting::prefer_external_sort_block_bytes];
    read_in_order_use_buffering = settings[Setting::read_in_order_use_buffering];
}

SortingStep::Settings::Settings(size_t max_block_size_)
{
    max_block_size = max_block_size_;
}

static ITransformingStep::Traits getTraits(size_t limit)
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = limit == 0,
        }
    };
}

SortingStep::SortingStep(
    const DataStream & input_stream,
    SortDescription description_,
    UInt64 limit_,
    const Settings & settings_,
    bool optimize_sorting_by_input_stream_properties_)
    : ITransformingStep(input_stream, input_stream.header, getTraits(limit_))
    , type(Type::Full)
    , result_description(std::move(description_))
    , limit(limit_)
    , sort_settings(settings_)
    , optimize_sorting_by_input_stream_properties(optimize_sorting_by_input_stream_properties_)
{
    if (sort_settings.max_bytes_before_external_sort && sort_settings.tmp_data == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary data storage for external sorting is not provided");

    /// TODO: check input_stream is partially sorted by the same description.
    output_stream->sort_description = result_description;
    output_stream->sort_scope = DataStream::SortScope::Global;
}

SortingStep::SortingStep(
        const DataStream & input_stream,
        const SortDescription & description_,
        const SortDescription & partition_by_description_,
        UInt64 limit_,
        const Settings & settings_,
        bool optimize_sorting_by_input_stream_properties_)
    : SortingStep(input_stream, description_, limit_, settings_, optimize_sorting_by_input_stream_properties_)
{
    partition_by_description = partition_by_description_;

    output_stream->sort_description = result_description;
    output_stream->sort_scope = DataStream::SortScope::Stream;
}

SortingStep::SortingStep(
    const DataStream & input_stream_,
    SortDescription prefix_description_,
    SortDescription result_description_,
    size_t max_block_size_,
    UInt64 limit_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits(limit_))
    , type(Type::FinishSorting)
    , prefix_description(std::move(prefix_description_))
    , result_description(std::move(result_description_))
    , limit(limit_)
    , sort_settings(max_block_size_)
{
    /// TODO: check input_stream is sorted by prefix_description.
    output_stream->sort_description = result_description;
    output_stream->sort_scope = DataStream::SortScope::Global;
}

SortingStep::SortingStep(
    const DataStream & input_stream,
    SortDescription sort_description_,
    size_t max_block_size_,
    UInt64 limit_,
    bool always_read_till_end_)
    : ITransformingStep(input_stream, input_stream.header, getTraits(limit_))
    , type(Type::MergingSorted)
    , result_description(std::move(sort_description_))
    , limit(limit_)
    , always_read_till_end(always_read_till_end_)
    , sort_settings(max_block_size_)
{
    sort_settings.max_block_size = max_block_size_;
    /// TODO: check input_stream is partially sorted (each port) by the same description.
    output_stream->sort_description = result_description;
    output_stream->sort_scope = DataStream::SortScope::Global;
}

void SortingStep::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), input_streams.front().header, getDataStreamTraits());
    output_stream->sort_description = result_description;

    if (partition_by_description.empty())
        output_stream->sort_scope = DataStream::SortScope::Global;
    else
        output_stream->sort_scope = DataStream::SortScope::Stream;
}

void SortingStep::updateLimit(size_t limit_)
{
    if (limit_ && (limit == 0 || limit_ < limit))
    {
        limit = limit_;
        transform_traits.preserves_number_of_rows = false;
    }
}

void SortingStep::convertToFinishSorting(SortDescription prefix_description_, bool use_buffering_)
{
    type = Type::FinishSorting;
    prefix_description = std::move(prefix_description_);
    use_buffering = use_buffering_;
}

void SortingStep::scatterByPartitionIfNeeded(QueryPipelineBuilder& pipeline)
{
    size_t threads = pipeline.getNumThreads();
    size_t streams = pipeline.getNumStreams();

    if (!partition_by_description.empty() && threads > 1)
    {
        Block stream_header = pipeline.getHeader();

        ColumnNumbers key_columns;
        key_columns.reserve(partition_by_description.size());
        for (auto & col : partition_by_description)
        {
            key_columns.push_back(stream_header.getPositionByName(col.column_name));
        }

        pipeline.transform([&](OutputPortRawPtrs ports)
        {
            Processors processors;
            for (auto * port : ports)
            {
                auto scatter = std::make_shared<ScatterByPartitionTransform>(stream_header, threads, key_columns);
                connect(*port, scatter->getInputs().front());
                processors.push_back(scatter);
            }
            return processors;
        });

        if (streams > 1)
        {
            pipeline.transform([&](OutputPortRawPtrs ports)
            {
                Processors processors;
                for (size_t i = 0; i < threads; ++i)
                {
                    size_t output_it = i;
                    auto resize = std::make_shared<ResizeProcessor>(stream_header, streams, 1);
                    auto & inputs = resize->getInputs();

                    for (auto input_it = inputs.begin(); input_it != inputs.end(); output_it += threads, ++input_it)
                        connect(*ports[output_it], *input_it);
                    processors.push_back(resize);
                }
                return processors;
            });
        }
    }
}

void SortingStep::finishSorting(
    QueryPipelineBuilder & pipeline, const SortDescription & input_sort_desc, const SortDescription & result_sort_desc, const UInt64 limit_)
{
    pipeline.addSimpleTransform(
        [&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;

            return std::make_shared<PartialSortingTransform>(header, result_sort_desc, limit_);
        });

    bool increase_sort_description_compile_attempts = true;

    /// NOTE limits are not applied to the size of temporary sets in FinishSortingTransform
    pipeline.addSimpleTransform(
        [&, increase_sort_description_compile_attempts](const Block & header) mutable -> ProcessorPtr
        {
            /** For multiple FinishSortingTransform we need to count identical comparators only once per QueryPlan
                  * To property support min_count_to_compile_sort_description.
                  */
            bool increase_sort_description_compile_attempts_current = increase_sort_description_compile_attempts;

            if (increase_sort_description_compile_attempts)
                increase_sort_description_compile_attempts = false;

            return std::make_shared<FinishSortingTransform>(
                header, input_sort_desc, result_sort_desc, sort_settings.max_block_size, limit_, increase_sort_description_compile_attempts_current);
        });
}

void SortingStep::mergingSorted(QueryPipelineBuilder & pipeline, const SortDescription & result_sort_desc, const UInt64 limit_)
{
    /// If there are several streams, then we merge them into one
    if (pipeline.getNumStreams() > 1)
    {
        if (use_buffering && sort_settings.read_in_order_use_buffering)
        {
            pipeline.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<BufferChunksTransform>(header, sort_settings.max_block_size, sort_settings.max_block_bytes, limit_);
            });
        }

        auto transform = std::make_shared<MergingSortedTransform>(
            pipeline.getHeader(),
            pipeline.getNumStreams(),
            result_sort_desc,
            sort_settings.max_block_size,
            /*max_block_size_bytes=*/0,
            SortingQueueStrategy::Batch,
            limit_,
            always_read_till_end);

        pipeline.addTransform(std::move(transform));
    }
}

void SortingStep::mergeSorting(
    QueryPipelineBuilder & pipeline, const Settings & sort_settings, const SortDescription & result_sort_desc, UInt64 limit_)
{
    bool increase_sort_description_compile_attempts = true;

    pipeline.addSimpleTransform(
        [&, increase_sort_description_compile_attempts](
            const Block & header, QueryPipelineBuilder::StreamType stream_type) mutable -> ProcessorPtr
        {
            if (stream_type == QueryPipelineBuilder::StreamType::Totals)
                return nullptr;

            // For multiple FinishSortingTransform we need to count identical comparators only once per QueryPlan.
            // To property support min_count_to_compile_sort_description.
            bool increase_sort_description_compile_attempts_current = increase_sort_description_compile_attempts;

            if (increase_sort_description_compile_attempts)
                increase_sort_description_compile_attempts = false;

            auto tmp_data_on_disk = sort_settings.tmp_data
                ? std::make_unique<TemporaryDataOnDisk>(sort_settings.tmp_data, CurrentMetrics::TemporaryFilesForSort)
                : std::unique_ptr<TemporaryDataOnDisk>();

            return std::make_shared<MergeSortingTransform>(
                header,
                result_sort_desc,
                sort_settings.max_block_size,
                sort_settings.max_block_bytes,
                limit_,
                increase_sort_description_compile_attempts_current,
                sort_settings.max_bytes_before_remerge / pipeline.getNumStreams(),
                sort_settings.remerge_lowered_memory_bytes_ratio,
                sort_settings.max_bytes_before_external_sort,
                std::move(tmp_data_on_disk),
                sort_settings.min_free_disk_space);
        });
}

void SortingStep::fullSortStreams(
    QueryPipelineBuilder & pipeline,
    const Settings & sort_settings,
    const SortDescription & result_sort_desc,
    const UInt64 limit_,
    const bool skip_partial_sort)
{
    if (!skip_partial_sort || limit_)
    {
        pipeline.addSimpleTransform(
            [&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
            {
                if (stream_type != QueryPipelineBuilder::StreamType::Main)
                    return nullptr;

                return std::make_shared<PartialSortingTransform>(header, result_sort_desc, limit_);
            });

        StreamLocalLimits limits;
        limits.mode = LimitsMode::LIMITS_CURRENT;
        limits.size_limits = sort_settings.size_limits;

        pipeline.addSimpleTransform(
            [&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
            {
                if (stream_type != QueryPipelineBuilder::StreamType::Main)
                    return nullptr;

                return std::make_shared<LimitsCheckingTransform>(header, limits);
            });
    }

    mergeSorting(pipeline, sort_settings, result_sort_desc, limit_);
}

void SortingStep::fullSort(
    QueryPipelineBuilder & pipeline, const SortDescription & result_sort_desc, const UInt64 limit_, const bool skip_partial_sort)
{
    scatterByPartitionIfNeeded(pipeline);

    fullSortStreams(pipeline, sort_settings, result_sort_desc, limit_, skip_partial_sort);

    /// If there are several streams, then we merge them into one
    if (pipeline.getNumStreams() > 1 && (partition_by_description.empty() || pipeline.getNumThreads() == 1))
    {
        auto transform = std::make_shared<MergingSortedTransform>(
            pipeline.getHeader(),
            pipeline.getNumStreams(),
            result_sort_desc,
            sort_settings.max_block_size,
            /*max_block_size_bytes=*/0,
            SortingQueueStrategy::Batch,
            limit_,
            always_read_till_end);

        pipeline.addTransform(std::move(transform));
    }
}

void SortingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    /// We consider that a caller has more information what type of sorting to apply.
    /// The type depends on constructor used to create sorting step.
    /// So we'll try to infer sorting to use only in case of Full sorting

    if (type == Type::MergingSorted)
    {
        mergingSorted(pipeline, result_description, limit);
        return;
    }

    if (type == Type::FinishSorting)
    {
        bool need_finish_sorting = (prefix_description.size() < result_description.size());
        mergingSorted(pipeline, prefix_description, (need_finish_sorting ? 0 : limit));

        if (need_finish_sorting)
            finishSorting(pipeline, prefix_description, result_description, limit);

        return;
    }

    const auto input_sort_mode = input_streams.front().sort_scope;
    const SortDescription & input_sort_desc = input_streams.front().sort_description;
    if (optimize_sorting_by_input_stream_properties)
    {
        /// skip sorting if stream is already sorted
        if (input_sort_mode == DataStream::SortScope::Global && input_sort_desc.hasPrefix(result_description))
        {
            if (pipeline.getNumStreams() != 1)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "If input stream is globally sorted then there should be only 1 input stream at this stage. Number of input streams: "
                    "{}",
                    pipeline.getNumStreams());

            return;
        }

        /// merge sorted
        if (input_sort_mode == DataStream::SortScope::Stream && input_sort_desc.hasPrefix(result_description))
        {
            mergingSorted(pipeline, result_description, limit);
            return;
        }

        /// if chunks already sorted according to result_sort_desc, then we can skip chunk sorting
        if (input_sort_mode == DataStream::SortScope::Chunk && input_sort_desc.hasPrefix(result_description))
        {
            const bool skip_partial_sort = true;
            fullSort(pipeline, result_description, limit, skip_partial_sort);
            return;
        }
    }

    fullSort(pipeline, result_description, limit);
}

void SortingStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    if (!prefix_description.empty())
    {
        settings.out << prefix << "Prefix sort description: ";
        dumpSortDescription(prefix_description, settings.out);
        settings.out << '\n';

        settings.out << prefix << "Result sort description: ";
        dumpSortDescription(result_description, settings.out);
        settings.out << '\n';
    }
    else
    {
        settings.out << prefix << "Sort description: ";
        dumpSortDescription(result_description, settings.out);
        settings.out << '\n';
    }

    if (limit)
        settings.out << prefix << "Limit " << limit << '\n';
}

void SortingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    if (!prefix_description.empty())
    {
        map.add("Prefix Sort Description", explainSortDescription(prefix_description));
        map.add("Result Sort Description", explainSortDescription(result_description));
    }
    else
        map.add("Sort Description", explainSortDescription(result_description));

    if (limit)
        map.add("Limit", limit);
}

}
