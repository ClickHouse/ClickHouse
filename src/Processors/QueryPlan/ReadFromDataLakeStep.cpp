#include <Processors/QueryPlan/ReadFromDataLakeStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Core/Settings.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeSource.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/Sources/NullSource.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Storages/VirtualColumnUtils.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool parallelize_output_from_storages;
}


ReadFromDataLakeStep::ReadFromDataLakeStep(
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    const Names & columns_to_read,
    const NamesAndTypesList & virtual_columns_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const std::optional<DB::FormatSettings> & format_settings_,
    ReadFromFormatInfo info_,
    bool need_only_count_,
    ContextPtr context_,
    size_t max_block_size_,
    size_t num_streams_,
    IDataLakeMetadata * metadata_)
    : SourceStepWithFilter(std::make_shared<const Block>(info_.source_header), columns_to_read, query_info_, storage_snapshot_, context_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , info(std::move(info_))
    , virtual_columns(virtual_columns_)
    , format_settings(format_settings_)
    , need_only_count(need_only_count_)
    , max_block_size(max_block_size_)
    , num_streams(num_streams_)
    , max_num_streams(num_streams_)
    , metadata(metadata_)
{
}

QueryPlanStepPtr ReadFromDataLakeStep::clone() const
{
    return std::make_unique<ReadFromDataLakeStep>(*this);
}

void ReadFromDataLakeStep::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));
    if (!filter_actions_dag)
        return;
    VirtualColumnUtils::buildOrderedSetsForDAG(*filter_actions_dag, getContext());
}

void ReadFromDataLakeStep::updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value)
{
    info = updateFormatPrewhereInfo(info, query_info.row_level_filter, prewhere_info_value);
    query_info.prewhere_info = prewhere_info_value;
    output_header = std::make_shared<const Block>(info.source_header);
}

void ReadFromDataLakeStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    createIterator();

    Pipes pipes;
    auto context = getContext();
    size_t estimated_keys_count = iterator_wrapper->estimatedKeysCount();

    if (estimated_keys_count > 1)
        num_streams = std::min(num_streams, estimated_keys_count);
    else
    {
        /// The amount of keys (zero) was probably underestimated.
        /// We will keep one stream for this particular case.
        num_streams = 1;
    }

    auto parser_shared_resources = std::make_shared<FormatParserSharedResources>(context->getSettingsRef(), num_streams);

    auto format_filter_info = std::make_shared<FormatFilterInfo>(
        filter_actions_dag,
        context,
        metadata->getColumnMapperForCurrentSchema(storage_snapshot->metadata, context),
        query_info.row_level_filter,
        query_info.prewhere_info);

    for (size_t i = 0; i < num_streams; ++i)
    {
        auto source = std::make_shared<DataLakeSource>(
            getName(),
            object_storage,
            configuration,
            storage_snapshot,
            info,
            format_settings,
            context,
            max_block_size,
            iterator_wrapper,
            parser_shared_resources,
            format_filter_info,
            need_only_count,
            metadata);

        pipes.emplace_back(std::move(source));
    }
    auto pipe = Pipe::unitePipes(std::move(pipes));
    if (pipe.empty())
        pipe = Pipe(std::make_shared<NullSource>(std::make_shared<const Block>(info.source_header)));

    size_t output_ports = pipe.numOutputPorts();
    const bool parallelize_output = context->getSettingsRef()[Setting::parallelize_output_from_storages];
    if (parallelize_output
        && FormatFactory::instance().checkParallelizeOutputAfterReading(configuration->format, context)
        && output_ports > 0 && output_ports < max_num_streams)
        pipe.resize(max_num_streams);

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

void ReadFromDataLakeStep::createIterator()
{
    if (iterator_wrapper)
        return;

    auto context = getContext();
    auto query_settings = configuration->getQuerySettings(context);

    iterator_wrapper = metadata->iterate(
        filter_actions_dag.get(),
        context->getFileProgressCallback(),
        query_settings.list_object_keys_size,
        storage_snapshot->metadata,
        context);
}

static InputOrderInfoPtr convertSortingKeyToInputOrder(const KeyDescription & key_description)
{
    SortDescription sort_description_for_merging;
    for (size_t i = 0; i < key_description.column_names.size(); ++i)
        sort_description_for_merging.push_back(
            SortColumnDescription(key_description.column_names[i], (!key_description.reverse_flags.empty() && key_description.reverse_flags[i]) ? -1 : 1));
    return std::make_shared<const InputOrderInfo>(sort_description_for_merging, sort_description_for_merging.size(), 1, 0);
}

bool ReadFromDataLakeStep::requestReadingInOrder() const
{
    return metadata->isDataSortedBySortingKey(storage_snapshot->metadata, getContext());
}

InputOrderInfoPtr ReadFromDataLakeStep::getDataOrder() const
{
    return convertSortingKeyToInputOrder(getStorageMetadata()->getSortingKey());
}

}
