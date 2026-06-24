#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Core/Settings.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/Serialization.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Storages/VirtualColumnUtils.h>
#include <boost/algorithm/string/predicate.hpp>


namespace DB
{

namespace
{

void formatExplainIndexes(
    IQueryPlanStep::FormatSettings & explain_settings,
    const std::vector<IDataLakeMetadata::ExplainIndexDescription> & indexes)
{
    if (indexes.empty())
        return;

    const std::string & prefix = explain_settings.detail_prefix;
    std::string indent(explain_settings.base_indent, explain_settings.indent_char);
    explain_settings.out << prefix << "Indexes:\n";

    for (const auto & stat : indexes)
    {
        explain_settings.out << prefix << indent << stat.type << '\n';

        if (!stat.description.empty())
            explain_settings.out << prefix << indent << indent << "Description: " << stat.description << '\n';

        if (!stat.used_keys.empty())
        {
            explain_settings.out << prefix << indent << indent << "Keys:" << '\n';
            for (const auto & used_key : stat.used_keys)
                explain_settings.out << prefix << indent << indent << indent << used_key << '\n';
        }

        if (!stat.condition.empty())
            explain_settings.out << prefix << indent << indent << "Condition: " << stat.condition << '\n';

        explain_settings.out << prefix << indent << indent << "Partitions: " << stat.selected_partitions << '/' << stat.initial_partitions << '\n';
        explain_settings.out << prefix << indent << indent << "Files: " << stat.selected_files << '/' << stat.initial_files << '\n';
    }
}

void formatExplainIndexes(
    JSONBuilder::JSONMap & map,
    const std::vector<IDataLakeMetadata::ExplainIndexDescription> & indexes)
{
    if (indexes.empty())
        return;

    auto indexes_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & stat : indexes)
    {
        auto index_map = std::make_unique<JSONBuilder::JSONMap>();
        index_map->add("Type", stat.type);

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

        index_map->add("Initial Partitions", stat.initial_partitions);
        index_map->add("Selected Partitions", stat.selected_partitions);
        index_map->add("Initial Files", stat.initial_files);
        index_map->add("Selected Files", stat.selected_files);
        indexes_array->add(std::move(index_map));
    }

    map.add("Indexes", std::move(indexes_array));
}

std::vector<IDataLakeMetadata::ExplainIndexDescription> getExplainIndexes(
    const StorageObjectStorageConfigurationPtr & configuration,
    const std::shared_ptr<const ActionsDAG> & filter_actions_dag,
    StorageMetadataPtr storage_metadata,
    ContextPtr context)
{
    if (!filter_actions_dag)
        return {};

    const auto * metadata = configuration->getExternalMetadata();
    if (!metadata)
        return {};

    return metadata->getExplainIndexDescriptions(filter_actions_dag.get(), storage_metadata, context);
}

}

namespace Setting
{
    extern const SettingsBool parallelize_output_from_storages;
}


ReadFromObjectStorageStep::ReadFromObjectStorageStep(
    const StorageID & storage_id_,
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    const Names & columns_to_read,
    const NamesAndTypesList & virtual_columns_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const std::optional<DB::FormatSettings> & format_settings_,
    bool distributed_processing_,
    ReadFromFormatInfo info_,
    bool need_only_count_,
    ContextPtr context_,
    size_t max_block_size_,
    size_t num_streams_)
    : SourceStepWithFilter(std::make_shared<const Block>(info_.source_header), columns_to_read, query_info_, storage_snapshot_, context_)
    , storage_id(storage_id_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , info(std::move(info_))
    , virtual_columns(virtual_columns_)
    , format_settings(format_settings_)
    , need_only_count(need_only_count_)
    , max_block_size(max_block_size_)
    , num_streams(num_streams_)
    , max_num_streams(num_streams_)
    , distributed_processing(distributed_processing_)
{
}

QueryPlanStepPtr ReadFromObjectStorageStep::clone() const
{
    return std::make_unique<ReadFromObjectStorageStep>(*this);
}

void ReadFromObjectStorageStep::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));
    if (!filter_actions_dag)
        return;

    if (boost::iequals(configuration->format, "Parquet") || boost::iequals(configuration->format, "ORC"))
        prepareEagerKeyConditionSets(
            filter_actions_dag,
            storage_snapshot, info.source_header,
            query_info.prewhere_info, query_info.row_level_filter, getContext());

    // It is important to build the inplace sets for the filter here, before reading data from object storage.
    // If we delay building these sets until later in the pipeline, the filter can be applied after the data
    // has already been read, potentially in parallel across many streams. This can significantly reduce the
    // effectiveness of an Iceberg partition pruning, as unnecessary data may be read. Additionally, building ordered sets
    // at this stage enables the KeyCondition class to apply more efficient optimizations than for unordered sets.
    /// Idempotent — sets already built above are skipped via !future_set->get() check.
    VirtualColumnUtils::buildSetsForDAGExcludingGlobalIn(*filter_actions_dag, getContext());
}

void ReadFromObjectStorageStep::updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value)
{
    info = updateFormatPrewhereInfo(info, query_info.row_level_filter, prewhere_info_value);
    query_info.prewhere_info = prewhere_info_value;
    output_header = std::make_shared<const Block>(info.source_header);
}

void ReadFromObjectStorageStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
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

    // here create for node -> query -> level thread pool
    auto parser_shared_resources = std::make_shared<FormatParserSharedResources>(context->getSettingsRef(), num_streams);

    auto format_filter_info = std::make_shared<FormatFilterInfo>(
        filter_actions_dag,
        context,
        configuration->getColumnMapperForCurrentSchema(storage_snapshot->metadata, context),
        query_info.row_level_filter,
        query_info.prewhere_info);

    for (size_t i = 0; i < num_streams; ++i)
    {
        auto source = std::make_shared<StorageObjectStorageSource>(
            storage_id,
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
            need_only_count);

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

void ReadFromObjectStorageStep::describeIndexes(FormatSettings & explain_settings) const
{
    formatExplainIndexes(explain_settings, getExplainIndexes(configuration, filter_actions_dag, storage_snapshot->metadata, getContext()));
}

void ReadFromObjectStorageStep::describeIndexes(JSONBuilder::JSONMap & map) const
{
    formatExplainIndexes(map, getExplainIndexes(configuration, filter_actions_dag, storage_snapshot->metadata, getContext()));
}

void ReadFromObjectStorageStep::createIterator()
{
    if (iterator_wrapper)
        return;

    const ActionsDAG::Node * predicate = nullptr;
    if (filter_actions_dag)
        predicate = filter_actions_dag->getOutputs().at(0);

    auto context = getContext();

    iterator_wrapper = StorageObjectStorageSource::createFileIterator(
        configuration, configuration->getQuerySettings(context), object_storage, storage_snapshot->metadata, distributed_processing,
        context, predicate, filter_actions_dag.get(), virtual_columns, info.hive_partition_columns_to_read_from_file_path, nullptr, context->getFileProgressCallback(),
        /*ignore_archive_globs=*/ false, /*skip_object_metadata=*/ false, /*with_tags=*/ info.requested_virtual_columns.contains("_tags"));
}

static InputOrderInfoPtr convertSortingKeyToInputOrder(const KeyDescription & key_description)
{
    SortDescription sort_description_for_merging;
    for (size_t i = 0; i < key_description.column_names.size(); ++i)
        sort_description_for_merging.push_back(
            SortColumnDescription(key_description.column_names[i], (!key_description.reverse_flags.empty() && key_description.reverse_flags[i]) ? -1 : 1));
    return std::make_shared<const InputOrderInfo>(sort_description_for_merging, sort_description_for_merging.size(), 1, 0);
}

bool ReadFromObjectStorageStep::requestReadingInOrder() const
{
    return configuration->isDataSortedBySortingKey(storage_snapshot->metadata, getContext());
}

InputOrderInfoPtr ReadFromObjectStorageStep::getDataOrder() const
{
    return convertSortingKeyToInputOrder(getStorageMetadata()->getSortingKey());
}

}
