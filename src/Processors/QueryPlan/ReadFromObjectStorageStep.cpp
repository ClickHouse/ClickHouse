#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <Processors/QueryPlan/LazilyReadFromObjectStorage.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
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
            need_only_count,
            lazy_row_index_registry);

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

bool ReadFromObjectStorageStep::canUseLazyMaterialization() const
{
    if (need_only_count)
        return false;

    /// The global row index requires per-row file row numbers (ChunkInfoRowNumbers), and the lazy
    /// branch requires reading an explicit set of rows (FormatFilterInfo::rows_to_read).
    /// Only the Parquet reader supports both.
    if (!boost::iequals(configuration->format, "Parquet"))
        return false;

    /// Data lakes can have per-file formats, deletes, and schema evolution; the configuration
    /// proves against the concrete data snapshot that every file can take the lazy path.
    if (!configuration->supportsLazyMaterialization(storage_snapshot->metadata, getContext()))
        return false;

    /// The lazy pass rereads the surviving files and must prove it sees the same generation of
    /// each object (see `LazyRowsObjectIterator::validateObjectGeneration`). A backend whose
    /// metadata may carry no comparable token at all (no `ETag`, unknown size and modification
    /// time — e.g. a web origin) would fail close on that reread even without any concurrent
    /// overwrite, so keep it on the single-pass plan.
    if (!object_storage->supportsObjectGenerationComparison())
        return false;

#if CLICKHOUSE_CLOUD
    /// The transformed plan is not serializable.
    if (distributed_read_bucket_count)
        return false;
#endif

    return true;
}

std::unique_ptr<LazilyReadFromObjectStorage> ReadFromObjectStorageStep::keepOnlyRequiredColumnsAndCreateLazyReadStep(const NameSet & required_names)
{
    /// Columns that the PREWHERE / row-level filter needs as inputs must stay in the main read
    /// because filtering happens there.
    NameSet columns_to_keep = required_names;
    if (info.row_level_filter)
        for (const auto & column : info.row_level_filter->actions.getRequiredColumns())
            columns_to_keep.insert(column.name);
    if (info.prewhere_info)
        for (const auto & column : info.prewhere_info->prewhere_actions.getRequiredColumns())
            columns_to_keep.insert(column.name);

    /// Hive partition columns are parsed from the file path, reading them is cheap; keep them.
    for (const auto & column : info.hive_partition_columns_to_read_from_file_path)
        columns_to_keep.insert(column.name);

    /// Virtual columns are cheap as well.
    for (const auto & column : info.requested_virtual_columns)
        columns_to_keep.insert(column.name);

    NameSet requested_from_format;
    for (const auto & column : info.requested_columns)
        requested_from_format.insert(column.name);

    /// Defer the physical columns that the format reads and nothing needs before the LIMIT.
    NameSet lazy_names;
    Block lazy_source_header;
    for (const auto & column : info.source_header)
    {
        if (!columns_to_keep.contains(column.name) && requested_from_format.contains(column.name))
        {
            lazy_names.insert(column.name);
            lazy_source_header.insert(column);
        }
    }

    if (!lazy_source_header.columns())
        return {};

    /// The info for the lazy read: only the deferred columns, no virtual columns, no filters.
    ReadFromFormatInfo lazy_info;
    lazy_info.source_header = lazy_source_header;
    lazy_info.columns_description = info.columns_description;
    lazy_info.serialization_hints = info.serialization_hints;
    for (const auto & column : info.format_header)
        if (lazy_names.contains(column.name))
            lazy_info.format_header.insert(column);
    for (const auto & column : info.requested_columns)
        if (lazy_names.contains(column.name))
            lazy_info.requested_columns.push_back(column);

    /// Remove the deferred columns from the main read and make it produce the global row index.
    Block main_source_header;
    for (const auto & column : info.source_header)
        if (!lazy_names.contains(column.name))
            main_source_header.insert(column);
    main_source_header.insert({std::make_shared<DataTypeUInt64>(), "__global_row_index"});

    Block main_format_header;
    for (const auto & column : info.format_header)
        if (!lazy_names.contains(column.name))
            main_format_header.insert(column);

    NamesAndTypesList main_requested_columns;
    for (const auto & column : info.requested_columns)
        if (!lazy_names.contains(column.name))
            main_requested_columns.push_back(column);

    info.source_header = std::move(main_source_header);
    info.format_header = std::move(main_format_header);
    info.requested_columns = std::move(main_requested_columns);
    output_header = std::make_shared<const Block>(info.source_header);

    std::erase_if(required_source_columns, [&](const String & name) { return lazy_names.contains(name); });

    lazy_row_index_registry = std::make_shared<LazyObjectStorageFileRegistry>();

    auto lazy_step = std::make_unique<LazilyReadFromObjectStorage>(
        std::make_shared<const Block>(std::move(lazy_source_header)),
        storage_id,
        object_storage,
        configuration,
        storage_snapshot,
        format_settings,
        std::move(lazy_info),
        getContext(),
        max_block_size);

    return lazy_step;
}

bool ReadFromObjectStorageStep::requestReadingInOrder() const
{
    return configuration->isDataSortedBySortingKey(storage_snapshot->metadata, getContext());
}

InputOrderInfoPtr ReadFromObjectStorageStep::getDataOrder() const
{
    return convertSortingKeyToInputOrder(storage_snapshot->metadata->getSortingKey());
}

}
