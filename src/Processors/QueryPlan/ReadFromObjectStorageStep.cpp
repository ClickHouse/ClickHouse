#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <Processors/QueryPlan/LazilyReadFromObjectStorage.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Core/Block.h>
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
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Storages/VirtualColumnUtils.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Common/CurrentMetrics.h>
#include <boost/algorithm/string/predicate.hpp>
#include <Common/CurrentThread.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/CompressionMethod.h>
#include <Processors/Formats/Impl/Parquet/FileStatistics.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Storages/Statistics/DistinctValuesFromCompressedSize.h>
#include <Storages/Statistics/Statistics.h>

#include <algorithm>

#include "config.h"

#if USE_AWS_S3
#include <IO/S3/Client.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#endif

namespace CurrentMetrics
{
    extern const Metric StorageObjectStorageThreads;
    extern const Metric StorageObjectStorageThreadsActive;
    extern const Metric StorageObjectStorageThreadsScheduled;
}


namespace DB
{

namespace Setting
{
    extern const SettingsBool parallelize_output_from_storages;
    extern const SettingsBool use_statistics;
    extern const SettingsBool use_parquet_metadata_statistics;
    extern const SettingsBool estimate_distinct_values_from_metadata;
    extern const SettingsBool s3_validate_etag_on_read;
    extern const SettingsBool azure_validate_etag_on_read;
    extern const SettingsBool use_query_condition_cache;
    extern const SettingsBool use_query_condition_cache_for_top_k;
    extern const SettingsUInt64 read_in_order_two_level_merge_threshold;
    extern const SettingsBool compile_sort_description;
    extern const SettingsUInt64 min_count_to_compile_sort_description;
    extern const SettingsBool use_join_runtime_filters_for_iceberg_pruning;
    extern const SettingsBool enable_join_runtime_filters_index_analysis;
}

namespace DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsBool paimon_incremental_read;
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
    auto copy = std::make_unique<ReadFromObjectStorageStep>(*this);
    copy->iterator_wrapper.reset();
    copy->read_in_order_files.clear();
    return copy;
}

void ReadFromObjectStorageStep::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));
    data_lake_read_estimate.reset();
    if (!filter_actions_dag)
        return;

    if (formatNeedsEagerKeyConditionSets(configuration->format))
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

bool ReadFromObjectStorageStep::supportsTopKDynamicFilter(const ColumnWithTypeAndName & sort_column) const
{
    if (!boost::iequals(configuration->format, "Parquet"))
        return false;

    /// With a structure of only Hive partition columns the format reads them from the file, while the
    /// query takes their values from the path, so the reader would compare other values than the
    /// ones the threshold is made from.
    if (info.formatReadsHivePartitionColumns())
        return false;

    /// A column of `format_header` with the name of a column read under another name is another column.
    if (info.columns_read_under_other_names.contains(sort_column.name))
        return false;

    /// The output header is broader than what the format reads: Hive partition columns (from the
    /// path) and virtual columns (`_path`, `_file`, `_row_id`, ...) are added after the format has
    /// produced its chunk, so the reader could never compare them against the threshold.
    const auto * format_column = info.format_header.findByName(sort_column.name);
    if (!format_column || !format_column->type->equals(*sort_column.type))
        return false;

    /// A column with a `DEFAULT` / `MATERIALIZED` / `ALIAS` expression is recomputed above the format
    /// by `AddingDefaultsTransform` for the values the reader reported as missing, so the threshold
    /// would come from other values than the ones the reader compares against it.
    return !info.columns_description.hasDefault(sort_column.name);
}

void ReadFromObjectStorageStep::updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value)
{
    info = updateFormatPrewhereInfo(info, prewhere_info_value);
    query_info.prewhere_info = prewhere_info_value;
    output_header = std::make_shared<const Block>(info.source_header);
    data_lake_read_estimate.reset();
}

std::shared_ptr<const DataLakeReadEstimate> ReadFromObjectStorageStep::getOrCreateDataLakeReadEstimate() const
{
    /// The initiator assigns the files of this replica, so its own metadata may describe other files.
    if (!data_lake_read_estimate && !distributed_processing)
    {
        auto estimate = configuration->estimateRead(storage_snapshot->metadata, filter_actions_dag.get(), requiredSourceColumns(), getContext());
        if (!estimate)
            estimate = estimatePlainParquetRead();
        if (estimate)
            data_lake_read_estimate = std::make_shared<const DataLakeReadEstimate>(std::move(*estimate));
    }
    return data_lake_read_estimate;
}

std::optional<DataLakeReadEstimate> ReadFromObjectStorageStep::estimatePlainParquetRead() const
{
#if USE_PARQUET
    /// Each file costs a read of its footer at planning, so a read of more files gets no estimate.
    constexpr size_t max_files = 100;
    /// The headers of the dictionary pages cost a read each.
    constexpr size_t max_dictionary_pages_per_column_and_file = 4;

    const auto context = getContext();
    const auto & settings = context->getSettingsRef();
    if (!settings[Setting::use_statistics] || !settings[Setting::use_parquet_metadata_statistics])
        return std::nullopt;
    if (configuration->isDataLakeConfiguration() || configuration->isArchive() || !boost::iequals(configuration->format, "Parquet"))
        return std::nullopt;
    /// The files are listed with the filter, which cannot be analyzed before its sets are built.
    if (filter_actions_dag && QueryPlanOptimizations::dagContainsNonReadySet(*filter_actions_dag))
        return std::nullopt;

    /// An iterator of its own, so that the read lists the files with the filter it ends up with.
    const ActionsDAG::Node * predicate = filter_actions_dag ? filter_actions_dag->getOutputs().at(0) : nullptr;
    auto iterator = StorageObjectStorageSource::createFileIterator(
        configuration, configuration->getQuerySettings(context), object_storage, storage_snapshot->metadata, /*distributed_processing=*/ false,
        context, predicate, filter_actions_dag.get(), virtual_columns, info.hive_partition_columns_to_read_from_file_path, nullptr,
        /*file_progress_callback=*/ {}, /*ignore_archive_globs=*/ false, /*skip_object_metadata=*/ false, /*with_tags=*/ false);

    ObjectInfos files;
    while (auto object_info = iterator->next(0))
    {
        if (files.size() == max_files)
            return std::nullopt;
        /// A compressed or archived file is not read by ranges.
        if (object_info->isArchive() || object_info->getFileFormat().value_or(configuration->format) != configuration->format
            || chooseCompressionMethod(object_info->getFileName(), configuration->compression_method) != CompressionMethod::None)
            return std::nullopt;
        files.push_back(object_info);
    }

    /// The requested columns that can have statistics, as for Iceberg.
    struct Target
    {
        String name;
        DataTypePtr type;
        UInt64 rows = 0;
        UInt64 compressed_bytes = 0;
        /// All files have the column, and all its column chunks are dictionary-encoded.
        bool fully_dictionary_encoded = true;
        std::optional<UInt64> max_dictionary_values = {};
        bool has_statistics = false;
    };
    std::vector<Target> targets;
    Names target_names;
    for (const auto & name : requiredSourceColumns())
    {
        auto column = storage_snapshot->metadata->getColumns().tryGetPhysical(name);
        if (!column || info.hive_partition_columns_to_read_from_file_path.contains(name))
            continue;
        const auto type_id = removeLowCardinalityAndNullable(column->type)->getTypeId();
        if (type_id == TypeIndex::Tuple || type_id == TypeIndex::Map || type_id == TypeIndex::Array || type_id == TypeIndex::Variant
            || type_id == TypeIndex::Object || type_id == TypeIndex::Dynamic || type_id == TypeIndex::Nothing)
            continue;
        targets.push_back(Target{.name = name, .type = column->type});
        target_names.push_back(name);
    }

    const DB::FormatSettings read_format_settings = format_settings ? *format_settings : getFormatSettings(context);
    ReadSettings read_settings = context->getReadSettings();
    read_settings.remote_fs_settings.random_access = true;
    const auto log = getLogger("ReadFromObjectStorageStep");

    ConditionSelectivityEstimatorBuilder statistics_builder(context);
    UInt64 total_rows = 0;
    for (const auto & object_info : files)
    {
        auto & object = object_info->relative_path_with_metadata;
        if (!object.metadata)
            object.metadata = getObjectMetadataForRead(*object_storage, object, /*with_tags=*/ false);
        auto buf = createReadBuffer(object, object_storage, context, log, read_settings, /*allow_page_cache=*/ true);
        const auto summary = summarizeParquetFile(*buf, object, read_format_settings, context, target_names, max_dictionary_pages_per_column_and_file);
        total_rows += summary.rows;

        for (auto & target : targets)
        {
            const auto it = summary.columns.find(target.name);
            /// A file without the column has only defaults in it.
            if (it == summary.columns.end())
            {
                target.fully_dictionary_encoded = false;
                continue;
            }
            const auto & column = it->second;
            target.rows += column.rows;
            target.compressed_bytes += column.compressed_bytes;
            target.fully_dictionary_encoded = target.fully_dictionary_encoded && column.fully_dictionary_encoded && column.max_dictionary_values;
            if (column.max_dictionary_values)
                target.max_dictionary_values = std::max(target.max_dictionary_values.value_or(0), *column.max_dictionary_values);

            /// As for Iceberg, a file without the NULL count adds no statistics.
            if (column.nulls)
            {
                statistics_builder.addStatistics(
                    target.name, ColumnStatistics::createBasicFromSummary(target.type, column.rows, Field(), Field(), *column.nulls));
                target.has_statistics = true;
            }
        }
    }

    DataLakeReadEstimate estimate;
    estimate.rows = total_rows;
    statistics_builder.incrementRowCount(total_rows);
    const auto estimator = statistics_builder.getEstimator();
    if (!estimator)
        return estimate;

    for (const auto & target : targets)
    {
        if (!target.has_statistics)
            continue;
        /// A dictionary page has the distinct values of its column chunk; the row groups and the files mostly share their
        /// values, so the largest dictionary counts them. Otherwise, the compressed size.
        std::optional<UInt64> distinct_values;
        if (target.fully_dictionary_encoded && target.max_dictionary_values)
            distinct_values = *target.max_dictionary_values;
        else if (settings[Setting::estimate_distinct_values_from_metadata])
            distinct_values = estimateDistinctValuesFromCompressedSize(target.rows, target.compressed_bytes);
        if (!distinct_values)
            continue;
        statistics_builder.setCardinality(target.name, std::clamp<UInt64>(*distinct_values, 1, std::max<UInt64>(total_rows, 1)));
        estimate.distinct_values_from_metadata.insert(target.name);
    }
    estimate.estimator = estimator;
    return estimate;
#else
    return std::nullopt;
#endif
}

/// Zero means "always preliminary-merge", so it must never reach the group-count division.
/// The cap and the grouping have to read the same value or the cap admits a file count the
/// grouping cannot divide.
static size_t effectiveTwoLevelMergeThreshold(const Settings & settings)
{
    return std::max<size_t>(1, settings[Setting::read_in_order_two_level_merge_threshold]);
}

/// One source per file, as ReadFromMergeTree uses one source per part: a source reads its files
/// serially, so a multi-file source emits a concatenation rather than a sorted run.
Pipe ReadFromObjectStorageStep::buildInOrderPipe(
    const ContextPtr & local_context,
    const FormatFilterInfoPtr & format_filter_info,
    const std::shared_ptr<const ObjectStorageTopKQueryConditionCacheKey> & top_k_query_condition_cache_key)
{
    const auto & settings = local_context->getSettingsRef();
    const size_t num_files = read_in_order_files.size();

    auto parser_shared_resources = std::make_shared<FormatParserSharedResources>(settings, num_files);

    /// Shared by every source of this read. Unlimited queue: scheduling must not be refused.
    auto shared_reader_pool = std::make_shared<ThreadPool>(
        CurrentMetrics::StorageObjectStorageThreads,
        CurrentMetrics::StorageObjectStorageThreadsActive,
        CurrentMetrics::StorageObjectStorageThreadsScheduled,
        /*max_threads=*/ std::max<size_t>(1, num_streams),
        /*max_free_threads=*/ std::max<size_t>(1, num_streams),
        /*queue_size=*/ 0);

    Pipes pipes;
    pipes.reserve(num_files);
    for (const auto & object_info : read_in_order_files)
    {
        auto file_iterator = std::make_shared<SingleObjectIterator>(object_info, iterator_wrapper);
        file_iterator->setEmitProfileEvents(iterator_wrapper->emit_profile_events);

        pipes.emplace_back(std::make_shared<StorageObjectStorageSource>(
            storage_id,
            getName(),
            object_storage,
            configuration,
            storage_snapshot,
            info,
            format_settings,
            local_context,
            max_block_size,
            std::move(file_iterator),
            parser_shared_resources,
            format_filter_info,
            need_only_count,
            /*lazy_row_index_registry_=*/ nullptr,
            top_k_query_condition_cache_key,
            shared_reader_pool));
    }

    const size_t threshold = effectiveTwoLevelMergeThreshold(settings);
    if (num_files <= threshold || num_files <= 2)
        return Pipe::unitePipes(std::move(pipes));

    /// An expression sorting key is absent from source_header, so materialize it, merge on it,
    /// then convert back to source_header.
    const auto & sorting_key = storage_snapshot->metadata->getSortingKey();
    auto key_ast = sorting_key.expression_list_ast->clone();
    auto syntax_result = TreeRewriter(local_context).analyze(key_ast, info.source_header.getNamesAndTypesList());
    auto key_dag = ExpressionAnalyzer(key_ast, syntax_result, local_context).getActionsDAG(false);
    auto key_expr = std::make_shared<ExpressionActions>(std::move(key_dag));

    const auto & reverse_flags = storage_snapshot->metadata->getSortingKeyReverseFlags();
    SortDescription sort_description;
    sort_description.compile_sort_description = settings[Setting::compile_sort_description];
    sort_description.min_count_to_compile_sort_description = settings[Setting::min_count_to_compile_sort_description];
    sort_description.reserve(sorting_key.column_names.size());
    for (size_t i = 0; i < sorting_key.column_names.size(); ++i)
        sort_description.emplace_back(sorting_key.column_names[i], (!reverse_flags.empty() && reverse_flags[i]) ? -1 : 1);

    /// Never collapse to one port: the sorting step installs no merge above a single port, so a
    /// single group would leave the concatenation unmerged.
    const size_t num_groups = std::max<size_t>(2, (num_files + threshold - 1) / threshold);
    std::vector<Pipes> grouped(num_groups);
    for (size_t i = 0; i < num_files; ++i)
        grouped[i % num_groups].emplace_back(std::move(pipes[i]));

    Pipes merged;
    merged.reserve(num_groups);
    for (auto & group_pipes : grouped)
    {
        if (group_pipes.empty())
            continue;

        auto group = Pipe::unitePipes(std::move(group_pipes));
        group.addSimpleTransform([&](const SharedHeader & header)
                                 { return std::make_shared<ExpressionTransform>(header, key_expr); });
        if (group.numOutputPorts() > 1)
            group.addTransform(std::make_shared<MergingSortedTransform>(
                group.getSharedHeader(),
                group.numOutputPorts(),
                sort_description,
                max_block_size,
                /*max_block_size_bytes=*/ 0,
                /*max_dynamic_subcolumns=*/ std::nullopt,
                SortingQueueStrategy::Batch,
                /*limit=*/ 0,
                /*always_read_till_end=*/ false,
                /*out_row_sources_buf=*/ nullptr));
        /// Drop the temporary sorting-key columns. An identity DAG cannot do it: unmatched header
        /// columns are re-emitted (ActionsDAG::updateHeader), so the key would survive, and out of
        /// position. Converting actions output exactly the requested columns, in order.
        if (!blocksHaveEqualStructure(group.getHeader(), info.source_header))
        {
            auto converting = std::make_shared<ExpressionActions>(ActionsDAG::makeConvertingActions(
                group.getHeader().getColumnsWithTypeAndName(),
                info.source_header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name,
                local_context));
            group.addSimpleTransform([&](const SharedHeader & header)
                                     { return std::make_shared<ExpressionTransform>(header, converting); });
        }
        merged.emplace_back(std::move(group));
    }

    return Pipe::unitePipes(std::move(merged));
}

void ReadFromObjectStorageStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & build_settings)
{
    createIterator();
    if (read_in_order && read_in_order_files.empty())
        while (auto object_info = iterator_wrapper->next(0))
            read_in_order_files.push_back(object_info);

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

    auto format_filter_actions_dag = info.getFormatFilter(filter_actions_dag);
    auto make_format_filter_info = [&]
    {
        auto result = std::make_shared<FormatFilterInfo>(
            format_filter_actions_dag,
            context,
            configuration->getColumnMapperForCurrentSchema(storage_snapshot->metadata, context),
            query_info.row_level_filter,
            query_info.prewhere_info);
        result->current_schema_column_mapper
            = configuration->getCurrentSchemaColumnMapperForFilters(storage_snapshot->metadata, context);
        /// The delete transforms of a data lake need row numbers only for files with attached deletes;
        /// `StorageObjectStorageSource::createReader` asks for them per file in that case.
        result->need_row_numbers = lazy_row_index_registry != nullptr
            || VirtualColumnUtils::hasRowDependentVirtualColumns(info.requested_virtual_columns);
        return result;
    };
    auto format_filter_info = make_format_filter_info();
    format_filter_info->top_k_filter = top_k_filter;
    auto top_k_query_condition_cache_key = makeTopKQueryConditionCacheKey();
    if (top_k_query_condition_cache_key)
    {
        /// The verdicts written under the key also cover row groups whose rows were all returned
        /// before the threshold got tight enough to drop them. Tracking the best values costs work
        /// per row group, so only the files that actually use the key are read with it (for plain
        /// object storage, the key may turn out not to be made, see `createReader`).
        auto top_k_filter_tracking_best_values = std::make_shared<FormatTopKFilterInfo>(*top_k_filter);
        top_k_filter_tracking_best_values->track_row_group_best_values = true;
        auto format_filter_info_tracking_best_values = make_format_filter_info();
        format_filter_info_tracking_best_values->top_k_filter = std::move(top_k_filter_tracking_best_values);
        top_k_query_condition_cache_key->format_filter_info_tracking_best_values = std::move(format_filter_info_tracking_best_values);
    }

    const auto & settings = context->getSettingsRef();
    if (join_runtime_filter_predicate && configuration->isIcebergConfiguration()
        && settings[Setting::use_join_runtime_filters_for_iceberg_pruning] && settings[Setting::enable_join_runtime_filters_index_analysis])
    {
        format_filter_info->join_runtime_filter_predicate = join_runtime_filter_predicate;
        if (top_k_query_condition_cache_key && top_k_query_condition_cache_key->format_filter_info_tracking_best_values)
            top_k_query_condition_cache_key->format_filter_info_tracking_best_values->join_runtime_filter_predicate = join_runtime_filter_predicate;
    }

    auto file_iterator = iterator_wrapper;
    if (top_k_query_condition_cache_key && !top_k_query_condition_cache_key->condition_hash)
        file_iterator = std::make_shared<ObjectIteratorListingAllForTopKQueryConditionCache>(
            file_iterator, top_k_query_condition_cache_key, context->getProcessListElement());

    if (read_in_order && !read_in_order_files.empty())
    {
        auto in_order_pipe = buildInOrderPipe(context, format_filter_info, top_k_query_condition_cache_key);
        /// Deliberately no pipe.resize here: it redistributes chunks across ports and would
        /// destroy the per-port ordering established above.
        for (const auto & processor : in_order_pipe.getProcessors())
            processors.emplace_back(processor);
        pipeline.init(std::move(in_order_pipe));
        return;
    }

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
            file_iterator,
            parser_shared_resources,
            format_filter_info,
            need_only_count,
            lazy_row_index_registry,
            top_k_query_condition_cache_key);

        pipes.emplace_back(std::move(source));
    }
    auto pipe = Pipe::unitePipes(std::move(pipes));
    if (pipe.empty())
        pipe = Pipe(std::make_shared<NullSource>(std::make_shared<const Block>(info.source_header)));

    size_t output_ports = pipe.numOutputPorts();
    const bool parallelize_output = context->getSettingsRef()[Setting::parallelize_output_from_storages];
    /// `max_num_streams` is a read-parallelism request, not a thread budget.
    const size_t resize_to = std::min(max_num_streams, build_settings.max_threads);
    /// The format of a data lake configuration is only the format new files are written in, while
    /// the files of the snapshot that is read can have any format, and resizing after a reader
    /// that keeps the file order would scramble it again.
    const bool format_allows_parallelize = configuration->isDataLakeConfiguration()
        ? FormatFactory::instance().checkParallelizeOutputAfterReadingForAnyFormat(context)
        : FormatFactory::instance().checkParallelizeOutputAfterReading(configuration->format, context);
    if (parallelize_output && format_allows_parallelize && output_ports > 0 && output_ports < resize_to)
        pipe.resize(resize_to);

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

void ReadFromObjectStorageStep::addJoinRuntimeFilterIndexAnalysisOnDataRead(
    const String & filter_id, const String & column_name, const DataTypePtr & column_type)
{
    join_runtime_filters_for_file_pruning.push_back({filter_id, column_name, column_type});
}

void ReadFromObjectStorageStep::createIterator()
{
    if (iterator_wrapper)
        return;

    const ActionsDAG::Node * predicate = nullptr;
    if (filter_actions_dag)
        predicate = filter_actions_dag->getOutputs().at(0);

    auto context = getContext();

    /// The TopK query condition cache key is made from the same table state the files are read from.
    if (!iterator_metadata)
        iterator_metadata = StorageObjectStorageSource::ensureDataLakeTableState(*configuration, storage_snapshot->metadata, context);

    iterator_wrapper = StorageObjectStorageSource::createFileIterator(
        configuration, configuration->getQuerySettings(context), object_storage, iterator_metadata, distributed_processing,
        context, predicate, filter_actions_dag.get(), virtual_columns, info.hive_partition_columns_to_read_from_file_path, nullptr, context->getFileProgressCallback(),
        /*ignore_archive_globs=*/ false, /*skip_object_metadata=*/ false, /*with_tags=*/ info.requested_virtual_columns.contains("_tags"));

    /// The build side registers a join runtime filter in the lookup of the thread's query context, which is not always this step's context.
    if (!join_runtime_filters_for_file_pruning.empty())
        if (auto query_context = CurrentThread::tryGetQueryContext())
            if (auto lookup = query_context->getRuntimeFilterLookup())
            {
                join_runtime_filter_predicate
                    = std::make_shared<const RuntimeFilterPruningPredicate>(join_runtime_filters_for_file_pruning, std::move(lookup), context);
                iterator_wrapper->setJoinRuntimeFilters(join_runtime_filter_predicate);
            }
}

std::shared_ptr<ObjectStorageTopKQueryConditionCacheKey> ReadFromObjectStorageStep::makeTopKQueryConditionCacheKey() const
{
    const auto & settings = getContext()->getSettingsRef();
    if (!top_k_filter || !allow_top_k_query_condition_cache
        || !settings[Setting::use_query_condition_cache] || !settings[Setting::use_query_condition_cache_for_top_k])
        return {};

    if (!top_k_filter->filter_rows)
        return {};

    /// Only a table with a UUID has cache entries. With distributed processing each worker reads a
    /// set of files that differs from run to run, and makes its own threshold from them.
    if (storage_id.uuid == UUIDHelpers::Nil || distributed_processing)
        return {};
#if CLICKHOUSE_CLOUD
    if (distributed_read_bucket_count)
        return {};
#endif

    /// The threshold of the TopN filter comes from the rows of every file of the query, so the key
    /// must identify the set of files and their contents. A data lake snapshot does: it lists the data
    /// files (and the deletes applied to them), which are never modified in place, and the read
    /// iterates exactly the snapshot pinned in the storage metadata (see `createFileIterator`). For
    /// plain object storage, the key covers the ETags of all objects as they are listed (see
    /// `ObjectIteratorListingAllForTopKQueryConditionCache`). An object can be overwritten after it is
    /// listed, so the read of every object must be pinned to the listed ETag: otherwise an entry of
    /// one file could be used while another file is read in a version the key does not describe.
    const bool is_data_lake = configuration->isDataLakeConfiguration();
    const auto & datalake_table_state = iterator_metadata->datalake_table_state;
    if (is_data_lake ? !datalake_table_state : !isReadPinnedToListedGeneration())
        return {};

    /// Listing explicit keys (or a brace expansion) takes a `HEAD` request for each object, which the
    /// sources issue concurrently otherwise, while listing all objects ahead of the read serializes them.
    if (!is_data_lake && iterator_wrapper->estimatedKeysCount() > 1
        && dynamic_cast<const StorageObjectStorageSource::KeysIterator *>(iterator_wrapper.get()))
        return {};

    /// Which column of a file is read and how its values are decoded depend on the format settings (e.g.
    /// `input_format_parquet_case_insensitive_column_matching`). A table captures them when it is loaded,
    /// so a `DETACH` / `ATTACH`, which keeps the UUID, may change them, and the hash of the filter does
    /// not cover them.
    if (!format_settings || !format_settings->source_settings_hash)
        return {};

    /// An incremental `Paimon` read iterates only the delta since the watermark committed in Keeper
    /// (or of `paimon_target_snapshot_id`, bounded by `max_consume_snapshots`), so the same snapshot
    /// yields different sets of files from query to query.
    if (is_data_lake && (*configuration->getDataLakeSettings())[DataLakeStorageSetting::paimon_incremental_read])
        return {};

    /// The members of an archive make no key (see `ObjectIteratorListingAllForTopKQueryConditionCache`),
    /// and listing them ahead would keep a reader of the archive open for each of them.
    if (configuration->isArchive())
        return {};

    /// The tags and the modification time of an object can change without changing its `ETag` (a copy
    /// of an object onto itself with replaced metadata keeps the contents) or the snapshot of a data lake,
    /// so neither the key of the files nor the hash of the filter describes them. Likewise `_table` comes
    /// from the name of the table, while the entries are keyed by its UUID, which a `RENAME TABLE` keeps.
    if (info.requested_virtual_columns.contains("_tags") || info.requested_virtual_columns.contains("_time")
        || info.requested_virtual_columns.contains("_table"))
        return {};

    /// The rows the reader drops besides those of the TopN filter must be dropped by a condition the
    /// key covers, and so must the rows the filter drops after the format, which do not make the threshold.
    /// So the key covers the whole filter of the step and a PREWHERE collected with it, not only the part
    /// pushed into the format (`condition_hash` of `format_filter_info` lacks the conjuncts on virtual
    /// columns such as `_path`, which narrow down the set of files read).
    /// Without any of them nothing else drops rows. A row policy is covered by neither.
    /// With the whole filter in the key, the pruning of data files (by partitions or statistics, depending
    /// on settings) does not matter: it only removes files without rows passing the filter, which do not
    /// make the threshold either.
    if (query_info.row_level_filter)
        return {};

    /// A column with a `DEFAULT` / `MATERIALIZED` expression is computed above the format by
    /// `AddingDefaultsTransform` when a file lacks it, and the hash of the filter sees it only as an
    /// input, not the expression (which may be `queryID()`, `now()`, or depend on other columns). So
    /// the same files and filter could let other rows through from query to query. The sort column
    /// itself never has one (see `supportsTopKDynamicFilter`).
    auto has_default = [&](const String & name)
    {
        if (info.columns_description.hasDefault(name))
            return true;
        /// A subcolumn (`t.a`) is read from its column.
        for (size_t pos = name.find('.'); pos != String::npos; pos = name.find('.', pos + 1))
            if (info.columns_description.hasDefault(name.substr(0, pos)))
                return true;
        return false;
    };
    auto filter_reads_column_with_default = [&](const ActionsDAG & dag)
    {
        return std::ranges::any_of(dag.getInputs(), [&](const auto * input) { return has_default(input->result_name); });
    };
    if ((filter_actions_dag && filter_reads_column_with_default(*filter_actions_dag))
        || (query_info.prewhere_info && filter_reads_column_with_default(query_info.prewhere_info->prewhere_actions)))
        return {};

    /// The metadata of an object (its modification time, ETag, tags) can change without a new snapshot,
    /// and so can the name of the table (`_table`, by `RENAME TABLE` or `EXCHANGE TABLES`, which keep
    /// the UUID), so a condition on them may let other files make the threshold on a rerun.
    auto reads_mutable_virtual_columns = [&](const ActionsDAG & dag)
    {
        return std::ranges::any_of(dag.getInputs(), [&](const auto * input)
        {
            const auto & name = input->result_name;
            return (name == "_time" || name == "_etag" || name == "_tags" || name == "_table") && virtual_columns.contains(name);
        });
    };
    if ((filter_actions_dag && reads_mutable_virtual_columns(*filter_actions_dag))
        || (query_info.prewhere_info && reads_mutable_virtual_columns(query_info.prewhere_info->prewhere_actions)))
        return {};

    auto key = std::make_shared<ObjectStorageTopKQueryConditionCacheKey>();
    UInt64 condition_hash = 0;
    if (filter_actions_dag)
    {
        auto filter_condition_hash = FormatFilterInfo::computeConditionHash(*filter_actions_dag, query_info.prewhere_info, getContext());
        if (!filter_condition_hash)
            return {};
        condition_hash = *filter_condition_hash;
        key->condition = filter_actions_dag->dumpNames() + ", ";
    }
    else if (!query_info.prewhere_info)
        condition_hash = queryConditionCacheHash(0, queryConditionCacheSettingsSalt(settings));
    else
        return {};

    key->condition += fmt::format("TopK by {}", top_k_filter->column_name);

    SipHash hash;
    hash.update(condition_hash);
    hash.update(top_k_filter->plan_hash);
    hash.update(*format_settings->source_settings_hash);

    /// The paths of the files are relative to the storage (endpoint, bucket, container, ...), which a
    /// table keeps across a `DETACH` / `ATTACH` with its UUID, while e.g. an `ALTER NAMED COLLECTION`
    /// may point it to another bucket with identical copies of the objects, where `_path` differs.
    const String data_source = configuration->getDataSourceDescription();
    hash.update(data_source.size());
    hash.update(data_source);

#if USE_AWS_S3
    /// A URL with `versionId` makes every read fetch that version of the object, bypassing the check of
    /// the listed `ETag`, while the metadata of the object (and so its `ETag` in the key of the file)
    /// describes the latest version. An `ALTER NAMED COLLECTION` followed by a `DETACH` / `ATTACH` may
    /// select another version, keeping the UUID of the table, the paths and the `ETag`s.
    if (const auto * s3_object_storage = dynamic_cast<const S3ObjectStorage *>(object_storage.get()))
    {
        const String version_id = s3_object_storage->getURI().version_id;
        hash.update(version_id.size());
        hash.update(version_id);
    }
#endif

    /// Whether a column is read from the file or from the Hive partition in its path (and with which type)
    /// is captured by the table when it is loaded (`use_hive_partitioning`), so a `DETACH` / `ATTACH`,
    /// which keeps the UUID, may change it, while the paths, the ETags and the hash of the filter stay.
    hash.update(info.hive_partition_columns_to_read_from_file_path.size());
    for (const auto & column : info.hive_partition_columns_to_read_from_file_path)
    {
        hash.update(column.name.size());
        hash.update(column.name);
        const String type_name = column.type->getName();
        hash.update(type_name.size());
        hash.update(type_name);
    }

    if (!is_data_lake)
    {
        key->condition_hash_without_files = hash.get64();
        return key;
    }

    WriteBufferFromOwnString table_state;
    serializeDataLakeTableStateSnapshot(*datalake_table_state, table_state);
    table_state.finalize();

    hash.update(table_state.str().size());
    hash.update(table_state.str());
    key->condition_hash = hash.get64();
    return key;
}

bool ReadFromObjectStorageStep::isReadPinnedToListedGeneration() const
{
    /// On most backends a read is unconditional: `HDFSObjectStorage` and the local disk ignore
    /// `StoredObject::etag`, so a concurrent in-place overwrite after the object was listed goes
    /// unnoticed. A read is pinned to the listed generation when the backend issues it with the
    /// captured `ETag` and rejects a response whose `ETag` drifted from it, which is atomic with respect
    /// to an overwrite: S3 with `s3_validate_etag_on_read` (see `ReadBufferFromS3`), and Azure with
    /// `azure_validate_etag_on_read` (see `ReadBufferFromAzureBlobStorage`). The pin only takes effect
    /// when the captured metadata actually carries a non-empty `ETag` (see `createReadBuffer`), and
    /// `GCS` accessed through the S3 API is documented to legitimately return objects without one -
    /// so a `GCS`-provider client is not considered pinned even with the setting on.
    /// A URL with `versionId` replaces the check of the `ETag` with a read of that version, which
    /// pins the read for any real version, but not for `versionId=null`: in a bucket with versioning
    /// suspended, an upload under the same key overwrites the null version in place.
#if USE_AWS_S3
    if (object_storage->getType() == ObjectStorageType::S3
        && getContext()->getSettingsRef()[Setting::s3_validate_etag_on_read])
    {
        if (const auto * s3_object_storage = dynamic_cast<const S3ObjectStorage *>(object_storage.get());
            s3_object_storage && s3_object_storage->getURI().version_id == "null")
            return false;
        const auto s3_client = object_storage->tryGetS3StorageClient();
        return s3_client && s3_client->getProviderType() != S3::ProviderType::GCS;
    }
#endif
    return object_storage->getType() == ObjectStorageType::Azure
        && getContext()->getSettingsRef()[Setting::azure_validate_etag_on_read];
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

    /// Reading in order (granted earlier in this optimization pass) rebuilds the pipeline as one
    /// sorted run per file, without the row-index registry the lazy branch needs, so the two
    /// rewrites do not compose; the in-order read was promised first, decline the lazy rewrite.
    if (read_in_order)
        return false;

    /// The global row index requires per-row file row numbers (ChunkInfoRowNumbers), and the lazy
    /// branch requires reading an explicit set of rows (FormatFilterInfo::rows_to_read).
    /// Only the Parquet and Vortex readers support both. The format of a data lake configuration is
    /// only the format new files are written in, while the files of the snapshot that is read can
    /// have other formats, so for a data lake the per-file check below decides.
    if (!configuration->isDataLakeConfiguration()
        && !boost::iequals(configuration->format, "Parquet") && !boost::iequals(configuration->format, "Vortex"))
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

    /// Even when the two generations are comparable, the second pass could still stitch together rows
    /// of two versions of a file overwritten in place between the metadata probe and the read. The
    /// reread is only generation-safe when either:
    ///   - the data files are immutable by the format's contract — a data lake never overwrites a
    ///     data file in place, so a file identified by path is always the same generation; or
    ///   - the backend pins the actual read to the captured generation (see `isReadPinnedToListedGeneration`).
    /// Any provider that unexpectedly yields an empty `ETag` at read time fails close in
    /// `LazyRowsObjectIterator::validateObjectGeneration` instead of degrading to an unpinned read.
    /// For a mutable file on a backend that cannot pin the read, keep the single-pass plan.
    if (!configuration->dataFilesAreImmutable() && !isReadPinnedToListedGeneration())
        return false;

#if CLICKHOUSE_CLOUD
    /// The transformed plan is not serializable.
    if (distributed_read_bucket_count)
        return false;
#endif

    return true;
}

NameSet ReadFromObjectStorageStep::getNamesToKeepInMainRead(const NameSet & required_names) const
{
    /// A row policy is not part of `info`, but the source evaluates it in the main pass via
    /// `FormatFilterInfo`, so its input columns must not be deferred to the lazy branch.
    NameSet names_to_keep = required_names;
    if (query_info.row_level_filter)
        for (const auto & column : query_info.row_level_filter->actions.getRequiredColumns())
            names_to_keep.insert(column.name);
    return names_to_keep;
}

NameSet ReadFromObjectStorageStep::getLazilyReadColumns(const NameSet & required_names) const
{
    /// The split works on the info in place, so ask it on a copy.
    auto main_info = info;
    const auto lazy_info = splitLazilyReadColumnsFromFormatInfo(main_info, getNamesToKeepInMainRead(required_names));

    NameSet lazy_names;
    if (lazy_info)
        for (const auto & column : lazy_info->source_header)
            lazy_names.insert(column.name);
    return lazy_names;
}

std::unique_ptr<LazilyReadFromObjectStorage> ReadFromObjectStorageStep::keepOnlyRequiredColumnsAndCreateLazyReadStep(const NameSet & required_names)
{
    auto lazy_info = splitLazilyReadColumnsFromFormatInfo(info, getNamesToKeepInMainRead(required_names));
    if (!lazy_info)
        return {};

    output_header = std::make_shared<const Block>(info.source_header);

    NameSet lazy_names;
    for (const auto & column : lazy_info->source_header)
        lazy_names.insert(column.name);
    std::erase_if(required_source_columns, [&](const String & name) { return lazy_names.contains(name); });

    lazy_row_index_registry = std::make_shared<LazyObjectStorageFileRegistry>();

    auto lazy_source_header = std::make_shared<const Block>(lazy_info->source_header);
    auto lazy_step = std::make_unique<LazilyReadFromObjectStorage>(
        std::move(lazy_source_header),
        storage_id,
        object_storage,
        configuration,
        storage_snapshot,
        format_settings,
        std::move(*lazy_info),
        getContext(),
        max_block_size);

    return lazy_step;
}

/// Only Parquet can keep a file's row groups in order (`preserve_order`).
static bool formatCanDeliverSortedRun(const String & format)
{
    return boost::iequals(format, "Parquet");
}

bool ReadFromObjectStorageStep::requestReadingInOrder(int direction, const QueryPlanOptimizationSettings & optimization_settings)
{
    /// Called by several optimizations, and listing the files consumes the iterator.
    if (read_in_order_attempted)
        return read_in_order && direction == 1;
    read_in_order_attempted = true;

    /// The in-order pipe has no row-index registry, see `canUseLazyMaterialization`.
    if (lazy_row_index_registry)
        return false;

    if (!configuration->isDataSortedBySortingKey(storage_snapshot->metadata, getContext()))
        return false;

    /// Files are only read forwards.
    if (direction != 1)
        return false;

    /// Buckets are assigned after planning, and the file list is not serialized.
    if (optimization_settings.make_distributed_plan || distributed_processing)
        return false;

    auto context = getContext();
    const auto & settings = context->getSettingsRef();

    if (!sortingKeyIsComputableFromSourceHeader())
        return false;

    /// Listing evaluates the filter, and a `GLOBAL IN` set is only built with the pipeline.
    if (filter_actions_dag && QueryPlanOptimizations::dagContainsNonReadySet(*filter_actions_dag))
        return false;

    createIterator();

    /// Stop at cap + 1. Every path below, rejections included, must still read all files.
    const size_t max_files = effectiveTwoLevelMergeThreshold(settings) * std::max<size_t>(1, num_streams);

    ObjectInfos files;
    bool over_cap = false;
    while (auto object_info = iterator_wrapper->next(0))
    {
        files.push_back(object_info);
        if (files.size() > max_files)
        {
            over_cap = true;
            break;
        }
    }

    iterator_wrapper = std::make_shared<ObjectIteratorWithPeekedObjects>(files, iterator_wrapper);

    if (over_cap)
        return false;

    /// Data lake files carry their own format.
    for (const auto & object_info : files)
    {
        if (object_info->isArchive())
            return false;
        if (!formatCanDeliverSortedRun(object_info->getFileFormat().value_or(configuration->format)))
            return false;
    }

    /// Otherwise row groups arrive in completion order.
    if (!format_settings)
        format_settings.emplace(getFormatSettings(context));
    format_settings->parquet.preserve_order = true;

    read_in_order = true;
    read_in_order_files = std::move(files);
    return true;
}

bool ReadFromObjectStorageStep::sortingKeyIsComputableFromSourceHeader() const
{
    const auto & metadata = storage_snapshot->metadata;
    if (!metadata->hasSortingKey())
        return false;

    /// The key expression is analyzed once against every storage column, and this header is a
    /// projection of those same columns, so the expression is computable here exactly when the
    /// query still reads every column the key consumes.
    for (const auto & column_name : metadata->getColumnsRequiredForSortingKey())
        if (!info.source_header.has(column_name))
            return false;

    return true;
}

InputOrderInfoPtr ReadFromObjectStorageStep::getDataOrder() const
{
    return convertSortingKeyToInputOrder(storage_snapshot->metadata->getSortingKey());
}

}
