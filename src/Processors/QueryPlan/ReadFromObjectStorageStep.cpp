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
#include <Storages/prepareReadingFromFormat.h>
#include <Storages/VirtualColumnUtils.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Common/CurrentMetrics.h>
#include <boost/algorithm/string/predicate.hpp>

#include <algorithm>

#include "config.h"

#if USE_AWS_S3
#include <IO/S3/Client.h>
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
    extern const SettingsBool s3_validate_etag_on_read;
    extern const SettingsBool azure_validate_etag_on_read;
    extern const SettingsUInt64 read_in_order_two_level_merge_threshold;
    extern const SettingsBool compile_sort_description;
    extern const SettingsUInt64 min_count_to_compile_sort_description;
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

#if USE_PARQUET
    /// The legacy Delta Lake reader (`allow_delta_kernel_rs = 0`) keeps the partition columns in the
    /// format header and replaces them in the chunks the format returns with the partition values, so
    /// the reader would compare whatever a data file stores under that name against the threshold.
    if (std::dynamic_pointer_cast<const DeltaLakeMetadata>(configuration->getExternalMetadata()))
        return false;
#endif

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
Pipe ReadFromObjectStorageStep::buildInOrderPipe(const ContextPtr & local_context, const FormatFilterInfoPtr & format_filter_info)
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

    /// Iceberg reads the row lineage columns from the data file.
    auto format_filter_info = std::make_shared<FormatFilterInfo>(
        info.getFormatFilter(filter_actions_dag, /*keep_row_lineage_columns=*/ configuration->isIcebergConfiguration()),
        context,
        configuration->getColumnMapperForCurrentSchema(storage_snapshot->metadata, context),
        query_info.row_level_filter,
        query_info.prewhere_info);
    format_filter_info->top_k_filter = top_k_filter;

    if (read_in_order && !read_in_order_files.empty())
    {
        auto in_order_pipe = buildInOrderPipe(context, format_filter_info);
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
    /// `max_num_streams` is a read-parallelism request, not a thread budget.
    const size_t resize_to = std::min(max_num_streams, build_settings.max_threads);
    if (parallelize_output
        && FormatFactory::instance().checkParallelizeOutputAfterReading(configuration->format, context)
        && output_ports > 0 && output_ports < resize_to)
        pipe.resize(resize_to);

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

    /// Reading in order (granted earlier in this optimization pass) rebuilds the pipeline as one
    /// sorted run per file, without the row-index registry the lazy branch needs, so the two
    /// rewrites do not compose; the in-order read was promised first, decline the lazy rewrite.
    if (read_in_order)
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

    /// Even when the two generations are comparable, on most backends the second pass opens an
    /// unconditional read: `HDFSObjectStorage` and the local disk ignore `StoredObject::etag`, so a concurrent in-place overwrite between the metadata probe and the
    /// read could still stitch together rows of two versions of the file. The reread is only
    /// generation-safe when either:
    ///   - the data files are immutable by the format's contract — a data lake never overwrites a
    ///     data file in place, so a file identified by path is always the same generation; or
    ///   - the backend pins the actual read to the captured generation — S3 with
    ///     `s3_validate_etag_on_read` issues the GET with an `If-Match` on the captured ETag and
    ///     rejects a response whose ETag drifted from it (see `ReadBufferFromS3`), which is atomic
    ///     with respect to an overwrite; Azure with `azure_validate_etag_on_read` does the same
    ///     (see `ReadBufferFromAzureBlobStorage`).
    /// The pin only takes effect when the captured metadata actually carries a non-empty `ETag`
    /// (see `createReadBuffer`), and `GCS` accessed through the S3 API is documented to legitimately
    /// return objects without one — so a `GCS`-provider client is not pinned even with the setting
    /// on. Any other provider that unexpectedly yields an empty `ETag` at read time fails close in
    /// `LazyRowsObjectIterator::validateObjectGeneration` instead of degrading to an unpinned read.
    /// For a mutable file on a backend that cannot pin the read, keep the single-pass plan.
    bool reread_is_generation_pinned = false;
#if USE_AWS_S3
    if (object_storage->getType() == ObjectStorageType::S3
        && getContext()->getSettingsRef()[Setting::s3_validate_etag_on_read])
    {
        const auto s3_client = object_storage->tryGetS3StorageClient();
        reread_is_generation_pinned = s3_client && s3_client->getProviderType() != S3::ProviderType::GCS;
    }
#endif
    if (object_storage->getType() == ObjectStorageType::Azure
        && getContext()->getSettingsRef()[Setting::azure_validate_etag_on_read])
        reread_is_generation_pinned = true;
    if (!configuration->dataFilesAreImmutable() && !reread_is_generation_pinned)
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
