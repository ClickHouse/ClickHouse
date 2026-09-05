#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <Processors/QueryPlan/LazilyReadFromObjectStorage.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Core/Settings.h>
#include <Interpreters/QueryConsumedObjectSets.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
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

#include <algorithm>

#include "config.h"

#if USE_AWS_S3
#include <IO/S3/Client.h>
#endif


namespace DB
{

namespace Setting
{
    extern const SettingsBool parallelize_output_from_storages;
    extern const SettingsBool s3_validate_etag_on_read;
}

namespace
{

/// Wraps an object iterator to record every object the read consumes into the query's
/// `QueryConsumedObjectSets`. This lets `StorageObjectStorage::getModificationHash` validate the query
/// result cache against the exact object set the read used, rather than a fresh listing that could
/// differ under a concurrent change. See `QueryConsumedObjectSets`.
class CapturingObjectIterator : public IObjectIterator
{
public:
    CapturingObjectIterator(ObjectIterator inner_, QueryConsumedObjectSetsPtr consumed_object_sets_, UUID table_uuid_)
        : inner(std::move(inner_)), consumed_object_sets(std::move(consumed_object_sets_)), table_uuid(table_uuid_)
    {
    }

    ObjectInfoPtr next(size_t thread_num) override
    {
        auto object_info = inner->next(thread_num);
        if (object_info)
        {
            QueryConsumedObjectSets::Object object;
            object.path = object_info->getPath();
            if (auto metadata = object_info->getObjectMetadata())
            {
                object.etag = metadata->etag;
                object.size = metadata->size_bytes;
                object.last_modified = metadata->last_modified.epochTime();
                object.has_metadata = true;
            }
            consumed_object_sets->add(table_uuid, std::move(object));
        }
        return object_info;
    }

    size_t estimatedKeysCount() override { return inner->estimatedKeysCount(); }
    std::optional<UInt64> getSnapshotVersion() const override { return inner->getSnapshotVersion(); }

    void setEmitProfileEvents(bool value) override
    {
        emit_profile_events = value;
        inner->setEmitProfileEvents(value);
    }

private:
    ObjectIterator inner;
    QueryConsumedObjectSetsPtr consumed_object_sets;
    UUID table_uuid;
};

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

void ReadFromObjectStorageStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & build_settings)
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

    /// When the query result cache consistency check is active, record the exact object set this read
    /// consumes so `StorageObjectStorage::getModificationHash` can hash it at finalization instead of
    /// re-listing (closes a listing `A -> B -> A` race). Only wrapped when there is a capture to fill
    /// and the table has a UUID to key it by. See `QueryConsumedObjectSets`.
    if (storage_id.hasUUID())
    {
        if (auto consumed_object_sets = context->getQueryConsumedObjectSets())
        {
            /// A filter over `_path`/`_file` or Hive-partition columns makes the iterator enumerate
            /// only a pruned subset of the table's objects (glob listing filter, `_path` value
            /// extraction). The consumed set is then a subset of the full listing the pre-read hash
            /// was built from, so the two hashes could never compare equal even for an unchanged
            /// table. Mark the capture pruned instead of filling it with the subset - the consistency
            /// check then fails closed for this table rather than comparing incomparable sets.
            const bool may_prune_object_set = predicate
                && VirtualColumnUtils::createPathAndFileFilterDAG(
                       predicate, virtual_columns, context, info.hive_partition_columns_to_read_from_file_path)
                       .has_value();
            if (may_prune_object_set)
            {
                consumed_object_sets->markPruned(storage_id.uuid);
            }
            else
            {
                /// Record an empty consumed set before the read starts, so that a read consuming no
                /// object at all is distinguishable from "this table was never read". Otherwise the
                /// finalization check would fall back to a fresh listing and could reproduce the
                /// pre-read hash of an object set the query never read. See `beginCapture`.
                consumed_object_sets->beginCapture(storage_id.uuid);
                iterator_wrapper = std::make_shared<CapturingObjectIterator>(
                    std::move(iterator_wrapper), std::move(consumed_object_sets), storage_id.uuid);
            }
        }
    }
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

    /// Even when the two generations are comparable, on most backends the second pass opens an
    /// unconditional read: `AzureObjectStorage`, `HDFSObjectStorage` and the local disk ignore
    /// `StoredObject::etag`, so a concurrent in-place overwrite between the metadata probe and the
    /// read could still stitch together rows of two versions of the file. The reread is only
    /// generation-safe when either:
    ///   - the data files are immutable by the format's contract — a data lake never overwrites a
    ///     data file in place, so a file identified by path is always the same generation; or
    ///   - the backend pins the actual read to the captured generation — S3 with
    ///     `s3_validate_etag_on_read` issues the GET with an `If-Match` on the captured ETag and
    ///     rejects a response whose ETag drifted from it (see `ReadBufferFromS3`), which is atomic
    ///     with respect to an overwrite.
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
    if (!configuration->dataFilesAreImmutable() && !reread_is_generation_pinned)
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
    /// `StorageObjectStorage::read` propagates a bare row policy (no PREWHERE) into
    /// `info.row_level_filter`, which the split pins to the main pass; keep this guard in case a
    /// caller constructs the step without that propagation, since the source would still evaluate
    /// the filter in the main pass via `FormatFilterInfo`.
    NameSet names_to_keep = required_names;
    if (!info.row_level_filter && query_info.row_level_filter)
        for (const auto & column : query_info.row_level_filter->actions.getRequiredColumns())
            names_to_keep.insert(column.name);

    auto lazy_info = splitLazilyReadColumnsFromFormatInfo(info, names_to_keep);
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

bool ReadFromObjectStorageStep::requestReadingInOrder() const
{
    return configuration->isDataSortedBySortingKey(storage_snapshot->metadata, getContext());
}

InputOrderInfoPtr ReadFromObjectStorageStep::getDataOrder() const
{
    return convertSortingKeyToInputOrder(storage_snapshot->metadata->getSortingKey());
}

}
