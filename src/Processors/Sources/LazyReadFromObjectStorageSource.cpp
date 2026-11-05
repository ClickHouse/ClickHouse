#include <Common/VectorWithMemoryTracking.h>
#include <Processors/Sources/LazyReadFromObjectStorageSource.h>
#include <Processors/Formats/AggregateFunctionStatesFromValuesInputFormat.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

#include <boost/algorithm/string/predicate.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int S3_OBJECT_CHANGED_DURING_READ;
}

namespace Setting
{
    extern const SettingsBool s3_validate_etag_on_read;
    extern const SettingsBool azure_validate_etag_on_read;
}

namespace
{

/// Iterates over the files that contain surviving rows, in the file index order,
/// attaching the set of rows to read to each of them.
class LazyRowsObjectIterator : public IObjectIterator
{
public:
    LazyRowsObjectIterator(
        VectorWithMemoryTracking<ObjectStorageLazyMaterializingRows::FileRows> files_,
        ObjectStoragePtr object_storage_,
        bool etag_validated_on_read_,
        bool data_files_are_immutable_)
        : files(std::move(files_))
        , object_storage(std::move(object_storage_))
        , etag_validated_on_read(etag_validated_on_read_)
        , data_files_are_immutable(data_files_are_immutable_)
    {
    }

    ObjectInfoPtr next(size_t) override
    {
        size_t i = index.fetch_add(1);
        if (i >= files.size())
            return nullptr;

        const auto & file = files[i];
        validateObjectGeneration(*file.object);
        file.object->rows_to_read = file.rows;
        return file.object;
    }

    size_t estimatedKeysCount() override { return files.size(); }

private:
    /// The row numbers to read were produced by the main pass of the query from a concrete
    /// generation of the object. Reading the deferred columns from a different generation
    /// (the object was overwritten in place between the two passes) would silently combine
    /// rows of two versions of the file, so fail close instead.
    void validateObjectGeneration(const ObjectInfo & object) const
    {
        /// The main pass fetches the metadata before reading (see `createReadBuffer`) and the
        /// registry keeps the same `ObjectInfo`, so the captured generation is always here.
        const auto captured = object.getObjectMetadata();
        if (!captured)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Lazy materialization: no object metadata was captured for {} in the main reading pass",
                object.getPath());

        /// For S3 with `s3_validate_etag_on_read` (and Azure with `azure_validate_etag_on_read`), the GET
        /// itself is pinned to the captured ETag (see `ReadBufferFromS3`, `ReadBufferFromAzureBlobStorage`),
        /// which is race-free; an extra HEAD here would buy nothing.
        if (etag_validated_on_read && !captured->etag.empty())
            return;

        if (captured->is_from_table_records)
            return;

        /// A mutable file is only admitted to the lazy plan when the reread is pinned to the
        /// captured ETag (see `ReadFromObjectStorageStep::canUseLazyMaterialization`). Reaching
        /// this point means the storage returned no ETag for the object, so the GET cannot be
        /// pinned, and a metadata probe below cannot close the probe-to-read race for a file that
        /// may be overwritten in place — fail close instead of degrading to an unpinned read.
        /// (For immutable data lake files the probe is a sufficient defensive check: a data file
        /// identified by path is never overwritten, so there is no race to close.)
        if (!data_files_are_immutable)
            throw Exception(ErrorCodes::S3_OBJECT_CHANGED_DURING_READ,
                "Lazy materialization: file {} cannot be pinned to the generation seen by the main reading pass "
                "(the storage returned no ETag for it). "
                "Rerun the query, or disable the query_plan_optimize_lazy_materialization_for_object_storage setting",
                object.getPath());

        const auto & seen = *captured;

        /// Probe the same physical object that the reading pass identifies (mirroring `createReader`):
        /// for an entry of an archive, the object read (and whose metadata was captured by the main pass)
        /// is the archive itself, not the synthetic `archive::inner` path; and the probe must go through
        /// the `RelativePathWithMetadata` overload so that `read_source_index` is preserved — for web URL
        /// shards the same path can be served from different URL options, and the plain string overload
        /// would drop the shard identity and could validate one shard using another's metadata.
        const auto & path = object.isArchive() ? object.getPathToArchive() : object.getPath();
        auto metadata_object = object.relative_path_with_metadata;
        metadata_object.relative_path = path;
        const auto current = object_storage->tryGetObjectMetadata(metadata_object, /*with_tags=*/ false);

        bool changed = false;
        bool comparable = false;

        if (current)
        {
            if (!seen.etag.empty() && !current->etag.empty())
            {
                comparable = true;
                changed = seen.etag != current->etag;
            }
            else
            {
                if (seen.is_size_known && current->is_size_known)
                {
                    comparable = true;
                    changed = seen.size_bytes != current->size_bytes;
                }
                if (!changed && seen.is_last_modified_known && current->is_last_modified_known)
                {
                    comparable = true;
                    changed = seen.last_modified != current->last_modified;
                }
            }
        }

        if (changed || !comparable)
            throw Exception(ErrorCodes::S3_OBJECT_CHANGED_DURING_READ,
                "Lazy materialization: file {} {} between the main reading pass and the lazy reading pass. "
                "Rerun the query, or disable the query_plan_optimize_lazy_materialization_for_object_storage setting",
                path,
                changed ? "was modified" : "cannot be proven unchanged");
    }

    const VectorWithMemoryTracking<ObjectStorageLazyMaterializingRows::FileRows> files;
    const ObjectStoragePtr object_storage;
    const bool etag_validated_on_read;
    const bool data_files_are_immutable;
    std::atomic<size_t> index = 0;
};

}

LazyReadFromObjectStorageSource::LazyReadFromObjectStorageSource(
    SharedHeader header,
    StorageID storage_id_,
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    StorageSnapshotPtr storage_snapshot_,
    std::optional<DB::FormatSettings> format_settings_,
    ReadFromFormatInfo info_,
    ContextPtr context_,
    size_t max_block_size_,
    size_t max_threads_,
    size_t main_read_leaf_columns_,
    ObjectStorageLazyMaterializingRowsPtr lazy_materializing_rows_)
    : IProcessor({}, {std::move(header)})
    , storage_id(std::move(storage_id_))
    , object_storage(std::move(object_storage_))
    , configuration(std::move(configuration_))
    , storage_snapshot(std::move(storage_snapshot_))
    , format_settings(std::move(format_settings_))
    , info(std::move(info_))
    , context(std::move(context_))
    , max_block_size(max_block_size_)
    , max_threads(max_threads_)
    , main_read_leaf_columns(main_read_leaf_columns_)
    , lazy_materializing_rows(std::move(lazy_materializing_rows_))
    , next_input_to_process(inputs.end())
{
}

IProcessor::Status LazyReadFromObjectStorageSource::prepare(const UpdatedInputPorts & updated_input_ports, const UpdatedOutputPorts & /*updated_output_ports*/)
{
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    /// Pull the chunk of every updated reader before checking the output (an updated port is reported once,
    /// and a reader whose chunk stays in its port stops), but push the chunks strictly in the order of the groups.
    if (next_input_to_process != inputs.end())
    {
        for (InputPort * input_port : updated_input_ports)
        {
            auto & reader_chunks = chunks[input_port_to_index.at(input_port)];
            if (!input_port->isFinished() && input_port->hasData())
                reader_chunks.emplace_back(input_port->pull());
        }
    }

    if (!output.canPush())
        return Status::PortFull;

    if (lazy_materializing_rows)
        return Status::UpdatePipeline;

    while (next_input_to_process != inputs.end())
    {
        auto & input = *next_input_to_process;
        auto & pending = chunks[next_chunk_to_process];

        if (!pending.empty())
        {
            output.push(std::move(pending.front()));
            pending.pop_front();
            return Status::PortFull;
        }

        if (input.isFinished())
        {
            ++next_input_to_process;
            ++next_chunk_to_process;
            continue;
        }

        if (!input.hasData())
            return Status::NeedData;

        output.push(input.pull());
        return Status::PortFull;
    }

    /// All the readers have finished, or no rows survived the LIMIT and there is nothing to read.
    output.finish();
    return Status::Finished;
}

IProcessor::PipelineUpdate LazyReadFromObjectStorageSource::updatePipeline()
{
    if (!lazy_materializing_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LazyReadFromObjectStorageSource: no lazy materializing rows");

    auto rows = std::move(lazy_materializing_rows);
    lazy_materializing_rows.reset();
    next_input_to_process = inputs.end();

    auto & files = rows->rows_in_files;
    if (files.empty())
        return {};

    const auto & settings = context->getSettingsRef();
    const bool etag_validated_on_read
        = (object_storage->getType() == ObjectStorageType::S3 && settings[Setting::s3_validate_etag_on_read])
        || (object_storage->getType() == ObjectStorageType::Azure && settings[Setting::azure_validate_etag_on_read]);

    /// The rows of a file must be returned in ascending order, otherwise LazyMaterializingTransform
    /// would restore the original row order incorrectly.
    FormatSettings modified_format_settings = format_settings ? *format_settings : getFormatSettings(context);
    modified_format_settings.parquet.preserve_order = true;
    modified_format_settings.vortex.preserve_order = true;

    /// All readers together read no more leaf columns at once than the main pass; a lazy read of unknown width keeps one reader.
    /// Vortex reads whole top-level fields and converts them to the requested types, so the header does not show their width.
    const auto header_to_parse = AggregateFunctionStatesFromValuesInputFormat::getHeaderToParse(
        info.format_header, modified_format_settings.aggregate_function_input_format);
    const Block & parsed_header = header_to_parse ? *header_to_parse : info.format_header;
    const bool reads_vortex = std::ranges::any_of(
        files, [&](const auto & file) { return boost::iequals(file.object->getFileFormat().value_or(configuration->format), "Vortex"); });
    const auto lazy_read_leaf_columns = reads_vortex ? std::nullopt : countLeafColumns(parsed_header);
    /// A source of the main pass reads one file at a time, and every file that returned rows is registered.
    size_t main_read_streams = 0;
    {
        std::lock_guard lock(rows->file_registry->mutex);
        main_read_streams = std::min(rows->file_registry->main_read_streams, rows->file_registry->files.size());
    }
    main_read_streams = std::clamp<size_t>(main_read_streams, 1, std::max<size_t>(max_threads, 1));
    const size_t max_streams_for_columns = lazy_read_leaf_columns
        ? std::max<size_t>(main_read_streams * main_read_leaf_columns / std::max<size_t>(*lazy_read_leaf_columns, 1), 1)
        : 1;
    const size_t num_streams = std::min({std::max<size_t>(max_threads, 1), max_streams_for_columns, files.size()});
    size_t total_rows = 0;
    for (const auto & file : files)
        total_rows += file.rows->size();

    /// Contiguous groups of files in file order, with about the same number of rows and at least one file each.
    VectorWithMemoryTracking<VectorWithMemoryTracking<ObjectStorageLazyMaterializingRows::FileRows>> groups(num_streams);
    size_t group = 0;
    size_t rows_up_to_current_file = 0;
    for (size_t i = 0; i < files.size(); ++i)
    {
        rows_up_to_current_file += files[i].rows->size();
        groups[group].push_back(std::move(files[i]));
        const size_t files_left = files.size() - i - 1;
        const size_t groups_left = num_streams - group - 1;
        if (groups_left > 0 && (files_left == groups_left || rows_up_to_current_file * num_streams >= total_rows * (group + 1)))
            ++group;
    }

    auto parser_shared_resources = std::make_shared<FormatParserSharedResources>(settings, num_streams);

    /// The per-file sets of rows to read are attached to the objects by the iterator
    /// (see ObjectInfo::rows_to_read). There is no filter and no prewhere here: the surviving rows
    /// are known exactly, so the deferred columns are read without any filtering expressions.
    auto format_filter_info = std::make_shared<FormatFilterInfo>(
        /*filter_actions_dag_=*/ nullptr,
        context,
        configuration->getColumnMapperForCurrentSchema(storage_snapshot->metadata, context),
        /*row_level_filter_=*/ nullptr,
        /*prewhere_info_=*/ nullptr);

    Processors sources;
    for (auto & group_files : groups)
    {
        auto iterator = std::make_shared<LazyRowsObjectIterator>(
            std::move(group_files), object_storage, etag_validated_on_read, configuration->dataFilesAreImmutable());

        auto source = std::make_shared<StorageObjectStorageSource>(
            storage_id,
            getName(),
            object_storage,
            configuration,
            storage_snapshot,
            info,
            modified_format_settings,
            context,
            max_block_size,
            iterator,
            parser_shared_resources,
            format_filter_info,
            /*need_only_count_=*/ false);

        auto & source_output = source->getOutputs().front();
        inputs.emplace_back(source_output.getHeader(), this);
        connect(source_output, inputs.back());
        inputs.back().setNeeded();
        const size_t input_index = input_port_to_index.size();
        input_port_to_index[&inputs.back()] = input_index;
        sources.push_back(std::move(source));
    }

    next_input_to_process = inputs.begin();
    chunks.resize(sources.size());
    return PipelineUpdate{.to_add = std::move(sources), .to_remove = {}};
}

}
