#include <Processors/Sources/LazyReadFromObjectStorageSource.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

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
}

namespace
{

/// Iterates over the files that contain surviving rows, in the file index order,
/// attaching the set of rows to read to each of them.
class LazyRowsObjectIterator : public IObjectIterator
{
public:
    LazyRowsObjectIterator(
        std::vector<ObjectStorageLazyMaterializingRows::FileRows> files_,
        ObjectStoragePtr object_storage_,
        bool etag_validated_on_read_)
        : files(std::move(files_))
        , object_storage(std::move(object_storage_))
        , etag_validated_on_read(etag_validated_on_read_)
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

        /// For S3 with `s3_validate_etag_on_read`, the GET itself is pinned to the captured ETag
        /// (see `ReadBufferFromS3`), which is race-free; an extra HEAD here would buy nothing.
        if (etag_validated_on_read && !captured->etag.empty())
            return;

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

    const std::vector<ObjectStorageLazyMaterializingRows::FileRows> files;
    const ObjectStoragePtr object_storage;
    const bool etag_validated_on_read;
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
    , lazy_materializing_rows(std::move(lazy_materializing_rows_))
{
}

IProcessor::Status LazyReadFromObjectStorageSource::prepare()
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
        return Status::UpdatePipeline;

    if (inputs.empty())
    {
        /// No rows survived the LIMIT, nothing to read.
        output.finish();
        return Status::Finished;
    }

    auto & input = inputs.front();
    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    output.push(input.pull());
    return Status::PortFull;
}

IProcessor::PipelineUpdate LazyReadFromObjectStorageSource::updatePipeline()
{
    if (!lazy_materializing_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LazyReadFromObjectStorageSource: no lazy materializing rows");

    auto rows = std::move(lazy_materializing_rows);
    lazy_materializing_rows.reset();

    if (rows->rows_in_files.empty())
        return {};

    const bool etag_validated_on_read = object_storage->getType() == ObjectStorageType::S3
        && context->getSettingsRef()[Setting::s3_validate_etag_on_read];
    auto iterator = std::make_shared<LazyRowsObjectIterator>(
        std::move(rows->rows_in_files), object_storage, etag_validated_on_read);

    /// The rows of a file must be returned in ascending order, otherwise LazyMaterializingTransform
    /// would restore the original row order incorrectly.
    FormatSettings modified_format_settings = format_settings ? *format_settings : getFormatSettings(context);
    modified_format_settings.parquet.preserve_order = true;

    auto parser_shared_resources = std::make_shared<FormatParserSharedResources>(context->getSettingsRef(), /*num_streams_=*/ 1);

    /// The per-file sets of rows to read are attached to the objects by the iterator
    /// (see ObjectInfo::rows_to_read). There is no filter and no prewhere here: the surviving rows
    /// are known exactly, so the deferred columns are read without any filtering expressions.
    auto format_filter_info = std::make_shared<FormatFilterInfo>(
        /*filter_actions_dag_=*/ nullptr,
        context,
        configuration->getColumnMapperForCurrentSchema(storage_snapshot->metadata, context),
        /*row_level_filter_=*/ nullptr,
        /*prewhere_info_=*/ nullptr);

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

    return PipelineUpdate{.to_add = {source}, .to_remove = {}};
}

}
