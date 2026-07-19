#include <Processors/Sources/LazyReadFromObjectStorageSource.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Iterates over the files that contain surviving rows, in the file index order,
/// attaching the set of rows to read to each of them.
class LazyRowsObjectIterator : public IObjectIterator
{
public:
    explicit LazyRowsObjectIterator(std::vector<ObjectStorageLazyMaterializingRows::FileRows> files_)
        : files(std::move(files_))
    {
    }

    ObjectInfoPtr next(size_t) override
    {
        size_t i = index.fetch_add(1);
        if (i >= files.size())
            return nullptr;

        const auto & file = files[i];
        file.object->rows_to_read = file.rows;
        return file.object;
    }

    size_t estimatedKeysCount() override { return files.size(); }

private:
    const std::vector<ObjectStorageLazyMaterializingRows::FileRows> files;
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

    auto iterator = std::make_shared<LazyRowsObjectIterator>(std::move(rows->rows_in_files));

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
