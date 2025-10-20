#include <Core/Settings.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakeSink.h>

#if USE_DELTA_KERNEL_RS
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/WriteTransaction.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>


namespace DB
{
namespace Setting
{
    extern const SettingsNonZeroUInt64 delta_lake_insert_max_rows_in_data_file;
    extern const SettingsNonZeroUInt64 delta_lake_insert_max_bytes_in_data_file;
}

DeltaLakeSink::DeltaLakeSink(
    DeltaLake::WriteTransactionPtr delta_transaction_,
    StorageObjectStorageConfigurationPtr configuration_,
    ObjectStoragePtr object_storage_,
    ContextPtr context_,
    SharedHeader sample_block_,
    const std::optional<FormatSettings> & format_settings_)
    : SinkToStorage(sample_block_)
    , WithContext(context_)
    , delta_transaction(delta_transaction_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , format_settings(format_settings_)
    , sample_block(sample_block_)
    , data_file_max_rows(context_->getSettingsRef()[Setting::delta_lake_insert_max_rows_in_data_file])
    , data_file_max_bytes(context_->getSettingsRef()[Setting::delta_lake_insert_max_bytes_in_data_file])
{
    delta_transaction->validateSchema(getHeader());
}

DeltaLakeSink::StorageSinkPtr DeltaLakeSink::createStorageSink() const
{
    return std::make_unique<StorageObjectStorageSink>(
        DeltaLake::generateWritePath(
            delta_transaction->getDataPath(),
            configuration->format),
        object_storage,
        configuration,
        format_settings,
        sample_block,
        getContext());
}

void DeltaLakeSink::consume(Chunk & chunk)
{
    if (isCancelled())
        return;

    if (data_files.empty()
        || data_files.back().written_bytes >= data_file_max_bytes
        || data_files.back().written_rows >= data_file_max_rows)
    {
        data_files.emplace_back(createStorageSink());
    }

    auto & data_file = data_files.back();
    data_file.written_bytes += chunk.bytes();
    data_file.written_rows += chunk.getNumRows();
    data_file.sink->consume(chunk);
}

void DeltaLakeSink::onFinish()
{
    if (isCancelled())
        return;

    std::vector<DeltaLake::WriteTransaction::CommitFile> files;
    files.reserve(data_files.size());
    for (const auto & [sink, written_bytes, written_rows] : data_files)
    {
        sink->onFinish();
        auto file_location = sink->getPath().substr(delta_transaction->getDataPath().size());
        /// We use file size from sink to count all file, not just actual data.
        auto file_size = sink->getFileSize();
        files.emplace_back(std::move(file_location), file_size, written_rows, Map{});
    }

    try
    {
        delta_transaction->commit(files);
    }
    catch (...)
    {
        for (const auto & [sink, written_bytes, written_rows] : data_files)
        {
            /// FIXME: this should be just removeObject,
            /// but IObjectStorage does not have such method.
           object_storage->removeObjectIfExists(StoredObject(sink->getPath()));

        }
        throw;
    }
}

}

#endif
