#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakeSink.h>

#if USE_DELTA_KERNEL_RS
#include <Common/logger_useful.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/WriteTransaction.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>


namespace DB
{

DeltaLakeSink::DeltaLakeSink(
    DeltaLake::WriteTransactionPtr delta_transaction_,
    StorageObjectStorageConfigurationPtr configuration_,
    ObjectStoragePtr object_storage_,
    ContextPtr context_,
    SharedHeader sample_block_,
    const std::optional<FormatSettings> & format_settings_)
    : StorageObjectStorageSink(
        DeltaLake::generateWritePath(
            delta_transaction_->getDataPath(),
            configuration_->format),
        object_storage_,
        configuration_,
        format_settings_,
        sample_block_,
        context_)
    , delta_transaction(delta_transaction_)
    , object_storage(object_storage_)
{
    delta_transaction->validateSchema(getHeader());
}

void DeltaLakeSink::consume(Chunk & chunk)
{
    if (isCancelled())
        return;

    written_bytes += chunk.bytes();
    StorageObjectStorageSink::consume(chunk);
}

void DeltaLakeSink::onFinish()
{
    if (isCancelled())
        return;

    StorageObjectStorageSink::onFinish();

    try
    {
        std::vector<DeltaLake::WriteTransaction::CommitFile> files;
        auto file_location = getPath().substr(delta_transaction->getDataPath().size());
        files.emplace_back(std::move(file_location), written_bytes, Map{});
        delta_transaction->commit(files);
    }
    catch (...)
    {
        /// FIXME: this should be just removeObject,
        /// but IObjectStorage does not have such method.
        object_storage->removeObjectIfExists(StoredObject(getPath()));
        throw;
    }
}

}

#endif
