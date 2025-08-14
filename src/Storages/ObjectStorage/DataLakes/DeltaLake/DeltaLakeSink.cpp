#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakeSink.h>
#include <Common/logger_useful.h>
#include <Core/UUID.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/WriteTransaction.h>


namespace DB
{

namespace
{
    std::string generatePath(const std::string & prefix)
    {
        return std::filesystem::path(prefix) / (toString(UUIDHelpers::generateV4()) + ".parquet");
    }
}

DeltaLakeSink::DeltaLakeSink(
    const DeltaLakeMetadataDeltaKernel & metadata,
    StorageObjectStorageConfigurationPtr configuration_,
    ObjectStoragePtr object_storage_,
    ContextPtr context_,
    SharedHeader sample_block_,
    const FormatSettings & format_settings_)
    : StorageObjectStorageSink(
        generatePath(metadata.getKernelHelper()->getDataPath()),
        object_storage_,
        configuration_,
        format_settings_,
        sample_block_,
        context_)
    , data_prefix(metadata.getKernelHelper()->getDataPath())
    , delta_transaction(std::make_shared<DeltaLake::WriteTransaction>(metadata.getKernelHelper()))
    , object_storage(object_storage_)
{
    delta_transaction->create();
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

    std::vector<DeltaLake::WriteTransaction::CommitFile> files;
    chassert(startsWith(getPath(), data_prefix));
    files.emplace_back(getPath().substr(data_prefix.size()), written_bytes, Map{});
    try
    {
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
