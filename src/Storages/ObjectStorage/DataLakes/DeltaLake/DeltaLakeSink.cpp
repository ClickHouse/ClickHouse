#include "DeltaLakeSink.h"
#include <Common/logger_useful.h>
#include <Core/UUID.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/WriteTransaction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
    extern const int LOGICAL_ERROR;
}

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
    , log(getLogger("DeltaLakeSink"))
    , file_name()
    , delta_transaction(std::make_shared<DeltaLake::WriteTransaction>(metadata.getKernelHelper()))
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

    std::vector<DeltaLake::WriteTransaction::CommitFile> files;
    files.emplace_back(file_name, written_bytes, Map{});
    delta_transaction->commit(files);

    StorageObjectStorageSink::onFinish();
}

}
