#pragma once
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>


namespace DeltaLake
{
class WriteTransaction;
using WriteTransactionPtr = std::shared_ptr<WriteTransaction>;
}

namespace DB
{
class DeltaLakeMetadataDeltaKernel;

class DeltaLakeSink : public StorageObjectStorageSink
{
public:
    DeltaLakeSink(
        const DeltaLakeMetadataDeltaKernel & metadata,
        StorageObjectStorageConfigurationPtr configuration_,
        ObjectStoragePtr object_storage_,
        ContextPtr context_,
        SharedHeader sample_block_,
        const FormatSettings & format_settings_);

    ~DeltaLakeSink() override = default;

    String getName() const override { return "DeltaLakeSink"; }

    void consume(Chunk & chunk) override;

    void onFinish() override;

private:
    const LoggerPtr log;
    const std::string file_name;
    DeltaLake::WriteTransactionPtr delta_transaction;
    size_t written_bytes = 0;
};

}
