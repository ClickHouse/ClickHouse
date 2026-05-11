#pragma once
#include "config.h"

#if USE_DELTA_KERNEL_RS
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

/**
 * Sink to write non-partitioned data to DeltaLake.
 * Writes a single data file and commits it to DeltaLake metadata.
 */
class DeltaLakeSink : public SinkToStorage, private WithContext
{
public:
    DeltaLakeSink(
        DeltaLake::WriteTransactionPtr delta_transaction_,
        StorageObjectStorageConfigurationPtr configuration_,
        ObjectStoragePtr object_storage_,
        ContextPtr context_,
        SharedHeader sample_block_,
        const std::optional<FormatSettings> & format_settings_);

    ~DeltaLakeSink() override = default;

    String getName() const override { return "DeltaLakeSink"; }

    void consume(Chunk & chunk) override;

    void onFinish() override;

private:
    using StorageSinkPtr = std::unique_ptr<StorageObjectStorageSink>;
    StorageSinkPtr createStorageSink() const;

    const DeltaLake::WriteTransactionPtr delta_transaction;
    const ObjectStoragePtr object_storage;
    const StorageObjectStorageConfigurationPtr configuration;
    const std::optional<FormatSettings> format_settings;
    const SharedHeader sample_block;
    const size_t data_file_max_rows;
    const size_t data_file_max_bytes;

    struct DataFileInfo
    {
        explicit DataFileInfo(StorageSinkPtr sink_) : sink(std::move(sink_)) {}
        StorageSinkPtr sink;
        size_t written_bytes = 0;
        size_t written_rows = 0;
    };
    std::vector<DataFileInfo> data_files;
};

}

#endif
