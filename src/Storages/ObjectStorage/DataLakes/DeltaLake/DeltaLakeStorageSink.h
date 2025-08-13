#pragma once
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/ObjectStorage/IObjectIterator.h>

namespace DeltaLake
{
class WriteTransaction;
using WriteTransactionPtr = std::shared_ptr<WriteTransaction>;
}

namespace DB
{

class IOutputFormat;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;
class DeltaLakeMetadataDeltaKernel;

class DeltaLakeStorageSink : public SinkToStorage
{
public:
    explicit DeltaLakeStorageSink(
        const DeltaLakeMetadataDeltaKernel & metadata,
        ObjectStoragePtr object_storage,
        ContextPtr context,
        SharedHeader sample_block_,
        const FormatSettings & format_settings_);

    ~DeltaLakeStorageSink() override = default;

    String getName() const override { return "DeltaLakeStorageSink"; }

    void consume(Chunk & chunk) override;

    void onFinish() override;

private:
    void finalizeBuffers();
    void releaseBuffers();
    void cancelBuffers();

    const LoggerPtr log;
    const FormatSettings format_settings;
    const std::string file_name;

    DeltaLake::WriteTransactionPtr delta_transaction;
    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;
};

}
