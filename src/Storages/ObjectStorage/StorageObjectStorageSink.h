#pragma once
#include <Storages/PartitionedSink.h>
#include <Storages/ObjectStorage/StorageObejctStorageConfiguration.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Disks/ObjectStorages/IObjectStorage.h>

namespace DB
{
class StorageObjectStorageSink : public SinkToStorage
{
public:
    StorageObjectStorageSink(
        ObjectStoragePtr object_storage,
        StorageObjectStorageConfigurationPtr configuration,
        std::optional<FormatSettings> format_settings_,
        const Block & sample_block_,
        ContextPtr context,
        const std::string & blob_path = "")
        : SinkToStorage(sample_block_)
        , sample_block(sample_block_)
        , format_settings(format_settings_)
    {
        const auto & settings = context->getSettingsRef();
        const auto path = blob_path.empty() ? configuration->getPaths().back() : blob_path;
        const auto chosen_compression_method = chooseCompressionMethod(path, configuration->compression_method);

        auto buffer = object_storage->writeObject(
            StoredObject(path), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

        write_buf = wrapWriteBufferWithCompressionMethod(
                        std::move(buffer),
                        chosen_compression_method,
                        static_cast<int>(settings.output_format_compression_level),
                        static_cast<int>(settings.output_format_compression_zstd_window_log));

        writer = FormatFactory::instance().getOutputFormatParallelIfPossible(
            configuration->format, *write_buf, sample_block, context, format_settings);
    }

    String getName() const override { return "StorageObjectStorageSink"; }

    void consume(Chunk chunk) override
    {
        std::lock_guard lock(cancel_mutex);
        if (cancelled)
            return;
        writer->write(getHeader().cloneWithColumns(chunk.detachColumns()));
    }

    void onCancel() override
    {
        std::lock_guard lock(cancel_mutex);
        finalize();
        cancelled = true;
    }

    void onException(std::exception_ptr exception) override
    {
        std::lock_guard lock(cancel_mutex);
        try
        {
            std::rethrow_exception(exception);
        }
        catch (...)
        {
            /// An exception context is needed to proper delete write buffers without finalization.
            release();
        }
    }

    void onFinish() override
    {
        std::lock_guard lock(cancel_mutex);
        finalize();
    }

private:
    const Block sample_block;
    const std::optional<FormatSettings> format_settings;

    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;
    bool cancelled = false;
    std::mutex cancel_mutex;

    void finalize()
    {
        if (!writer)
            return;

        try
        {
            writer->finalize();
            writer->flush();
            write_buf->finalize();
        }
        catch (...)
        {
            /// Stop ParallelFormattingOutputFormat correctly.
            release();
            throw;
        }
    }

    void release()
    {
        writer.reset();
        write_buf->finalize();
    }
};

class PartitionedStorageObjectStorageSink : public PartitionedSink
{
public:
    PartitionedStorageObjectStorageSink(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        std::optional<FormatSettings> format_settings_,
        const Block & sample_block_,
        ContextPtr context_,
        const ASTPtr & partition_by)
        : PartitionedSink(partition_by, context_, sample_block_)
        , object_storage(object_storage_)
        , configuration(configuration_)
        , format_settings(format_settings_)
        , sample_block(sample_block_)
        , context(context_)
    {
    }

    SinkPtr createSinkForPartition(const String & partition_id) override
    {
        auto blob = configuration->getPaths().back();
        auto partition_key = replaceWildcards(blob, partition_id);
        validatePartitionKey(partition_key, true);
        return std::make_shared<StorageObjectStorageSink>(
            object_storage,
            configuration,
            format_settings,
            sample_block,
            context,
            partition_key
        );
    }

private:
    ObjectStoragePtr object_storage;
    StorageObjectStorageConfigurationPtr configuration;
    const std::optional<FormatSettings> format_settings;
    const Block sample_block;
    const ContextPtr context;
};

}
