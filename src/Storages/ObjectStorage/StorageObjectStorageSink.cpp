#include <IO/CompressionMethod.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Common/isValidUTF8.h>
#include <Core/Settings.h>
#include <Storages/NumberedFileName.h>
#include <Storages/ObjectStorage/Utils.h>
#include <base/defines.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 output_format_compression_level;
    extern const SettingsUInt64 output_format_compression_zstd_window_log;
    extern const SettingsSnappyMode snappy_mode;
}

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{
    void validateKey(const String & str)
    {
        /// See:
        /// - https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
        /// - https://cloud.ibm.com/apidocs/cos/cos-compatibility#putobject

        if (str.empty() || str.size() > 1024)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect key length (not empty, max 1023 characters), got: {}", str.size());

        if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(str.data()), str.size()))
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Incorrect non-UTF8 sequence in key");

        PartitionedSink::validatePartitionKey(str, true);
    }

    void validateNamespace(const String & str, StorageObjectStorageConfigurationPtr configuration)
    {
        configuration->validateNamespace(str);

        if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(str.data()), str.size()))
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Incorrect non-UTF8 sequence in bucket name");

        PartitionedSink::validatePartitionKey(str, false);
    }
}

StorageObjectStorageSink::StorageObjectStorageSink(
    const std::string & path_,
    ObjectStoragePtr object_storage_,
    const std::optional<FormatSettings> & format_settings_,
    SharedHeader sample_block_,
    ContextPtr context_,
    const String & format_,
    const String & compression_method_,
    size_t split_on_write_by_size_bytes_,
    GetNextPathCallback get_next_path_)
    : SinkToStorage(sample_block_)
    , path(path_)
    , object_storage(object_storage_)
    , format_settings(format_settings_)
    , sample_block(sample_block_)
    , context(context_)
    , format(format_)
    , compression_method(compression_method_)
    , split_on_write_by_size_bytes(split_on_write_by_size_bytes_)
    , get_next_path(std::move(get_next_path_))
{
    if (split_on_write_by_size_bytes && !get_next_path)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Splitting the data by size is requested without a way to get the name of the next object");

    initialize();
}

void StorageObjectStorageSink::initialize()
{
    const auto & settings = context->getSettingsRef();
    const auto chosen_compression_method = chooseCompressionMethod(path, compression_method);

    auto buffer = object_storage->writeObject(
        StoredObject(path), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

    /// The pointer is taken before the move, but it is assigned to the field only afterwards,
    /// otherwise the lifetime analysis considers the field to be dangling.
    auto * buffer_ptr = buffer.get();
    write_buf = wrapWriteBufferWithCompressionMethod(
        std::move(buffer),
        chosen_compression_method,
        static_cast<int>(settings[Setting::output_format_compression_level]),
        static_cast<int>(settings[Setting::output_format_compression_zstd_window_log]),
        settings[Setting::snappy_mode]);
    destination_buf = buffer_ptr;

    /// With the parallel formatting, the data is written into the buffer by a background thread,
    /// and the amount of the written data cannot be checked after every block without a data race.
    writer = split_on_write_by_size_bytes
        ? FormatFactory::instance().getOutputFormat(format, *write_buf, *sample_block, context, format_settings)
        : FormatFactory::instance().getOutputFormatParallelIfPossible(format, *write_buf, *sample_block, context, format_settings);
}

void StorageObjectStorageSink::consume(Chunk & chunk)
{
    if (isCancelled())
        return;

    /// The previous object is already finished. Start the next one only now, when there is data for it.
    if (split_on_write_by_size_bytes && !writer)
    {
        path = get_next_path();
        initialize();
    }

    writer->write(getHeader().cloneWithColumns(chunk.getColumns()));

    /// Continue writing into a new object as soon as the current one became large enough.
    /// The current block is always written in full, so the object can be larger than the requested size.
    if (split_on_write_by_size_bytes && destination_buf->count() >= split_on_write_by_size_bytes)
    {
        finalizeBuffers();
        releaseBuffers();
    }
}

void StorageObjectStorageSink::onFinish()
{
    if (isCancelled())
        return;

    finalizeBuffers();
    releaseBuffers();
}

void StorageObjectStorageSink::finalizeBuffers()
{
    if (!writer)
        return;

    try
    {
        writer->flush();
        writer->finalize();
    }
    catch (...)
    {
        /// Stop ParallelFormattingOutputFormat correctly.
        cancelBuffers();
        releaseBuffers();
        throw;
    }

    write_buf->finalize();
    result_file_size = write_buf->count();
}

void StorageObjectStorageSink::releaseBuffers()
{
    writer.reset();
    write_buf.reset();
    destination_buf = nullptr;
}

void StorageObjectStorageSink::cancelBuffers()
{
    if (writer)
        writer->cancel();
    if (write_buf)
        write_buf->cancel();
}

size_t StorageObjectStorageSink::getFileSize() const
{
    if (!result_file_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Sink must be finalized before requesting result file size");
    return *result_file_size;
}

PartitionedStorageObjectStorageSink::PartitionedStorageObjectStorageSink(
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    std::optional<FormatSettings> format_settings_,
    SharedHeader sample_block_,
    ContextPtr context_)
    : PartitionedSink(configuration_->partition_strategy, context_, sample_block_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , query_settings(configuration_->getQuerySettings(context_))
    , format_settings(format_settings_)
    , sample_block(sample_block_)
    , context(context_)
{
}

StorageObjectStorageSink::~StorageObjectStorageSink()
{
    if (isCancelled())
        cancelBuffers();
}

SinkPtr PartitionedStorageObjectStorageSink::createSinkForPartition(const String & partition_id)
{
    auto file_path = configuration->getPathForWrite(partition_id).path;

    validateNamespace(configuration->getNamespace(), configuration);
    validateKey(file_path);

    /// The objects written after the first one are named after the key of the partition: `data.1.tsv`, `data.2.tsv`, ...
    const String key_for_splitting = file_path;

    if (auto new_key = checkAndGetNewFileOnInsertIfNeeded(
            *object_storage, *configuration, query_settings, file_path, getStartSequenceNumber(file_path, 1)))
    {
        file_path = *new_key;
    }

    last_written_object_path = file_path;

    StorageObjectStorageSink::GetNextPathCallback get_next_path;
    if (query_settings.split_on_write_by_size_bytes)
    {
        if (query_settings.truncate_on_insert)
            removeStaleSplitObjects(
                *object_storage, key_for_splitting, getStartSequenceNumber(key_for_splitting, 1));

        get_next_path = [storage = object_storage, config = configuration, settings = query_settings,
                         key = key_for_splitting,
                         sequence_number = getStartSequenceNumber(key_for_splitting, 1)]() mutable -> String
        {
            return getNextKeyForSplittingBySize(*storage, *config, settings, key, sequence_number);
        };
    }

    return std::make_shared<StorageObjectStorageSink>(
        file_path,
        object_storage,
        format_settings,
        std::make_shared<Block>(partition_strategy->getFormatHeader()),
        context,
        configuration->format,
        configuration->compression_method,
        query_settings.split_on_write_by_size_bytes,
        std::move(get_next_path));
}

}
