#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Formats/FormatFactory.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/isValidUTF8.h>
#include <Core/Settings.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 output_format_compression_level;
    extern const SettingsUInt64 output_format_compression_zstd_window_log;
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
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    const std::optional<FormatSettings> & format_settings_,
    SharedHeader sample_block_,
    ContextPtr context)
    : StorageObjectStorageSink(path_, object_storage, configuration, format_settings_, sample_block_, sample_block_, context)
{
}

StorageObjectStorageSink::StorageObjectStorageSink(
    const std::string & path_,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    const std::optional<FormatSettings> & format_settings_,
    SharedHeader input_header_,
    SharedHeader format_header_,
    ContextPtr context)
    : SinkToStorage(format_header_)
    , path(path_)
    , sample_block(format_header_)
    , input_header(input_header_)
{
    const auto & settings = context->getSettingsRef();
    const auto chosen_compression_method = chooseCompressionMethod(path, configuration->compression_method);

    auto buffer = object_storage->writeObject(
        StoredObject(path), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

    write_buf = wrapWriteBufferWithCompressionMethod(
        std::move(buffer),
        chosen_compression_method,
        static_cast<int>(settings[Setting::output_format_compression_level]),
        static_cast<int>(settings[Setting::output_format_compression_zstd_window_log]));

    writer = FormatFactory::instance().getOutputFormatParallelIfPossible(
        configuration->format, *write_buf, *sample_block, context, format_settings_);

    /// build format header from sample_block (input header) by finding it's position in the sample block.
    input_to_format_pos.clear();
    input_to_format_pos.reserve(sample_block->columns());
    for (size_t i = 0; i < sample_block->columns(); ++i)
    {
        const auto & format_col = sample_block->getByPosition(i);
        if (!input_header->has(format_col.name))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Input header does not contain column '{}' required by the writer header", format_col.name);
        input_to_format_pos.push_back(input_header->getPositionByName(format_col.name));
    }
}

void StorageObjectStorageSink::consume(Chunk & chunk)
{
    if (isCancelled())
        return;

    /// If chunk has already been shaped upstream exactly like the writer header, write it as such.
    /// For e.g., HIVE style partitioned table with partition columns dropped upstream.
    const auto & src_cols = chunk.getColumns();
    if (src_cols.size() == sample_block->columns())
    {
        writer->write(getHeader().cloneWithColumns(src_cols));
        return;
    }

    /// Else, project from input_header shape to writer_header shape using the precomputed map.
    Columns selected;
    selected.reserve(input_to_format_pos.size());
    for (size_t pos : input_to_format_pos)
    {
        if (pos >= src_cols.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Incoming chunk has {} column(s) but position {} is required by writer header",
                src_cols.size(),
                pos);
        selected.emplace_back(src_cols[pos]);
    }
    writer->write(getHeader().cloneWithColumns(selected));
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

    if (auto new_key = checkAndGetNewFileOnInsertIfNeeded(
            *object_storage, *configuration, query_settings, file_path, /* sequence_number */1))
    {
        file_path = *new_key;
    }

    /// Build the final writer header in the following way:
    /// - Take column names/order from the partition strategy (this may drop partition columns for HIVE).
    /// - Take column types from sample_block (the current pipeline schema) to reflect any ALTERs.
    const Block format_header = partition_strategy->getFormatHeader();
    auto final_format_header = std::make_shared<Block>();
    for (size_t i = 0; i < format_header.columns(); ++i)
    {
        const auto & name = format_header.getByPosition(i).name;
        if (!sample_block->has(name))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column '{}' in format header is not present in sample_block", name);
        const size_t pos_in = sample_block->getPositionByName(name);
        final_format_header->insert(sample_block->getByPosition(pos_in));
    }

    return std::make_shared<StorageObjectStorageSink>(
        file_path,
        object_storage,
        configuration,
        format_settings,
        /* input_header */ sample_block,
        /* format_header */ final_format_header,
        context);
}

}
