#include <Storages/ObjectStorage/DataLakes/Iceberg/MultipleFileWriter.h>

#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Interpreters/Context.h>


namespace DB
{

#if USE_AVRO

MultipleFileWriter::MultipleFileWriter(
    UInt64 max_data_file_num_rows_,
    UInt64 max_data_file_num_bytes_,
    Poco::JSON::Array::Ptr schema,
    FileNamesGenerator & filename_generator_,
    ObjectStoragePtr object_storage_,
    ContextPtr context_,
    const std::optional<FormatSettings> & format_settings_,
    const String & write_format_,
    SharedHeader sample_block_)
    : max_data_file_num_rows(max_data_file_num_rows_)
    , max_data_file_num_bytes(max_data_file_num_bytes_)
    , stats(schema)
    , filename_generator(filename_generator_)
    , object_storage(object_storage_)
    , context(context_)
    , format_settings(format_settings_)
    , write_format(std::move(write_format_))
    , sample_block(sample_block_)
{
}

void MultipleFileWriter::startNewFile()
{
    if (buffer)
        finalize();

    current_file_num_rows = 0;
    current_file_num_bytes = 0;
    auto filename = filename_generator.generateDataFileName();

    data_file_names.push_back(filename.path_in_storage);
    buffer = object_storage->writeObject(
        StoredObject(filename.path_in_storage), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

    if (format_settings)
    {
        format_settings->parquet.write_page_index = true;
        format_settings->parquet.bloom_filter_push_down = true;
        format_settings->parquet.filter_push_down = true;
    }
    output_format = FormatFactory::instance().getOutputFormatParallelIfPossible(
        write_format, *buffer, *sample_block, context, format_settings);
}

void MultipleFileWriter::consume(const Chunk & chunk)
{
    if (!current_file_num_rows || *current_file_num_rows >= max_data_file_num_rows || *current_file_num_bytes >= max_data_file_num_bytes)
    {
        startNewFile();
    }
    output_format->write(sample_block->cloneWithColumns(chunk.getColumns()));
    output_format->flush();
    *current_file_num_rows += chunk.getNumRows();
    *current_file_num_bytes += chunk.bytes();
    stats.update(chunk);
}

void MultipleFileWriter::finalize()
{
    output_format->flush();
    output_format->finalize();
    buffer->finalize();
    total_bytes += buffer->count();
}

void MultipleFileWriter::release()
{
    output_format.reset();
    buffer.reset();
}

void MultipleFileWriter::cancel()
{
    if (output_format)
        output_format->cancel();
    if (buffer)
        buffer->cancel();
}

void MultipleFileWriter::clearAllDataFiles() const
{
    for (const auto & data_filename : data_file_names)
        object_storage->removeObjectIfExists(StoredObject(data_filename));
}

UInt64 MultipleFileWriter::getResultBytes() const
{
    return total_bytes;
}

#endif

}
