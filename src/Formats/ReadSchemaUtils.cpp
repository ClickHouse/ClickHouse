#include <DataTypes/DataTypeMap.h>
#include <Formats/ReadSchemaUtils.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Storages/IStorage.h>
#include <Common/assert_cast.h>
#include <IO/WithFileName.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int EMPTY_DATA_PASSED;
    extern const int BAD_ARGUMENTS;
    extern const int ONLY_NULLS_WHILE_READING_SCHEMA;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}

static std::optional<NamesAndTypesList> getOrderedColumnsList(const NamesAndTypesList & columns_list, const Names & columns_order_hint)
{
    if (columns_list.size() != columns_order_hint.size())
        return {};

    std::unordered_map<String, DataTypePtr> available_columns;
    for (const auto & [name, type] : columns_list)
        available_columns.emplace(name, type);

    NamesAndTypesList res;
    for (const auto & name : columns_order_hint)
    {
        auto it = available_columns.find(name);
        if (it == available_columns.end())
            return {};

        res.emplace_back(name, it->second);
    }
    return res;
}

bool isRetryableSchemaInferenceError(int code)
{
    return code == ErrorCodes::EMPTY_DATA_PASSED || code == ErrorCodes::ONLY_NULLS_WHILE_READING_SCHEMA;
}

ColumnsDescription readSchemaFromFormat(
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    IReadBufferIterator & read_buffer_iterator,
    bool retry,
    ContextPtr & context,
    std::unique_ptr<ReadBuffer> & buf)
try
{
    NamesAndTypesList names_and_types;
    if (FormatFactory::instance().checkIfFormatHasExternalSchemaReader(format_name))
    {
        auto external_schema_reader = FormatFactory::instance().getExternalSchemaReader(format_name, context, format_settings);
        try
        {
            names_and_types = external_schema_reader->readSchema();
        }
        catch (Exception & e)
        {
            e.addMessage(
                fmt::format("Cannot extract table structure from {} format file. You can specify the structure manually", format_name));
            throw;
        }
    }
    else if (FormatFactory::instance().checkIfFormatHasSchemaReader(format_name))
    {
        std::string exception_messages;
        SchemaReaderPtr schema_reader;
        size_t max_rows_to_read = format_settings ? format_settings->max_rows_to_read_for_schema_inference
                                                  : context->getSettingsRef().input_format_max_rows_to_read_for_schema_inference;
        size_t max_bytes_to_read = format_settings ? format_settings->max_bytes_to_read_for_schema_inference
                                                                             : context->getSettingsRef().input_format_max_bytes_to_read_for_schema_inference;
        size_t iterations = 0;
        while (true)
        {
            bool is_eof = false;
            try
            {
                buf = read_buffer_iterator.next();
                if (!buf)
                    break;
                is_eof = buf->eof();
            }
            catch (Exception & e)
            {
                e.addMessage(
                    fmt::format("Cannot extract table structure from {} format file. You can specify the structure manually", format_name));
                throw;
            }
            catch (...)
            {
                auto exception_message = getCurrentExceptionMessage(false);
                throw Exception(
                    ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                    "Cannot extract table structure from {} format file:\n{}\nYou can specify the structure manually",
                    format_name,
                    exception_message);
            }

            ++iterations;

            if (is_eof)
            {
                auto exception_message = fmt::format("Cannot extract table structure from {} format file, file is empty", format_name);

                if (!retry)
                    throw Exception(
                        ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "{}. You can specify the structure manually", exception_message);

                exception_messages += "\n" + exception_message;
                continue;
            }

            try
            {
                schema_reader = FormatFactory::instance().getSchemaReader(format_name, *buf, context, format_settings);
                schema_reader->setMaxRowsAndBytesToRead(max_rows_to_read, max_bytes_to_read);
                names_and_types = schema_reader->readSchema();
                auto num_rows = schema_reader->readNumberOrRows();
                if (num_rows)
                    read_buffer_iterator.setNumRowsToLastFile(*num_rows);
                break;
            }
            catch (...)
            {
                auto exception_message = getCurrentExceptionMessage(false);
                if (schema_reader)
                {
                    size_t rows_read = schema_reader->getNumRowsRead();
                    assert(rows_read <= max_rows_to_read);
                    max_rows_to_read -= schema_reader->getNumRowsRead();
                    size_t bytes_read = buf->count();
                    /// We could exceed max_bytes_to_read a bit to complete row parsing.
                    max_bytes_to_read -= std::min(bytes_read, max_bytes_to_read);
                    if (rows_read != 0 && (max_rows_to_read == 0 || max_bytes_to_read == 0))
                    {
                        exception_message += "\nTo increase the maximum number of rows/bytes to read for structure determination, use setting "
                                             "input_format_max_rows_to_read_for_schema_inference/input_format_max_bytes_to_read_for_schema_inference";

                        if (iterations > 1)
                        {
                            exception_messages += "\n" + exception_message;
                            break;
                        }
                        retry = false;
                    }
                }

                if (!retry || !isRetryableSchemaInferenceError(getCurrentExceptionCode()))
                {
                    try
                    {
                        throw;
                    }
                    catch (Exception & e)
                    {
                        e.addMessage(fmt::format(
                            "Cannot extract table structure from {} format file. You can specify the structure manually", format_name));
                        throw;
                    }
                    catch (...)
                    {
                        throw Exception(
                            ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                            "Cannot extract table structure from {} format file. "
                            "Error: {}. You can specify the structure manually",
                            format_name,
                            exception_message);
                    }
                }

                exception_messages += "\n" + exception_message;
            }
        }

        if (auto cached_columns = read_buffer_iterator.getCachedColumns())
            return *cached_columns;

        if (names_and_types.empty())
            throw Exception(
                ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                "All attempts to extract table structure from files failed. "
                "Errors:{}\nYou can specify the structure manually",
                exception_messages);

        /// If we have "INSERT SELECT" query then try to order
        /// columns as they are ordered in table schema for formats
        /// without strict column order (like JSON and TSKV).
        /// It will allow to execute simple data loading with query
        /// "INSERT INTO table SELECT * FROM ..."
        const auto & insertion_table = context->getInsertionTable();
        if (!schema_reader->hasStrictOrderOfColumns() && !insertion_table.empty())
        {
            auto storage = DatabaseCatalog::instance().getTable(insertion_table, context);
            auto metadata = storage->getInMemoryMetadataPtr();
            auto names_in_storage = metadata->getColumns().getNamesOfPhysical();
            auto ordered_list = getOrderedColumnsList(names_and_types, names_in_storage);
            if (ordered_list)
                names_and_types = *ordered_list;
        }
    }
    else
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "{} file format doesn't support schema inference. You must specify the structure manually",
            format_name);

    /// Some formats like CSVWithNames can contain empty column names. We don't support empty column names and further processing can fail with an exception. Let's just remove columns with empty names from the structure.
    names_and_types.erase(
        std::remove_if(names_and_types.begin(), names_and_types.end(), [](const NameAndTypePair & pair) { return pair.name.empty(); }),
        names_and_types.end());
    return ColumnsDescription(names_and_types);
}
catch (Exception & e)
{
    if (!buf)
        throw;
    auto file_name = getFileNameFromReadBuffer(*buf);
    if (!file_name.empty())
        e.addMessage(fmt::format("(in file/uri {})", file_name));
    throw;
}


ColumnsDescription readSchemaFromFormat(
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    IReadBufferIterator & read_buffer_iterator,
    bool retry,
    ContextPtr & context)
{
    std::unique_ptr<ReadBuffer> buf_out;
    return readSchemaFromFormat(format_name, format_settings, read_buffer_iterator, retry, context, buf_out);
}

SchemaCache::Key getKeyForSchemaCache(
    const String & source, const String & format, const std::optional<FormatSettings> & format_settings, const ContextPtr & context)
{
    return getKeysForSchemaCache({source}, format, format_settings, context).front();
}

static SchemaCache::Key makeSchemaCacheKey(const String & source, const String & format, const String & additional_format_info)
{
    return SchemaCache::Key{source, format, additional_format_info};
}

SchemaCache::Keys getKeysForSchemaCache(
    const Strings & sources, const String & format, const std::optional<FormatSettings> & format_settings, const ContextPtr & context)
{
    /// For some formats data schema depends on some settings, so it's possible that
    /// two queries to the same source will get two different schemas. To process this
    /// case we add some additional information specific for the format to the cache key.
    /// For example, for Protobuf format additional information is the path to the schema
    /// and message name.
    String additional_format_info = FormatFactory::instance().getAdditionalInfoForSchemaCache(format, context, format_settings);
    SchemaCache::Keys cache_keys;
    cache_keys.reserve(sources.size());
    std::transform(
        sources.begin(),
        sources.end(),
        std::back_inserter(cache_keys),
        [&](const auto & source) { return makeSchemaCacheKey(source, format, additional_format_info); });
    return cache_keys;
}

}
