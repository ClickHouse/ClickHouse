#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <IO/EmptyReadBuffer.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/WithFileSize.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Storages/IStorage.h>
#include <Common/assert_cast.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 input_format_max_rows_to_read_for_schema_inference;
    extern const SettingsUInt64 input_format_max_bytes_to_read_for_schema_inference;
    extern const SettingsSchemaInferenceMode schema_inference_mode;
}

namespace ErrorCodes
{
    extern const int EMPTY_DATA_PASSED;
    extern const int BAD_ARGUMENTS;
    extern const int ONLY_NULLS_WHILE_READING_SCHEMA;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
    extern const int CANNOT_DETECT_FORMAT;
    extern const int TYPE_MISMATCH;
    extern const int LOGICAL_ERROR;
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

/// Order of formats to try in automatic format detection.
/// If we can successfully detect some format, we won't try next ones.
static const std::vector<String> & getFormatsOrderForDetection()
{
    static const std::vector<String> formats_order =
    {
        "Parquet",
        "ORC",
        "Arrow",
        "ArrowStream",
        "Avro",
        "AvroConfluent",
        "Npy",
        "Native",
        "BSONEachRow",
        "JSONCompact",
        "Values",
        "TSKV",
        "JSONObjectEachRow",
        "JSONColumns",
        "JSONCompactColumns",
        "JSONCompact",
        "JSON",
    };

    return formats_order;
}

/// The set of similar formats to try in automatic format detection.
/// We will try all formats from this set and then choose the best one
/// according to inferred schema.
static const std::vector<String> & getSimilarFormatsSetForDetection()
{
    static const std::vector<String> formats_order =
    {
        "TSV",
        "CSV",
    };

    return formats_order;
}

std::pair<ColumnsDescription, String> readSchemaFromFormatImpl(
    std::optional<String> format_name,
    const std::optional<FormatSettings> & format_settings,
    IReadBufferIterator & read_buffer_iterator,
    const ContextPtr & context)
try
{
    NamesAndTypesList names_and_types;
    SchemaInferenceMode mode = context->getSettingsRef()[Setting::schema_inference_mode];
    if (format_name && mode == SchemaInferenceMode::UNION && !FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(*format_name, context, format_settings))
    {
        String additional_message;
        /// Better exception message for WithNames(AndTypes) formats.
        if (format_name->ends_with("WithNames") || format_name->ends_with("WithNamesAndTypes"))
            additional_message = " (formats -WithNames(AndTypes) support reading subset of columns only when setting input_format_with_names_use_header is enabled)";

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "UNION schema inference mode is not supported for format {}, because it doesn't support reading subset of columns{}", *format_name, additional_message);
    }

    if (format_name && FormatFactory::instance().checkIfFormatHasExternalSchemaReader(*format_name))
    {
        auto external_schema_reader = FormatFactory::instance().getExternalSchemaReader(*format_name, context, format_settings);
        try
        {
            return {ColumnsDescription(external_schema_reader->readSchema()), *format_name};
        }
        catch (Exception & e)
        {
            e.addMessage(
                fmt::format("The table structure cannot be extracted from a {} format file. You can specify the structure manually", *format_name));
            throw;
        }
    }

    if (!format_name || FormatFactory::instance().checkIfFormatHasSchemaReader(*format_name))
    {
        IReadBufferIterator::Data iterator_data;
        std::vector<std::pair<NamesAndTypesList, String>> schemas_for_union_mode;
        std::string exception_messages;
        size_t max_rows_to_read = format_settings ? format_settings->max_rows_to_read_for_schema_inference
                                                  : context->getSettingsRef()[Setting::input_format_max_rows_to_read_for_schema_inference];
        size_t max_bytes_to_read = format_settings ? format_settings->max_bytes_to_read_for_schema_inference
                                                   : context->getSettingsRef()[Setting::input_format_max_bytes_to_read_for_schema_inference];
        size_t iterations = 0;
        while (true)
        {
            /// When we finish working with current buffer we should put it back to iterator.
            SCOPE_EXIT(if (iterator_data.buf) read_buffer_iterator.setPreviousReadBuffer(std::move(iterator_data.buf)));
            bool is_eof = false;
            try
            {
                iterator_data = read_buffer_iterator.next();

                /// Read buffer iterator can determine the data format if it's unknown.
                /// For example by scanning schema cache or by finding new file with format extension.
                if (!format_name && iterator_data.format_name)
                {
                    format_name = *iterator_data.format_name;
                    read_buffer_iterator.setFormatName(*iterator_data.format_name);
                }

                if (iterator_data.cached_columns)
                {
                    /// If we have schema in cache, we must also know the format.
                    if (!format_name)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Schema from cache was returned, but format name is unknown");

                    if (mode == SchemaInferenceMode::DEFAULT)
                    {
                        read_buffer_iterator.setResultingSchema(*iterator_data.cached_columns);
                        return {*iterator_data.cached_columns, *format_name};
                    }

                    schemas_for_union_mode.emplace_back(iterator_data.cached_columns->getAll(), read_buffer_iterator.getLastFilePath());
                    continue;
                }

                if (!iterator_data.buf)
                    break;

                /// We just want to check for eof, but eof() can be pretty expensive.
                /// So we use getFileSize() when available, which has better worst case.
                /// (For remote files, typically eof() would read 1 MB from S3, which may be much
                ///  more than what the schema reader and even data reader will read).
                auto size = tryGetFileSizeFromReadBuffer(*iterator_data.buf);
                if (size.has_value())
                    is_eof = *size == 0;
                else
                    is_eof = iterator_data.buf->eof();
            }
            catch (Exception & e)
            {
                if (format_name)
                    e.addMessage(fmt::format("The table structure cannot be extracted from a {} format file. You can specify the structure manually", *format_name));
                else
                    e.addMessage("The data format cannot be detected by the contents of the files. You can specify the format manually");
                throw;
            }
            catch (...)
            {
                auto exception_message = getCurrentExceptionMessage(false);
                if (format_name)
                    throw Exception(
                        ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                        "The table structure cannot be extracted from a {} format file:\n{}.\nYou can specify the structure manually",
                        *format_name,
                        exception_message);

                throw Exception(
                    ErrorCodes::CANNOT_DETECT_FORMAT,
                    "The data format cannot be detected by the contents of the files:\n{}.\nYou can specify the format manually",
                    exception_message);
            }

            ++iterations;

            if (is_eof)
            {
                String exception_message;
                if (format_name)
                    exception_message = fmt::format("The table structure cannot be extracted from a {} format file: the file is empty", *format_name);
                else
                    exception_message = fmt::format("The data format cannot be detected by the contents of the files: the file is empty");

                if (mode == SchemaInferenceMode::UNION)
                {
                    if (!format_name)
                        throw Exception(ErrorCodes::CANNOT_DETECT_FORMAT, "The data format cannot be detected by the contents of the files: the file is empty. You can specify the format manually");

                    throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "{}. You can specify the structure manually", exception_message);
                }

                if (!exception_messages.empty())
                    exception_messages += "\n";
                exception_messages += exception_message;
                continue;
            }

            std::unique_ptr<PeekableReadBuffer> peekable_buf; /// Can be used in format detection. Should be destroyed after schema reader.

            if (format_name)
            {
                SchemaReaderPtr schema_reader;

                try
                {
                    schema_reader = FormatFactory::instance().getSchemaReader(*format_name, *iterator_data.buf, context, format_settings);
                    schema_reader->setMaxRowsAndBytesToRead(max_rows_to_read, max_bytes_to_read);
                    names_and_types = schema_reader->readSchema();
                    auto num_rows = schema_reader->readNumberOrRows();
                    if (num_rows)
                        read_buffer_iterator.setNumRowsToLastFile(*num_rows);

                    /// In default mode, we finish when schema is inferred successfully from any file.
                    if (mode == SchemaInferenceMode::DEFAULT)
                        break;

                    if (!names_and_types.empty())
                        read_buffer_iterator.setSchemaToLastFile(ColumnsDescription(names_and_types));
                    schemas_for_union_mode.emplace_back(names_and_types, read_buffer_iterator.getLastFilePath());
                }
                catch (...)
                {
                    auto exception_message = getCurrentExceptionMessage(false);
                    if (schema_reader && mode == SchemaInferenceMode::DEFAULT)
                    {
                        size_t rows_read = schema_reader->getNumRowsRead();
                        assert(rows_read <= max_rows_to_read);
                        max_rows_to_read -= schema_reader->getNumRowsRead();
                        size_t bytes_read = iterator_data.buf->count();
                        /// We could exceed max_bytes_to_read a bit to complete row parsing.
                        max_bytes_to_read -= std::min(bytes_read, max_bytes_to_read);
                        if (rows_read != 0 && (max_rows_to_read == 0 || max_bytes_to_read == 0))
                        {
                            exception_message
                                += "\nTo increase the maximum number of rows/bytes to read for structure determination, use setting "
                                   "input_format_max_rows_to_read_for_schema_inference/input_format_max_bytes_to_read_for_schema_inference";
                            if (!exception_messages.empty())
                                exception_messages += "\n";
                            exception_messages += exception_message;
                            break;
                        }
                    }

                    if (mode == SchemaInferenceMode::UNION || !isRetryableSchemaInferenceError(getCurrentExceptionCode()))
                    {
                        throw Exception(
                            ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                            "The table structure cannot be extracted from a {} format file. "
                            "Error:\n{}.\nYou can specify the structure manually",
                            *format_name,
                            exception_message);
                    }

                    if (!exception_messages.empty())
                        exception_messages += "\n";
                    exception_messages += exception_message;
                }
            }
            else
            {
                /// If the format is unknown we try some formats in order and try to apply their schema readers.
                /// If we can successfully infer the schema in some format, most likely we can use this format to read this data.

                /// If read_buffer_iterator supports recreation of last buffer, we will recreate it for
                /// each format. Otherwise we will use PeekableReadBuffer and will rollback to the
                /// beginning of the file before each format. Using PeekableReadBuffer can lead
                /// to high memory usage as it will save all the read data from the beginning of the file,
                /// especially it will be noticeable for formats like Parquet/ORC/Arrow that do seeks to the
                /// end of file.
                bool support_buf_recreation = read_buffer_iterator.supportsLastReadBufferRecreation();
                if (!support_buf_recreation)
                {
                    peekable_buf = std::make_unique<PeekableReadBuffer>(*iterator_data.buf);
                    peekable_buf->setCheckpoint();
                }

                /// First, try some formats in order. If we successfully inferred the schema for any format,
                /// we will use this format.
                for (const auto & format_to_detect : getFormatsOrderForDetection())
                {
                    try
                    {
                        SchemaReaderPtr schema_reader = FormatFactory::instance().getSchemaReader(format_to_detect, support_buf_recreation ? *iterator_data.buf : *peekable_buf, context, format_settings);
                        schema_reader->setMaxRowsAndBytesToRead(max_rows_to_read, max_bytes_to_read);
                        names_and_types = schema_reader->readSchema();
                        if (names_and_types.empty())
                            continue;

                        /// We successfully inferred schema from this file using current format.
                        format_name = format_to_detect;
                        read_buffer_iterator.setFormatName(format_to_detect);

                        auto num_rows = schema_reader->readNumberOrRows();
                        if (num_rows)
                            read_buffer_iterator.setNumRowsToLastFile(*num_rows);

                        break;
                    }
                    catch (...)
                    {
                        /// We failed to infer the schema for this format.
                        /// Recreate read buffer or rollback to the beginning of the data
                        /// before trying next format.
                        if (support_buf_recreation)
                        {
                            read_buffer_iterator.setPreviousReadBuffer(std::move(iterator_data.buf));
                            iterator_data.buf = read_buffer_iterator.recreateLastReadBuffer();
                        }
                        else
                        {
                            peekable_buf->rollbackToCheckpoint();
                        }
                    }
                }

                /// If no format was detected from first set of formats, we try second set.
                /// In this set formats are similar and it can happen that data matches some of them.
                /// We try to infer schema for all of the formats from this set and then choose the best
                /// one according to the inferred schema.
                if (!format_name)
                {
                    std::unordered_map<String, NamesAndTypesList> format_to_schema;
                    const auto & formats_set_to_detect = getSimilarFormatsSetForDetection();
                    for (size_t i = 0; i != formats_set_to_detect.size(); ++i)
                    {
                        try
                        {
                            SchemaReaderPtr schema_reader = FormatFactory::instance().getSchemaReader(
                                formats_set_to_detect[i], support_buf_recreation ? *iterator_data.buf : *peekable_buf, context, format_settings);
                            schema_reader->setMaxRowsAndBytesToRead(max_rows_to_read, max_bytes_to_read);
                            auto tmp_names_and_types = schema_reader->readSchema();
                            /// If schema was inferred successfully for this format, remember it and try next format.
                            if (!tmp_names_and_types.empty())
                                format_to_schema[formats_set_to_detect[i]] = tmp_names_and_types;
                        }
                        catch (...) // NOLINT(bugprone-empty-catch)
                        {
                            /// Try next format.
                        }

                        if (i != formats_set_to_detect.size() - 1)
                        {
                            if (support_buf_recreation)
                            {
                                read_buffer_iterator.setPreviousReadBuffer(std::move(iterator_data.buf));
                                iterator_data.buf = read_buffer_iterator.recreateLastReadBuffer();
                            }
                            else
                            {
                                peekable_buf->rollbackToCheckpoint();
                            }
                        }
                    }

                    /// We choose the format with larger number of columns in inferred schema.
                    size_t max_number_of_columns = 0;
                    for (const auto & [format_to_detect, schema] : format_to_schema)
                    {
                        if (schema.size() > max_number_of_columns)
                        {
                            names_and_types = schema;
                            format_name = format_to_detect;
                            max_number_of_columns = schema.size();
                        }
                    }

                    if (format_name)
                        read_buffer_iterator.setFormatName(*format_name);
                }

                if (mode == SchemaInferenceMode::UNION)
                {
                    /// For UNION mode we need to know the schema of each file,
                    /// if we failed to detect the format, we failed to detect the schema of this file
                    /// in any format. It doesn't make sense to continue.
                    if (!format_name)
                        throw Exception(ErrorCodes::CANNOT_DETECT_FORMAT, "The data format cannot be detected by the contents of the files. You can specify the format manually");

                    read_buffer_iterator.setSchemaToLastFile(ColumnsDescription(names_and_types));
                    schemas_for_union_mode.emplace_back(names_and_types, read_buffer_iterator.getLastFilePath());
                }

                if (format_name && mode == SchemaInferenceMode::DEFAULT)
                    break;
            }
        }

        if (!format_name)
            throw Exception(ErrorCodes::CANNOT_DETECT_FORMAT, "The data format cannot be detected by the contents of the files. You can specify the format manually");

        /// We need some stateless methods of ISchemaReader, but during reading schema we
        /// could not even create a schema reader (for example when we got schema from cache).
        /// Let's create stateless schema reader from empty read buffer.
        EmptyReadBuffer empty;
        SchemaReaderPtr stateless_schema_reader = FormatFactory::instance().getSchemaReader(*format_name, empty, context, format_settings);

        if (mode == SchemaInferenceMode::UNION)
        {
            Names names_order; /// Try to save original columns order;
            std::unordered_map<String, DataTypePtr> names_to_types;


            for (const auto & [schema, file_name] : schemas_for_union_mode)
            {
                for (const auto & [name, type] : schema)
                {
                    auto it = names_to_types.find(name);
                    if (it == names_to_types.end())
                    {
                        names_order.push_back(name);
                        names_to_types[name] = type;
                    }
                    else
                    {
                        /// We already have column with such name.
                        /// Check if types are the same.
                        if (!type->equals(*it->second))
                        {
                            /// If types are not the same, try to transform them according
                            /// to the format to find common type.
                            auto new_type_copy = type;
                            stateless_schema_reader->transformTypesFromDifferentFilesIfNeeded(it->second, new_type_copy);

                            /// If types are not the same after transform, we cannot do anything, throw an exception.
                            if (!it->second->equals(*new_type_copy))
                                throw Exception(
                                    ErrorCodes::TYPE_MISMATCH,
                                    "Automatically inferred type {} for column '{}'{} differs from type inferred from previous files: {}",
                                    type->getName(),
                                    name,
                                    file_name.empty() ? "" : " in file " + file_name,
                                    it->second->getName());
                        }
                    }
                }
            }

            names_and_types.clear();
            for (const auto & name : names_order)
                names_and_types.emplace_back(name, names_to_types[name]);
        }

        if (names_and_types.empty())
        {
            if (iterations <= 1)
            {
                throw Exception(
                    ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                    "The table structure cannot be extracted from a {} format file. "
                    "Error:\n{}.\nYou can specify the structure manually",
                    *format_name,
                    exception_messages);
            }

            throw Exception(
                ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                "All attempts to extract table structure from files failed. "
                "Errors:\n{}\nYou can specify the structure manually",
                exception_messages);
        }

        /// If we have "INSERT SELECT" query then try to order
        /// columns as they are ordered in table schema for formats
        /// without strict column order (like JSON and TSKV).
        /// It will allow to execute simple data loading with query
        /// "INSERT INTO table SELECT * FROM ..."
        const auto & insertion_table = context->getInsertionTable();
        if (!stateless_schema_reader->hasStrictOrderOfColumns() && !insertion_table.empty())
        {
            auto storage = DatabaseCatalog::instance().getTable(insertion_table, context);
            auto metadata = storage->getInMemoryMetadataPtr();
            auto names_in_storage = metadata->getColumns().getNamesOfPhysical();
            auto ordered_list = getOrderedColumnsList(names_and_types, names_in_storage);
            if (ordered_list)
                names_and_types = *ordered_list;
        }

        /// Some formats like CSVWithNames can contain empty column names. We don't support empty column names and further processing can fail with an exception. Let's just remove columns with empty names from the structure.
        names_and_types.erase(
            std::remove_if(names_and_types.begin(), names_and_types.end(), [](const NameAndTypePair & pair) { return pair.name.empty(); }),
            names_and_types.end());

        auto columns = ColumnsDescription(names_and_types);
        if (mode == SchemaInferenceMode::DEFAULT)
            read_buffer_iterator.setResultingSchema(columns);
        return {columns, *format_name};
    }

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "{} file format doesn't support schema inference. You must specify the structure manually",
        *format_name);
}
catch (Exception & e)
{
    auto file_path = read_buffer_iterator.getLastFilePath();
    if (!file_path.empty())
        e.addMessage(fmt::format("(in file/uri {})", file_path));
    throw;
}

ColumnsDescription readSchemaFromFormat(
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    IReadBufferIterator & read_buffer_iterator,
    const ContextPtr & context)
{
    return readSchemaFromFormatImpl(format_name, format_settings, read_buffer_iterator, context).first;
}

std::pair<ColumnsDescription, String> detectFormatAndReadSchema(
    const std::optional<FormatSettings> & format_settings,
    IReadBufferIterator & read_buffer_iterator,
    const ContextPtr & context)
{
    return readSchemaFromFormatImpl(std::nullopt, format_settings, read_buffer_iterator, context);
}

SchemaCache::Key getKeyForSchemaCache(
    const String & source, const String & format, const std::optional<FormatSettings> & format_settings, const ContextPtr & context)
{
    return getKeysForSchemaCache({source}, format, format_settings, context).front();
}

static SchemaCache::Key makeSchemaCacheKey(const String & source, const String & format, const String & additional_format_info, const String & schema_inference_mode)
{
    return SchemaCache::Key{source, format, additional_format_info, schema_inference_mode};
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
    String schema_inference_mode(magic_enum::enum_name(context->getSettingsRef()[Setting::schema_inference_mode].value));
    SchemaCache::Keys cache_keys;
    cache_keys.reserve(sources.size());
    std::transform(
        sources.begin(),
        sources.end(),
        std::back_inserter(cache_keys),
        [&](const auto & source) { return makeSchemaCacheKey(source, format, additional_format_info, schema_inference_mode); });
    return cache_keys;
}

}
