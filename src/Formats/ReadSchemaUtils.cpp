#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Formats/ReadSchemaUtils.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Common/assert_cast.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int EMPTY_DATA_PASSED;
    extern const int BAD_ARGUMENTS;
    extern const int ONLY_NULLS_WHILE_READING_SCHEMA;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}

static std::optional<NamesAndTypesList> getOrderedColumnsList(
    const NamesAndTypesList & columns_list, const Names & columns_order_hint)
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
    ReadBufferIterator & read_buffer_iterator,
    bool retry,
    ContextPtr & context,
    std::unique_ptr<ReadBuffer> & buf)
{
    NamesAndTypesList names_and_types;
    if (FormatFactory::instance().checkIfFormatHasExternalSchemaReader(format_name))
    {
        auto external_schema_reader = FormatFactory::instance().getExternalSchemaReader(format_name, context, format_settings);
        try
        {
            names_and_types = external_schema_reader->readSchema();
        }
        catch (const DB::Exception & e)
        {
            throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "Cannot extract table structure from {} format file. Error: {}. You can specify the structure manually", format_name, e.message());
        }
    }
    else if (FormatFactory::instance().checkIfFormatHasSchemaReader(format_name))
    {
        std::string exception_messages;
        SchemaReaderPtr schema_reader;
        size_t max_rows_to_read = format_settings ? format_settings->max_rows_to_read_for_schema_inference : context->getSettingsRef().input_format_max_rows_to_read_for_schema_inference;
        size_t iterations = 0;
        ColumnsDescription cached_columns;
        while (true)
        {
            bool is_eof = false;
            try
            {
                buf = read_buffer_iterator(cached_columns);
                if (!buf)
                    break;
                is_eof = buf->eof();
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
                    throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "{}. You can specify the structure manually", exception_message);

                exception_messages += "\n" + exception_message;
                continue;
            }

            try
            {
                schema_reader = FormatFactory::instance().getSchemaReader(format_name, *buf, context, format_settings);
                schema_reader->setMaxRowsToRead(max_rows_to_read);
                names_and_types = schema_reader->readSchema();
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
                    if (rows_read != 0 && max_rows_to_read == 0)
                    {
                        exception_message += "\nTo increase the maximum number of rows to read for structure determination, use setting input_format_max_rows_to_read_for_schema_inference";
                        if (iterations > 1)
                        {
                            exception_messages += "\n" + exception_message;
                            break;
                        }
                        retry = false;
                    }
                }

                if (!retry || !isRetryableSchemaInferenceError(getCurrentExceptionCode()))
                    throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "Cannot extract table structure from {} format file. Error: {}. You can specify the structure manually", format_name, exception_message);

                exception_messages += "\n" + exception_message;
            }
        }

        if (!cached_columns.empty())
            return cached_columns;

        if (names_and_types.empty())
            throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "All attempts to extract table structure from files failed. Errors:{}\nYou can specify the structure manually", exception_messages);

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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} file format doesn't support schema inference. You must specify the structure manually", format_name);

    return ColumnsDescription(names_and_types);
}

ColumnsDescription readSchemaFromFormat(const String & format_name, const std::optional<FormatSettings> & format_settings, ReadBufferIterator & read_buffer_iterator, bool retry, ContextPtr & context)
{
    std::unique_ptr<ReadBuffer> buf_out;
    return readSchemaFromFormat(format_name, format_settings, read_buffer_iterator, retry, context, buf_out);
}

DataTypePtr makeNullableRecursivelyAndCheckForNothing(DataTypePtr type)
{
    if (!type)
        return nullptr;

    WhichDataType which(type);

    if (which.isNothing())
        return nullptr;

    if (which.isNullable())
    {
        const auto * nullable_type = assert_cast<const DataTypeNullable *>(type.get());
        return makeNullableRecursivelyAndCheckForNothing(nullable_type->getNestedType());
    }

    if (which.isArray())
    {
        const auto * array_type = assert_cast<const DataTypeArray *>(type.get());
        auto nested_type = makeNullableRecursivelyAndCheckForNothing(array_type->getNestedType());
        return nested_type ? std::make_shared<DataTypeArray>(nested_type) : nullptr;
    }

    if (which.isTuple())
    {
        const auto * tuple_type = assert_cast<const DataTypeTuple *>(type.get());
        DataTypes nested_types;
        for (const auto & element : tuple_type->getElements())
        {
            auto nested_type = makeNullableRecursivelyAndCheckForNothing(element);
            if (!nested_type)
                return nullptr;
            nested_types.push_back(nested_type);
        }
        return std::make_shared<DataTypeTuple>(std::move(nested_types));
    }

    if (which.isMap())
    {
        const auto * map_type = assert_cast<const DataTypeMap *>(type.get());
        auto key_type = makeNullableRecursivelyAndCheckForNothing(map_type->getKeyType());
        auto value_type = makeNullableRecursivelyAndCheckForNothing(map_type->getValueType());
        return key_type && value_type ? std::make_shared<DataTypeMap>(removeNullable(key_type), value_type) : nullptr;
    }

    if (which.isLowCarnality())
    {
        const auto * lc_type = assert_cast<const DataTypeLowCardinality *>(type.get());
        auto nested_type = makeNullableRecursivelyAndCheckForNothing(lc_type->getDictionaryType());
        return nested_type ? std::make_shared<DataTypeLowCardinality>(nested_type) : nullptr;
    }

    return makeNullable(type);
}

NamesAndTypesList getNamesAndRecursivelyNullableTypes(const Block & header)
{
    NamesAndTypesList result;
    for (auto & [name, type] : header.getNamesAndTypesList())
        result.emplace_back(name, makeNullableRecursivelyAndCheckForNothing(type));
    return result;
}

String getKeyForSchemaCache(const String & source, const String & format, const std::optional<FormatSettings> & format_settings, const ContextPtr & context)
{
    return getKeysForSchemaCache({source}, format, format_settings, context).front();
}

static String makeSchemaCacheKey(const String & source, const String & format, const String & additional_format_info)
{
    return source + "@@" + format + "@@" + additional_format_info;
}

void splitSchemaCacheKey(const String & key, String & source, String & format, String & additional_format_info)
{
    size_t additional_format_info_pos = key.rfind("@@");
    additional_format_info = key.substr(additional_format_info_pos + 2, key.size() - additional_format_info_pos - 2);
    size_t format_pos = key.rfind("@@", additional_format_info_pos - 1);
    format = key.substr(format_pos + 2, additional_format_info_pos - format_pos - 2);
    source = key.substr(0, format_pos);
}

Strings getKeysForSchemaCache(const Strings & sources, const String & format, const std::optional<FormatSettings> & format_settings, const ContextPtr & context)
{
    /// For some formats data schema depends on some settings, so it's possible that
    /// two queries to the same source will get two different schemas. To process this
    /// case we add some additional information specific for the format to the cache key.
    /// For example, for Protobuf format additional information is the path to the schema
    /// and message name.
    String additional_format_info = FormatFactory::instance().getAdditionalInfoForSchemaCache(format, context, format_settings);
    Strings cache_keys;
    cache_keys.reserve(sources.size());
    std::transform(sources.begin(), sources.end(), std::back_inserter(cache_keys), [&](const auto & source){ return makeSchemaCacheKey(source, format, additional_format_info); });
    return cache_keys;
}

}
