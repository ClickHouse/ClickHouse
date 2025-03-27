#include <Processors/Formats/Impl/JSONColumnsBlockInputFormatBase.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/JSONUtils.h>
#include <Formats/SchemaInferenceUtils.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <IO/ReadHelpers.h>
#include <base/find_symbols.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int EMPTY_DATA_PASSED;
    extern const int TYPE_MISMATCH;
}


JSONColumnsReaderBase::JSONColumnsReaderBase(ReadBuffer & in_) : in(&in_)
{
}

bool JSONColumnsReaderBase::checkColumnEnd()
{
    return JSONUtils::checkAndSkipArrayEnd(*in);
}

bool JSONColumnsReaderBase::checkColumnEndOrSkipFieldDelimiter()
{
    if (checkColumnEnd())
        return true;
    JSONUtils::skipComma(*in);
    return false;
}

bool JSONColumnsReaderBase::checkChunkEndOrSkipColumnDelimiter()
{
    if (checkChunkEnd())
        return true;
    JSONUtils::skipComma(*in);
    return false;
}

void JSONColumnsReaderBase::skipColumn()
{
    /// We assume that we already read '[', so we should skip until matched ']'.
    size_t balance = 1;
    bool inside_quotes = false;
    char * pos;
    while (!in->eof() && balance)
    {
        if (inside_quotes)
            pos = find_first_symbols<'\\', '"'>(in->position(), in->buffer().end());
        else
            pos = find_first_symbols<'[', ']', '"', '\\'>(in->position(), in->buffer().end());

        in->position() = pos;
        if (in->position() == in->buffer().end())
            continue;

        if (*in->position() == '\\')
        {
            ++in->position();
            if (!in->eof())
                ++in->position();
            continue;
        }

        if (*in->position() == '"')
            inside_quotes = !inside_quotes;
        else if (*in->position() == ']')
            --balance;
        else if (*in->position() == '[')
            ++balance;
        ++in->position();
    }
}

JSONColumnsBlockInputFormatBase::JSONColumnsBlockInputFormatBase(
    ReadBuffer & in_, const Block & header_, const FormatSettings & format_settings_, std::unique_ptr<JSONColumnsReaderBase> reader_)
    : IInputFormat(header_, &in_)
    , format_settings(format_settings_)
    , fields(header_.getNamesAndTypes())
    , serializations(header_.getSerializations())
    , reader(std::move(reader_))
    , block_missing_values(getPort().getHeader().columns())
{
    name_to_index = getPort().getHeader().getNamesToIndexesMap();
}

size_t JSONColumnsBlockInputFormatBase::readColumn(
    IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, const String & column_name)
{
    /// Check for empty column.
    if (reader->checkColumnEnd())
        return 0;

    do
    {
        JSONUtils::readField(*in, column, type, serialization, column_name, format_settings, false);
    }
    while (!reader->checkColumnEndOrSkipFieldDelimiter());

    return column.size();
}

void JSONColumnsBlockInputFormatBase::setReadBuffer(ReadBuffer & in_)
{
    reader->setReadBuffer(in_);
    IInputFormat::setReadBuffer(in_);
}

Chunk JSONColumnsBlockInputFormatBase::read()
{
    MutableColumns columns = getPort().getHeader().cloneEmptyColumns();
    block_missing_values.clear();

    skipWhitespaceIfAny(*in);
    if (in->eof())
        return {};

    reader->readChunkStart();
    /// Check for empty block.
    if (reader->checkChunkEnd())
        return Chunk(std::move(columns), 0);

    size_t chunk_start = getDataOffsetMaybeCompressed(*in);

    if (need_only_count)
    {
        /// Count rows in first column and skip the rest columns.
        reader->readColumnStart();
        size_t num_rows = 0;
        if (!reader->checkColumnEnd())
        {
            do
            {
                skipJSONField(*in, "skip_field", format_settings.json);
                ++num_rows;
            } while (!reader->checkColumnEndOrSkipFieldDelimiter());
        }

        while (!reader->checkChunkEndOrSkipColumnDelimiter())
        {
            reader->readColumnStart();
            reader->skipColumn();
        }

        approx_bytes_read_for_chunk = getDataOffsetMaybeCompressed(*in) - chunk_start;
        return getChunkForCount(num_rows);
    }

    std::vector<UInt8> seen_columns(columns.size(), 0);
    Int64 rows = -1;
    size_t iteration = 0;
    do
    {
        auto column_name = reader->readColumnStart();
        size_t column_index = iteration;
        if (column_name.has_value())
        {
            /// Check if this name appears in header. If no, skip this column or throw
            /// an exception according to setting input_format_skip_unknown_fields
            if (name_to_index.find(*column_name) == name_to_index.end())
            {
                if (!format_settings.skip_unknown_fields)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown column found in input data: {}", *column_name);

                reader->skipColumn();
                continue;
            }
            column_index = name_to_index[*column_name];
        }

        if (column_index >= columns.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Input data has too many columns, expected {} columns", columns.size());

        seen_columns[column_index] = 1;
        size_t columns_size = readColumn(*columns[column_index], fields[column_index].type, serializations[column_index], fields[column_index].name);
        if (rows != -1 && size_t(rows) != columns_size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Number of rows differs in different columns: {} != {}", rows, columns_size);
        rows = columns_size;
        ++iteration;
    }
    while (!reader->checkChunkEndOrSkipColumnDelimiter());

    approx_bytes_read_for_chunk = getDataOffsetMaybeCompressed(*in) - chunk_start;

    if (rows <= 0)
        return Chunk(std::move(columns), 0);

    /// Insert defaults in columns that were not presented in block and fill
    /// block_missing_values accordingly if setting input_format_defaults_for_omitted_fields is enabled
    for (size_t i = 0; i != seen_columns.size(); ++i)
    {
        if (!seen_columns[i])
        {
            if (format_settings.force_null_for_omitted_fields && !isNullableOrLowCardinalityNullable(fields[i].type))
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot insert NULL value into a column `{}` of type '{}'", fields[i].name, fields[i].type->getName());
            columns[i]->insertManyDefaults(rows);
            if (format_settings.defaults_for_omitted_fields)
                block_missing_values.setBits(i, rows);
        }
    }

    return Chunk(std::move(columns), rows);
}

JSONColumnsSchemaReaderBase::JSONColumnsSchemaReaderBase(
    ReadBuffer & in_, const FormatSettings & format_settings_, std::unique_ptr<JSONColumnsReaderBase> reader_)
    : ISchemaReader(in_)
    , format_settings(format_settings_)
    , hints_str(format_settings_.schema_inference_hints)
    , reader(std::move(reader_))
    , column_names_from_settings(splitColumnNames(format_settings_.column_names_for_schema_inference))
    , max_rows_to_read(format_settings_.max_rows_to_read_for_schema_inference)
    , max_bytes_to_read(format_settings_.max_bytes_to_read_for_schema_inference)
{
}

void JSONColumnsSchemaReaderBase::setContext(const ContextPtr & ctx)
{
    ColumnsDescription columns;
    if (tryParseColumnsListFromString(hints_str, columns, ctx, hints_parsing_error))
    {
        for (const auto & [name, type] : columns.getAll())
            hints[name] = type;
    }
}

void JSONColumnsSchemaReaderBase::transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type)
{
    transformInferredJSONTypesIfNeeded(type, new_type, format_settings, &inference_info);
}

void JSONColumnsSchemaReaderBase::transformTypesFromDifferentFilesIfNeeded(DataTypePtr & type, DataTypePtr & new_type)
{
    transformInferredJSONTypesFromDifferentFilesIfNeeded(type, new_type, format_settings);
}

NamesAndTypesList JSONColumnsSchemaReaderBase::readSchema()
{
    std::unordered_map<String, DataTypePtr> names_to_types;
    std::vector<String> names_order;
    /// Read data block by block and determine the type for each column
    /// until max_rows_to_read/max_bytes_to_read is reached.
    /// Note that we can exceed max_bytes_to_read to compete block parsing.
    while (total_rows_read < max_rows_to_read && in.count() < max_bytes_to_read)
    {
        if (in.eof())
            break;

        reader->readChunkStart();
        /// Check for empty block.
        if (reader->checkChunkEnd())
            continue;

        size_t iteration = 0;
        size_t rows_in_block = 0;
        do
        {
            auto column_name_opt = reader->readColumnStart();
            /// If format doesn't have names for columns, use names from setting column_names_for_schema_inference or default names 'c1', 'c2', ...
            String column_name;
            if (column_name_opt.has_value())
                column_name = *column_name_opt;
            else if (iteration < column_names_from_settings.size())
                column_name = column_names_from_settings[iteration];
            else
                column_name = "c" + std::to_string(iteration + 1);

            /// Keep order of column names as it is in input data.
            if (!names_to_types.contains(column_name))
                names_order.push_back(column_name);

            if (const auto it = hints.find(column_name); it != hints.end())
            {
                names_to_types[column_name] = it->second;
            }
            else
            {
                rows_in_block = 0;
                auto column_type = readColumnAndGetDataType(
                    column_name, rows_in_block, format_settings.max_rows_to_read_for_schema_inference - total_rows_read);
                chooseResultColumnType(*this, names_to_types[column_name], column_type, nullptr, column_name, total_rows_read + 1, hints_parsing_error);
            }

            ++iteration;
        }
        while (!reader->checkChunkEndOrSkipColumnDelimiter());

        total_rows_read += rows_in_block;
    }

    if (names_to_types.empty())
        throw Exception(ErrorCodes::EMPTY_DATA_PASSED, "Cannot read rows from the data");

    NamesAndTypesList result;
    for (auto & name : names_order)
    {
        auto & type = names_to_types[name];
        /// Don't check/change types from hints.
        if (!hints.contains(name))
        {
            transformFinalInferredJSONTypeIfNeeded(type, format_settings, &inference_info);
            /// Check that we could determine the type of this column.
            checkFinalInferredType(type, name, format_settings, nullptr, format_settings.max_rows_to_read_for_schema_inference, hints_parsing_error);
        }
        result.emplace_back(name, type);
    }

    return result;
}

DataTypePtr JSONColumnsSchemaReaderBase::readColumnAndGetDataType(const String & column_name, size_t & rows_read, size_t max_rows)
{
    /// Check for empty column.
    if (reader->checkColumnEnd())
        return nullptr;

    String field;
    DataTypePtr column_type;
    do
    {
        /// If we reached max_rows_to_read, skip the rest part of this column.
        if (rows_read == max_rows)
        {
            reader->skipColumn();
            break;
        }

        readJSONField(field, in, format_settings.json);
        DataTypePtr field_type = tryInferDataTypeForSingleJSONField(field, format_settings, &inference_info);
        chooseResultColumnType(*this, column_type, field_type, nullptr, column_name, rows_read);
        ++rows_read;
    }
    while (!reader->checkColumnEndOrSkipFieldDelimiter());

    return column_type;
}

}
