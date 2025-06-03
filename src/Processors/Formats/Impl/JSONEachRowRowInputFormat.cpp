#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>

#include <Processors/Formats/Impl/JSONEachRowRowInputFormat.h>
#include <Formats/JSONUtils.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/SchemaInferenceUtils.h>
#include <Formats/FormatFactory.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/getLeastSupertype.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
}

namespace
{

enum
{
    UNKNOWN_FIELD = size_t(-1),
    NESTED_FIELD = size_t(-2)
};

}


JSONEachRowRowInputFormat::JSONEachRowRowInputFormat(
    ReadBuffer & in_,
    const Block & header_,
    Params params_,
    const FormatSettings & format_settings_,
    bool yield_strings_)
    : IRowInputFormat(header_, in_, std::move(params_))
    , prev_positions(header_.columns())
    , yield_strings(yield_strings_)
    , format_settings(format_settings_)
{
    const auto & header = getPort().getHeader();
    name_map = header.getNamesToIndexesMap();
    if (format_settings_.import_nested_json)
    {
        for (size_t i = 0; i != header.columns(); ++i)
        {
            const StringRef column_name = header.getByPosition(i).name;
            const auto split = Nested::splitName(column_name.toView());
            if (!split.second.empty())
            {
                const StringRef table_name(column_name.data, split.first.size());
                name_map[table_name] = NESTED_FIELD;
            }
        }
    }
}

const String & JSONEachRowRowInputFormat::columnName(size_t i) const
{
    return getPort().getHeader().getByPosition(i).name;
}

inline size_t JSONEachRowRowInputFormat::columnIndex(StringRef name, size_t key_index)
{
    /// Optimization by caching the order of fields (which is almost always the same)
    /// and a quick check to match the next expected field, instead of searching the hash table.

    if (prev_positions.size() > key_index
        && prev_positions[key_index] != Block::NameMap::const_iterator{}
        && name == prev_positions[key_index]->first)
    {
        return prev_positions[key_index]->second;
    }
    else
    {
        const auto it = name_map.find(name);
        if (it != name_map.end())
        {
            if (key_index < prev_positions.size())
                prev_positions[key_index] = it;

            return it->second;
        }
        else
            return UNKNOWN_FIELD;
    }
}

/** Read the field name and convert it to column name
  *  (taking into account the current nested name prefix)
  * Resulting StringRef is valid only before next read from buf.
  */
StringRef JSONEachRowRowInputFormat::readColumnName(ReadBuffer & buf)
{
    // This is just an optimization: try to avoid copying the name into current_column_name

    if (nested_prefix_length == 0 && !buf.eof() && buf.position() + 1 < buf.buffer().end())
    {
        char * next_pos = find_first_symbols<'\\', '"'>(buf.position() + 1, buf.buffer().end());

        if (next_pos != buf.buffer().end() && *next_pos != '\\')
        {
            /// The most likely option is that there is no escape sequence in the key name, and the entire name is placed in the buffer.
            assertChar('"', buf);
            StringRef res(buf.position(), next_pos - buf.position());
            buf.position() = next_pos + 1;
            return res;
        }
    }

    current_column_name.resize(nested_prefix_length);
    readJSONStringInto(current_column_name, buf, format_settings.json);
    return current_column_name;
}

void JSONEachRowRowInputFormat::skipUnknownField(StringRef name_ref)
{
    if (!format_settings.skip_unknown_fields)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown field found while parsing JSONEachRow format: {}", name_ref.toString());

    skipJSONField(*in, name_ref, format_settings.json);
}

void JSONEachRowRowInputFormat::readField(size_t index, MutableColumns & columns)
{
    if (seen_columns[index])
        throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicate field found while parsing JSONEachRow format: {}", columnName(index));

    seen_columns[index] = true;
    seen_columns_count++;
    const auto & type = getPort().getHeader().getByPosition(index).type;
    const auto & serialization = serializations[index];
    read_columns[index] = JSONUtils::readField(*in, *columns[index], type, serialization, columnName(index), format_settings, yield_strings);
}

inline bool JSONEachRowRowInputFormat::advanceToNextKey(size_t key_index)
{
    skipWhitespaceIfAny(*in);

    if (in->eof())
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Unexpected end of stream while parsing JSONEachRow format");
    else if (*in->position() == '}')
    {
        ++in->position();
        return false;
    }

    if (key_index > 0)
        JSONUtils::skipComma(*in);
    return true;
}

void JSONEachRowRowInputFormat::readJSONObject(MutableColumns & columns)
{
    assertChar('{', *in);

    for (size_t key_index = 0; advanceToNextKey(key_index); ++key_index)
    {
        StringRef name_ref = readColumnName(*in);
        if (seen_columns_count >= total_columns && format_settings.json.ignore_unnecessary_fields)
        {
            // Keep parsing the remaining fields in case of the json is invalid.
            // But not look up the name in the name_map since the cost cannot be ignored
            JSONUtils::skipColon(*in);
            skipUnknownField(name_ref);
            continue;
        }
        const size_t column_index = columnIndex(name_ref, key_index);

        if (unlikely(ssize_t(column_index) < 0))
        {
            /// name_ref may point directly to the input buffer
            /// and input buffer may be filled with new data on next read
            /// If we want to use name_ref after another reads from buffer, we must copy it to temporary string.

            current_column_name.assign(name_ref.data, name_ref.size);
            name_ref = StringRef(current_column_name);

            JSONUtils::skipColon(*in);

            if (column_index == UNKNOWN_FIELD)
                skipUnknownField(name_ref);
            else if (column_index == NESTED_FIELD)
                readNestedData(name_ref.toString(), columns);
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal value of column_index");
        }
        else
        {
            JSONUtils::skipColon(*in);
            readField(column_index, columns);
        }
    }
}

void JSONEachRowRowInputFormat::readNestedData(const String & name, MutableColumns & columns)
{
    current_column_name = name;
    current_column_name.push_back('.');
    nested_prefix_length = current_column_name.size();
    readJSONObject(columns);
    nested_prefix_length = 0;
}


bool JSONEachRowRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (!allow_new_rows)
        return false;
    skipWhitespaceIfAny(*in);

    bool is_first_row = getRowNum() == 0;
    if (checkEndOfData(is_first_row))
        return false;

    size_t num_columns = columns.size();
    total_columns = num_columns;
    seen_columns_count = 0;

    read_columns.assign(num_columns, false);
    seen_columns.assign(num_columns, false);

    nested_prefix_length = 0;
    readRowStart(columns);
    readJSONObject(columns);

    const auto & header = getPort().getHeader();
    /// Fill non-visited columns with the default values.
    for (size_t i = 0; i < num_columns; ++i)
        if (!seen_columns[i])
        {
            const auto & type = header.getByPosition(i).type;
            if (format_settings.force_null_for_omitted_fields && !isNullableOrLowCardinalityNullable(type))
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot insert NULL value into a column `{}` of type '{}'", columnName(i), type->getName());
            else
                type->insertDefaultInto(*columns[i]);
        }


    /// Return info about defaults set.
    /// If defaults_for_omitted_fields is set to 0, we should just leave already inserted defaults.
    if (format_settings.defaults_for_omitted_fields)
        ext.read_columns = read_columns;
    else
        ext.read_columns.assign(read_columns.size(), true);

    return true;
}

bool JSONEachRowRowInputFormat::checkEndOfData(bool is_first_row)
{
    /// We consume ',' or '\n' before scanning a new row, instead scanning to next row at the end.
    /// The reason is that if we want an exact number of rows read with LIMIT x
    /// from a streaming table engine with text data format, like File or Kafka
    /// then seeking to next ';,' or '\n' would trigger reading of an extra row at the end.

    /// Semicolon is added for convenience as it could be used at end of INSERT query.
    if (!in->eof())
    {
        /// There may be optional ',' (but not before the first row)
        if (!is_first_row && *in->position() == ',')
            ++in->position();
        else if (!data_in_square_brackets && *in->position() == ';')
        {
            /// ';' means the end of query (but it cannot be before ']')
            allow_new_rows = false;
            return true;
        }
        else if (data_in_square_brackets && *in->position() == ']')
        {
            /// ']' means the end of query
            allow_new_rows = false;
            return true;
        }
    }

    skipWhitespaceIfAny(*in);
    return in->eof();
}


void JSONEachRowRowInputFormat::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(*in);
}

void JSONEachRowRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    nested_prefix_length = 0;
    read_columns.clear();
    seen_columns.clear();
    prev_positions.clear();
    allow_new_rows = true;
}

void JSONEachRowRowInputFormat::readPrefix()
{
    /// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
    skipBOMIfExists(*in);
    data_in_square_brackets = JSONUtils::checkAndSkipArrayStart(*in);
}

void JSONEachRowRowInputFormat::readSuffix()
{
    skipWhitespaceIfAny(*in);
    if (data_in_square_brackets)
        JSONUtils::skipArrayEnd(*in);

    if (!in->eof() && *in->position() == ';')
    {
        ++in->position();
        skipWhitespaceIfAny(*in);
    }
    assertEOF(*in);
}

size_t JSONEachRowRowInputFormat::countRows(size_t max_block_size)
{
    if (unlikely(!allow_new_rows))
        return 0;

    size_t num_rows = 0;
    bool is_first_row = getRowNum() == 0;
    skipWhitespaceIfAny(*in);
    while (num_rows < max_block_size && !checkEndOfData(is_first_row))
    {
        skipRowStart();
        JSONUtils::skipRowForJSONEachRow(*in);
        ++num_rows;
        is_first_row = false;
        skipWhitespaceIfAny(*in);
    }

    return num_rows;
}

JSONEachRowSchemaReader::JSONEachRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : IRowWithNamesSchemaReader(in_, format_settings_)
{
}

NamesAndTypesList JSONEachRowSchemaReader::readRowAndGetNamesAndDataTypes(bool & eof)
{
    if (first_row)
    {
        skipBOMIfExists(in);
        data_in_square_brackets = JSONUtils::checkAndSkipArrayStart(in);
        first_row = false;
    }
    else
    {
        skipWhitespaceIfAny(in);
        /// If data is in square brackets then ']' means the end of data.
        if (data_in_square_brackets && checkChar(']', in))
            return {};

        /// ';' means end of data.
        if (checkChar(';', in))
            return {};

        /// There may be optional ',' between rows.
        checkChar(',', in);
    }

    skipWhitespaceIfAny(in);
    if (in.eof())
    {
        eof = true;
        return {};
    }

    return JSONUtils::readRowAndGetNamesAndDataTypesForJSONEachRow(in, format_settings, &inference_info);
}

void JSONEachRowSchemaReader::transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type)
{
    transformInferredJSONTypesIfNeeded(type, new_type, format_settings, &inference_info);
}

void JSONEachRowSchemaReader::transformTypesFromDifferentFilesIfNeeded(DB::DataTypePtr & type, DB::DataTypePtr & new_type)
{
    transformInferredJSONTypesFromDifferentFilesIfNeeded(type, new_type, format_settings);
}

void JSONEachRowSchemaReader::transformFinalTypeIfNeeded(DataTypePtr & type)
{
    transformFinalInferredJSONTypeIfNeeded(type, format_settings, &inference_info);
}

void registerInputFormatJSONEachRow(FormatFactory & factory)
{
    auto register_format = [&](const String & format_name, bool json_strings)
    {
        factory.registerInputFormat(format_name, [json_strings](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
        {
            return std::make_shared<JSONEachRowRowInputFormat>(buf, sample, std::move(params), settings, json_strings);
        });
    };

    register_format("JSONEachRow", false);
    register_format("JSONLines", false);
    register_format("NDJSON", false);

    factory.registerFileExtension("ndjson", "JSONEachRow");
    factory.registerFileExtension("jsonl", "JSONEachRow");

    register_format("JSONStringsEachRow", true);

    factory.markFormatSupportsSubsetOfColumns("JSONEachRow");
    factory.markFormatSupportsSubsetOfColumns("JSONLines");
    factory.markFormatSupportsSubsetOfColumns("NDJSON");
    factory.markFormatSupportsSubsetOfColumns("JSONStringsEachRow");
}

void registerFileSegmentationEngineJSONEachRow(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("JSONEachRow", &JSONUtils::fileSegmentationEngineJSONEachRow);
    factory.registerFileSegmentationEngine("JSONStringsEachRow", &JSONUtils::fileSegmentationEngineJSONEachRow);
    factory.registerFileSegmentationEngine("JSONLines", &JSONUtils::fileSegmentationEngineJSONEachRow);
    factory.registerFileSegmentationEngine("NDJSON", &JSONUtils::fileSegmentationEngineJSONEachRow);
}

void registerNonTrivialPrefixAndSuffixCheckerJSONEachRow(FormatFactory & factory)
{
    factory.registerNonTrivialPrefixAndSuffixChecker("JSONEachRow", JSONUtils::nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
    factory.registerNonTrivialPrefixAndSuffixChecker("JSONStringsEachRow", JSONUtils::nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
    factory.registerNonTrivialPrefixAndSuffixChecker("JSONLines", JSONUtils::nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
    factory.registerNonTrivialPrefixAndSuffixChecker("NDJSON", JSONUtils::nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
}

void registerJSONEachRowSchemaReader(FormatFactory & factory)
{
    auto register_schema_reader = [&](const String & format_name)
    {
        factory.registerSchemaReader(format_name, [](ReadBuffer & buf, const FormatSettings & settings)
        {
            return std::make_unique<JSONEachRowSchemaReader>(buf, settings);
        });
        factory.registerAdditionalInfoForSchemaCacheGetter(format_name, [](const FormatSettings & settings)
        {
            return getAdditionalFormatInfoByEscapingRule(settings, FormatSettings::EscapingRule::JSON);
        });
    };

    register_schema_reader("JSONEachRow");
    register_schema_reader("JSONLines");
    register_schema_reader("NDJSON");
    register_schema_reader("JSONStringsEachRow");
}

}
