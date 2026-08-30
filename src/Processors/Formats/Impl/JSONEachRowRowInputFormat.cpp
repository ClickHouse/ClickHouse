#include <algorithm>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <Core/CaseAwareBlockNameMap.h>

#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>
#include <Formats/SchemaInferenceUtils.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <Processors/Formats/Impl/JSONEachRowRowInputFormat.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeFactory.h>

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
    NESTED_FIELD = size_t(-2),
    NOT_INITIALIZED = size_t(-3)
};

}


JSONEachRowRowInputFormat::JSONEachRowRowInputFormat(
    ReadBuffer & in_,
    SharedHeader header_,
    Params params_,
    const FormatSettings & format_settings_,
    bool yield_strings_,
    bool with_names_,
    bool with_types_)
    : IRowInputFormat(header_, in_, std::move(params_))
    , name_map(format_settings_.input_format_column_matching_case_sensitivity)
    , prev_positions(header_->columns(), {std::string_view{}, NOT_INITIALIZED})
    , yield_strings(yield_strings_)
    , with_names(with_names_)
    , with_types(with_types_)
    , format_settings(format_settings_)
{
    name_map.initFromBlock(getPort().getHeader());
    const auto & header = getPort().getHeader();
    if (format_settings_.import_nested_json)
    {
        for (size_t i = 0; i != header.columns(); ++i)
        {
            const std::string_view column_name = header.getByPosition(i).name;
            const auto split = Nested::splitName(column_name);
            if (!split.second.empty())
            {
                const std::string_view table_name = column_name.substr(0, split.first.size());
                name_map.add(table_name, NESTED_FIELD);
            }
        }
    }
}

const String & JSONEachRowRowInputFormat::columnName(size_t i) const
{
    return getPort().getHeader().getByPosition(i).name;
}

inline size_t JSONEachRowRowInputFormat::columnIndex(std::string_view name, size_t key_index)
{
    /// Optimization by caching the order of fields (which is almost always the same)
    /// and a quick check to match the next expected field, instead of searching the hash table.
    if (prev_positions.size() > key_index && prev_positions[key_index].second != NOT_INITIALIZED
        && name_map.equal(name, prev_positions[key_index].first))
    {
        return prev_positions[key_index].second;
    }

    auto position = name_map.get(name);
    if (position != CaseAwareBlockNameMap::NOT_FOUND)
    {
        if (key_index < prev_positions.size() && position < getPort().getHeader().columns())
            prev_positions[key_index] = {getPort().getHeader().getByPosition(position).name, position};

        return position;
    }
    return UNKNOWN_FIELD;
}

/** Read the field name and convert it to column name
  *  (taking into account the current nested name prefix)
  * Resulting std::string_view is valid only before next read from buf.
  */
std::string_view JSONEachRowRowInputFormat::readColumnName(ReadBuffer & buf)
{
    // This is just an optimization: try to avoid copying the name into current_column_name

    if (nested_prefix_length == 0 && !buf.eof() && buf.position() + 1 < buf.buffer().end())
    {
        char * next_pos = find_first_symbols<'\\', '"'>(buf.position() + 1, buf.buffer().end());

        if (next_pos != buf.buffer().end() && *next_pos != '\\')
        {
            /// The most likely option is that there is no escape sequence in the key name, and the entire name is placed in the buffer.
            assertChar('"', buf);
            std::string_view res(buf.position(), next_pos - buf.position());
            buf.position() = next_pos + 1;
            return res;
        }
    }

    current_column_name.resize(nested_prefix_length);
    readJSONStringInto(current_column_name, buf, format_settings.json);
    return current_column_name;
}

void JSONEachRowRowInputFormat::skipUnknownField(std::string_view name_ref)
{
    if (!format_settings.skip_unknown_fields)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown field found while parsing JSONEachRow format: {}", name_ref);

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
    if (*in->position() == '}')
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
        std::string_view name_ref = readColumnName(*in);
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

            current_column_name.assign(name_ref.data(), name_ref.size());
            name_ref = std::string_view(current_column_name);

            JSONUtils::skipColon(*in);

            if (column_index == UNKNOWN_FIELD)
                skipUnknownField(name_ref);
            else if (column_index == NESTED_FIELD)
                readNestedData(std::string{name_ref}, columns);
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

    if (with_names || with_types)
    {
        std::vector<String> column_names;
        if (with_names)
            column_names = JSONUtils::readStringFieldsFromJSONArrayRow(*in, format_settings);

        if (with_types)
        {
            auto type_names = JSONUtils::readStringFieldsFromJSONArrayRow(*in, format_settings);
            if (format_settings.with_types_use_header)
                validateTypesFromHeader(column_names, type_names);
        }
    }
    else
    {
        data_in_square_brackets = JSONUtils::checkAndSkipArrayStart(*in);
    }
}

void JSONEachRowRowInputFormat::validateTypesFromHeader(const std::vector<String> & column_names, const std::vector<String> & type_names)
{
    if (type_names.size() != column_names.size())
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "The number of data types differs from the number of column names in input data");

    const auto & header = getPort().getHeader();
    for (size_t i = 0; i < column_names.size(); ++i)
    {
        auto position = name_map.get(column_names[i]);
        if (position != CaseAwareBlockNameMap::NOT_FOUND)
        {
            const auto & type = header.getByPosition(position).type;
            if (type->getName() != type_names[i])
            {
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Type of '{}' must be {}, not {}",
                    header.getByPosition(position).name,
                    type->getName(),
                    type_names[i]);
            }
        }
    }
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

JSONEachRowSchemaReader::JSONEachRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_, bool with_names_, bool with_types_)
    : IRowWithNamesSchemaReader(in_, format_settings_)
    , with_names(with_names_)
    , with_types(with_types_)
{
}

NamesAndTypesList JSONEachRowSchemaReader::readRowAndGetNamesAndDataTypes(bool & eof)
{
    if (first_row)
    {
        skipBOMIfExists(in);
        if ((with_names || with_types) && !header_rows_read)
        {
            if (with_names)
                JSONUtils::readStringFieldsFromJSONArrayRow(in, format_settings);
            if (with_types)
                JSONUtils::readStringFieldsFromJSONArrayRow(in, format_settings);
            header_rows_read = true;
        }
        else if (!with_names && !with_types)
        {
            data_in_square_brackets = JSONUtils::checkAndSkipArrayStart(in);
        }
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

NamesAndTypesList JSONEachRowSchemaReader::readSchema()
{
    if (with_names || with_types)
        skipBOMIfExists(in);

    std::vector<String> column_names;

    if (with_names)
    {
        column_names = JSONUtils::readStringFieldsFromJSONArrayRow(in, format_settings);
        first_row = false;
        header_rows_read = true;
    }

    if (with_types)
    {
        std::vector<String> type_names = JSONUtils::readStringFieldsFromJSONArrayRow(in, format_settings);
        if (type_names.size() != column_names.size())
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "The number of column names {} differs with the number of types {}",
                column_names.size(),
                type_names.size());

        NamesAndTypesList result;
        for (size_t i = 0; i != type_names.size(); ++i)
            result.emplace_back(column_names[i], DataTypeFactory::instance().get(type_names[i]));
        return result;
    }

    return IRowWithNamesSchemaReader::readSchema();
}

void registerInputFormatJSONEachRow(FormatFactory & factory);
void registerInputFormatJSONEachRow(FormatFactory & factory)
{
    auto register_format = [&](const String & format_name, bool json_strings, bool with_names, bool with_types)
    {
        factory.registerInputFormat(format_name, [json_strings, with_names, with_types](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
        {
            return std::make_shared<JSONEachRowRowInputFormat>(buf, std::make_shared<const Block>(sample), std::move(params), settings, json_strings, with_names, with_types);
        });
    };

    /// JSONEachRow family (typed JSON values)
    register_format("JSONEachRow", false, false, false);
    register_format("JSONEachRowWithNames", false, true, false);
    register_format("JSONEachRowWithNamesAndTypes", false, true, true);

    /// JSONStringsEachRow family (all values as JSON strings)
    register_format("JSONStringsEachRow", true, false, false);
    register_format("JSONStringsEachRowWithNames", true, true, false);
    register_format("JSONStringsEachRowWithNamesAndTypes", true, true, true);

    register_format("JSONLines", false, false, false);
    register_format("JSONL", false, false, false);
    register_format("NDJSON", false, false, false);

    factory.registerFileExtension("ndjson", "JSONEachRow");
    factory.registerFileExtension("jsonl", "JSONEachRow");

    factory.markFormatSupportsSubsetOfColumns("JSONEachRow");
    factory.markFormatSupportsSubsetOfColumns("JSONLines");
    factory.markFormatSupportsSubsetOfColumns("NDJSON");
    factory.markFormatSupportsSubsetOfColumns("JSONL");
    factory.markFormatSupportsSubsetOfColumns("JSONStringsEachRow");

    markFormatWithNamesAndTypesSupportsSamplingColumns("JSONEachRow", factory);
    markFormatWithNamesAndTypesSupportsSamplingColumns("JSONStringsEachRow", factory);

    factory.setDocumentation("JSONEachRow", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias                          |
|-------|--------|--------------------------------|
| ✔     | ✔      | `JSONLines`, `NDJSON`, `JSONL` |

## Description {#description}

In this format, ClickHouse outputs each row as a separated, newline-delimited JSON Object.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
{"date":"2022-04-30","season":2021,"home_team":"Sutton United","away_team":"Bradford City","home_team_goals":1,"away_team_goals":4}
{"date":"2022-04-30","season":2021,"home_team":"Swindon Town","away_team":"Barrow","home_team_goals":2,"away_team_goals":1}
{"date":"2022-04-30","season":2021,"home_team":"Tranmere Rovers","away_team":"Oldham Athletic","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-02","season":2021,"home_team":"Port Vale","away_team":"Newport County","home_team_goals":1,"away_team_goals":2}
{"date":"2022-05-02","season":2021,"home_team":"Salford City","away_team":"Mansfield Town","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Barrow","away_team":"Northampton Town","home_team_goals":1,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Bradford City","away_team":"Carlisle United","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Bristol Rovers","away_team":"Scunthorpe United","home_team_goals":7,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Exeter City","away_team":"Port Vale","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Harrogate Town A.F.C.","away_team":"Sutton United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Hartlepool United","away_team":"Colchester United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Leyton Orient","away_team":"Tranmere Rovers","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Mansfield Town","away_team":"Forest Green Rovers","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Newport County","away_team":"Rochdale","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Oldham Athletic","away_team":"Crawley Town","home_team_goals":3,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Stevenage Borough","away_team":"Salford City","home_team_goals":4,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Walsall","away_team":"Swindon Town","home_team_goals":0,"away_team_goals":3}
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONEachRow;
```

### Reading data {#reading-data}

Read data using the `JSONEachRow` format:

```sql
SELECT *
FROM football
FORMAT JSONEachRow
```

The output will be in JSON format:

```json
{"date":"2022-04-30","season":2021,"home_team":"Sutton United","away_team":"Bradford City","home_team_goals":1,"away_team_goals":4}
{"date":"2022-04-30","season":2021,"home_team":"Swindon Town","away_team":"Barrow","home_team_goals":2,"away_team_goals":1}
{"date":"2022-04-30","season":2021,"home_team":"Tranmere Rovers","away_team":"Oldham Athletic","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-02","season":2021,"home_team":"Port Vale","away_team":"Newport County","home_team_goals":1,"away_team_goals":2}
{"date":"2022-05-02","season":2021,"home_team":"Salford City","away_team":"Mansfield Town","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Barrow","away_team":"Northampton Town","home_team_goals":1,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Bradford City","away_team":"Carlisle United","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Bristol Rovers","away_team":"Scunthorpe United","home_team_goals":7,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Exeter City","away_team":"Port Vale","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Harrogate Town A.F.C.","away_team":"Sutton United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Hartlepool United","away_team":"Colchester United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Leyton Orient","away_team":"Tranmere Rovers","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Mansfield Town","away_team":"Forest Green Rovers","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Newport County","away_team":"Rochdale","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Oldham Athletic","away_team":"Crawley Town","home_team_goals":3,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Stevenage Borough","away_team":"Salford City","home_team_goals":4,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Walsall","away_team":"Swindon Town","home_team_goals":0,"away_team_goals":3}
```

Importing data columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/reference/settings/formats/input-format#input_format_skip_unknown_fields) is set to 1.

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONL", Documentation{
        .description = "An alias for the `JSONEachRow` format. See the `JSONEachRow` entry for the full documentation.",
        .related = {"JSONEachRow"}});

    factory.setDocumentation("JSONLines", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias                                      |
|-------|--------|--------------------------------------------|
| ✔     | ✔      | `JSONEachRow`, `JSONLines`, `NDJSON`, `JSONL` |

## Description {#description}

In this format, ClickHouse outputs each row as a separated, newline-delimited JSON Object.

This format is also known as `JSONEachRow`, `JSONLines`, `NDJSON` (Newline Delimited JSON), or `JSONL`. These names are aliases for the same format and can be used interchangeably for both input and output.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
{"date":"2022-04-30","season":2021,"home_team":"Sutton United","away_team":"Bradford City","home_team_goals":1,"away_team_goals":4}
{"date":"2022-04-30","season":2021,"home_team":"Swindon Town","away_team":"Barrow","home_team_goals":2,"away_team_goals":1}
{"date":"2022-04-30","season":2021,"home_team":"Tranmere Rovers","away_team":"Oldham Athletic","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-02","season":2021,"home_team":"Port Vale","away_team":"Newport County","home_team_goals":1,"away_team_goals":2}
{"date":"2022-05-02","season":2021,"home_team":"Salford City","away_team":"Mansfield Town","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Barrow","away_team":"Northampton Town","home_team_goals":1,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Bradford City","away_team":"Carlisle United","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Bristol Rovers","away_team":"Scunthorpe United","home_team_goals":7,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Exeter City","away_team":"Port Vale","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Harrogate Town A.F.C.","away_team":"Sutton United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Hartlepool United","away_team":"Colchester United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Leyton Orient","away_team":"Tranmere Rovers","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Mansfield Town","away_team":"Forest Green Rovers","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Newport County","away_team":"Rochdale","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Oldham Athletic","away_team":"Crawley Town","home_team_goals":3,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Stevenage Borough","away_team":"Salford City","home_team_goals":4,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Walsall","away_team":"Swindon Town","home_team_goals":0,"away_team_goals":3}
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONLines;
```

### Reading data {#reading-data}

Read data using the `JSONLines` format:

```sql
SELECT *
FROM football
FORMAT JSONLines
```

The output will be in JSON format:

```json
{"date":"2022-04-30","season":2021,"home_team":"Sutton United","away_team":"Bradford City","home_team_goals":1,"away_team_goals":4}
{"date":"2022-04-30","season":2021,"home_team":"Swindon Town","away_team":"Barrow","home_team_goals":2,"away_team_goals":1}
{"date":"2022-04-30","season":2021,"home_team":"Tranmere Rovers","away_team":"Oldham Athletic","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-02","season":2021,"home_team":"Port Vale","away_team":"Newport County","home_team_goals":1,"away_team_goals":2}
{"date":"2022-05-02","season":2021,"home_team":"Salford City","away_team":"Mansfield Town","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Barrow","away_team":"Northampton Town","home_team_goals":1,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Bradford City","away_team":"Carlisle United","home_team_goals":2,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Bristol Rovers","away_team":"Scunthorpe United","home_team_goals":7,"away_team_goals":0}
{"date":"2022-05-07","season":2021,"home_team":"Exeter City","away_team":"Port Vale","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Harrogate Town A.F.C.","away_team":"Sutton United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Hartlepool United","away_team":"Colchester United","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Leyton Orient","away_team":"Tranmere Rovers","home_team_goals":0,"away_team_goals":1}
{"date":"2022-05-07","season":2021,"home_team":"Mansfield Town","away_team":"Forest Green Rovers","home_team_goals":2,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Newport County","away_team":"Rochdale","home_team_goals":0,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Oldham Athletic","away_team":"Crawley Town","home_team_goals":3,"away_team_goals":3}
{"date":"2022-05-07","season":2021,"home_team":"Stevenage Borough","away_team":"Salford City","home_team_goals":4,"away_team_goals":2}
{"date":"2022-05-07","season":2021,"home_team":"Walsall","away_team":"Swindon Town","home_team_goals":0,"away_team_goals":3}
```

Importing data columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/reference/settings/formats/input-format#input_format_skip_unknown_fields) is set to 1.

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("JSONStringsEachRow", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Differs from the [`JSONEachRow`](/reference/formats/JSON/JSONEachRow) only in that data fields are output in strings, not in typed JSON values.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a JSON file with the following data, named as `football.json`:

```json
{"date":"2022-04-30","season":"2021","home_team":"Sutton United","away_team":"Bradford City","home_team_goals":"1","away_team_goals":"4"}
{"date":"2022-04-30","season":"2021","home_team":"Swindon Town","away_team":"Barrow","home_team_goals":"2","away_team_goals":"1"}
{"date":"2022-04-30","season":"2021","home_team":"Tranmere Rovers","away_team":"Oldham Athletic","home_team_goals":"2","away_team_goals":"0"}
{"date":"2022-05-02","season":"2021","home_team":"Port Vale","away_team":"Newport County","home_team_goals":"1","away_team_goals":"2"}
{"date":"2022-05-02","season":"2021","home_team":"Salford City","away_team":"Mansfield Town","home_team_goals":"2","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Barrow","away_team":"Northampton Town","home_team_goals":"1","away_team_goals":"3"}
{"date":"2022-05-07","season":"2021","home_team":"Bradford City","away_team":"Carlisle United","home_team_goals":"2","away_team_goals":"0"}
{"date":"2022-05-07","season":"2021","home_team":"Bristol Rovers","away_team":"Scunthorpe United","home_team_goals":"7","away_team_goals":"0"}
{"date":"2022-05-07","season":"2021","home_team":"Exeter City","away_team":"Port Vale","home_team_goals":"0","away_team_goals":"1"}
{"date":"2022-05-07","season":"2021","home_team":"Harrogate Town A.F.C.","away_team":"Sutton United","home_team_goals":"0","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Hartlepool United","away_team":"Colchester United","home_team_goals":"0","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Leyton Orient","away_team":"Tranmere Rovers","home_team_goals":"0","away_team_goals":"1"}
{"date":"2022-05-07","season":"2021","home_team":"Mansfield Town","away_team":"Forest Green Rovers","home_team_goals":"2","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Newport County","away_team":"Rochdale","home_team_goals":"0","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Oldham Athletic","away_team":"Crawley Town","home_team_goals":"3","away_team_goals":"3"}
{"date":"2022-05-07","season":"2021","home_team":"Stevenage Borough","away_team":"Salford City","home_team_goals":"4","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Walsall","away_team":"Swindon Town","home_team_goals":"0","away_team_goals":"3"}
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONStringsEachRow;
```

### Reading data {#reading-data}

Read data using the `JSONStringsEachRow` format:

```sql
SELECT *
FROM football
FORMAT JSONStringsEachRow
```

The output will be in JSON format:

```json
{"date":"2022-04-30","season":"2021","home_team":"Sutton United","away_team":"Bradford City","home_team_goals":"1","away_team_goals":"4"}
{"date":"2022-04-30","season":"2021","home_team":"Swindon Town","away_team":"Barrow","home_team_goals":"2","away_team_goals":"1"}
{"date":"2022-04-30","season":"2021","home_team":"Tranmere Rovers","away_team":"Oldham Athletic","home_team_goals":"2","away_team_goals":"0"}
{"date":"2022-05-02","season":"2021","home_team":"Port Vale","away_team":"Newport County","home_team_goals":"1","away_team_goals":"2"}
{"date":"2022-05-02","season":"2021","home_team":"Salford City","away_team":"Mansfield Town","home_team_goals":"2","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Barrow","away_team":"Northampton Town","home_team_goals":"1","away_team_goals":"3"}
{"date":"2022-05-07","season":"2021","home_team":"Bradford City","away_team":"Carlisle United","home_team_goals":"2","away_team_goals":"0"}
{"date":"2022-05-07","season":"2021","home_team":"Bristol Rovers","away_team":"Scunthorpe United","home_team_goals":"7","away_team_goals":"0"}
{"date":"2022-05-07","season":"2021","home_team":"Exeter City","away_team":"Port Vale","home_team_goals":"0","away_team_goals":"1"}
{"date":"2022-05-07","season":"2021","home_team":"Harrogate Town A.F.C.","away_team":"Sutton United","home_team_goals":"0","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Hartlepool United","away_team":"Colchester United","home_team_goals":"0","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Leyton Orient","away_team":"Tranmere Rovers","home_team_goals":"0","away_team_goals":"1"}
{"date":"2022-05-07","season":"2021","home_team":"Mansfield Town","away_team":"Forest Green Rovers","home_team_goals":"2","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Newport County","away_team":"Rochdale","home_team_goals":"0","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Oldham Athletic","away_team":"Crawley Town","home_team_goals":"3","away_team_goals":"3"}
{"date":"2022-05-07","season":"2021","home_team":"Stevenage Borough","away_team":"Salford City","home_team_goals":"4","away_team_goals":"2"}
{"date":"2022-05-07","season":"2021","home_team":"Walsall","away_team":"Swindon Town","home_team_goals":"0","away_team_goals":"3"}
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("NDJSON", Documentation{
        .description = "An alias for the `JSONEachRow` format. See the `JSONEachRow` entry for the full documentation.",
        .related = {"JSONEachRow"}});
}

void registerFileSegmentationEngineJSONEachRow(FormatFactory & factory);
void registerFileSegmentationEngineJSONEachRow(FormatFactory & factory)
{
    auto register_func = [&](const String & format_name, bool with_names, bool with_types)
    {
        if (!with_names && !with_types)
        {
            factory.registerFileSegmentationEngineCreator(format_name, [](const FormatSettings & settings) -> FormatFactory::FileSegmentationEngine
            {
                return [max_row_size = settings.json.max_row_size_for_json_each_row](ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
                {
                    return JSONUtils::fileSegmentationEngineJSONEachRow(in, memory, min_bytes, max_rows, max_row_size);
                };
            });
            return;
        }

        /// Header rows (names and/or types) are JSON arrays; read at least one data row together with them.
        size_t min_rows = 1 + int(with_names) + int(with_types);
        factory.registerFileSegmentationEngineCreator(format_name, [min_rows](const FormatSettings & settings) -> FormatFactory::FileSegmentationEngine
        {
            return [min_rows, max_row_size = settings.json.max_row_size_for_json_each_row](ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
            {
                return JSONUtils::fileSegmentationEngineJSONCompactEachRow(in, memory, min_bytes, min_rows, max_rows, max_row_size);
            };
        });
    };

    registerWithNamesAndTypes("JSONEachRow", register_func);
    registerWithNamesAndTypes("JSONStringsEachRow", register_func);

    register_func("JSONLines", false, false);
    register_func("NDJSON", false, false);
    register_func("JSONL", false, false);
}

void registerNonTrivialPrefixAndSuffixCheckerJSONEachRow(FormatFactory & factory);
void registerNonTrivialPrefixAndSuffixCheckerJSONEachRow(FormatFactory & factory)
{
    factory.registerNonTrivialPrefixAndSuffixChecker("JSONEachRow", JSONUtils::nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
    factory.registerNonTrivialPrefixAndSuffixChecker("JSONStringsEachRow", JSONUtils::nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
    factory.registerNonTrivialPrefixAndSuffixChecker("JSONLines", JSONUtils::nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
    factory.registerNonTrivialPrefixAndSuffixChecker("NDJSON", JSONUtils::nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
    factory.registerNonTrivialPrefixAndSuffixChecker("JSONL", JSONUtils::nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
}

void registerJSONEachRowSchemaReader(FormatFactory & factory);
void registerJSONEachRowSchemaReader(FormatFactory & factory)
{
    auto register_schema_reader = [&](const String & format_name, bool with_names, bool with_types)
    {
        factory.registerSchemaReader(format_name, [with_names, with_types](ReadBuffer & buf, const FormatSettings & settings)
        {
            return std::make_unique<JSONEachRowSchemaReader>(buf, settings, with_names, with_types);
        });
        factory.registerAdditionalInfoForSchemaCacheGetter(format_name, [](const FormatSettings & settings)
        {
            return getAdditionalFormatInfoByEscapingRule(settings, FormatSettings::EscapingRule::JSON);
        });
    };

    registerWithNamesAndTypes("JSONEachRow", register_schema_reader);
    registerWithNamesAndTypes("JSONStringsEachRow", register_schema_reader);

    register_schema_reader("JSONLines", false, false);
    register_schema_reader("NDJSON", false, false);
    register_schema_reader("JSONL", false, false);
}

}
