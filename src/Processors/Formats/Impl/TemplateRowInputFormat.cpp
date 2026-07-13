#include <Processors/Formats/Impl/TemplateRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/verbosePrintString.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/SchemaInferenceUtils.h>
#include <IO/Operators.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/Serializations/SerializationNullable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int SYNTAX_ERROR;
}

[[noreturn]] static void throwUnexpectedEof(size_t row_num)
{
    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Unexpected EOF while parsing row {}. "
                           "Maybe last row has wrong format or input doesn't contain specified suffix before EOF.",
                           std::to_string(row_num));
}

static void updateFormatSettingsIfNeeded(FormatSettings::EscapingRule escaping_rule, FormatSettings & settings, const ParsedTemplateFormatString & row_format, char default_csv_delimiter, size_t file_column)
{
    if (escaping_rule != FormatSettings::EscapingRule::CSV)
        return;

    /// Clean custom_delimiter from previous column.
    settings.csv.custom_delimiter.clear();
    /// If field delimiter is empty, we read until default csv delimiter.
    if (row_format.delimiters[file_column + 1].empty())
        settings.csv.delimiter = default_csv_delimiter;
    /// If field delimiter has length = 1, it will be more efficient to use csv.delimiter.
    else if (row_format.delimiters[file_column + 1].size() == 1)
        settings.csv.delimiter = row_format.delimiters[file_column + 1].front();
    /// If we have some complex delimiter, normal CSV reading will now work properly if we will
    /// use the first character of delimiter (for example, if delimiter='||' and we have data 'abc|d||')
    /// We have special implementation for such case that uses custom delimiter, it's not so efficient,
    /// but works properly.
    else
        settings.csv.custom_delimiter = row_format.delimiters[file_column + 1];
}

TemplateRowInputFormat::TemplateRowInputFormat(
    SharedHeader header_,
    ReadBuffer & in_,
    const Params & params_,
    FormatSettings settings_,
    bool ignore_spaces_,
    ParsedTemplateFormatString format_,
    ParsedTemplateFormatString row_format_,
    std::string row_between_delimiter_)
    : TemplateRowInputFormat(
        header_, std::make_unique<PeekableReadBuffer>(in_), params_, settings_, ignore_spaces_, format_, row_format_, row_between_delimiter_)
{
}

TemplateRowInputFormat::TemplateRowInputFormat(SharedHeader header_, std::unique_ptr<PeekableReadBuffer> buf_, const Params & params_,
                                               FormatSettings settings_, bool ignore_spaces_,
                                               ParsedTemplateFormatString format_, ParsedTemplateFormatString row_format_,
                                               std::string row_between_delimiter_)
    : RowInputFormatWithDiagnosticInfo(header_, *buf_, params_), buf(std::move(buf_)), data_types(header_->getDataTypes()),
      settings(std::move(settings_)), ignore_spaces(ignore_spaces_),
      format(std::move(format_)), row_format(std::move(row_format_)),
      default_csv_delimiter(settings.csv.delimiter), row_between_delimiter(row_between_delimiter_),
      format_reader(std::make_unique<TemplateFormatReader>(*buf, ignore_spaces_, format, row_format, row_between_delimiter, settings))
{
    /// Validate format string for rows
    std::vector<UInt8> column_in_format(header_->columns(), false);
    for (size_t i = 0; i < row_format.columnsCount(); ++i)
    {
        const auto & column_index = row_format.format_idx_to_column_idx[i];
        if (column_index)
        {
            if (header_->columns() <= *column_index)
                row_format.throwInvalidFormat("Column index " + std::to_string(*column_index) +
                                              " must be less then number of columns (" + std::to_string(header_->columns()) + ")", i);
            if (row_format.escaping_rules[i] == EscapingRule::None)
                row_format.throwInvalidFormat("Column is not skipped, but deserialization type is None", i);

            size_t col_idx = *column_index;
            if (column_in_format[col_idx])
                row_format.throwInvalidFormat("Duplicate column", i);
            column_in_format[col_idx] = true;

            checkSupportedDelimiterAfterField(row_format.escaping_rules[i], row_format.delimiters[i + 1], data_types[*column_index]);
        }
        else
        {
            checkSupportedDelimiterAfterField(row_format.escaping_rules[i], row_format.delimiters[i + 1], nullptr);
        }
    }

    for (size_t i = 0; i < header_->columns(); ++i)
        if (!column_in_format[i])
            always_default_columns.push_back(i);
}

void TemplateRowInputFormat::readPrefix()
{
    format_reader->readPrefix();
}

bool TemplateRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & extra)
{
    /// This function can be called again after it returned false
    if (unlikely(end_of_stream))
        return false;

    if (unlikely(format_reader->checkForSuffix()))
    {
        end_of_stream = true;
        return false;
    }

    updateDiagnosticInfo();

    if (likely(getRowNum() != 0))
    {
        if (row_between_delimiter_already_skipped)
            row_between_delimiter_already_skipped = false;
        else
            format_reader->skipRowBetweenDelimiter();
    }

    extra.read_columns.assign(columns.size(), false);

    for (size_t i = 0; i < row_format.columnsCount(); ++i)
    {
        format_reader->skipDelimiter(i);

        if (row_format.format_idx_to_column_idx[i])
        {
            size_t col_idx = *row_format.format_idx_to_column_idx[i];
            extra.read_columns[col_idx] = deserializeField(data_types[col_idx], serializations[col_idx], *columns[col_idx], i);
        }
        else
            format_reader->skipField(row_format.escaping_rules[i]);
    }

    format_reader->skipRowEndDelimiter();

    for (const auto & idx : always_default_columns)
        data_types[idx]->insertDefaultInto(*columns[idx]);

    return true;
}

bool TemplateRowInputFormat::deserializeField(const DataTypePtr & type,
    const SerializationPtr & serialization, IColumn & column, size_t file_column)
{
    EscapingRule escaping_rule = row_format.escaping_rules[file_column];
    updateFormatSettingsIfNeeded(escaping_rule, settings, row_format, default_csv_delimiter, file_column);

    try
    {
        return deserializeFieldByEscapingRule(type, serialization, column, *buf, escaping_rule, settings);
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
            throwUnexpectedEof(getRowNum());
        throw;
    }
}

bool TemplateRowInputFormat::parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out)
{
    out << "Suffix does not match: ";
    size_t last_successfully_parsed_idx = format_reader->getFormatDataIdx() + 1;
    auto * const row_begin_pos = buf->position();
    bool caught = false;
    try
    {
        PeekableReadBufferCheckpoint checkpoint{*buf, true};
        format_reader->tryReadPrefixOrSuffix<void>(last_successfully_parsed_idx, format.columnsCount());
    }
    catch (Exception & e)
    {
        out << e.message() << " Near column " << last_successfully_parsed_idx;
        caught = true;
    }
    if (!caught)
    {
        out << " There is some data after suffix (EOF expected, got ";
        verbosePrintString(buf->position(), std::min(buf->buffer().end(), buf->position() + 16), out);
        out << "). ";
    }
    out << " Format string (from format_schema): \n" << format.dump() << "\n";

    if (row_begin_pos != buf->position())
    {
        /// Pointers to buffer memory were invalidated during checking for suffix
        out << "\nCannot print more diagnostic info.";
        return false;
    }

    out << "\nUsing format string (from format_schema_rows): " << row_format.dump() << "\n";
    out << "\nTrying to parse next row, because suffix does not match:\n";
    if (likely(getRowNum() != 0) && !parseDelimiterWithDiagnosticInfo(out, *buf, row_between_delimiter, "delimiter between rows", ignore_spaces))
        return false;

    for (size_t i = 0; i < row_format.columnsCount(); ++i)
    {
        if (!parseDelimiterWithDiagnosticInfo(out, *buf, row_format.delimiters[i], "delimiter before field " + std::to_string(i), ignore_spaces))
            return false;

        format_reader->skipSpaces();
        if (row_format.format_idx_to_column_idx[i])
        {
            const auto & header = getPort().getHeader();
            size_t col_idx = *row_format.format_idx_to_column_idx[i];
            if (!deserializeFieldAndPrintDiagnosticInfo(header.getByPosition(col_idx).name, data_types[col_idx],
                                                        *columns[col_idx], out, i))
            {
                out << "Maybe it's not possible to deserialize field " + std::to_string(i) +
                       " as " + escapingRuleToString(row_format.escaping_rules[i]);
                return false;
            }
        }
        else
        {
            static const String skipped_column_str = "<SKIPPED COLUMN>";
            static const DataTypePtr skipped_column_type = std::make_shared<DataTypeNothing>();
            static const MutableColumnPtr skipped_column = skipped_column_type->createColumn();
            if (!deserializeFieldAndPrintDiagnosticInfo(skipped_column_str, skipped_column_type, *skipped_column, out, i))
                return false;
        }
    }

    return parseDelimiterWithDiagnosticInfo(out, *buf, row_format.delimiters.back(), "delimiter after last field", ignore_spaces);
}

bool parseDelimiterWithDiagnosticInfo(WriteBuffer & out, ReadBuffer & buf, const String & delimiter, const String & description, bool skip_spaces)
{
    if (skip_spaces)
        skipWhitespaceIfAny(buf);
    try
    {
        assertString(delimiter, buf);
    }
    catch (const DB::Exception &)
    {
        out << "ERROR: There is no " << description << ": expected ";
        verbosePrintString(delimiter.data(), delimiter.data() + delimiter.size(), out);
        out << ", got ";
        if (buf.eof())
            out << "<End of stream>";
        else
            verbosePrintString(buf.position(), std::min(buf.position() + delimiter.size() + 10, buf.buffer().end()), out);
        out << '\n';
        return false;
    }
    return true;
}

void TemplateRowInputFormat::tryDeserializeField(const DataTypePtr & type, IColumn & column, size_t file_column)
{
    const auto & index = row_format.format_idx_to_column_idx[file_column];
    if (index)
        deserializeField(type, serializations[*index], column, file_column);
    else
        format_reader->skipField(row_format.escaping_rules[file_column]);
}

bool TemplateRowInputFormat::isGarbageAfterField(size_t, ReadBuffer::Position)
{
    /// Garbage will be considered as wrong delimiter
    return false;
}

bool TemplateRowInputFormat::allowSyncAfterError() const
{
    return !row_format.delimiters.back().empty() || !row_between_delimiter.empty();
}

void TemplateRowInputFormat::syncAfterError()
{
    skipToNextRowOrEof(*buf, row_format.delimiters.back(), row_between_delimiter, ignore_spaces);
    end_of_stream = buf->eof();
    row_between_delimiter_already_skipped = !end_of_stream;
    /// It can happen that buf->position() is not at the beginning of row
    /// if some delimiters is similar to row_format.delimiters.back() and row_between_delimiter.
    /// It will cause another parsing error.
}

void TemplateRowInputFormat::resetParser()
{
    RowInputFormatWithDiagnosticInfo::resetParser();
    end_of_stream = false;
    row_between_delimiter_already_skipped = false;
}

void TemplateRowInputFormat::setReadBuffer(ReadBuffer & in_)
{
    buf = std::make_unique<PeekableReadBuffer>(in_);
    RowInputFormatWithDiagnosticInfo::setReadBuffer(*buf);
    format_reader->setReadBuffer(*buf);
}

void TemplateRowInputFormat::resetReadBuffer()
{
    buf.reset();
    RowInputFormatWithDiagnosticInfo::resetReadBuffer();
}

TemplateFormatReader::TemplateFormatReader(
    PeekableReadBuffer & buf_,
    bool ignore_spaces_,
    const ParsedTemplateFormatString & format_,
    const ParsedTemplateFormatString & row_format_,
    std::string row_between_delimiter_,
    const FormatSettings & format_settings_)
    : buf(&buf_)
    , ignore_spaces(ignore_spaces_)
    , format(format_)
    , row_format(row_format_)
    , row_between_delimiter(row_between_delimiter_)
    , format_settings(format_settings_)
{
    /// Validate format string for result set
    bool has_data = false;
    for (size_t i = 0; i < format.columnsCount(); ++i)
    {
        if (format.format_idx_to_column_idx[i])
        {
            if (*format.format_idx_to_column_idx[i] != 0)
                format.throwInvalidFormat("Invalid input part", i);
            if (has_data)
                format.throwInvalidFormat("${data} can occur only once", i);
            if (format.escaping_rules[i] != EscapingRule::None)
                format.throwInvalidFormat("${data} must have empty or None deserialization type", i);
            has_data = true;
            format_data_idx = i;
        }
        else
        {
            if (format.escaping_rules[i] == EscapingRule::XML)
                format.throwInvalidFormat("XML deserialization is not supported", i);
        }
    }

    /// Validate format string for rows
    for (size_t i = 0; i < row_format.columnsCount(); ++i)
    {
        if (row_format.escaping_rules[i] == EscapingRule::XML)
            row_format.throwInvalidFormat("XML deserialization is not supported", i);
    }
}

void TemplateFormatReader::readPrefix()
{
    size_t last_successfully_parsed_idx = 0;
    try
    {
        tryReadPrefixOrSuffix<void>(last_successfully_parsed_idx, format_data_idx);
    }
    catch (Exception & e)
    {
        format.throwInvalidFormat(e.message() + " While parsing prefix", last_successfully_parsed_idx);
    }
}

void TemplateFormatReader::skipField(EscapingRule escaping_rule)
{
    try
    {
        skipFieldByEscapingRule(*buf, escaping_rule, format_settings);
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
            throwUnexpectedEof(row_num);
        throw;
    }
}

/// Asserts delimiters and skips fields in prefix or suffix.
/// tryReadPrefixOrSuffix<bool>(...) is used in checkForSuffix() to avoid throwing an exception after read of each row
/// (most likely false will be returned on first call of checkString(...))
template <typename ReturnType>
ReturnType TemplateFormatReader::tryReadPrefixOrSuffix(size_t & input_part_beg, size_t input_part_end)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    skipSpaces();
    if constexpr (throw_exception)
        assertString(format.delimiters[input_part_beg], *buf);
    else
    {
        if (likely(!checkString(format.delimiters[input_part_beg], *buf)))
            return ReturnType(false);
    }

    while (input_part_beg < input_part_end)
    {
        skipSpaces();
        if constexpr (throw_exception)
            skipField(format.escaping_rules[input_part_beg]);
        else
        {
            try
            {
                skipField(format.escaping_rules[input_part_beg]);
            }
            catch (const Exception & e)
            {
                if (e.code() != ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF &&
                    e.code() != ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE &&
                    e.code() != ErrorCodes::CANNOT_PARSE_QUOTED_STRING)
                    throw;
                /// If it's parsing error, then suffix is not found
                return ReturnType(false);
            }
        }
        ++input_part_beg;

        skipSpaces();
        if constexpr (throw_exception)
            assertString(format.delimiters[input_part_beg], *buf);
        else
        {
            if (likely(!checkString(format.delimiters[input_part_beg], *buf)))
                return ReturnType(false);
        }
    }

    if constexpr (!throw_exception)
        return ReturnType(true);
}

/// Returns true if all rows have been read i.e. there are only suffix and spaces (if ignore_spaces == true) before EOF.
/// Otherwise returns false
bool TemplateFormatReader::checkForSuffix()
{
    PeekableReadBufferCheckpoint checkpoint{*buf};
    bool suffix_found = false;
    size_t last_successfully_parsed_idx = format_data_idx + 1;
    try
    {
        suffix_found = tryReadPrefixOrSuffix<bool>(last_successfully_parsed_idx, format.columnsCount());
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF &&
            e.code() != ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE &&
            e.code() != ErrorCodes::CANNOT_PARSE_QUOTED_STRING)
            throw;
    }

    if (unlikely(suffix_found))
    {
        skipSpaces();
        if (buf->eof())
            return true;
    }

    buf->rollbackToCheckpoint();
    return false;
}

void TemplateFormatReader::skipDelimiter(size_t index)
{
    skipSpaces();
    assertString(row_format.delimiters[index], *buf);
    skipSpaces();
}

void TemplateFormatReader::skipRowEndDelimiter()
{
    ++row_num;
    skipSpaces();
    assertString(row_format.delimiters.back(), *buf);
    skipSpaces();
}

void TemplateFormatReader::skipRowBetweenDelimiter()
{
    skipSpaces();
    assertString(row_between_delimiter, *buf);
    skipSpaces();
}

TemplateSchemaReader::TemplateSchemaReader(
    ReadBuffer & in_,
    bool ignore_spaces_,
    const ParsedTemplateFormatString & format_,
    const ParsedTemplateFormatString & row_format_,
    std::string row_between_delimiter,
    const FormatSettings & format_settings_)
    : IRowSchemaReader(buf, format_settings_, getDefaultDataTypeForEscapingRules(row_format_.escaping_rules))
    , buf(in_)
    , format(format_)
    , row_format(row_format_)
    , format_reader(buf, ignore_spaces_, format, row_format, row_between_delimiter, format_settings)
    , default_csv_delimiter(format_settings_.csv.delimiter)
{
    setColumnNames(row_format.column_names);
}

std::optional<DataTypes> TemplateSchemaReader::readRowAndGetDataTypes()
{
    if (first_row)
        format_reader.readPrefix();

    if (format_reader.checkForSuffix())
        return {};

    if (first_row)
        first_row = false;
    else
        format_reader.skipRowBetweenDelimiter();

    DataTypes data_types;
    data_types.reserve(row_format.columnsCount());
    String field;
    for (size_t i = 0; i != row_format.columnsCount(); ++i)
    {
        format_reader.skipDelimiter(i);
        updateFormatSettingsIfNeeded(row_format.escaping_rules[i], format_settings, row_format, default_csv_delimiter, i);
        field = readFieldByEscapingRule(buf, row_format.escaping_rules[i], format_settings);
        data_types.push_back(tryInferDataTypeByEscapingRule(field, format_settings, row_format.escaping_rules[i], &json_inference_info));
    }

    format_reader.skipRowEndDelimiter();
    return data_types;
}

void TemplateSchemaReader::transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type)
{
    transformInferredTypesByEscapingRuleIfNeeded(type, new_type, format_settings, row_format.escaping_rules[field_index], &json_inference_info);
}

static ParsedTemplateFormatString fillResultSetFormat(const FormatSettings & settings)
{
    ParsedTemplateFormatString resultset_format;
    if (settings.template_settings.resultset_format.empty())
    {
        /// Default format string: "${data}"
        resultset_format.delimiters.resize(2);
        resultset_format.escaping_rules.emplace_back(ParsedTemplateFormatString::EscapingRule::None);
        resultset_format.format_idx_to_column_idx.emplace_back(0);
        resultset_format.column_names.emplace_back("data");
    }
    else
    {
        /// Read format string from file
        resultset_format = ParsedTemplateFormatString(
            FormatSchemaInfo(
                /*format_schema_source=*/FormatSettings::FORMAT_SCHEMA_SOURCE_FILE,
                /*format_schema=*/settings.template_settings.resultset_format,
                /*format_schema_message_name=*/"",
                /*format=*/"Template",
                /*require_message=*/false,
                /*is_server=*/settings.schema.is_server,
                /*format_schema_path=*/settings.schema.format_schema_path),
            [&](const String & partName) -> std::optional<size_t>
            {
                if (partName == "data")
                    return 0;
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Unknown input part {}", partName);
            });
    }
    return resultset_format;
}

static ParsedTemplateFormatString fillRowFormat(const FormatSettings & settings, ParsedTemplateFormatString::ColumnIdxGetter idx_getter, bool allow_indexes)
{
    return ParsedTemplateFormatString(
        FormatSchemaInfo(
            /*format_schema_source=*/FormatSettings::FORMAT_SCHEMA_SOURCE_FILE,
            /*format_schema=*/settings.template_settings.row_format,
            /*format_schema_message_name=*/"",
            /*format=*/"Template",
            /*require_message=*/false,
            /*is_server=*/settings.schema.is_server,
            /*format_schema_path=*/settings.schema.format_schema_path),
        idx_getter,
        allow_indexes);
}

void registerInputFormatTemplate(FormatFactory & factory);
void registerInputFormatTemplate(FormatFactory & factory)
{
    for (bool ignore_spaces : {false, true})
    {
        factory.registerInputFormat(ignore_spaces ? "TemplateIgnoreSpaces" : "Template", [=](
                ReadBuffer & buf,
                const Block & sample,
                IRowInputFormat::Params params,
                const FormatSettings & settings)
        {
            auto idx_getter = [&](const String & colName) -> std::optional<size_t>
            {
                return sample.getPositionByName(colName);
            };

            return std::make_shared<TemplateRowInputFormat>(
                std::make_shared<const Block>(sample),
                buf,
                params,
                settings,
                ignore_spaces,
                fillResultSetFormat(settings),
                fillRowFormat(settings, idx_getter, true),
                settings.template_settings.row_between_delimiter);
        });
    }

    factory.setDocumentation("Template", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

For cases where you need more customization than other standard formats offer, 
the `Template` format allows the user to specify their own custom format string with placeholders for values,
and specifying escaping rules for the data.

It uses the following settings:

| Setting                                                                                                  | Description                                                                                                                |
|----------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| [`format_template_row`](#format_template_row)                                                            | Specifies the path to the file which contains format strings for rows.                                                     |
| [`format_template_resultset`](#format_template_resultset)                                                | Specifies the path to the file which contains format strings for rows                                                      |
| [`format_template_rows_between_delimiter`](#format_template_rows_between_delimiter)                      | Specifies the delimiter between rows, which is printed (or expected) after every row except the last one (`\n` by default) |
| `format_template_row_format`                                                                             | Specifies the format string for rows [in-line](#inline_specification).                                                     |                                                                           
| `format_template_resultset_format`                                                                       | Specifies the result set format string [in-line](#inline_specification).                                                   |
| Some settings of other formats (e.g.`output_format_json_quote_64bit_integers` when using `JSON` escaping |                                                                                                                            |

## Settings and escaping rules {#settings-and-escaping-rules}

### format_template_row {#format_template_row}

The setting `format_template_row` specifies the path to the file which contains format strings for rows with the following syntax:

```text
delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N
```

Where:

| Part of syntax | Description                                                                                                       |
|----------------|-------------------------------------------------------------------------------------------------------------------|
| `delimiter_i`  | A delimiter between values (`$` symbol can be escaped as `$$`)                                                    |
| `column_i`     | The name or index of a column whose values are to be selected or inserted (if empty, then the column will be skipped) |
|`serializeAs_i` | An escaping rule for the column values.                                                                           |

The following escaping rules are supported:

| Escaping Rule        | Description                              |
|----------------------|------------------------------------------|
| `CSV`, `JSON`, `XML` | Similar to the formats of the same names |
| `Escaped`            | Similar to `TSV`                         |
| `Quoted`             | Similar to `Values`                      |
| `Raw`                | Without escaping, similar to `TSVRaw`    |   
| `None`               | No escaping rule - see note below        |

:::note
If an escaping rule is omitted, then `None` will be used. `XML` is suitable only for output.
:::

Let's look at an example. Given the following format string:

```text
Search phrase: ${s:Quoted}, count: ${c:Escaped}, ad price: $$${p:JSON};
```

The following values will be printed (if using `SELECT`) or expected (if using `INPUT`), 
between columns `Search phrase:`, `, count:`, `, ad price: $` and `;` delimiters respectively:

- `s` (with escape rule `Quoted`)
- `c` (with escape rule `Escaped`)
- `p` (with escape rule `JSON`)

For example:

- If `INSERT`ing, the line below matches the expected template and would read values `bathroom interior design`, `2166`, `$3` into columns `Search phrase`, `count`, `ad price`.
- If `SELECT`ing the line below is the output, assuming that values `bathroom interior design`, `2166`, `$3` are already stored in a table under columns `Search phrase`, `count`, `ad price`.  

```yaml
Search phrase: 'bathroom interior design', count: 2166, ad price: $3;
```

### format_template_rows_between_delimiter {#format_template_rows_between_delimiter}

The setting `format_template_rows_between_delimiter` setting specifies the delimiter between rows, which is printed (or expected) after every row except the last one (`\n` by default)

### format_template_resultset {#format_template_resultset}

The setting `format_template_resultset` specifies the path to the file, which contains a format string for the result set. 

The format string for the result set has the same syntax as a format string for rows. 
It allows for specifying a prefix, a suffix and a way to print some additional information and contains the following placeholders instead of column names:

- `data` is the rows with data in `format_template_row` format, separated by `format_template_rows_between_delimiter`. This placeholder must be the first placeholder in the format string.
- `totals` is the row with total values in `format_template_row` format (when using WITH TOTALS).
- `min` is the row with minimum values in `format_template_row` format (when extremes are set to 1).
- `max` is the row with maximum values in `format_template_row` format (when extremes are set to 1).
- `rows` is the total number of output rows.
- `rows_before_limit` is the minimal number of rows there would have been without LIMIT. Output only if the query contains LIMIT. If the query contains GROUP BY, rows_before_limit_at_least is the exact number of rows there would have been without a LIMIT.
- `time` is the request execution time in seconds.
- `rows_read` is the number of rows has been read.
- `bytes_read` is the number of bytes (uncompressed) has been read.

The placeholders `data`, `totals`, `min` and `max` must not have escaping rule specified (or `None` must be specified explicitly). The remaining placeholders may have any escaping rule specified.

:::note
If the `format_template_resultset` setting is an empty string, `${data}` is used as the default value.
:::

For insert queries format allows skipping some columns or fields if prefix or suffix (see example).

### In-line specification {#inline_specification}

Often times it is challenging or not possible to deploy the format configurations
(set by `format_template_row`, `format_template_resultset`) for the template format to a directory on all nodes in a cluster. 
Furthermore, the format may be so trivial that it does not require being placed in a file.

For these cases, `format_template_row_format` (for `format_template_row`) and `format_template_resultset_format` (for `format_template_resultset`) can be used to set the template string directly in the query, 
rather than as a path to the file which contains it.

:::note
The rules for format strings and escape sequences are the same as those for:
- [`format_template_row`](#format_template_row) when using `format_template_row_format`.
- [`format_template_resultset`](#format_template_resultset) when using `format_template_resultset_format`.
:::

## Example usage {#example-usage}

Let's look at two examples of how we can use the `Template` format, first for selecting data and then for inserting data.

### Selecting data {#selecting-data}

```sql title="Query"
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 5 FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = '\n    '
```

```text title="/some/path/resultset.format"
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    ${data}
  </table>
  <table border="1"> <caption>Max</caption>
    ${max}
  </table>
  <b>Processed ${rows_read:XML} rows in ${time:XML} sec</b>
 </body>
</html>
```

```text title="/some/path/row.format"
<tr> <td>${0:XML}</td> <td>${1:XML}</td> </tr>
```

```html title="Response"
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    <tr> <td></td> <td>8267016</td> </tr>
    <tr> <td>bathroom interior design</td> <td>2166</td> </tr>
    <tr> <td>clickhouse</td> <td>1655</td> </tr>
    <tr> <td>spring 2014 fashion</td> <td>1549</td> </tr>
    <tr> <td>freeform photos</td> <td>1480</td> </tr>
  </table>
  <table border="1"> <caption>Max</caption>
    <tr> <td></td> <td>8873898</td> </tr>
  </table>
  <b>Processed 3095973 rows in 0.1569913 sec</b>
 </body>
</html>
```

### Inserting data {#inserting-data}

```text
Some header
Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
Total rows: 2
```

```sql
INSERT INTO UserActivity SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format'
FORMAT Template
```

```text title="/some/path/resultset.format"
Some header\n${data}\nTotal rows: ${:CSV}\n
```

```text title="/some/path/row.format"
Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}
```

`PageViews`, `UserID`, `Duration` and `Sign` inside placeholders are names of columns in the table. Values after `Useless field` in rows and after `\nTotal rows:` in suffix will be ignored.
All delimiters in the input data must be strictly equal to delimiters in specified format strings.

### In-line specification {#in-line-specification}

Tired of manually formatting markdown tables? In this example we'll look at how we can use the `Template` format and in-line specification settings to achieve a simple task - `SELECT`ing the names of some ClickHouse formats from the `system.formats` table and formatting them as a markdown table. This can be easily achieved using the `Template` format and settings `format_template_row_format` and `format_template_resultset_format`.

In previous examples we specified the result-set and row format strings in separate files, with the paths to those files specified using the `format_template_resultset` and `format_template_row` settings respectively. Here we'll do it in-line because our template is trivial, consisting only of a few `|` and `-` to make the markdown table. We'll specify our result-set template string using the setting `format_template_resultset_format`. To make the table header we've added `|ClickHouse Formats|\n|---|\n` before `${data}`. We use setting `format_template_row_format` to specify the template string `` |`{0:XML}`| `` for our rows. The `Template` format will insert our rows with the given format into placeholder `${data}`. In this example we have only one column, but if you wanted to add more you could do so by adding `{1:XML}`, `{2:XML}`... etc to your row template string, choosing the escaping rule as appropriate. In this example we've gone with escaping rule `XML`. 

```sql title="Query"
WITH formats AS
(
 SELECT * FROM system.formats
 ORDER BY rand()
 LIMIT 5
)
SELECT * FROM formats
FORMAT Template
SETTINGS
 format_template_row_format='|`${0:XML}`|',
 format_template_resultset_format='|ClickHouse Formats|\n|---|\n${data}\n'
```

Look at that! We've saved ourselves the trouble of having to manually add all those `|`s and `-`s to make that markdown table:

```response title="Response"
|ClickHouse Formats|
|---|
|`BSONEachRow`|
|`CustomSeparatedWithNames`|
|`Prometheus`|
|`DWARF`|
|`Avro`|
```
)DOCS_MD"});

    factory.setDocumentation("TemplateIgnoreSpaces", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |       |

## Description {#description}

Similar to [`Template`], but skips whitespace characters between delimiters and values in the input stream. 
However, if format strings contain whitespace characters, these characters will be expected in the input stream. 
Also allows specifying empty placeholders (`${}` or `${:None}`) to split some delimiter into separate parts to ignore spaces between them. 
Such placeholders are used only for skipping whitespace characters.
It's possible to read `JSON` using this format if the values of columns have the same order in all rows.

:::note
This format is suitable only for input.
:::

## Example usage {#example-usage}

The following request can be used for inserting data from its output example of format [JSON](/interfaces/formats/JSON):

```sql
INSERT INTO table_name 
SETTINGS
    format_template_resultset = '/some/path/resultset.format',
    format_template_row = '/some/path/row.format',
    format_template_rows_between_delimiter = ','
FORMAT TemplateIgnoreSpaces
```

```text title="/some/path/resultset.format"
{${}"meta"${}:${:JSON},${}"data"${}:${}[${data}]${},${}"totals"${}:${:JSON},${}"extremes"${}:${:JSON},${}"rows"${}:${:JSON},${}"rows_before_limit_at_least"${}:${:JSON}${}}
```

```text title="/some/path/row.format"
{${}"SearchPhrase"${}:${}${phrase:JSON}${},${}"c"${}:${}${cnt:JSON}${}}
```

## Format settings {#format-settings}
)DOCS_MD"});
}

void registerTemplateSchemaReader(FormatFactory & factory);
void registerTemplateSchemaReader(FormatFactory & factory)
{
    for (bool ignore_spaces : {false, true})
    {
        String format_name = ignore_spaces ? "TemplateIgnoreSpaces" : "Template";
        factory.registerSchemaReader(format_name, [ignore_spaces](ReadBuffer & buf, const FormatSettings & settings)
        {
            size_t index = 0;
            auto idx_getter = [&](const String &) -> std::optional<size_t> { return index++; };
            auto row_format = fillRowFormat(settings, idx_getter, false);
            return std::make_shared<TemplateSchemaReader>(buf, ignore_spaces, fillResultSetFormat(settings), row_format, settings.template_settings.row_between_delimiter, settings);
        });
        factory.registerAdditionalInfoForSchemaCacheGetter(format_name, [](const FormatSettings & settings)
        {
            size_t index = 0;
            auto idx_getter = [&](const String &) -> std::optional<size_t> { return index++; };
            ParsedTemplateFormatString row_format;
            if (!settings.template_settings.row_format.empty())
                row_format = fillRowFormat(settings, idx_getter, false);
            std::unordered_set<FormatSettings::EscapingRule> visited_escaping_rules;
            String result = fmt::format("row_format={}, resultset_format={}, row_between_delimiter={}",
                settings.template_settings.row_format,
                settings.template_settings.resultset_format,
                settings.template_settings.row_between_delimiter);
            for (auto escaping_rule : row_format.escaping_rules)
            {
                if (!visited_escaping_rules.contains(escaping_rule))
                    result += ", " + getAdditionalFormatInfoByEscapingRule(settings, settings.regexp.escaping_rule);
                visited_escaping_rules.insert(escaping_rule);
            }
            return result;
        });
    }
}

}
