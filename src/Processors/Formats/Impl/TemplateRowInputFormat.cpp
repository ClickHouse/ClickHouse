#include <Processors/Formats/Impl/TemplateRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/verbosePrintString.h>
#include <Formats/EscapingRuleUtils.h>
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
    throw ParsingException("Unexpected EOF while parsing row " + std::to_string(row_num) + ". "
                           "Maybe last row has wrong format or input doesn't contain specified suffix before EOF.",
                           ErrorCodes::CANNOT_READ_ALL_DATA);
}

TemplateRowInputFormat::TemplateRowInputFormat(
    const Block & header_,
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

TemplateRowInputFormat::TemplateRowInputFormat(const Block & header_, std::unique_ptr<PeekableReadBuffer> buf_, const Params & params_,
                                               FormatSettings settings_, bool ignore_spaces_,
                                               ParsedTemplateFormatString format_, ParsedTemplateFormatString row_format_,
                                               std::string row_between_delimiter_)
    : RowInputFormatWithDiagnosticInfo(header_, *buf_, params_), buf(std::move(buf_)), data_types(header_.getDataTypes()),
      settings(std::move(settings_)), ignore_spaces(ignore_spaces_),
      format(std::move(format_)), row_format(std::move(row_format_)),
      default_csv_delimiter(settings.csv.delimiter), row_between_delimiter(row_between_delimiter_),
      format_reader(std::make_unique<TemplateFormatReader>(*buf, ignore_spaces_, format, row_format, row_between_delimiter, settings))
{
    /// Validate format string for rows
    std::vector<UInt8> column_in_format(header_.columns(), false);
    for (size_t i = 0; i < row_format.columnsCount(); ++i)
    {
        if (row_format.format_idx_to_column_idx[i])
        {
            if (header_.columns() <= *row_format.format_idx_to_column_idx[i])
                row_format.throwInvalidFormat("Column index " + std::to_string(*row_format.format_idx_to_column_idx[i]) +
                                              " must be less then number of columns (" + std::to_string(header_.columns()) + ")", i);
            if (row_format.escaping_rules[i] == EscapingRule::None)
                row_format.throwInvalidFormat("Column is not skipped, but deserialization type is None", i);

            size_t col_idx = *row_format.format_idx_to_column_idx[i];
            if (column_in_format[col_idx])
                row_format.throwInvalidFormat("Duplicate column", i);
            column_in_format[col_idx] = true;
        }
    }

    for (size_t i = 0; i < header_.columns(); ++i)
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

    if (likely(row_num != 1))
        format_reader->skipRowBetweenDelimiter();

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
    if (escaping_rule == EscapingRule::CSV)
        /// Will read unquoted string until settings.csv.delimiter
        settings.csv.delimiter = row_format.delimiters[file_column + 1].empty() ? default_csv_delimiter :
                                                                                row_format.delimiters[file_column + 1].front();
    try
    {
        return deserializeFieldByEscapingRule(type, serialization, column, *buf, escaping_rule, settings);
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
            throwUnexpectedEof(row_num);
        throw;
    }
}

bool TemplateRowInputFormat::parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out)
{
    out << "Suffix does not match: ";
    size_t last_successfully_parsed_idx = format_reader->getFormatDataIdx() + 1;
    const ReadBuffer::Position row_begin_pos = buf->position();
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
    if (likely(row_num != 1) && !parseDelimiterWithDiagnosticInfo(out, *buf, row_between_delimiter, "delimiter between rows", ignore_spaces))
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
    /// It can happen that buf->position() is not at the beginning of row
    /// if some delimiters is similar to row_format.delimiters.back() and row_between_delimiter.
    /// It will cause another parsing error.
}

void TemplateRowInputFormat::resetParser()
{
    RowInputFormatWithDiagnosticInfo::resetParser();
    end_of_stream = false;
    buf->reset();
}

void TemplateRowInputFormat::setReadBuffer(ReadBuffer & in_)
{
    buf = std::make_unique<PeekableReadBuffer>(in_);
    IInputFormat::setReadBuffer(*buf);
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
    , format_settings(format_settings_)
    , format_reader(buf, ignore_spaces_, format, row_format, row_between_delimiter, format_settings)
{
    setColumnNames(row_format.column_names);
}

DataTypes TemplateSchemaReader::readRowAndGetDataTypes()
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
        if (row_format.escaping_rules[i] == FormatSettings::EscapingRule::CSV)
            format_settings.csv.delimiter = row_format.delimiters[i + 1].empty() ? format_settings.csv.delimiter : row_format.delimiters[i + 1].front();

        field = readFieldByEscapingRule(buf, row_format.escaping_rules[i], format_settings);
        data_types.push_back(determineDataTypeByEscapingRule(field, format_settings, row_format.escaping_rules[i]));
    }

    format_reader.skipRowEndDelimiter();
    return data_types;
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
            FormatSchemaInfo(settings.template_settings.resultset_format, "Template", false,
                             settings.schema.is_server, settings.schema.format_schema_path),
            [&](const String & partName) -> std::optional<size_t>
            {
                if (partName == "data")
                    return 0;
                throw Exception("Unknown input part " + partName,
                                ErrorCodes::SYNTAX_ERROR);
            });
    }
    return resultset_format;
}

static ParsedTemplateFormatString fillRowFormat(const FormatSettings & settings, ParsedTemplateFormatString::ColumnIdxGetter idx_getter, bool allow_indexes)
{
    return ParsedTemplateFormatString(
        FormatSchemaInfo(
            settings.template_settings.row_format, "Template", false, settings.schema.is_server, settings.schema.format_schema_path),
        idx_getter, allow_indexes);
}

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
                sample,
                buf,
                params,
                settings,
                ignore_spaces,
                fillResultSetFormat(settings),
                fillRowFormat(settings, idx_getter, true),
                settings.template_settings.row_between_delimiter);
        });
    }
}

void registerTemplateSchemaReader(FormatFactory & factory)
{
    for (bool ignore_spaces : {false, true})
    {
        factory.registerSchemaReader(ignore_spaces ? "TemplateIgnoreSpaces" : "Template", [ignore_spaces](ReadBuffer & buf, const FormatSettings & settings)
        {
            size_t index = 0;
            auto idx_getter = [&](const String &) -> std::optional<size_t> { return index++; };
            auto row_format = fillRowFormat(settings, idx_getter, false);
            return std::make_shared<TemplateSchemaReader>(buf, ignore_spaces, fillResultSetFormat(settings), row_format, settings.template_settings.row_between_delimiter, settings);
        });
    }
}

}
