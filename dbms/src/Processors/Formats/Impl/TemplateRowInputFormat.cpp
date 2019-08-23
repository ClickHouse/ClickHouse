#include <Processors/Formats/Impl/TemplateRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/verbosePrintString.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_TEMPLATE_FORMAT;
extern const int ATTEMPT_TO_READ_AFTER_EOF;
extern const int CANNOT_READ_ALL_DATA;
}


TemplateRowInputFormat::TemplateRowInputFormat(ReadBuffer & in_, const Block & header_, const Params & params_,
        const FormatSettings & settings_, bool ignore_spaces_)
    : RowInputFormatWithDiagnosticInfo(header_, in_, params_), buf(in_), data_types(header_.getDataTypes()),
    settings(settings_), ignore_spaces(ignore_spaces_)
{
    static const String default_format("${data}");
    const String & format_str = settings.template_settings.format.empty() ? default_format : settings.template_settings.format;
    format = ParsedTemplateFormat(format_str, [&](const String & partName)
    {
        if (partName == "data")
            return 0;
        throw Exception("invalid template format: unknown input part " + partName, ErrorCodes::INVALID_TEMPLATE_FORMAT);
    });

    if (format.formats.size() != 1 || format.formats[0] != ColumnFormat::Default)
        throw Exception("invalid template format: format_schema must be \"prefix ${data} suffix\"", ErrorCodes::INVALID_TEMPLATE_FORMAT);


    row_format = ParsedTemplateFormat(settings.template_settings.row_format, [&](const String & colName)
    {
        return header_.getPositionByName(colName);
    });

    std::vector<UInt8> column_in_format(header_.columns(), false);
    for (size_t i = 0; i < row_format.columnsCount(); ++i)
    {
        size_t col_idx = row_format.format_idx_to_column_idx[i];
        if (column_in_format[col_idx])
            throw Exception("invalid template format: duplicate column " + header_.getColumnsWithTypeAndName()[col_idx].name,
                    ErrorCodes::INVALID_TEMPLATE_FORMAT);
        column_in_format[col_idx] = true;

        if (row_format.formats[i] == ColumnFormat::Xml || row_format.formats[i] == ColumnFormat::Raw)
            throw Exception("invalid template format: XML and Raw deserialization is not supported", ErrorCodes::INVALID_TEMPLATE_FORMAT);
    }
}

void TemplateRowInputFormat::readPrefix()
{
    skipSpaces();
    assertString(format.delimiters.front(), buf);
}

bool TemplateRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & extra)
{
    skipSpaces();

    if (checkForSuffix())
        return false;

    updateDiagnosticInfo();

    if (likely(row_num != 1))
        assertString(settings.template_settings.row_between_delimiter, buf);

    extra.read_columns.assign(columns.size(), false);

    for (size_t i = 0; i < row_format.columnsCount(); ++i)
    {
        skipSpaces();
        assertString(row_format.delimiters[i], buf);
        size_t col_idx = row_format.format_idx_to_column_idx[i];
        skipSpaces();
        deserializeField(*data_types[col_idx], *columns[col_idx], row_format.formats[i]);
        extra.read_columns[col_idx] = true;
    }

    skipSpaces();
    assertString(row_format.delimiters.back(), buf);

    for (size_t i = 0; i < columns.size(); ++i)
        if (!extra.read_columns[i])
            data_types[row_format.format_idx_to_column_idx[i]]->insertDefaultInto(*columns[i]);

    return true;
}

void TemplateRowInputFormat::deserializeField(const IDataType & type, IColumn & column, ColumnFormat col_format)
{
    try
    {
        switch (col_format)
        {
            case ColumnFormat::Default:
            case ColumnFormat::Escaped:
                type.deserializeAsTextEscaped(column, buf, settings);
                break;
            case ColumnFormat::Quoted:
                type.deserializeAsTextQuoted(column, buf, settings);
                break;
            case ColumnFormat::Csv:
                type.deserializeAsTextCSV(column, buf, settings);
                break;
            case ColumnFormat::Json:
                type.deserializeAsTextJSON(column, buf, settings);
                break;
            default:
                break;
        }
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
            throwUnexpectedEof();
        throw;
    }
}

/// Returns true if all rows have been read i.e. there are only suffix and spaces (if ignnore_spaces == true) before EOF.
/// Otherwise returns false
bool TemplateRowInputFormat::checkForSuffix()
{
    if (unlikely(synced_after_error_at_last_row))
        return true;

    StringRef suffix(format.delimiters.back());
    if (likely(!compareSuffixPart(suffix, buf.position(), buf.available())))
        return false;

    while (buf.peekNext())
    {
        BufferBase::Buffer peeked = buf.lastPeeked();
        if (likely(!compareSuffixPart(suffix, peeked.begin(), peeked.size())))
            return false;
    }

    if (suffix.size)
        throwUnexpectedEof();
    return true;
}

/// Returns true if buffer contains only suffix and maybe some spaces after it
/// If there are not enough data in buffer, compares available data and removes it from reference to suffix
bool TemplateRowInputFormat::compareSuffixPart(StringRef & suffix, BufferBase::Position pos, size_t available)
{
    if (suffix.size < available)
    {
        if (!ignore_spaces)
            return false;
        if (likely(suffix != StringRef(pos, suffix.size)))
            return false;

        BufferBase::Position end = pos + available;
        pos += suffix.size;
        suffix.size = 0;
        while (pos != end)
            if (!isWhitespaceASCII(*pos++))
                return false;
        return true;
    }

    if (likely(StringRef(suffix.data, available) != StringRef(pos, available)))
        return false;
    suffix.data += available;
    suffix.size -= available;
    return true;
}

bool TemplateRowInputFormat::parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out)
{
    try
    {
        if (likely(row_num != 1))
            assertString(settings.template_settings.row_between_delimiter, buf);
    }
    catch (const DB::Exception &)
    {
        writeErrorStringForWrongDelimiter(out, "delimiter between rows", settings.template_settings.row_between_delimiter);

        return false;
    }
    for (size_t i = 0; i < row_format.columnsCount(); ++i)
    {
        skipSpaces();
        try
        {
            assertString(row_format.delimiters[i], buf);
        }
        catch (const DB::Exception &)
        {
            writeErrorStringForWrongDelimiter(out, "delimiter before field " + std::to_string(i), row_format.delimiters[i]);
            return false;
        }

        skipSpaces();
        auto & header = getPort().getHeader();
        size_t col_idx = row_format.format_idx_to_column_idx[i];
        if (!deserializeFieldAndPrintDiagnosticInfo(header.getByPosition(col_idx).name, data_types[col_idx], *columns[col_idx], out, i))
        {
            out << "Maybe it's not possible to deserialize field " + std::to_string(i) +
                   " as " + ParsedTemplateFormat::formatToString(row_format.formats[i]);
            return false;
        }
    }

    skipSpaces();
    try
    {
        assertString(row_format.delimiters.back(), buf);
    }
    catch (const DB::Exception &)
    {
        writeErrorStringForWrongDelimiter(out, "delimiter after last field", row_format.delimiters.back());
        return false;
    }

    return true;
}

void TemplateRowInputFormat::writeErrorStringForWrongDelimiter(WriteBuffer & out, const String & description, const String & delim)
{
    out << "ERROR: There is no " << description << ": expected ";
    verbosePrintString(delim.data(), delim.data() + delim.size(), out);
    out << ", got ";
    if (buf.eof())
        out << "<End of stream>";
    else
        verbosePrintString(buf.position(), std::min(buf.position() + delim.size() + 10, buf.buffer().end()), out);
    out << '\n';
}

void TemplateRowInputFormat::tryDeserializeFiled(const DataTypePtr & type, IColumn & column, size_t input_position, ReadBuffer::Position & prev_pos,
                                                 ReadBuffer::Position & curr_pos)
{
    prev_pos = buf.position();
    deserializeField(*type, column, row_format.formats[input_position]);
    curr_pos = buf.position();
}

bool TemplateRowInputFormat::isGarbageAfterField(size_t, ReadBuffer::Position)
{
    /// Garbage will be considered as wrong delimiter
    return false;
}

bool TemplateRowInputFormat::allowSyncAfterError() const
{
    return !row_format.delimiters.back().empty() || !settings.template_settings.row_between_delimiter.empty();
}

void TemplateRowInputFormat::syncAfterError()
{
    skipToNextDelimiterOrEof(row_format.delimiters.back());
    if (buf.eof())
    {
        synced_after_error_at_last_row = true;
        return;
    }
    buf.ignore(row_format.delimiters.back().size());

    skipSpaces();
    if (checkForSuffix())
        return;

    skipToNextDelimiterOrEof(settings.template_settings.row_between_delimiter);
    if (buf.eof())
        synced_after_error_at_last_row = true;
}

/// Searches for delimiter in input stream and sets buffer position to the beginning of delimiter (if found) or EOF (if not)
void TemplateRowInputFormat::skipToNextDelimiterOrEof(const String & delimiter)
{
    StringRef delim(delimiter);
    if (!delim.size) return;
    while (!buf.eof())
    {
        void* pos = memchr(buf.position(), *delim.data, buf.available());
        if (!pos)
        {
            buf.position() += buf.available();
            continue;
        }

        buf.position() = static_cast<ReadBuffer::Position>(pos);

        /// Peek data until we can compare it with whole delim
        while (buf.available() < delim.size && buf.peekNext());

        if (buf.available() < delim.size)
            buf.position() += buf.available();      /// EOF, there is no delim
        else if (delim != StringRef(buf.position(), delim.size))
            ++buf.position();
        else
            return;
    }
}

void TemplateRowInputFormat::throwUnexpectedEof()
{
    throw Exception("Unexpected EOF while parsing row " + std::to_string(row_num) + ". "
                    "Maybe last row has wrong format or input doesn't contain specified suffix before EOF.",
                    ErrorCodes::CANNOT_READ_ALL_DATA);
}


void registerInputFormatProcessorTemplate(FormatFactory & factory)
{
    for (bool ignore_spaces : {false, true})
    {
        factory.registerInputFormatProcessor(ignore_spaces ? "TemplateIgnoreSpaces" : "Template", [=](
                ReadBuffer & buf,
                const Block & sample,
                const Context &,
                IRowInputFormat::Params params,
                const FormatSettings & settings)
        {
            return std::make_shared<TemplateRowInputFormat>(buf, sample, std::move(params), settings, ignore_spaces);
        });
    }
}

}
