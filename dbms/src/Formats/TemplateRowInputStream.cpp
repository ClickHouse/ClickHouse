#include <Formats/TemplateRowInputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/BlockInputStreamFromRowInputStream.h>
#include <Formats/verbosePrintString.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_TEMPLATE_FORMAT;
}


TemplateRowInputStream::TemplateRowInputStream(ReadBuffer & istr_, const Block & header_, const FormatSettings & settings_,
        bool ignore_spaces_)
    : RowInputStreamWithDiagnosticInfo(buf, header_), buf(istr_), settings(settings_), ignore_spaces(ignore_spaces_)
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
        return header.getPositionByName(colName);
    });

    std::vector<UInt8> column_in_format(header.columns(), false);
    for (size_t i = 0; i < row_format.columnsCount(); ++i)
    {
        size_t col_idx = row_format.format_idx_to_column_idx[i];
        if (column_in_format[col_idx])
            throw Exception("invalid template format: duplicate column " + header.getColumnsWithTypeAndName()[col_idx].name,
                    ErrorCodes::INVALID_TEMPLATE_FORMAT);
        column_in_format[col_idx] = true;

        if (row_format.formats[i] == ColumnFormat::Xml || row_format.formats[i] == ColumnFormat::Raw)
            throw Exception("invalid template format: XML and Raw deserialization is not supported", ErrorCodes::INVALID_TEMPLATE_FORMAT);
    }
}

void TemplateRowInputStream::readPrefix()
{
    skipSpaces();
    assertString(format.delimiters.front(), buf);
}

bool TemplateRowInputStream::read(MutableColumns & columns, RowReadExtension & extra)
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
            header.getByPosition(i).type->insertDefaultInto(*columns[i]);

    return true;
}

void TemplateRowInputStream::deserializeField(const IDataType & type, IColumn & column, ColumnFormat col_format)
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
        case ColumnFormat::Json:
            type.deserializeAsTextJSON(column, buf, settings);
            break;
        default:
            break;
    }
}

/// Returns true if all rows have been read i.e. there are only suffix and spaces (if ignnore_spaces == true) before EOF.
/// Otherwise returns false
bool TemplateRowInputStream::checkForSuffix()
{
    StringRef suffix(format.delimiters.back());
    if (likely(!compareSuffixPart(suffix, buf.position(), buf.available())))
        return false;

    while (buf.peekNext())
    {
        BufferBase::Buffer peeked = buf.lastPeeked();
        if (likely(!compareSuffixPart(suffix, peeked.begin(), peeked.size())))
            return false;
    }
    return suffix.size == 0;
}

/// Returns true if buffer contains only suffix and maybe some spaces after it
/// If there are not enough data in buffer, compares available data and removes it from reference to suffix
bool TemplateRowInputStream::compareSuffixPart(StringRef & suffix, BufferBase::Position pos, size_t available)
{
    if (suffix.size < available)
    {
        if (!ignore_spaces)
            return false;
        if (likely(suffix != StringRef(pos, suffix.size)))
            return false;
        suffix.size = 0;
        pos += suffix.size;
        BufferBase::Position end = pos + available;
        while (pos != end)
            if (!isWhitespaceASCII(*pos))
                return false;
    }

    if (likely(StringRef(suffix.data, available) != StringRef(pos, available)))
        return false;
    suffix.data += available;
    suffix.size -= available;
    return true;
}

bool TemplateRowInputStream::parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out,
                                                            size_t max_length_of_column_name, size_t max_length_of_data_type_name)
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
        size_t col_idx = row_format.format_idx_to_column_idx[i];
        if (!deserializeFieldAndPrintDiagnosticInfo(columns, out, max_length_of_column_name, max_length_of_data_type_name, col_idx))
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

void TemplateRowInputStream::writeErrorStringForWrongDelimiter(WriteBuffer & out, const String & description, const String & delim)
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

void TemplateRowInputStream::tryDeserializeFiled(MutableColumns & columns, size_t col_idx, ReadBuffer::Position & prev_pos,
                                                 ReadBuffer::Position & curr_pos)
{
    prev_pos = buf.position();
    auto format_iter = std::find(row_format.format_idx_to_column_idx.cbegin(), row_format.format_idx_to_column_idx.cend(), col_idx);
    if (format_iter == row_format.format_idx_to_column_idx.cend())
        throw DB::Exception("Parse error", ErrorCodes::INVALID_TEMPLATE_FORMAT);
    size_t format_idx = format_iter - row_format.format_idx_to_column_idx.begin();
    deserializeField(*data_types[col_idx], *columns[col_idx], row_format.formats[format_idx]);
    curr_pos = buf.position();
}

bool TemplateRowInputStream::isGarbageAfterField(size_t, ReadBuffer::Position)
{
    /// Garbage will be considered as wrong delimiter
    return false;
}


void registerInputFormatTemplate(FormatFactory & factory)
{
    for (bool ignore_spaces : {false, true})
    {
        factory.registerInputFormat(ignore_spaces ? "TemplateIgnoreSpaces" : "Template", [=](
                ReadBuffer & buf,
                const Block & sample,
                const Context &,
                UInt64 max_block_size,
                const FormatSettings & settings)
        {
            return std::make_shared<BlockInputStreamFromRowInputStream>(
                    std::make_shared<TemplateRowInputStream>(buf, sample, settings, ignore_spaces),
                    sample, max_block_size, settings);
        });
    }
}

}
