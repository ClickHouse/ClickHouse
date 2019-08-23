#include <Formats/TemplateBlockOutputStream.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_TEMPLATE_FORMAT;
}

ParsedTemplateFormat::ParsedTemplateFormat(const String & format_string, const ColumnIdxGetter & idxByName)
{
    enum ParserState
    {
        Delimiter,
        Column,
        Format
    };
    const char * pos = format_string.c_str();
    const char * token_begin = pos;
    ParserState state = Delimiter;
    delimiters.emplace_back();
    for (; *pos; ++pos)
    {
        switch (state)
        {
        case Delimiter:
            if (*pos == '$')
            {
                delimiters.back().append(token_begin, pos - token_begin);
                ++pos;
                if (*pos == '{')
                {
                    token_begin = pos + 1;
                    state = Column;
                }
                else if (*pos == '$')
                {
                    token_begin = pos;
                }
                else
                {
                    throw Exception("invalid template: pos " + std::to_string(pos - format_string.c_str()) +
                    ": expected '{' or '$' after '$'", ErrorCodes::INVALID_TEMPLATE_FORMAT);
                }
            }
            break;

        case Column:
            if (*pos == ':')
            {
                size_t column_idx = idxByName(String(token_begin, pos - token_begin));
                format_idx_to_column_idx.push_back(column_idx);
                token_begin = pos + 1;
                state = Format;
            }
            else if (*pos == '}')
            {
                size_t column_idx = idxByName(String(token_begin, pos - token_begin));
                format_idx_to_column_idx.push_back(column_idx);
                formats.push_back(ColumnFormat::Default);
                delimiters.emplace_back();
                token_begin = pos + 1;
                state = Delimiter;
            }
            break;

        case Format:
            if (*pos == '}')
            {
                formats.push_back(stringToFormat(String(token_begin, pos - token_begin)));
                token_begin = pos + 1;
                delimiters.emplace_back();
                state = Delimiter;
            }
        }
    }
    if (state != Delimiter)
        throw Exception("invalid template: check parentheses balance", ErrorCodes::INVALID_TEMPLATE_FORMAT);
    delimiters.back().append(token_begin, pos - token_begin);
}


ParsedTemplateFormat::ColumnFormat ParsedTemplateFormat::stringToFormat(const String & col_format)
{
    if (col_format.empty())
        return ColumnFormat::Default;
    else if (col_format == "Escaped")
        return ColumnFormat::Escaped;
    else if (col_format == "Quoted")
        return ColumnFormat::Quoted;
    else if (col_format == "CSV")
        return ColumnFormat::Csv;
    else if (col_format == "JSON")
        return ColumnFormat::Json;
    else if (col_format == "XML")
        return ColumnFormat::Xml;
    else if (col_format == "Raw")
        return ColumnFormat::Raw;
    else
        throw Exception("invalid template: unknown field format " + col_format, ErrorCodes::INVALID_TEMPLATE_FORMAT);
}

size_t ParsedTemplateFormat::columnsCount() const
{
    return format_idx_to_column_idx.size();
}

String ParsedTemplateFormat::formatToString(ParsedTemplateFormat::ColumnFormat format)
{
    switch (format)
    {
        case ColumnFormat::Default:
            return "Escaped (Default)";
        case ColumnFormat::Escaped:
            return "Escaped";
        case ColumnFormat::Quoted:
            return "Quoted";
        case ColumnFormat::Csv:
            return "CSV";
        case ColumnFormat::Json:
            return "Json";
        case ColumnFormat::Xml:
            return "Xml";
        case ColumnFormat::Raw:
            return "Raw";
    }
    __builtin_unreachable();
}


TemplateBlockOutputStream::TemplateBlockOutputStream(WriteBuffer & ostr_, const Block & sample, const FormatSettings & settings_)
        : ostr(ostr_), header(sample), settings(settings_)
{
    static const String default_format("${data}");
    const String & format_str = settings.template_settings.format.empty() ? default_format : settings.template_settings.format;
    format = ParsedTemplateFormat(format_str, [&](const String & partName)
    {
        return static_cast<size_t>(stringToOutputPart(partName));
    });

    size_t dataIdx = format.format_idx_to_column_idx.size() + 1;
    for (size_t i = 0; i < format.format_idx_to_column_idx.size(); ++i)
    {
        switch (static_cast<OutputPart>(format.format_idx_to_column_idx[i]))
        {
            case OutputPart::Data:
                dataIdx = i;
                BOOST_FALLTHROUGH;
            case OutputPart::Totals:
            case OutputPart::ExtremesMin:
            case OutputPart::ExtremesMax:
                if (format.formats[i] != ColumnFormat::Default)
                    throw Exception("invalid template: wrong serialization type for data, totals, min or max",
                                    ErrorCodes::INVALID_TEMPLATE_FORMAT);
                break;
            default:
                break;
        }
    }

    if (dataIdx != 0)
        throw Exception("invalid template: ${data} must be the first output part", ErrorCodes::INVALID_TEMPLATE_FORMAT);

    row_format = ParsedTemplateFormat(settings.template_settings.row_format, [&](const String & colName)
    {
        return header.getPositionByName(colName);
    });

    if (row_format.delimiters.size() == 1)
        throw Exception("invalid template: no columns specified", ErrorCodes::INVALID_TEMPLATE_FORMAT);
}

TemplateBlockOutputStream::OutputPart TemplateBlockOutputStream::stringToOutputPart(const String & part)
{
    if (part == "data")
        return OutputPart::Data;
    else if (part == "totals")
        return OutputPart::Totals;
    else if (part == "min")
        return OutputPart::ExtremesMin;
    else if (part == "max")
        return OutputPart::ExtremesMax;
    else if (part == "rows")
        return OutputPart::Rows;
    else if (part == "rows_before_limit")
        return OutputPart::RowsBeforeLimit;
    else if (part == "time")
        return OutputPart::TimeElapsed;
    else if (part == "rows_read")
        return OutputPart::RowsRead;
    else if (part == "bytes_read")
        return OutputPart::BytesRead;
    else
        throw Exception("invalid template: unknown output part " + part, ErrorCodes::INVALID_TEMPLATE_FORMAT);
}

void TemplateBlockOutputStream::flush()
{
    ostr.next();
}

void TemplateBlockOutputStream::writeRow(const Block & block, size_t row_num)
{
    size_t columns = row_format.format_idx_to_column_idx.size();
    for (size_t j = 0; j < columns; ++j)
    {
        writeString(row_format.delimiters[j], ostr);

        size_t col_idx = row_format.format_idx_to_column_idx[j];
        const ColumnWithTypeAndName & col = block.getByPosition(col_idx);
        serializeField(*col.column, *col.type, row_num, row_format.formats[j]);
    }
    writeString(row_format.delimiters[columns], ostr);
}

void TemplateBlockOutputStream::serializeField(const IColumn & column, const IDataType & type, size_t row_num, ColumnFormat col_format)
{
    switch (col_format)
    {
        case ColumnFormat::Default:
        case ColumnFormat::Escaped:
            type.serializeAsTextEscaped(column, row_num, ostr, settings);
            break;
        case ColumnFormat::Quoted:
            type.serializeAsTextQuoted(column, row_num, ostr, settings);
            break;
        case ColumnFormat::Csv:
            type.serializeAsTextCSV(column, row_num, ostr, settings);
            break;
        case ColumnFormat::Json:
            type.serializeAsTextJSON(column, row_num, ostr, settings);
            break;
        case ColumnFormat::Xml:
            type.serializeAsTextXML(column, row_num, ostr, settings);
            break;
        case ColumnFormat::Raw:
            type.serializeAsText(column, row_num, ostr, settings);
            break;
    }
}

template <typename U, typename V> void TemplateBlockOutputStream::writeValue(U value, ColumnFormat col_format)
{
    auto type = std::make_unique<V>();
    auto col = type->createColumn();
    col->insert(value);
    serializeField(*col, *type, 0, col_format);
}

void TemplateBlockOutputStream::write(const Block & block)
{
    size_t rows = block.rows();

    for (size_t i = 0; i < rows; ++i)
    {
        if (row_count)
            writeString(settings.template_settings.row_between_delimiter, ostr);

        writeRow(block, i);
        ++row_count;
    }
}

void TemplateBlockOutputStream::writePrefix()
{
    writeString(format.delimiters.front(), ostr);
}

void TemplateBlockOutputStream::writeSuffix()
{

    size_t parts = format.format_idx_to_column_idx.size();

    for (size_t j = 0; j < parts; ++j)
    {
        auto type = std::make_shared<DataTypeUInt64>();
        ColumnWithTypeAndName col(type->createColumnConst(1, row_count), type, String("tmp"));
        switch (static_cast<OutputPart>(format.format_idx_to_column_idx[j]))
        {
            case OutputPart::Totals:
                if (!totals)
                    throw Exception("invalid template: cannot print totals for this request", ErrorCodes::INVALID_TEMPLATE_FORMAT);
                writeRow(totals, 0);
                break;
            case OutputPart::ExtremesMin:
                if (!extremes)
                    throw Exception("invalid template: cannot print extremes for this request", ErrorCodes::INVALID_TEMPLATE_FORMAT);
                writeRow(extremes, 0);
                break;
            case OutputPart::ExtremesMax:
                if (!extremes)
                    throw Exception("invalid template: cannot print extremes for this request", ErrorCodes::INVALID_TEMPLATE_FORMAT);
                writeRow(extremes, 1);
                break;
            case OutputPart::Rows:
                writeValue<size_t, DataTypeUInt64>(row_count, format.formats[j]);
                break;
            case OutputPart::RowsBeforeLimit:
                if (!rows_before_limit_set)
                    throw Exception("invalid template: cannot print rows_before_limit for this request", ErrorCodes::INVALID_TEMPLATE_FORMAT);
                writeValue<size_t, DataTypeUInt64>(rows_before_limit, format.formats[j]);
                break;
            case OutputPart::TimeElapsed:
                writeValue<double, DataTypeFloat64>(watch.elapsedSeconds(), format.formats[j]);
                break;
            case OutputPart::RowsRead:
                writeValue<size_t, DataTypeUInt64>(progress.read_rows.load(), format.formats[j]);
                break;
            case OutputPart::BytesRead:
                writeValue<size_t, DataTypeUInt64>(progress.read_bytes.load(), format.formats[j]);
                break;
            default:
                break;
        }
        writeString(format.delimiters[j + 1], ostr);
    }

}


void registerOutputFormatTemplate(FormatFactory & factory)
{
    factory.registerOutputFormat("Template", [](
            WriteBuffer & buf,
            const Block & sample,
            const Context &,
            FormatFactory::WriteCallback,
            const FormatSettings & settings)
    {
        return std::make_shared<TemplateBlockOutputStream>(buf, sample, settings);
    });
}
}
