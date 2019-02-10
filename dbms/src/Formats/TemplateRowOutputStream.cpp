#include <Formats/TemplateRowOutputStream.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>


namespace DB {


namespace ErrorCodes {
    extern const int INVALID_TEMPLATE_FORMAT;
}

TemplateRowOutputStream::TemplateRowOutputStream(WriteBuffer &ostr_, const Block &sample,
        const FormatSettings &settings_, const String& format_template)
        : ostr(ostr_), settings(settings_)
{
    parseFormatString(format_template, sample);
}


void TemplateRowOutputStream::parseFormatString(const String & s, const Block & sample)
{
    enum ParserState
    {
        Delimiter,
        Column,
        Format
    };
    const char * pos = s.c_str();
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
                    throw Exception("invalid template: pos " + std::to_string(pos - s.c_str()) +
                    ": expected '{' or '$' after '$'", ErrorCodes::INVALID_TEMPLATE_FORMAT);
                }
            }
            break;

        case Column:
            if (*pos == ':')
            {
                size_t column_idx = sample.getPositionByName(String(token_begin, pos - token_begin));
                format_idx_to_column_idx.push_back(column_idx);
                token_begin = pos + 1;
                state = Format;
            }
            else if (*pos == '}')
            {
                size_t column_idx = sample.getPositionByName(String(token_begin, pos - token_begin));
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
    if (delimiters.size() == 1)
        throw Exception("invalid template: no columns specified", ErrorCodes::INVALID_TEMPLATE_FORMAT);
    delimiters.back().append(token_begin, pos - token_begin);
}


TemplateRowOutputStream::ColumnFormat TemplateRowOutputStream::stringToFormat(const String & format)
{
    if (format.empty())
        return ColumnFormat::Default;
    else if (format == "Escaped")
        return ColumnFormat::Escaped;
    else if (format == "Quoted")
        return ColumnFormat::Quoted;
    else if (format == "JSON")
        return ColumnFormat::Json;
    else if (format == "XML")
        return ColumnFormat::Xml;
    else if (format == "Raw")
        return ColumnFormat::Raw;
    else
        throw Exception("invalid template: unknown field format " + format, ErrorCodes::INVALID_TEMPLATE_FORMAT);

}

void TemplateRowOutputStream::flush()
{
    ostr.next();
}

void TemplateRowOutputStream::serializeField(const ColumnWithTypeAndName & col, size_t row_num, ColumnFormat format)
{
    switch (format)
    {
        case ColumnFormat::Default:
        case ColumnFormat::Escaped:
            col.type->serializeAsTextEscaped(*col.column, row_num, ostr, settings);
            break;
        case ColumnFormat::Quoted:
            col.type->serializeAsTextQuoted(*col.column, row_num, ostr, settings);
            break;
        case ColumnFormat::Json:
            col.type->serializeAsTextJSON(*col.column, row_num, ostr, settings);
            break;
        case ColumnFormat::Xml:
            col.type->serializeAsTextXML(*col.column, row_num, ostr, settings);
            break;
        case ColumnFormat::Raw:
            col.type->serializeAsText(*col.column, row_num, ostr, settings);
            break;
        default:
            __builtin_unreachable();
    }
}

void TemplateRowOutputStream::write(const Block & block, size_t row_num)
{
    size_t columns = format_idx_to_column_idx.size();
    for (size_t i = 0; i < columns; ++i)
    {
        writeString(delimiters[i], ostr);

        size_t col_idx = format_idx_to_column_idx[i];
        const ColumnWithTypeAndName & col = block.getByPosition(col_idx);
        serializeField(col, row_num, formats[i]);
    }
    writeString(delimiters[columns], ostr);
}

void TemplateBlockOutputStream::write(const Block & block)
{
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i)
        row_output->write(block, i);

}

void registerOutputFormatTemplate(FormatFactory &factory)
{
    factory.registerOutputFormat("Template", [](
            WriteBuffer &buf,
            const Block &sample,
            const Context & context,
            const FormatSettings &settings) {
        auto format_template = context.getSettingsRef().format_schema.toString();
        return std::make_shared<TemplateBlockOutputStream>(
                std::make_shared<TemplateRowOutputStream>(buf, sample, settings, format_template), sample);
    });
}
}
