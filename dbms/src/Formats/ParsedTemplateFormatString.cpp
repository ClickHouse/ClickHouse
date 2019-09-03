#include <Formats/ParsedTemplateFormatString.h>
#include <Formats/verbosePrintString.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_TEMPLATE_FORMAT;
}

ParsedTemplateFormatString::ParsedTemplateFormatString(const String & format_string, const ColumnIdxGetter & idx_by_name)
{
    try
    {
        parse(format_string, idx_by_name);
    }
    catch (DB::Exception & e)
    {
        if (e.code() != ErrorCodes::INVALID_TEMPLATE_FORMAT)
            throwInvalidFormat(e.message(), columnsCount());
        else
            throw;
    }
}


void ParsedTemplateFormatString::parse(const String & format_string, const ColumnIdxGetter & idx_by_name)
{
    enum ParserState
    {
        Delimiter,
        Column,
        Format
    };

    const char * pos = format_string.c_str();
    const char * end = format_string.c_str() + format_string.size();
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
                        throwInvalidFormat("at pos " + std::to_string(pos - format_string.c_str()) +
                                           ": expected '{' or '$' after '$'", columnsCount());
                }
                break;

            case Column:
                column_names.emplace_back();
                pos = readMayBeQuotedColumnNameInto(pos, end - pos, column_names.back());

                if (*pos == ':')
                    state = Format;
                else if (*pos == '}')
                {
                    formats.push_back(ColumnFormat::None);
                    delimiters.emplace_back();
                    state = Delimiter;
                }
                else
                    throwInvalidFormat("Expected ':' or '}' after column name: \"" + column_names.back() + "\"", columnsCount());

                token_begin = pos + 1;
                format_idx_to_column_idx.emplace_back(idx_by_name(column_names.back()));
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
        throwInvalidFormat("Unbalanced parentheses", columnsCount());
    delimiters.back().append(token_begin, pos - token_begin);
}


ParsedTemplateFormatString::ColumnFormat ParsedTemplateFormatString::stringToFormat(const String & col_format) const
{
    if (col_format.empty())
        return ColumnFormat::None;
    else if (col_format == "None")
        return ColumnFormat::None;
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
        throwInvalidFormat("Unknown field format " + col_format, columnsCount());
}

size_t ParsedTemplateFormatString::columnsCount() const
{
    return format_idx_to_column_idx.size();
}

String ParsedTemplateFormatString::formatToString(ParsedTemplateFormatString::ColumnFormat format)
{
    switch (format)
    {
        case ColumnFormat::None:
            return "None";
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

const char * ParsedTemplateFormatString::readMayBeQuotedColumnNameInto(const char * pos, size_t size, String & s)
{
    s.clear();
    if (!size)
        return pos;
    ReadBufferFromMemory buf{pos, size};
    if (*pos == '"')
        readDoubleQuotedStringWithSQLStyle(s, buf);
    else if (*pos == '`')
        readBackQuotedStringWithSQLStyle(s, buf);
    else if (isWordCharASCII(*pos))
    {
        size_t name_size = 1;
        while (name_size < size && isWordCharASCII(*(pos + name_size)))
            ++name_size;
        s = String{pos, name_size};
        return pos + name_size;
    }
    return pos + buf.count();
}

String ParsedTemplateFormatString::dump() const
{
    WriteBufferFromOwnString res;
    res << "Delimiter " << 0 << ": ";
    verbosePrintString(delimiters.front().c_str(), delimiters.front().c_str() + delimiters.front().size(), res);

    size_t num_columns = std::max(formats.size(), format_idx_to_column_idx.size());
    for (size_t i = 0; i < num_columns; ++i)
    {
        res << "\nColumn " << i << ": \"";
        if (column_names.size() <= i)
            res << "<ERROR>";
        else if (column_names[i].empty())
            res << "<SKIPPED>";
        else
            res << column_names[i];

        res << "\" (mapped to table column ";
        if (format_idx_to_column_idx.size() <= i)
            res << "<ERROR>";
        else if (!format_idx_to_column_idx[i])
            res << "<SKIPPED>";
        else
            res << *format_idx_to_column_idx[i];

        res << "), Format " << (i < formats.size() ? formatToString(formats[i]) : "<ERROR>");

        res << "\nDelimiter " << i + 1 << ": ";
        if (delimiters.size() <= i + 1)
            res << "<ERROR>";
        else
            verbosePrintString(delimiters[i + 1].c_str(), delimiters[i + 1].c_str() + delimiters[i + 1].size(), res);
    }

    return res.str();
}

void ParsedTemplateFormatString::throwInvalidFormat(const String & message, size_t column) const
{
    throw Exception("Invalid format string for Template: " + message + " (near column " + std::to_string(column) +
                    ")" + ". Parsed format string:\n" + dump() + "\n",
                    ErrorCodes::INVALID_TEMPLATE_FORMAT);
}

}
