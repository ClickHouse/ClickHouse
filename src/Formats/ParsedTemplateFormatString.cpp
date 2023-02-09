#include <Formats/ParsedTemplateFormatString.h>
#include <Formats/verbosePrintString.h>
#include <Formats/EscapingRuleUtils.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_TEMPLATE_FORMAT;
}

ParsedTemplateFormatString::ParsedTemplateFormatString(const FormatSchemaInfo & schema, const ColumnIdxGetter & idx_by_name, bool allow_indexes)
{
    ReadBufferFromFile schema_file(schema.absoluteSchemaPath(), 4096);
    String format_string;
    readStringUntilEOF(format_string, schema_file);
    try
    {
        parse(format_string, idx_by_name, allow_indexes);
    }
    catch (DB::Exception & e)
    {
        if (e.code() != ErrorCodes::INVALID_TEMPLATE_FORMAT)
            throwInvalidFormat(e.message(), columnsCount());
        else
            throw;
    }
}


void ParsedTemplateFormatString::parse(const String & format_string, const ColumnIdxGetter & idx_by_name, bool allow_indexes)
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
    char * col_idx_end;
    std::optional<size_t> column_idx;
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
                        throwInvalidFormat("At pos " + std::to_string(pos - format_string.c_str()) +
                                           ": Expected '{' or '$' after '$'" +
                                           ", got \"" + std::string(pos, std::min(end - pos, 16l)) + "\"", columnsCount());
                }
                break;

            case Column:
                column_names.emplace_back();
                pos = readMayBeQuotedColumnNameInto(pos, end - pos, column_names.back());

                if (*pos == ':')
                    state = Format;
                else if (*pos == '}')
                {
                    escaping_rules.push_back(EscapingRule::None);
                    delimiters.emplace_back();
                    state = Delimiter;
                }
                else
                    throwInvalidFormat("At pos " + std::to_string(pos - format_string.c_str()) +
                                       ": Expected ':' or '}' after column name \"" + column_names.back() + "\"" +
                                       ", got \"" + std::string(pos, std::min(end - pos, 16l)) + "\"", columnsCount());

                token_begin = pos + 1;
                column_idx.reset();
                if (!column_names.back().empty())
                {
                    col_idx_end = nullptr;
                    errno = 0;
                    column_idx = strtoull(column_names.back().c_str(), &col_idx_end, 10);
                    if (col_idx_end != column_names.back().c_str() + column_names.back().size() || errno)
                        column_idx = idx_by_name(column_names.back());
                    else if (!allow_indexes)
                        throw Exception(ErrorCodes::INVALID_TEMPLATE_FORMAT, "Indexes instead of names are not allowed");
                }
                format_idx_to_column_idx.emplace_back(column_idx);
                break;

            case Format:
                if (*pos == '}')
                {
                    escaping_rules.push_back(stringToEscapingRule(String(token_begin, pos - token_begin)));
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

size_t ParsedTemplateFormatString::columnsCount() const
{
    return format_idx_to_column_idx.size();
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
    res << "\nDelimiter " << 0 << ": ";
    verbosePrintString(delimiters.front().c_str(), delimiters.front().c_str() + delimiters.front().size(), res);

    size_t num_columns = std::max(escaping_rules.size(), format_idx_to_column_idx.size());
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

        res << "), Format " << (i < escaping_rules.size() ? escapingRuleToString(escaping_rules[i]) : "<ERROR>");

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
