#include <Databases/MySQL/tryConvertStringLiterals.h>
#include <Core/MySQL/MySQLCharset.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromMemory.h>
#include <Parsers/CommonParsers.h>
#include <Common/quoteString.h>
#include <Poco/String.h>

namespace DB
{

/// Reads current string literal using also double quotes
/// https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
static bool tryReadLiteral(
    IParser::Pos & pos,
    String & to)
{
    bool ret = false;
    try
    {
        if (pos->type == TokenType::StringLiteral ||
            (pos->type == TokenType::QuotedIdentifier && *pos->begin == '"'))
        {
            ReadBufferFromMemory buf(pos->begin, pos->size());
            if (*pos->begin == '"')
                readDoubleQuotedStringWithSQLStyle(to, buf);
            else
                readQuotedStringWithSQLStyle(to, buf);
            ret = true;
        }
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Ignore parsing errors
    }
    return ret;
}

/// Reads charset introducers before string literal
/// https://dev.mysql.com/doc/refman/8.0/en/charset-introducer.html
static bool tryReadCharset(
    IParser::Pos & pos,
    String & to)
{
    bool ret = false;
    if (pos->type == TokenType::BareWord &&
        *pos->begin == '_' &&
        pos->size() > 1)
    {
        String charset_name(pos->begin + 1, pos->size() - 1);
        charset_name = Poco::toLower(charset_name);
        if (MySQLCharset::isCharsetAvailable(charset_name))
        {
            to = charset_name;
            ret = true;
        }
    }
    return ret;
}

bool tryConvertStringLiterals(String & query)
{
    Tokens tokens(query.data(), query.data() + query.size());
    IParser::Pos pos(tokens, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    Expected expected;
    String rewritten_query;
    rewritten_query.reserve(query.size());
    MySQLCharset charset;
    String charset_name;
    auto charset_pos = pos;
    bool ret = false;
    const auto * copy_from = query.data();

    for (;pos->type != TokenType::EndOfStream; ++pos)
    {
        if (tryReadCharset(pos, charset_name))
        {
            charset_pos = pos;
            continue;
        }

        String literal;
        if (tryReadLiteral(pos, literal))
        {
            if (!charset_name.empty())
            {
                try
                {
                    bool need_convert = MySQLCharset::needConvert(charset_name);
                    String to;
                    if (!literal.empty() &&
                        need_convert &&
                        charset.convert(charset_name, to, literal) == 0)
                    {
                        literal = to;
                    }

                    /// Skip found charset from the query and spaces after
                    if (need_convert ||
                        charset_name == "utf8mb4" ||
                        charset_name == "utf8mb3")
                    {
                        rewritten_query.append(copy_from, charset_pos->begin - copy_from);
                        copy_from = pos->begin;
                        ret = true;
                    }
                }
                catch (...) // NOLINT(bugprone-empty-catch)
                {
                    /// Ignore unsupported charsets
                }
                charset_name.clear();
            }

            /// The query should be rewritten
            if (!ret)
                ret = *pos->begin == '"';
            if (ret)
            {
                rewritten_query.append(copy_from, pos->begin - copy_from).append(quoteString(literal));
                copy_from = pos->end;
            }
        }
        else
        {
            charset_name.clear();
        }
    }

    if (ret)
    {
        if (copy_from < pos->end)
            rewritten_query.append(copy_from, pos->begin - copy_from);
        query = rewritten_query;
    }
    return ret;
}

}
