#include <Common/StringUtils.h>
#include <Parsers/CommonParsers.h>
#include <common/find_first_symbols.h>

#include <string.h>        /// strncmp, strncasecmp


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


ParserString::ParserString(const char * s_, bool word_boundary_, bool case_insensitive_)
    : s(s_)
    , s_size(strlen(s))
    , word_boundary(word_boundary_)
    , case_insensitive(case_insensitive_)
{
}


const char * ParserString::getName() const
{
    return s;
}


bool ParserString::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    if (static_cast<ssize_t>(s_size) > end - pos || (case_insensitive ? strncasecmp : strncmp)(pos, s, s_size))
        return false;
    else
    {
        if (word_boundary && s_size && isWordCharASCII(s[s_size - 1])
            && pos + s_size != end && isWordCharASCII(pos[s_size]))
            return false;

        pos += s_size;
        return true;
    }
}


ParserKeyword::ParserKeyword(const char * s_) : s(s_)
{
}


const char * ParserKeyword::getName() const
{
    return s;
}


bool ParserKeyword::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    ParserWhitespaceOrComments ws;

    const char * current_word = s;

    size_t s_length = strlen(s);
    if (!s_length)
        throw Exception("Logical error: keyword cannot be empty string", ErrorCodes::LOGICAL_ERROR);

    const char * s_end = s + s_length;

    while (true)
    {
        const char * next_whitespace = find_first_symbols<' ', '\0'>(current_word, s_end);
        size_t word_length = next_whitespace - current_word;

        if (static_cast<ptrdiff_t>(word_length) > end - pos)
            return false;

        if (strncasecmp(pos, current_word, word_length))
            return false;

        pos += word_length;

        if (!*next_whitespace)
            break;

        if (!ws.ignore(pos, end))
            return false;

        current_word = next_whitespace + 1;
    }

    /// Check word break.
    if (isWordCharASCII(s_end[-1]) && pos < end && isWordCharASCII(pos[0]))
        return false;

    return true;
}


ParserWhitespace::ParserWhitespace(bool allow_newlines_)
    : allow_newlines(allow_newlines_)
{
}


const char * ParserWhitespace::getName() const
{
    return "white space";
}


bool ParserWhitespace::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;
    while (pos < end && (*pos == ' ' || *pos == '\t' || (allow_newlines && *pos == '\n') || *pos == '\r' || *pos == '\f'))
        ++pos;

    return pos != begin;
}


const char * ParserCStyleComment::getName() const
{
    return "C-style comment";
}


bool ParserCStyleComment::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    if (end - pos >= 4 && pos[0] == '/' && pos[1] == '*')
    {
        pos += 2;
        while (end - pos >= 2 && (pos[0] != '*' || pos[1] != '/'))
            ++pos;

        if (end - pos < 2)
        {
            expected = "closing of C-style comment '*/'";
            return false;
        }
        else
        {
            pos += 2;
            return true;
        }
    }
    else
        return false;
}


const char * ParserSQLStyleComment::getName() const
{
    return "SQL-style comment";
}


bool ParserSQLStyleComment::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    if (end - pos >= 2 && pos[0] == '-' && pos[1] == '-')
    {
        pos += 2;
        while (pos != end && *pos != '\n')
            ++pos;

        if (pos != end)
            ++pos;
        return true;
    }
    else
        return false;
}


const char * ParserComment::getName() const
{
    return "comment";
}


bool ParserComment::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    ParserCStyleComment p1;
    ParserSQLStyleComment p2;

    return p1.ignore(pos, end, max_parsed_pos, expected)
        || p2.ignore(pos, end, max_parsed_pos, expected);
}


ParserWhitespaceOrComments::ParserWhitespaceOrComments(bool allow_newlines_outside_comments_)
    : allow_newlines_outside_comments(allow_newlines_outside_comments_)
{
}


const char * ParserWhitespaceOrComments::getName() const
{
    return "white space or comments";
}


bool ParserWhitespaceOrComments::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    ParserWhitespace p1(allow_newlines_outside_comments);
    ParserComment p2;

    bool res = false;
    while (p1.ignore(pos, end, max_parsed_pos, expected) || p2.ignore(pos, end, max_parsed_pos, expected))
        res = true;
    return res;
}

}
