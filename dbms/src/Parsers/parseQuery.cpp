#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/Lexer.h>
#include <Parsers/TokenIterator.h>
#include <Common/StringUtils.h>
#include <Common/typeid_cast.h>
#include <Common/UTF8Helpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace
{

/** From position in (possible multiline) query, get line number and column number in line.
  * Used in syntax error message.
  */
std::pair<size_t, size_t> getLineAndCol(const char * begin, const char * pos)
{
    size_t line = 0;

    const char * nl;
    while (nullptr != (nl = reinterpret_cast<const char *>(memchr(begin, '\n', pos - begin))))
    {
        ++line;
        begin = nl + 1;
    }

    /// Lines numbered from 1.
    return { line + 1, pos - begin + 1 };
}


WriteBuffer & operator<< (WriteBuffer & out, const Expected & expected)
{
    if (expected.variants.empty())
        return out;

    if (expected.variants.size() == 1)
        return out << *expected.variants.begin();

    out << "one of: ";
    bool first = true;
    for (const auto & variant : expected.variants)
    {
        if (!first)
            out << ", ";
        first = false;

        out << variant;
    }
    return out;
}


/// Hilite place of syntax error.
void writeQueryWithHighlightedErrorPositions(
    WriteBuffer & out,
    const char * begin,
    const char * end,
    const char * const * positions_to_hilite,   /// must go in ascending order
    size_t num_positions_to_hilite)
{
    const char * pos = begin;
    for (size_t position_to_hilite_idx = 0; position_to_hilite_idx < num_positions_to_hilite; ++position_to_hilite_idx)
    {
        const char * current_position_to_hilite = positions_to_hilite[position_to_hilite_idx];
        out.write(pos, current_position_to_hilite - pos);

        if (current_position_to_hilite == end)
        {
            out << "\033[41;1m \033[0m";
            return;
        }
        else
        {
            size_t bytes_to_hilite = UTF8::seqLength(*current_position_to_hilite);

            /// Bright on red background.
            out << "\033[41;1m";
            out.write(current_position_to_hilite, bytes_to_hilite);
            out << "\033[0m";
            pos = current_position_to_hilite + bytes_to_hilite;
        }
    }
    out.write(pos, end - pos);
}


void writeQueryAroundTheError(
    WriteBuffer & out,
    const char * begin,
    const char * end,
    bool hilite,
    const char * const * positions_to_hilite,
    size_t num_positions_to_hilite)
{
    if (hilite)
    {
        out << ":\n\n";
        writeQueryWithHighlightedErrorPositions(out, begin, end, positions_to_hilite, num_positions_to_hilite);
        out << "\n\n";
    }
    else
    {
        if (num_positions_to_hilite)
            out << ": " << std::string(positions_to_hilite[0], std::min(SHOW_CHARS_ON_SYNTAX_ERROR, end - positions_to_hilite[0]));
    }
}


void writeCommonErrorMessage(
    WriteBuffer & out,
    const char * begin,
    const char * end,
    const char * max_parsed_pos,
    const std::string & query_description)
{
    out << "Syntax error";

    if (!query_description.empty())
        out << " (" << query_description << ")";

    out << ": failed at position " << (max_parsed_pos - begin + 1);

    if (max_parsed_pos == end || *max_parsed_pos == ';')
        out << " (end of query)";

    /// If query is multiline.
    const char * nl = reinterpret_cast<const char *>(memchr(begin, '\n', end - begin));
    if (nullptr != nl && nl + 1 != end)
    {
        size_t line = 0;
        size_t col = 0;
        std::tie(line, col) = getLineAndCol(begin, max_parsed_pos);

        out << " (line " << line << ", col " << col << ")";
    }
}


std::string getSyntaxErrorMessage(
    const char * begin,
    const char * end,
    const char * max_parsed_pos,
    const Expected & expected,
    bool hilite,
    const std::string & query_description)
{
    String message;

    {
        WriteBufferFromString out(message);
        writeCommonErrorMessage(out, begin, end, max_parsed_pos, query_description);
        writeQueryAroundTheError(out, begin, end, hilite, &max_parsed_pos, 1);

        if (!expected.variants.empty())
            out << "Expected " << expected;
    }

    return message;
}


std::string getLexicalErrorMessage(
    const char * begin,
    const char * end,
    const char * max_parsed_pos,
    const char * error_token_description,
    bool hilite,
    const std::string & query_description)
{
    String message;

    {
        WriteBufferFromString out(message);
        writeCommonErrorMessage(out, begin, end, max_parsed_pos, query_description);
        writeQueryAroundTheError(out, begin, end, hilite, &max_parsed_pos, 1);

        out << error_token_description;
    }

    return message;
}


std::string getUnmatchedParenthesesErrorMessage(
    const char * begin,
    const char * end,
    const UnmatchedParentheses & unmatched_parens,
    bool hilite,
    const std::string & query_description)
{
    String message;

    {
        WriteBufferFromString out(message);
        writeCommonErrorMessage(out, begin, end, unmatched_parens[0], query_description);
        writeQueryAroundTheError(out, begin, end, hilite, unmatched_parens.data(), unmatched_parens.size());

        out << "Unmatched parentheses: ";
        for (const char * paren : unmatched_parens)
            out << *paren;
    }

    return message;
}

}


ASTPtr tryParseQuery(
    IParser & parser,
    const char * & pos,
    const char * end,
    std::string & out_error_message,
    bool hilite,
    const std::string & query_description,
    bool allow_multi_statements)
{
    Tokens tokens(pos, end);
    TokenIterator token_iterator(tokens);

    if (token_iterator->isEnd()
        || token_iterator->type == TokenType::Semicolon)
    {
        out_error_message = "Empty query";
        return nullptr;
    }

    Expected expected;
    const char * begin = pos;

    ASTPtr res;
    bool parse_res = parser.parse(token_iterator, res, expected);
    Token last_token = token_iterator.max();
    const char * max_parsed_pos = last_token.begin;

    if (!parse_res)
    {
        /// Lexical error
        if (last_token.isError())
        {
            out_error_message = getLexicalErrorMessage(begin, end, max_parsed_pos, getErrorTokenDescription(last_token.type), hilite, query_description);
            return nullptr;
        }

        /// Unmatched parentheses
        UnmatchedParentheses unmatched_parens = checkUnmatchedParentheses(TokenIterator(tokens), &last_token);
        if (!unmatched_parens.empty())
        {
            out_error_message = getUnmatchedParenthesesErrorMessage(begin, end, unmatched_parens, hilite, query_description);
            return nullptr;
        }

        /// Parse error.
        out_error_message = getSyntaxErrorMessage(begin, end, max_parsed_pos, expected, hilite, query_description);
        return nullptr;
    }

    /// Excessive input after query. Parsed query must end with end of data or semicolon or data for INSERT.
    ASTInsertQuery * insert = typeid_cast<ASTInsertQuery *>(res.get());

    if (!token_iterator->isEnd()
        && token_iterator->type != TokenType::Semicolon
        && !(insert && insert->data))
    {
        expected.add(pos, "end of query");
        out_error_message = getSyntaxErrorMessage(begin, end, max_parsed_pos, expected, hilite, query_description);
        return nullptr;
    }

    while (token_iterator->type == TokenType::Semicolon)
        ++token_iterator;

    /// If multi-statements are not allowed, then after semicolon, there must be no non-space characters.
    if (!allow_multi_statements
        && !token_iterator->isEnd()
        && !(insert && insert->data))
    {
        out_error_message = getSyntaxErrorMessage(begin, end, max_parsed_pos, {}, hilite,
            (query_description.empty() ? std::string() : std::string(". ")) + "Multi-statements are not allowed");
        return nullptr;
    }

    pos = token_iterator->begin;
    return res;
}


ASTPtr parseQueryAndMovePosition(
    IParser & parser,
    const char * & pos,
    const char * end,
    const std::string & query_description,
    bool allow_multi_statements)
{
    std::string error_message;
    ASTPtr res = tryParseQuery(parser, pos, end, error_message, false, query_description, allow_multi_statements);

    if (res)
        return res;

    throw Exception(error_message, ErrorCodes::SYNTAX_ERROR);
}


ASTPtr parseQuery(
    IParser & parser,
    const char * begin,
    const char * end,
    const std::string & query_description)
{
    auto pos = begin;
    return parseQueryAndMovePosition(parser, pos, end, query_description, false);
}


std::pair<const char *, bool> splitMultipartQuery(const std::string & queries, std::vector<std::string> & queries_list)
{
    ASTPtr ast;

    const char * begin = queries.data(); /// begin of current query
    const char * pos = begin; /// parser moves pos from begin to the end of current query
    const char * end = begin + queries.size();

    ParserQuery parser(end);

    queries_list.clear();

    while (pos < end)
    {
        begin = pos;

        ast = parseQueryAndMovePosition(parser, pos, end, "", true);
        if (!ast)
            break;

        ASTInsertQuery * insert = typeid_cast<ASTInsertQuery *>(ast.get());

        if (insert && insert->data)
        {
            /// Data for INSERT is broken on new line
            pos = insert->data;
            while (*pos && *pos != '\n')
                ++pos;
            insert->end = pos;
        }

        queries_list.emplace_back(queries.substr(begin - queries.data(), pos - begin));

        while (isWhitespaceASCII(*pos) || *pos == ';')
            ++pos;
    }

    return std::make_pair(begin, pos == end);
}

}
