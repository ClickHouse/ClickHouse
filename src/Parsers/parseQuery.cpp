#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/Lexer.h>
#include <Parsers/TokenIterator.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>
#include <Common/UTF8Helpers.h>
#include <common/find_symbols.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
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
    while ((nl = find_first_symbols<'\n'>(begin, pos)) < pos)
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
    const Token * positions_to_hilite,   /// must go in ascending order
    size_t num_positions_to_hilite)
{
    const char * pos = begin;
    for (size_t position_to_hilite_idx = 0; position_to_hilite_idx < num_positions_to_hilite; ++position_to_hilite_idx)
    {
        const char * current_position_to_hilite = positions_to_hilite[position_to_hilite_idx].begin;
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
    const Token * positions_to_hilite,
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
            out << ": " << std::string(positions_to_hilite[0].begin, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, end - positions_to_hilite[0].begin)) << ". ";
    }
}


void writeCommonErrorMessage(
    WriteBuffer & out,
    const char * begin,
    const char * end,
    Token last_token,
    const std::string & query_description)
{
    out << "Syntax error";

    if (!query_description.empty())
        out << " (" << query_description << ")";

    out << ": failed at position " << (last_token.begin - begin + 1);

    if (last_token.type == TokenType::EndOfStream || last_token.type == TokenType::Semicolon)
        out << " (end of query)";

    /// If query is multiline.
    const char * nl = find_first_symbols<'\n'>(begin, end);
    if (nl + 1 < end)
    {
        size_t line = 0;
        size_t col = 0;
        std::tie(line, col) = getLineAndCol(begin, last_token.begin);

        out << " (line " << line << ", col " << col << ")";
    }
}


std::string getSyntaxErrorMessage(
    const char * begin,
    const char * end,
    Token last_token,
    const Expected & expected,
    bool hilite,
    const std::string & query_description)
{
    WriteBufferFromOwnString out;
    writeCommonErrorMessage(out, begin, end, last_token, query_description);
    writeQueryAroundTheError(out, begin, end, hilite, &last_token, 1);

    if (!expected.variants.empty())
        out << "Expected " << expected;

    return out.str();
}


std::string getLexicalErrorMessage(
    const char * begin,
    const char * end,
    Token last_token,
    bool hilite,
    const std::string & query_description)
{
    WriteBufferFromOwnString out;
    writeCommonErrorMessage(out, begin, end, last_token, query_description);
    writeQueryAroundTheError(out, begin, end, hilite, &last_token, 1);

    out << getErrorTokenDescription(last_token.type);

    return out.str();
}


std::string getUnmatchedParenthesesErrorMessage(
    const char * begin,
    const char * end,
    const UnmatchedParentheses & unmatched_parens,
    bool hilite,
    const std::string & query_description)
{
    WriteBufferFromOwnString out;
    writeCommonErrorMessage(out, begin, end, unmatched_parens[0], query_description);
    writeQueryAroundTheError(out, begin, end, hilite, unmatched_parens.data(), unmatched_parens.size());

    out << "Unmatched parentheses: ";
    for (const Token & paren : unmatched_parens)
        out << *paren.begin;

    return out.str();
}

}


ASTPtr tryParseQuery(
    IParser & parser,
    const char * & pos,
    const char * end,
    std::string & out_error_message,
    bool hilite,
    const std::string & query_description,
    bool allow_multi_statements,
    size_t max_query_size,
    size_t max_parser_depth)
{
    Tokens tokens(pos, end, max_query_size);
    IParser::Pos token_iterator(tokens, max_parser_depth);

    if (token_iterator->isEnd()
        || token_iterator->type == TokenType::Semicolon)
    {
        out_error_message = "Empty query";
        return nullptr;
    }

    Expected expected;

    ASTPtr res;
    bool parse_res = parser.parse(token_iterator, res, expected);
    Token last_token = token_iterator.max();

    /// If parsed query ends at data for insertion. Data for insertion could be in any format and not necessary be lexical correct.
    ASTInsertQuery * insert = nullptr;
    if (parse_res)
        insert = res->as<ASTInsertQuery>();

    if (!(insert && insert->data))
    {
        /// Lexical error
        if (last_token.isError())
        {
            out_error_message = getLexicalErrorMessage(pos, end, last_token, hilite, query_description);
            return nullptr;
        }

        /// Unmatched parentheses
        UnmatchedParentheses unmatched_parens = checkUnmatchedParentheses(TokenIterator(tokens), &last_token);
        if (!unmatched_parens.empty())
        {
            out_error_message = getUnmatchedParenthesesErrorMessage(pos, end, unmatched_parens, hilite, query_description);
            return nullptr;
        }
    }

    if (!parse_res)
    {
        /// Parse error.
        out_error_message = getSyntaxErrorMessage(pos, end, last_token, expected, hilite, query_description);
        return nullptr;
    }

    /// Excessive input after query. Parsed query must end with end of data or semicolon or data for INSERT.
    if (!token_iterator->isEnd()
        && token_iterator->type != TokenType::Semicolon
        && !(insert && insert->data))
    {
        expected.add(pos, "end of query");
        out_error_message = getSyntaxErrorMessage(pos, end, last_token, expected, hilite, query_description);
        return nullptr;
    }

    while (token_iterator->type == TokenType::Semicolon)
        ++token_iterator;

    /// If multi-statements are not allowed, then after semicolon, there must be no non-space characters.
    if (!allow_multi_statements
        && !token_iterator->isEnd()
        && !(insert && insert->data))
    {
        out_error_message = getSyntaxErrorMessage(pos, end, last_token, {}, hilite,
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
    bool allow_multi_statements,
    size_t max_query_size,
    size_t max_parser_depth)
{
    std::string error_message;
    ASTPtr res = tryParseQuery(parser, pos, end, error_message, false, query_description, allow_multi_statements, max_query_size, max_parser_depth);

    if (res)
        return res;

    throw Exception(error_message, ErrorCodes::SYNTAX_ERROR);
}


ASTPtr parseQuery(
    IParser & parser,
    const char * begin,
    const char * end,
    const std::string & query_description,
    size_t max_query_size,
    size_t max_parser_depth)
{
    const char * pos = begin;
    return parseQueryAndMovePosition(parser, pos, end, query_description, false, max_query_size, max_parser_depth);
}


ASTPtr parseQuery(
    IParser & parser,
    const std::string & query,
    const std::string & query_description,
    size_t max_query_size,
    size_t max_parser_depth)
{
    return parseQuery(parser, query.data(), query.data() + query.size(), query_description, max_query_size, max_parser_depth);
}


ASTPtr parseQuery(
    IParser & parser,
    const std::string & query,
    size_t max_query_size,
    size_t max_parser_depth)
{
    return parseQuery(parser, query.data(), query.data() + query.size(), parser.getName(), max_query_size, max_parser_depth);
}


std::pair<const char *, bool> splitMultipartQuery(
    const std::string & queries,
    std::vector<std::string> & queries_list,
    size_t max_query_size,
    size_t max_parser_depth)
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

        ast = parseQueryAndMovePosition(parser, pos, end, "", true, max_query_size, max_parser_depth);

        auto * insert = ast->as<ASTInsertQuery>();

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
