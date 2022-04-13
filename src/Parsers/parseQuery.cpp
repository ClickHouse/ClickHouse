#include <Parsers/parseQuery.h>

#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/Lexer.h>
#include <Parsers/TokenIterator.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>
#include <Common/UTF8Helpers.h>
#include <base/find_symbols.h>
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

        assert(current_position_to_hilite <= end);
        assert(current_position_to_hilite >= begin);

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
    {
        out << " (end of query)";
    }
    else
    {
        out << " ('" << std::string(last_token.begin, last_token.end - last_token.begin) << "')";
    }

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
    if (last_token.size())
    {
       out << ": '" << std::string_view{last_token.begin, last_token.size()} << "'";
    }

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
    const char * & _out_query_end, /* also query begin as input parameter */
    const char * all_queries_end,
    std::string & out_error_message,
    bool hilite,
    const std::string & query_description,
    bool allow_multi_statements,
    size_t max_query_size,
    size_t max_parser_depth)
{
    const char * query_begin = _out_query_end;
    Tokens tokens(query_begin, all_queries_end, max_query_size);
    IParser::Pos token_iterator(tokens, max_parser_depth);

    if (token_iterator->isEnd()
        || token_iterator->type == TokenType::Semicolon)
    {
        out_error_message = "Empty query";
        // Token iterator skips over comments, so we'll get this error for queries
        // like this:
        // "
        // -- just a comment
        // ;
        //"
        // Advance the position, so that we can use this parser for stream parsing
        // even in presence of such queries.
        _out_query_end = token_iterator->begin;
        return nullptr;
    }

    Expected expected;
    ASTPtr res;
    const bool parse_res = parser.parse(token_iterator, res, expected);
    const auto last_token = token_iterator.max();
    _out_query_end = last_token.end;

    ASTInsertQuery * insert = nullptr;
    if (parse_res)
        insert = res->as<ASTInsertQuery>();

    // If parsed query ends at data for insertion. Data for insertion could be
    // in any format and not necessary be lexical correct, so we can't perform
    // most of the checks.
    if (insert && insert->data)
    {
        return res;
    }

    // More granular checks for queries other than INSERT w/inline data.
    /// Lexical error
    if (last_token.isError())
    {
        out_error_message = getLexicalErrorMessage(query_begin, all_queries_end,
            last_token, hilite, query_description);
        return nullptr;
    }

    /// Unmatched parentheses
    UnmatchedParentheses unmatched_parens = checkUnmatchedParentheses(TokenIterator(tokens));
    if (!unmatched_parens.empty())
    {
        out_error_message = getUnmatchedParenthesesErrorMessage(query_begin,
            all_queries_end, unmatched_parens, hilite, query_description);
        return nullptr;
    }

    if (!parse_res)
    {
        /// Generic parse error.
        out_error_message = getSyntaxErrorMessage(query_begin, all_queries_end,
            last_token, expected, hilite, query_description);
        return nullptr;
    }

    /// Excessive input after query. Parsed query must end with end of data or semicolon or data for INSERT.
    if (!token_iterator->isEnd()
        && token_iterator->type != TokenType::Semicolon)
    {
        expected.add(last_token.begin, "end of query");
        out_error_message = getSyntaxErrorMessage(query_begin, all_queries_end,
            last_token, expected, hilite, query_description);
        return nullptr;
    }

    // Skip the semicolon that might be left after parsing the VALUES format.
    while (token_iterator->type == TokenType::Semicolon)
    {
        ++token_iterator;
    }

    // If multi-statements are not allowed, then after semicolon, there must
    // be no non-space characters.
    if (!allow_multi_statements
        && !token_iterator->isEnd())
    {
        out_error_message = getSyntaxErrorMessage(query_begin, all_queries_end,
            last_token, {}, hilite,
            (query_description.empty() ? std::string() : std::string(". "))
                + "Multi-statements are not allowed");
        return nullptr;
    }

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
    return parseQueryAndMovePosition(parser, begin, end, query_description, false, max_query_size, max_parser_depth);
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
