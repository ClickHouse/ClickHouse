#include <Parsers/parseQuery.h>

#include <Parsers/ParserQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/Lexer.h>
#include <Parsers/TokenIterator.h>
#include <Common/StringUtils.h>
#include <Common/levenshteinDistance.h>
#include <Common/typeid_cast.h>
#include <Common/UTF8Helpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <algorithm>


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

    const char * nl = nullptr;
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


/// Highlight the place of syntax error.
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

        chassert(current_position_to_hilite <= end);
        chassert(current_position_to_hilite >= begin);

        out.write(pos, current_position_to_hilite - pos);

        if (current_position_to_hilite == end)
        {
            out << "\033[41;1m \033[0m";
            return;
        }

        ssize_t bytes_to_hilite = std::min<ssize_t>(UTF8::seqLength(*current_position_to_hilite), end - current_position_to_hilite);

        /// Bright on red background.
        out << "\033[41;1m";
        out.write(current_position_to_hilite, bytes_to_hilite);
        out << "\033[0m";
        pos = current_position_to_hilite + bytes_to_hilite;
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
        {
            const char * example_begin = positions_to_hilite[0].begin;
            size_t total_bytes = end - example_begin;
            size_t show_bytes = UTF8::computeBytesBeforeWidth(
                reinterpret_cast<const UInt8 *>(example_begin), total_bytes, 0, SHOW_CHARS_ON_SYNTAX_ERROR);
            out << ": " << std::string(example_begin, show_bytes) << (show_bytes < total_bytes ? "... " : ". ");
        }
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
        /// Do not print too long tokens.
        size_t token_size_bytes = last_token.end - last_token.begin;
        size_t token_preview_size_bytes = UTF8::computeBytesBeforeWidth(
            reinterpret_cast<const UInt8 *>(last_token.begin), token_size_bytes, 0, SHOW_CHARS_ON_SYNTAX_ERROR);

        out << " (" << std::string(last_token.begin, token_preview_size_bytes)
            << (token_preview_size_bytes < token_size_bytes ? "..." : "") << ")";
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


/** A typo in a keyword is the most common syntax mistake, and the parser answers it with a list of every
  * alternative it could accept at that place - dozens of them, with the keyword the user meant being just
  * one item somewhere in the middle. Find that item by the edit distance and name it explicitly.
  *
  * Returns an empty string if there is no close enough keyword. The thresholds are deliberately tighter
  * than in `NamePrompter`: here the candidates are keywords of the language rather than names from the
  * query, so almost every short word is within a small distance of some keyword (`hits` is two edits away
  * from `WITH`), and a wrong guess is worse than no guess.
  */
std::string_view getKeywordTypoHint(const Token & last_token, const Expected & expected)
{
    if (last_token.type != TokenType::BareWord)
        return {};

    const String token(last_token.begin, last_token.end - last_token.begin);
    if (token.size() < 3)
        return {};

    /// Keywords consist of letters only, so a word with a digit in it is a name, not a mistyped keyword.
    /// Without this, aliases such as `an1` in a query with many joined tables get matched against `ANY`.
    if (std::any_of(token.begin(), token.end(), isNumericASCII))
        return {};

    /// One mistaken character, plus one more for longer words, where a transposition of two letters
    /// (`ESLECT`) already costs two edits.
    const size_t max_distance = token.size() < 6 ? 1 : 2;

    size_t best_distance = max_distance + 1;
    std::string_view best_variant;

    for (const char * variant : expected.variants)
    {
        /// Only a description that is a keyword itself can be meant literally: the parser also adds prose
        /// such as `SELECT query, possibly with UNION`. A single word cannot be a typo of a multi-word
        /// keyword either, so anything with a space is not a candidate.
        const std::string_view variant_view(variant);
        if (variant_view.size() + max_distance < token.size() || token.size() + max_distance < variant_view.size())
            continue;
        if (!std::all_of(variant_view.begin(), variant_view.end(), [](char c) { return isUpperAlphaASCII(c) || c == '_'; }))
            continue;

        const size_t distance = levenshteinDistanceCaseInsensitive(token, String(variant_view));
        /// Zero distance means the token is that keyword, so it cannot be the reason of the failure.
        if (distance > 0 && distance < best_distance)
        {
            best_distance = distance;
            best_variant = variant_view;
        }
    }

    return best_variant;
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
    {
        if (const std::string_view hint = getKeywordTypoHint(last_token, expected); !hint.empty())
            out << "Maybe you meant: " << hint << ". ";

        out << "Expected " << expected;
    }

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
    out << getErrorTokenDescription(last_token.type) << ": ";
    writeCommonErrorMessage(out, begin, end, last_token, query_description);
    writeQueryAroundTheError(out, begin, end, hilite, &last_token, 1);
    return out.str();
}


/// Describe a bracket as `'(' at position 42 (line 3, col 5)`.
void writeBracketPosition(WriteBuffer & out, const char * begin, const char * end, const Token & bracket)
{
    out << "'" << std::string_view(bracket.begin, bracket.end - bracket.begin)
        << "' at position " << (bracket.begin - begin + 1);

    /// If query is multiline.
    const char * nl = find_first_symbols<'\n'>(begin, end);
    if (nl + 1 < end)
    {
        const auto [line, col] = getLineAndCol(begin, bracket.begin);
        out << " (line " << line << ", col " << col << ")";
    }
}


std::string getUnmatchedParenthesesErrorMessage(
    const char * begin,
    const char * end,
    const UnmatchedParentheses & unmatched_parens,
    Token last_token,
    bool hilite,
    const std::string & query_description)
{
    /** `checkUnmatchedParentheses` reports either a closing bracket that has nothing to close, or the whole
      * stack of brackets that are never closed - and in the latter case the first element of that stack is
      * the outermost bracket, which is a poor place to point at. In
      *   SELECT a FROM (SELECT count(* FROM t) x
      * the bracket of `count(` gets matched with the `)` that was meant to close the subquery, so the
      * leftover bracket is the one of the subquery: bracket counting alone cannot tell which of the nested
      * brackets the user forgot to close, and in a large query the outermost one is thousands of characters
      * away from the mistake.
      *
      * The token where the parser stopped does not have that problem - it is right next to the mistake
      * (`FROM` in the example above) - so report it as the error position, and list the leftover brackets
      * with their own positions separately.
      */

    const bool closing_bracket_is_unmatched
        = unmatched_parens.back().type == TokenType::ClosingRoundBracket
        || unmatched_parens.back().type == TokenType::ClosingSquareBracket;

    /// An unmatched closing bracket is itself the place of the mistake, so keep pointing at it.
    const bool point_at_parser_position
        = !closing_bracket_is_unmatched && last_token.begin > unmatched_parens.front().begin;

    const Token error_token = point_at_parser_position ? last_token : unmatched_parens.front();

    WriteBufferFromOwnString out;
    writeCommonErrorMessage(out, begin, end, error_token, query_description);

    if (hilite)
    {
        /// Highlight both the brackets and the place where the parser stopped. The positions must be
        /// passed in ascending order, and the parser can stop before some of the brackets.
        UnmatchedParentheses positions_to_hilite(unmatched_parens);
        if (point_at_parser_position)
            positions_to_hilite.push_back(error_token);
        std::sort(positions_to_hilite.begin(), positions_to_hilite.end(),
            [](const Token & lhs, const Token & rhs) { return lhs.begin < rhs.begin; });

        writeQueryAroundTheError(out, begin, end, hilite, positions_to_hilite.data(), positions_to_hilite.size());
    }
    else
    {
        /// Without highlighting only a fragment of the query is printed, starting from the first passed
        /// position. Show the text around the mistake rather than around the outermost bracket.
        writeQueryAroundTheError(out, begin, end, hilite, &error_token, 1);
    }

    out << "Unmatched parentheses: ";

    if (closing_bracket_is_unmatched && unmatched_parens.size() >= 2)
    {
        writeBracketPosition(out, begin, end, unmatched_parens.back());
        out << " does not match ";
        writeBracketPosition(out, begin, end, unmatched_parens[unmatched_parens.size() - 2]);
        out << ".";
    }
    else if (closing_bracket_is_unmatched)
    {
        writeBracketPosition(out, begin, end, unmatched_parens.back());
        out << " has no matching opening bracket.";
    }
    else
    {
        for (size_t i = 0; i < unmatched_parens.size(); ++i)
        {
            if (i != 0)
                out << ", ";
            writeBracketPosition(out, begin, end, unmatched_parens[i]);
        }
        out << (unmatched_parens.size() == 1 ? " is never closed." : " are never closed.");
    }

    return out.str();
}

}


static ASTInsertQuery * getInsertAST(const ASTPtr & ast)
{
    /// Either it is INSERT or EXPLAIN INSERT.
    if (auto * explain = ast->as<ASTExplainQuery>())
    {
        if (auto explained_query = explain->getExplainedQuery())
        {
            return explained_query->as<ASTInsertQuery>();
        }
    }
    else
    {
        return ast->as<ASTInsertQuery>();
    }

    return nullptr;
}

const char * getInsertData(const ASTPtr & ast)
{
    if (const ASTInsertQuery * insert = getInsertAST(ast))
        return insert->data;
    return nullptr;
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
    size_t max_parser_depth,
    size_t max_parser_backtracks,
    bool skip_insignificant)
{
    const char * query_begin = _out_query_end;
    Tokens tokens(query_begin, all_queries_end, max_query_size, skip_insignificant);
    /// NOTE: consider use UInt32 for max_parser_depth setting.
    IParser::Pos token_iterator(tokens, static_cast<uint32_t>(max_parser_depth), static_cast<uint32_t>(max_parser_backtracks));

    if (token_iterator->isEnd()
        || token_iterator->type == TokenType::Semicolon)
    {
        // Token iterator skips over comments, so we'll get this error for queries
        // like this:
        // "
        // -- just a comment
        // ;
        //"
        out_error_message = "Empty query";

        /// Name what was empty, the same way the syntax errors below do. The text is not always a query
        /// the user has sent: it can be a fragment parsed on its own, such as the value of a setting
        /// (`parallel_replicas_custom_key`, `additional_result_filter`) or a stored expression, and a
        /// bare `Empty query` gives nothing to look for in that case.
        if (!query_description.empty())
            out_error_message += " (" + query_description + ")";

        // Advance the position, so that we can use this parser for stream parsing
        // even in presence of such queries.
        _out_query_end = token_iterator->begin;
        return nullptr;
    }

    /// End of the current statement (next `;` or end of input), used to scope error
    /// messages in multi-statement input (issue #101509). Walks a fresh iterator
    /// (the parser's may have backtracked). `ErrorMaxQuerySizeExceeded` is terminal:
    /// once `pos` is past `max_query_size`, `nextToken` forces that type on every
    /// call (including the natural `EndOfStream`), so we must stop on it or loop
    /// forever. Other lexer errors are recoverable - `pos` keeps advancing.
    /// `min_end` clamps against `size_t` underflow in `writeQueryAroundTheError`.
    auto current_statement_end = [&](const char * min_end) -> const char *
    {
        IParser::Pos iter(tokens, static_cast<uint32_t>(max_parser_depth), static_cast<uint32_t>(max_parser_backtracks));
        while (!iter->isEnd()
            && iter->type != TokenType::ErrorMaxQuerySizeExceeded
            && iter->type != TokenType::Semicolon)
            ++iter;
        return std::max(iter->end, min_end);
    };

    Expected expected;

    /** A shortcut - if Lexer found invalid tokens, fail early without full parsing.
      * But there are certain cases when invalid tokens are permitted:
      * 1. INSERT queries can have arbitrary data after the FORMAT clause, that is parsed by a different parser.
      * 2. It can also be the case when there are multiple queries separated by semicolons, and the first queries are ok
      * while subsequent queries have syntax errors.
      *
      * This shortcut is needed to avoid complex backtracking in case of obviously erroneous queries.
      */
    IParser::Pos lookahead(token_iterator);
    if (!ParserKeyword(Keyword::INSERT_INTO).ignore(lookahead))
    {
        while (lookahead->type != TokenType::Semicolon && lookahead->type != TokenType::EndOfStream)
        {
            if (lookahead->isError())
            {
                // Advance the position for further processing of possible test hint.
                // Capture max() BEFORE current_statement_end, which walks fresh tokens
                // and would otherwise inflate the max-visited position.
                _out_query_end = token_iterator.max().end;
                out_error_message = getLexicalErrorMessage(
                    query_begin, current_statement_end(lookahead->end), *lookahead, hilite, query_description);
                return nullptr;
            }

            ++lookahead;
        }

        /// We should not spoil the info about maximum parsed position in the original iterator.
        tokens.reset();
    }

    ASTPtr res;
    const bool parse_res = parser.parse(token_iterator, res, expected);
    const auto last_token = token_iterator.max();
    _out_query_end = last_token.end;

    /// Also check on the AST level, because the generated AST depth can be greater than the recursion depth of the parser.
    if (res && max_parser_depth)
        res->checkDepth(max_parser_depth);

    /// If parsed query ends at data for insertion. Data for insertion could be
    /// in any format and not necessary be lexical correct, so we can't perform
    /// most of the checks.
    if (res && getInsertData(res))
        return res;

    // More granular checks for queries other than INSERT w/inline data.
    /// Lexical error
    if (last_token.isError())
    {
        out_error_message = getLexicalErrorMessage(
            query_begin, current_statement_end(last_token.end), last_token, hilite, query_description);
        return nullptr;
    }

    /// Unmatched parentheses
    UnmatchedParentheses unmatched_parens = checkUnmatchedParentheses(TokenIterator(tokens));
    if (!unmatched_parens.empty())
    {
        /// `checkUnmatchedParentheses` walks the entire remaining input, so it can
        /// report parens that live in later statements. Restrict to parens inside
        /// the current statement; otherwise the highlight loop in
        /// `writeQueryWithHighlightedErrorPositions` asserts on positions past `end`.
        const char * statement_end = current_statement_end(last_token.end);
        UnmatchedParentheses scoped_parens;
        for (const auto & paren : unmatched_parens)
        {
            if (paren.begin >= query_begin && paren.begin < statement_end)
            {
                scoped_parens.push_back(paren);
                /// Extend `statement_end` to cover the paren itself: a multi-byte token
                /// at the very boundary must not underflow `size_t` in the formatter.
                statement_end = std::max(paren.end, statement_end);
            }
        }

        if (!scoped_parens.empty())
        {
            out_error_message = getUnmatchedParenthesesErrorMessage(
                query_begin, statement_end, scoped_parens, last_token, hilite, query_description);
            return nullptr;
        }
    }

    IParser::Pos this_query_end_pos = token_iterator;
    while (!this_query_end_pos->isEnd() && !this_query_end_pos->isError()
        && this_query_end_pos->type != TokenType::Semicolon)
        ++this_query_end_pos;

    if (!parse_res)
    {
        /// Generic parse error.
        out_error_message = getSyntaxErrorMessage(query_begin, this_query_end_pos->end,
            last_token, expected, hilite, query_description);
        return nullptr;
    }

    /// Excessive input after query. Parsed query must end with end of data or semicolon or data for INSERT.
    if (!token_iterator->isEnd()
        && token_iterator->type != TokenType::Semicolon)
    {
        expected.add(last_token.begin, "end of query");
        out_error_message = getSyntaxErrorMessage(query_begin, this_query_end_pos->end,
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
    size_t max_parser_depth,
    size_t max_parser_backtracks)
{
    std::string error_message;
    ASTPtr res = tryParseQuery(
        parser, pos, end, error_message, false, query_description, allow_multi_statements,
        max_query_size, max_parser_depth, max_parser_backtracks, true);

    if (res)
        return res;

    throw Exception::createDeprecated(error_message, ErrorCodes::SYNTAX_ERROR);
}


ASTPtr parseQuery(
    IParser & parser,
    const char * begin,
    const char * end,
    const std::string & query_description,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks)
{
    return parseQueryAndMovePosition(parser, begin, end, query_description, false, max_query_size, max_parser_depth, max_parser_backtracks);
}


ASTPtr parseQuery(
    IParser & parser,
    const std::string & query,
    const std::string & query_description,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks)
{
    return parseQuery(parser, query.data(), query.data() + query.size(), query_description, max_query_size, max_parser_depth, max_parser_backtracks);
}


ASTPtr parseQuery(
    IParser & parser,
    const std::string & query,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks)
{
    return parseQuery(parser, query.data(), query.data() + query.size(), parser.getName(), max_query_size, max_parser_depth, max_parser_backtracks);
}


std::pair<const char *, bool> splitMultipartQuery(
    const std::string & queries,
    std::vector<std::string> & queries_list,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks,
    bool allow_settings_after_format_in_insert,
    bool implicit_select)
{
    ASTPtr ast;

    const char * begin = queries.data(); /// begin of current query
    const char * pos = begin; /// parser moves pos from begin to the end of current query
    const char * end = begin + queries.size();

    ParserQuery parser(end, allow_settings_after_format_in_insert, implicit_select);

    queries_list.clear();

    while (pos < end)
    {
        begin = pos;

        ast = parseQueryAndMovePosition(parser, pos, end, "", true, max_query_size, max_parser_depth, max_parser_backtracks);

        bool is_insert_with_data = false;
        if (ASTInsertQuery * insert = getInsertAST(ast); insert && insert->data)
        {
            /// Data for INSERT is broken on the new line
            pos = insert->data;
            while (*pos && *pos != '\n')
                ++pos;
            insert->end = pos;
            is_insert_with_data = true;
        }

        /// `pos` now points at the end of the current query. For an INSERT with inline data this is the
        /// boundary of the data line and must not be extended further, otherwise a trailing comment would
        /// be handed to the format reader as input data.
        const char * query_end = pos;

        /// Skip trailing whitespace and semicolons before the next query.
        while (isWhitespaceASCII(*pos) || *pos == ';')
            ++pos;

        /// If only whitespace and/or comments remain, there is no further query to parse. Consume the rest
        /// so the trailing comment is not handed to the parser as a separate, comment-only `Empty query`.
        Tokens tokens(pos, end, max_query_size, true);
        IParser::Pos token_iterator(tokens, static_cast<uint32_t>(max_parser_depth), static_cast<uint32_t>(max_parser_backtracks));

        if (token_iterator->isEnd())
        {
            pos = end;
            /// For a non-INSERT query fold the trailing comment into the returned fragment (it is harmless);
            /// for an INSERT keep the boundary at the data line so the tail is dropped rather than parsed as data.
            if (!is_insert_with_data)
                query_end = end;
        }

        queries_list.emplace_back(queries.substr(begin - queries.data(), query_end - begin));
    }

    return std::make_pair(begin, pos == end);
}


}
