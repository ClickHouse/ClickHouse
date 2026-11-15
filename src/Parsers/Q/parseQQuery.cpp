#include <Parsers/Q/parseQQuery.h>

#include <Parsers/Q/ASTQQuery.h>
#include <Parsers/Q/QParser.h>

#include <Parsers/ParserSetQuery.h>
#include <Parsers/TokenIterator.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int QUERY_IS_TOO_LARGE;
}

namespace
{

bool isLineStart(const char * begin, const char * at)
{
    return at == begin || at[-1] == '\n';
}

/// Whether a comment starts at `at`: `/` after whitespace, or `--` followed by whitespace.
bool isCommentStart(const char * begin, const char * at, const char * end)
{
    bool after_space = isLineStart(begin, at) || isWhitespaceASCII(at[-1]);
    if (!after_space)
        return false;
    if (*at == '/')
        return true;
    return *at == '-' && at + 1 < end && at[1] == '-' && (at + 2 == end || isWhitespaceASCII(at[2]));
}

const char * skipToLineEnd(const char * at, const char * end)
{
    while (at < end && *at != '\n')
        ++at;
    return at;
}

/// Skips whitespace and comments.
const char * skipInsignificant(const char * begin, const char * at, const char * end)
{
    while (at < end)
    {
        if (isWhitespaceASCII(*at))
            ++at;
        else if (isCommentStart(begin, at, end))
            at = skipToLineEnd(at, end);
        else
            break;
    }
    return at;
}

struct QueryBounds
{
    /// The end of the text of the query, without the terminating `;`.
    const char * query_end;
    /// Where the next query starts: after the terminating `;`.
    const char * next;
};

/// Finds the end of the q query starting at `pos`: a top-level `;` that is the last significant character of its line.
QueryBounds findQueryEnd(const char * begin, const char * pos, const char * end)
{
    int depth = 0;
    const char * at = pos;
    while (at < end)
    {
        char c = *at;
        if (c == '"')
        {
            ++at;
            while (at < end && *at != '"')
            {
                if (*at == '\\' && at + 1 < end)
                    ++at;
                ++at;
            }
            if (at < end)
                ++at;
            continue;
        }
        if ((c == '/' || c == '-') && isCommentStart(begin, at, end))
        {
            at = skipToLineEnd(at, end);
            continue;
        }
        if (c == '(' || c == '[' || c == '{')
            ++depth;
        else if (c == ')' || c == ']' || c == '}')
            --depth;
        else if (c == ';' && depth <= 0)
        {
            const char * after = at + 1;
            while (after < end && (*after == ' ' || *after == '\t' || *after == '\r'))
                ++after;
            if (after == end || *after == '\n' || isCommentStart(begin, after, end))
                return {at, at + 1};
        }
        ++at;
    }
    return {end, end};
}

/// Parses a plain `SET` query that spans the whole text from `pos` to `query_end`, or returns `nullptr`.
ASTPtr tryParseSet(const char * pos, const char * query_end, size_t max_query_size, size_t max_parser_depth, size_t max_parser_backtracks)
{
    /// Only the unambiguous `SET name =` shape is recognized: q has no `SET` statement, but `set` is a q keyword.
    const char * at = pos;
    if (query_end - at < 4 || !(at[0] == 's' || at[0] == 'S') || !(at[1] == 'e' || at[1] == 'E') || !(at[2] == 't' || at[2] == 'T')
        || !isWhitespaceASCII(at[3]))
        return nullptr;

    Tokens tokens(pos, query_end, max_query_size, /*skip_insignificant=*/ true);
    IParser::Pos token_iterator(tokens, static_cast<uint32_t>(max_parser_depth), static_cast<uint32_t>(max_parser_backtracks));
    ParserSetQuery set_parser(/*parse_only_internals_=*/ false, /*shorthand_syntax_=*/ false);
    ASTPtr node;
    Expected expected;
    if (!set_parser.parse(token_iterator, node, expected))
        return nullptr;
    if (!token_iterator->isEnd())
        return nullptr;
    return node;
}

void checkNoMoreStatements(const char * begin, const char * next, const char * end)
{
    const char * rest = skipInsignificant(begin, next, end);
    while (rest < end && *rest == ';')
        rest = skipInsignificant(begin, rest + 1, end);
    if (rest != end)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Multi-statements are not allowed: unexpected text after the q query");
}

}

ASTPtr parseQQuery(
    const char *& pos,
    const char * end,
    bool allow_multi_statements,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks)
{
    const char * begin = pos;
    pos = skipInsignificant(begin, pos, end);
    if (pos == end)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Empty query");

    QueryBounds bounds = findQueryEnd(begin, pos, end);
    if (max_query_size && static_cast<size_t>(bounds.query_end - pos) > max_query_size)
        throw Exception(ErrorCodes::QUERY_IS_TOO_LARGE, "Query is too large ({} bytes), maximum is {} bytes", bounds.query_end - pos, max_query_size);

    if (!allow_multi_statements)
        checkNoMoreStatements(begin, bounds.next, end);

    if (ASTPtr set_query = tryParseSet(pos, bounds.query_end, max_query_size, max_parser_depth, max_parser_backtracks))
    {
        pos = bounds.next;
        return set_query;
    }

    auto ast = make_intrusive<ASTQQuery>();
    ast->text = String(pos, bounds.query_end);
    while (!ast->text.empty() && isWhitespaceASCII(ast->text.back()))
        ast->text.pop_back();
    ast->program = std::make_shared<QProgram>(parseQProgram(ast->text, max_parser_depth));

    bool has_statements = false;
    for (const auto & statement : ast->program->statements)
        has_statements |= statement != nullptr;
    if (!has_statements)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Empty query");

    pos = bounds.next;
    return ast;
}

ASTPtr tryParseQSetQuery(
    const char *& pos,
    const char * end,
    size_t max_query_size,
    size_t max_parser_depth,
    size_t max_parser_backtracks)
{
    const char * begin = pos;
    const char * start = skipInsignificant(begin, pos, end);
    QueryBounds bounds = findQueryEnd(begin, start, end);
    const char * rest = skipInsignificant(begin, bounds.next, end);
    if (rest != end)
        return nullptr;
    ASTPtr set_query = tryParseSet(start, bounds.query_end, max_query_size, max_parser_depth, max_parser_backtracks);
    if (set_query)
        pos = bounds.next;
    return set_query;
}

bool isQQueryTextEmpty(const char * pos, const char * end)
{
    return skipInsignificant(pos, pos, end) == end;
}

const char * skipQQuery(const char * pos, const char * end)
{
    const char * start = skipInsignificant(pos, pos, end);
    return findQueryEnd(pos, start, end).next;
}

}
