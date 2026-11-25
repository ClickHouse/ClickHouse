#include <Parsers/Q/parseQQuery.h>

#include <Parsers/Q/ASTQQuery.h>
#include <Parsers/Q/QLexer.h>
#include <Parsers/Q/QParser.h>

#include <Parsers/ParserQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/TokenIterator.h>
#include <Parsers/parseQuery.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/checkStackSize.h>

#include <algorithm>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int QUERY_IS_TOO_LARGE;
}

namespace
{

/// Skips whitespace and comments.
const char * skipInsignificant(const char * begin, const char * at, const char * end)
{
    while (at < end)
    {
        if (isWhitespaceASCII(*at))
            ++at;
        else if (const char * after = skipQComment(begin, at, end); after != at)
            at = after;
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
        /// The comments are those of the lexer, so that a `;` in a comment of any kind does not end the query.
        if (const char * after = skipQComment(begin, at, end); after != at)
        {
            at = after;
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
            if (after == end || *after == '\n' || skipQComment(begin, after, end) != after)
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

namespace
{

/// What a program has to do with the queries in SQL embedded into it (`.ch.sql`).
struct EmbeddedQueries
{
    /// Every string literal of the program. `.ch.sql` takes a string that is a literal, directly or through a variable,
    /// a parameter of a function, or another name of `.ch.sql`, so every embedded query is one of them.
    std::vector<const QNode *> strings;
    /// The ones that are passed to `.ch.sql` in place: `.ch.sql "SELECT ..."`.
    std::unordered_set<const QNode *> direct;
    /// The program mentions `.ch.sql`.
    bool mentioned = false;
    /// And it mentions it other than by applying it to a string literal: `.ch.sql s`, `f:.ch.sql`.
    /// Then any string literal of the program can be an embedded query.
    bool indirect = false;
};

bool isStringLiteral(const QNodePtr & node)
{
    return node && node->kind == QNodeKind::Literal && node->type == QType::Char && node->atoms.size() == 1
        && node->atoms.front().value.getType() == Field::Types::String;
}

void collectEmbeddedQueries(const QNodePtr & node, EmbeddedQueries & out)
{
    checkStackSize();
    if (!node)
        return;

    bool applied_to_literal = node->kind == QNodeKind::Apply && node->children.size() == 2 && node->children[0]
        && node->children[0]->kind == QNodeKind::Name && node->children[0]->name == ".ch.sql" && isStringLiteral(node->children[1]);
    if (applied_to_literal)
    {
        out.mentioned = true;
        out.direct.insert(node->children[1].get());
    }
    else if (node->kind == QNodeKind::Name && node->name == ".ch.sql")
    {
        out.mentioned = true;
        out.indirect = true;
    }

    if (isStringLiteral(node))
        out.strings.push_back(node.get());

    /// The name in `.ch.sql "..."` is not another mention.
    for (size_t i = applied_to_literal ? 1 : 0; i < node->children.size(); ++i)
        collectEmbeddedQueries(node->children[i], out);
    for (const auto * columns : {&node->columns, &node->key_columns})
        for (const auto & column : *columns)
            collectEmbeddedQueries(column.expression, out);
    if (node->sql)
    {
        for (const auto * columns : {&node->sql->columns, &node->sql->by})
            for (const auto & column : *columns)
                collectEmbeddedQueries(column.expression, out);
        collectEmbeddedQueries(node->sql->from, out);
        collectEmbeddedQueries(node->sql->limit, out);
        for (const auto & constraint : node->sql->where)
            collectEmbeddedQueries(constraint, out);
    }
}

String quoteQString(std::string_view value)
{
    String result = "\"";
    for (char c : value)
    {
        if (c == '\\' || c == '"')
            result += '\\';
        if (c == '\n')
            result += "\\n";
        else
            result += c;
    }
    result += '"';
    return result;
}

/// The text of the program with the secrets of the embedded queries hidden, or empty if there is nothing to hide.
///
/// A query in SQL can hold credentials, like the keys in `.ch.sql "SELECT * FROM s3('...', 'key', 'secret')"`.
/// When the program is compiled, they are parts of the generated query, which is logged without them as every query is.
/// But the text of the program itself is logged if the compilation fails, or if the embedded query does not get to
/// the generated query, so it needs a form without the secrets as well.
///
/// An embedded query is a string literal of the program: the one that `.ch.sql` is applied to, or, when the program
/// uses `.ch.sql` in another way (`s:"SELECT ..."; .ch.sql s`, `f:.ch.sql; f "SELECT ..."`), any of them. So in a program
/// that mentions `.ch.sql`, every string literal that is a query with secrets is replaced with its form for logging.
/// One that does not parse is hidden as a whole if it can be an embedded query, because it is not known what is
/// secret in it. A string literal over the limits of the parser (the depth, the backtracks) is an exception for
/// the whole program.
String hideSecretsOfEmbeddedQueries(
    const String & text, const QProgram & program, size_t max_query_size, size_t max_parser_depth, size_t max_parser_backtracks)
{
    EmbeddedQueries queries;
    for (const auto & statement : program.statements)
        collectEmbeddedQueries(statement, queries);
    if (!queries.mentioned)
        return {};

    /// From the last one to the first one, so that the offsets of the remaining ones stay valid.
    auto & literals = queries.strings;
    std::sort(literals.begin(), literals.end(), [](const QNode * left, const QNode * right) { return left->offset > right->offset; });

    String result = text;
    bool changed = false;
    for (const QNode * literal : literals)
    {
        const String & embedded_text = literal->atoms.front().value.safeGet<String>();
        bool is_direct = queries.direct.contains(literal);

        /// A credential in a query is a string literal of SQL: in quotes or a heredoc. A text without them has none,
        /// and is not parsed, unless it is certainly an embedded query.
        if (!is_direct && embedded_text.find_first_of("'$") == String::npos)
            continue;

        const char * embedded_pos = embedded_text.data();
        const char * embedded_end = embedded_pos + embedded_text.size();
        ParserQuery parser(embedded_end);
        String error_message;
        ASTPtr embedded = tryParseQuery(parser, embedded_pos, embedded_end, error_message, /*hilite=*/ false, "", /*allow_multi_statements=*/ false,
            max_query_size, max_parser_depth, max_parser_backtracks, /*skip_insignificant=*/ true);

        String shown;
        if (embedded)
        {
            if (!embedded->hasSecretParts())
                continue;
            shown = embedded->formatForLogging();
        }
        else
        {
            /// Not a query. It is hidden if the program can pass it to `.ch.sql`.
            if (!is_direct && !queries.indirect)
                continue;
            shown = "[HIDDEN]";
        }

        /// The literal in the text: from its opening quote to the closing one.
        /// If it is not where the parser said, nothing of the text is shown.
        size_t begin = literal->offset;
        if (begin >= result.size() || result[begin] != '"')
            return "[HIDDEN]";
        size_t end = begin + 1;
        while (end < result.size() && result[end] != '"')
            end += result[end] == '\\' ? 2 : 1;
        if (end >= result.size())
            return "[HIDDEN]";
        ++end;

        result.replace(begin, end - begin, quoteQString(shown));
        changed = true;
    }
    return changed ? result : String();
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
    ast->text_without_secrets = hideSecretsOfEmbeddedQueries(ast->text, *ast->program, max_query_size, max_parser_depth, max_parser_backtracks);

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
