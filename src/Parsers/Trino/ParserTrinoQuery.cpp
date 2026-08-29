#include <Parsers/Trino/ParserTrinoQuery.h>

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/Lexer.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/Trino/TrinoFunctionMapper.h>
#include <Parsers/Trino/TrinoSyntaxTranslator.h>
#include <Parsers/parseQuery.h>

#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
    extern const int SYNTAX_ERROR;
}

namespace
{

bool tokenIsKeyword(const Token & token, std::string_view keyword)
{
    if (token.type != TokenType::BareWord || token.size() != keyword.size())
        return false;
    return strncasecmp(token.begin, keyword.data(), keyword.size()) == 0;
}

}

bool ParserTrinoQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// SET queries are standard ClickHouse SQL and must be handled normally so
    /// that settings like `dialect` can be changed. This is checked before the
    /// feature gate so users can recover from misconfigured profiles
    /// (e.g. `SET dialect = 'clickhouse'`).
    ParserSetQuery set_p;
    if (set_p.parse(pos, node, expected))
        return true;

    if (!feature_enabled)
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Support for the Trino dialect is disabled (turn on setting 'allow_experimental_trino_dialect')");

    const Pos statement_begin = pos;
    const char * begin = pos->begin;

    /// Collect the tokens of one statement: until a top-level semicolon or the
    /// end of the input. Trino quoting and comments are compatible with the
    /// ClickHouse Lexer, so the token stream is reliable for delimiting.
    std::vector<Token> tokens;
    const char * statement_end = begin;
    size_t depth = 0;
    bool is_insert = false;
    bool insert_has_select = false;
    bool delegate_to_standard_parser = false;

    while (pos.isValid() && !(pos->type == TokenType::Semicolon && depth == 0))
    {
        const Token & token = *pos;

        if (token.type == TokenType::OpeningRoundBracket || token.type == TokenType::OpeningSquareBracket)
            ++depth;
        else if (token.type == TokenType::ClosingRoundBracket || token.type == TokenType::ClosingSquareBracket)
        {
            if (depth > 0)
                --depth;
        }

        if (tokens.empty())
            is_insert = tokenIsKeyword(token, "INSERT");

        /// A top-level SELECT (or the WITH of its CTEs) means the statement is
        /// INSERT ... SELECT: it has no inline-data tail, and its select list
        /// may legitimately contain barewords such as the `format` function or
        /// a column named `values`.
        if (is_insert && depth == 0 && (tokenIsKeyword(token, "SELECT") || tokenIsKeyword(token, "WITH")))
            insert_has_select = true;

        /// INSERT with inline data: the VALUES/FORMAT tail must not be translated
        /// (it can be arbitrary non-SQL data, and the parsed AST keeps zero-copy
        /// pointers into the original buffer), so the whole statement is
        /// delegated to the standard parser. Only the real inline-data tail is
        /// recognized: a top-level VALUES/FORMAT before any top-level SELECT,
        /// where FORMAT must be followed by a bare format name (and not by the
        /// opening parenthesis of a function call).
        if (is_insert && depth == 0 && !insert_has_select)
        {
            bool is_inline_data_tail = tokenIsKeyword(token, "VALUES");
            if (!is_inline_data_tail && tokenIsKeyword(token, "FORMAT"))
            {
                Pos next = pos;
                ++next;
                is_inline_data_tail = next.isValid() && next->type == TokenType::BareWord;
            }
            if (is_inline_data_tail)
            {
                delegate_to_standard_parser = true;
                break;
            }
        }

        tokens.push_back(token);
        statement_end = token.end;
        ++pos;
    }

    if (!delegate_to_standard_parser && !pos->isEnd() && pos->type != TokenType::Semicolon)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Cannot tokenize the query in the Trino dialect: {} at position {}",
            getErrorTokenDescription(pos->type),
            pos->begin - begin + 1);

    if (tokens.empty())
        return false;

    const size_t statement_size = static_cast<size_t>(statement_end - begin);
    if (max_query_size && statement_size > max_query_size)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Query size {} exceeds max_query_size {}", statement_size, max_query_size);

    if (delegate_to_standard_parser)
    {
        pos = statement_begin;
        ParserQuery standard_parser(raw_end);
        if (!standard_parser.parse(pos, node, expected))
            return false;
        mapTrinoFunctions(node);
        return true;
    }

    std::optional<String> translated = translateTrinoSyntax(tokens, begin, statement_end);

    if (!translated)
    {
        /// Nothing to translate at the token level: parse the original statement
        /// directly. A separate bounded token stream is used so that error
        /// positions are not distorted by the statement scan above (which has
        /// already advanced `pos` to the end of the statement).
        const char * query_begin = begin;
        String error_message;
        ParserQuery standard_parser(statement_end);
        node = tryParseQuery(
            standard_parser,
            query_begin,
            statement_end,
            error_message,
            false,
            "",
            false,
            max_query_size,
            max_parser_depth,
            max_parser_backtracks,
            true);
        if (!node)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "{}", error_message);
        mapTrinoFunctions(node);
        return true;
    }

    const char * translated_begin = translated->data();
    const char * const translated_end = translated->data() + translated->size();
    ParserQuery standard_parser(translated_end);
    String error_message;
    node = tryParseQuery(
        standard_parser,
        translated_begin,
        translated_end,
        error_message,
        false,
        "",
        false,
        max_query_size,
        max_parser_depth,
        max_parser_backtracks,
        true);

    if (!node)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Error while parsing the ClickHouse SQL produced by the Trino dialect translation: '{}'.\n"
            "Original query: '{}'\nTranslated SQL: '{}'",
            error_message,
            std::string_view(begin, statement_size),
            *translated);

    /// A safety net: the translated text is a temporary buffer, so an AST that
    /// keeps raw pointers into it (INSERT with inline data) must not escape.
    /// This is unreachable as long as such statements are delegated above.
    if (const auto * insert = node->as<ASTInsertQuery>(); insert && insert->data)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "INSERT with inline data is not supported in this form in the Trino dialect");

    mapTrinoFunctions(node);
    return true;
}

}
