#include <Parsers/Kusto/Utilities.h>

#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB
{
String extractLiteralArgumentWithoutQuotes(const std::string & function_name, IParser::Pos & pos)
{
    ++pos;
    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
    {
        auto result = extractTokenWithoutQuotes(pos);
        ++pos;
        return result;
    }

    --pos;
    return IParserKQLFunction::getArgument(function_name, pos, IParserKQLFunction::ArgumentState::Raw);
}

String extractTokenWithoutQuotes(IParser::Pos & pos)
{
    const auto offset = static_cast<int>(pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral);
    return {pos->begin + offset, pos->end - offset};
}

void setSelectAll(ASTSelectQuery & select_query)
{
    auto expression_list = make_intrusive<ASTExpressionList>();
    expression_list->children.push_back(make_intrusive<ASTAsterisk>());
    select_query.setExpression(ASTSelectQuery::Expression::SELECT, std::move(expression_list));
}

String wildcardToRegex(const String & wildcard)
{
    String regex;
    for (char c : wildcard)
    {
        if (c == '*')
        {
            regex += ".*";
        }
        else if (c == '?')
        {
            regex += ".";
        }
        else if (c == '.' || c == '+' || c == '(' || c == ')' || c == '[' || c == ']' || c == '\\' || c == '^' || c == '$')
        {
            regex += "\\";
            regex += c;
        }
        else
        {
            regex += c;
        }
    }
    return regex;
}

ASTPtr wrapInSelectWithUnion(const ASTPtr & select_query)
{
    auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
    auto & list_of_selects = select_with_union_query->list_of_selects;
    list_of_selects = make_intrusive<ASTExpressionList>();
    list_of_selects->children.push_back(select_query);
    select_with_union_query->children.push_back(list_of_selects);

    return select_with_union_query;
}

bool isValidKQLPos(IParser::Pos & pos)
{
    return (pos.isValid() ||
            pos->type == TokenType::ErrorSingleExclamationMark || // allow kql negative operators
            pos->type == TokenType::ErrorWrongNumber || // allow kql timespan data type with decimal like 2.6h
            std::string_view(pos->begin, pos->end) == "~");  // allow kql Case-Sensitive operators
}

/// Thread-local storage for KQL `let` bindings.
///
/// Lifecycle (managed in `ParserKQLStatement::parseImpl`):
///   - `let name = value;` records the binding and returns early without clearing,
///     so consecutive `let` statements (e.g. `let x = 5; let y = x + 1; print y;`)
///     can share state across separate `parseImpl` calls on the same thread.
///   - `SET dialect = ...` clears bindings — the user is switching languages.
///   - Any other KQL statement (the consuming `print`/pipe query) installs an
///     RAII guard that calls `kqlLetBindingsClear()` on scope exit, including
///     exception paths. This bounds the cross-query leak window: stale bindings
///     cannot persist past the next non-`let` query on the same worker thread.
///
/// Residual gap: a sequence of `let` statements that is never followed by a
/// consuming statement on the same thread will keep its bindings until the
/// next non-`let` statement (or dialect switch). A complete fix requires
/// associating bindings with a query/session `Context`; this is intentionally
/// out of scope for the conformance-test PR (#102159 review thread
/// "kqlLetBindings cleanup") because plumbing `Context` through the parser
/// stack is a separate refactor.
std::unordered_map<String, String> & kqlLetBindings()
{
    static thread_local std::unordered_map<String, String> bindings;
    return bindings;
}

void kqlLetBindingsClear()
{
    kqlLetBindings().clear();
}
}
