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
/// Cleared on dialect changes (`SET dialect = ...`) in `ParserKQLStatement::parseImpl`.
/// Multi-statement scripts of the form `let x = 5; print x;` rely on the binding being
/// observable across consecutive `parseImpl` calls within the same query, so we cannot
/// clear at every statement boundary without breaking the conformance tests.
///
/// Known limitation: because `thread_local` storage outlives a single query, bindings
/// can leak across independent queries that happen to be served by the same worker
/// thread when the user does not toggle dialect between them. A complete fix requires
/// associating bindings with a query/session `Context` (issue tracked in PR #102159
/// review thread "kqlLetBindings cleanup"); per-statement RAII clearing was rejected
/// because it would break valid multi-statement `let` scripts.
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
