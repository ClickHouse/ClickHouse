#include <Parsers/Kusto/KQLParser.h>

#include <Parsers/Kusto/KQLFunctions.h>
#include <Parsers/Kusto/KQLTranslator.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>

#include <base/arithmeticOverflow.h>
#include <base/hex.h>

#include <Poco/String.h>

#include <cmath>
#include <limits>
#include <optional>
#include <utility>


namespace DB
{

namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
extern const int TOO_DEEP_RECURSION;
extern const int BAD_ARGUMENTS;
}

namespace
{

ASTPtr makeLiteral(Field value)
{
    return make_intrusive<ASTLiteral>(std::move(value));
}

ASTPtr makeIdentifier(const String & name)
{
    return make_intrusive<ASTIdentifier>(name);
}

bool isLiteralWrapper(const String & name)
{
    static const std::set<String> literal_wrappers{
        "negate", "CAST", "accurateCastOrNull", "toIntervalNanosecond", "kqlToTimespan", "parseDateTime64BestEffortOrNull", "toUUIDOrNull"};
    return literal_wrappers.contains(name);
}

String formatKustoTimespan(Int64 nanoseconds)
{
    static constexpr Int64 ticks_per_second = 10'000'000;
    static constexpr Int64 ticks_per_minute = ticks_per_second * 60;
    static constexpr Int64 ticks_per_hour = ticks_per_minute * 60;
    static constexpr Int64 ticks_per_day = ticks_per_hour * 24;

    const Int64 ticks = nanoseconds / 100;
    const UInt64 absolute = ticks == std::numeric_limits<Int64>::min()
        ? static_cast<UInt64>(std::numeric_limits<Int64>::max()) + 1
        : static_cast<UInt64>(std::abs(ticks));

    String result = ticks < 0 ? "-" : "";
    if (absolute >= static_cast<UInt64>(ticks_per_day))
        result.append(fmt::format("{}.", absolute / ticks_per_day));

    result.append(fmt::format(
        "{:02}:{:02}:{:02}",
        (absolute / ticks_per_hour) % 24,
        (absolute / ticks_per_minute) % 60,
        (absolute / ticks_per_second) % 60));

    if (const auto fraction = absolute % ticks_per_second)
        result.append(fmt::format(".{:07}", fraction));

    return result;
}

/// The constant value of a parsed expression, when it has one that is known at parse time.
/// Typed literal wrappers keep their literal argument as a `Field`, because a dynamic array
/// has no independent type annotation for its elements.
std::optional<Field> tryFoldConstant(const ASTPtr & node)
{
    if (const auto * literal = node->as<ASTLiteral>())
        return literal->value;

    const auto * function = node->as<ASTFunction>();
    if (!function || !isLiteralWrapper(function->name) || function->arguments->children.empty())
        return {};

    std::optional<Field> operand = tryFoldConstant(function->arguments->children[0]);
    if (!operand)
        return {};

    if (function->name != "negate")
    {
        for (size_t i = 1; i < function->arguments->children.size(); ++i)
            if (!tryFoldConstant(function->arguments->children[i]))
                return {};

        /// A dynamic array has no interval element type. Preserve Kusto timespans as their
        /// textual carrier rather than exposing the physical nanosecond count as an integer.
        if (function->name == "toIntervalNanosecond" && operand->getType() == Field::Types::Int64)
            return Field(formatKustoTimespan(operand->safeGet<Int64>()));

        return operand;
    }

    switch (operand->getType())
    {
        case Field::Types::Int64: return Field(-operand->safeGet<Int64>());
        case Field::Types::UInt64: {
            /// Only hexadecimal literals parse as `UInt64`; the decimal ones are `Int64`.
            const UInt64 value = operand->safeGet<UInt64>();
            if (value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                return {};
            return Field(-static_cast<Int64>(value));
        }
        case Field::Types::Float64: return Field(-operand->safeGet<Float64>());
        default: return {};
    }
}

/// Whether a parsed expression is a literal in the KQL sense: a plain literal, or one of the
/// wrappers the typed literals (`long(-1)`, `datetime(...)`, `timespan(1d)`, `int(null)`) and
/// negative numbers lower to. Anything else - arithmetic, identifiers, calls - is not a valid
/// function parameter default.
bool isLiteralExpression(const ASTPtr & node)
{
    if (node->as<ASTLiteral>())
        return true;

    const auto * function = node->as<ASTFunction>();
    if (!function)
        return false;

    if (!isLiteralWrapper(function->name))
        return false;

    for (const auto & child : function->arguments->children)
        if (!isLiteralExpression(child))
            return false;
    return true;
}

/// The operators that may follow a `|`, mapped to the parse routine that handles them.
/// Anything not in this set is rejected by name, so an unsupported operator says so
/// instead of being silently reinterpreted (the old parser fell through to "table name",
/// which is why `search 'x'` used to fail with "Unknown table expression 'search'").
const std::map<String, KQLOperatorKind> & pipelineOperatorNames()
{
    static const std::map<String, KQLOperatorKind> names{
        {"where", KQLOperatorKind::Where},
        {"filter", KQLOperatorKind::Where},
        {"extend", KQLOperatorKind::Extend},
        {"project", KQLOperatorKind::Project},
        {"project-away", KQLOperatorKind::ProjectAway},
        {"project-keep", KQLOperatorKind::ProjectKeep},
        {"project-rename", KQLOperatorKind::ProjectRename},
        {"summarize", KQLOperatorKind::Summarize},
        {"sort", KQLOperatorKind::Sort},
        {"order", KQLOperatorKind::Sort},
        {"take", KQLOperatorKind::Take},
        {"limit", KQLOperatorKind::Take},
        {"top", KQLOperatorKind::Top},
        {"distinct", KQLOperatorKind::Distinct},
        {"count", KQLOperatorKind::Count},
        {"mv-expand", KQLOperatorKind::MvExpand},
        {"join", KQLOperatorKind::Join},
        {"union", KQLOperatorKind::Union},
        {"as", KQLOperatorKind::As},
        {"render", KQLOperatorKind::Render},
    };
    return names;
}

/// KQL operators this dialect does not implement. Listing them by name is what lets a
/// query that uses one fail with "not supported" instead of being reinterpreted - a bare
/// `search 'x'` would otherwise parse as a table called `search`.
const std::set<String> & unsupportedOperatorNames()
{
    static const std::set<String> names{
        "consume", "evaluate",    "externaldata",    "facet",    "find",   "fork",      "getschema",   "invoke",
        "lookup",  "make-series", "materialize",     "mv-apply", "parse",  "parse-kv",  "parse-where", "partition",
        "reduce",  "sample",      "sample-distinct", "scan",     "search", "serialize", "top-hitters", "top-nested",
    };
    return names;
}

const std::map<String, KQLJoinKind> & joinKindNames()
{
    static const std::map<String, KQLJoinKind> names{
        {"innerunique", KQLJoinKind::InnerUnique},
        {"inner", KQLJoinKind::Inner},
        {"leftouter", KQLJoinKind::LeftOuter},
        {"rightouter", KQLJoinKind::RightOuter},
        {"fullouter", KQLJoinKind::FullOuter},
        {"leftsemi", KQLJoinKind::LeftSemi},
        {"rightsemi", KQLJoinKind::RightSemi},
        {"leftanti", KQLJoinKind::LeftAnti},
        {"leftantisemi", KQLJoinKind::LeftAnti},
        {"rightanti", KQLJoinKind::RightAnti},
        {"rightantisemi", KQLJoinKind::RightAnti},
    };
    return names;
}

/// KQL scalar type names, as they appear in `datatable` schemas and `typeof(...)`.
/// `dynamic` is deliberately absent: dynamic array literals lower to ClickHouse `Array`,
/// but a schema annotation carries no element type, so a declared `dynamic` column has no
/// faithful representation and is rejected by name in `resolveScalarType`.
const std::map<String, String> & kqlTypeToClickHouseType()
{
    static const std::map<String, String> types{
        {"bool", "Bool"},
        {"boolean", "Bool"},
        {"datetime", "DateTime64(7, 'UTC')"},
        {"date", "DateTime64(7, 'UTC')"},
        {"decimal", "Decimal128(20)"},
        {"guid", "UUID"},
        {"uuid", "UUID"},
        {"int", "Int32"},
        {"long", "Int64"},
        {"real", "Float64"},
        {"double", "Float64"},
        {"string", "String"},
        {"timespan", "IntervalNanosecond"},
        {"time", "IntervalNanosecond"},
    };
    return types;
}

}

KQLParser::DepthGuard::DepthGuard(KQLParser & parser_, const char * what)
    : parser(parser_)
{
    ++parser.depth;
    if (parser.depth > parser.max_depth)
    {
        --parser.depth;
        throw Exception(
            ErrorCodes::TOO_DEEP_RECURSION,
            "Maximum parse depth ({}) exceeded while parsing a KQL {}. Consider raising the max_parser_depth setting",
            parser.max_depth,
            what);
    }
}

KQLParser::KQLParser(const char * query_begin_, std::vector<KQLToken> tokens_, size_t max_depth_)
    : query_begin(query_begin_)
    , tokens(std::move(tokens_))
    , max_depth(max_depth_)
{
}

const KQLToken & KQLParser::lookahead(size_t offset) const
{
    const size_t target = index + offset;
    return target < tokens.size() ? tokens[target] : tokens.back();
}

const char * KQLParser::getEndPosition() const
{
    return current().begin;
}

bool KQLParser::atKeyword(std::string_view keyword) const
{
    return tokenIsKeyword(index, keyword);
}

bool KQLParser::tokenIsKeyword(size_t position, std::string_view keyword) const
{
    if (position >= tokens.size() || tokens[position].type != KQLTokenType::BareWord)
        return false;
    const std::string_view text = tokens[position].text();
    if (text.size() != keyword.size())
        return false;
    for (size_t i = 0; i < text.size(); ++i)
        if (toLowerIfAlphaASCII(text[i]) != keyword[i])
            return false;
    return true;
}

bool KQLParser::consumeKeyword(std::string_view keyword)
{
    if (!atKeyword(keyword))
        return false;
    ++index;
    return true;
}

bool KQLParser::consume(KQLTokenType type)
{
    if (current().type != type)
        return false;
    ++index;
    return true;
}

void KQLParser::expect(KQLTokenType type)
{
    if (!consume(type))
        fail(fmt::format("expected {}", getKQLTokenName(type)));
}

void KQLParser::expectKeyword(std::string_view keyword)
{
    if (!consumeKeyword(keyword))
        fail(fmt::format("expected '{}'", keyword));
}

String KQLParser::expectIdentifierName()
{
    /// `['column name']` and `["column name"]` are how KQL quotes an identifier.
    if (at(KQLTokenType::OpeningSquareBracket) && lookahead().type == KQLTokenType::StringLiteral
        && lookahead(2).type == KQLTokenType::ClosingSquareBracket)
    {
        String name = lookahead().inner;
        index += 3;
        return name;
    }

    if (!at(KQLTokenType::BareWord))
        fail("expected a column or table name");

    String name(current().text());
    ++index;
    return name;
}

void KQLParser::fail(const String & message) const
{
    failAt(current(), message);
}

void KQLParser::failAt(const KQLToken & token, const String & message) const
{
    const size_t offset = token.begin >= query_begin ? static_cast<size_t>(token.begin - query_begin) : 0;

    /// Point at what is actually there, so "expected ')'" is followed by what was found.
    String found;
    if (token.isError())
        found = token.inner;
    else if (token.isEnd())
        found = "end of query";
    else
        found = fmt::format("'{}'", token.text());

    throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error in KQL query at position {}: {}, found {}", offset + 1, message, found);
}

const String & KQLParser::resolveScalarType(const KQLToken & type_token, const String & kql_type) const
{
    /// Rejected here rather than mapped: `dynamic([...])` literals lower to `Array`, but a
    /// schema annotation carries no element type, so a declared `dynamic` column has no
    /// faithful ClickHouse representation.
    if (kql_type == "dynamic")
        failAt(type_token, "a 'dynamic' column type is not supported (only 'dynamic([...])' array literals are)");

    auto it = kqlTypeToClickHouseType().find(kql_type);
    if (it == kqlTypeToClickHouseType().end())
        failAt(type_token, fmt::format("'{}' is not a KQL scalar type", kql_type));
    return it->second;
}

KQLTabularExpressionPtr KQLParser::parseQuery()
{
    /// `let` bindings accumulate until the tabular expression that uses them. They live in
    /// `scope`, so two queries parsed on the same thread cannot see each other's bindings.
    bool saw_let = false;
    while (atKeyword("let"))
    {
        saw_let = true;
        parseLetStatement();
        if (!consume(KQLTokenType::Semicolon))
        {
            if (at(KQLTokenType::EndOfStream))
                fail("a query must end with a tabular expression, not a 'let' statement");
            fail("expected ';' after a 'let' statement");
        }
    }

    if (at(KQLTokenType::EndOfStream))
        fail(saw_let ? "a query must end with a tabular expression, not a 'let' statement" : "empty query");

    KQLTabularExpressionPtr result = parseTabularExpression();

    consume(KQLTokenType::Semicolon);
    return result;
}

/// `let f = (x: long) { ... }` and `let v = view () { ... }`. The shape that identifies a
/// function is a parenthesized list followed by `{`: `let x = (1 + 2)` and
/// `let T = (Events | take 1)` both end at the `)`.
bool KQLParser::atFunctionDefinition() const
{
    size_t probe = index;
    if (probe < tokens.size() && tokens[probe].type == KQLTokenType::BareWord && Poco::toLower(String(tokens[probe].text())) == "view")
        ++probe;

    if (probe >= tokens.size() || tokens[probe].type != KQLTokenType::OpeningRoundBracket)
        return false;

    int nesting = 1;
    ++probe;
    while (probe < tokens.size() && nesting > 0)
    {
        const KQLTokenType type = tokens[probe].type;
        if (type == KQLTokenType::OpeningRoundBracket)
            ++nesting;
        else if (type == KQLTokenType::ClosingRoundBracket)
            --nesting;
        else if (type == KQLTokenType::EndOfStream || type == KQLTokenType::Error)
            return false;
        ++probe;
    }

    return probe < tokens.size() && tokens[probe].type == KQLTokenType::OpeningCurlyBrace;
}

void KQLParser::parseFunctionDefinition(const String & name)
{
    /// `view` marks a parameterless function that `union *` would pick up. Nothing here
    /// resolves wildcards, so the keyword is accepted and carries no further meaning.
    consumeKeyword("view");

    FunctionDefinition definition;
    expect(KQLTokenType::OpeningRoundBracket);

    bool seen_scalar = false;
    bool seen_default = false;
    if (!at(KQLTokenType::ClosingRoundBracket))
    {
        do
        {
            FunctionParameter parameter;
            const KQLToken & parameter_token = current();
            parameter.name = expectIdentifierName();
            expect(KQLTokenType::Colon);

            if (at(KQLTokenType::OpeningRoundBracket))
            {
                /// A tabular parameter: `T: (*)` accepts any schema, `T: (a: long, ...)`
                /// names the columns the body may use. The declared names are remembered,
                /// because the body may only read those columns of the argument.
                parameter.is_tabular = true;
                ++index;
                if (at(KQLTokenType::Asterisk))
                {
                    ++index;
                }
                else
                {
                    while (!at(KQLTokenType::ClosingRoundBracket))
                    {
                        const KQLToken & column_token = current();
                        String column = expectIdentifierName();
                        expect(KQLTokenType::Colon);
                        const KQLToken & column_type_token = current();
                        const String column_type = Poco::toLower(String(expectIdentifierName()));
                        resolveScalarType(column_type_token, column_type);

                        for (const TabularColumn & declared : parameter.tabular_columns)
                            if (declared.name == column)
                                failAt(
                                    column_token,
                                    fmt::format("column '{}' of parameter '{}' is declared twice", column, parameter.name));
                        parameter.tabular_columns.push_back(TabularColumn{std::move(column), column_type});

                        if (!consume(KQLTokenType::Comma))
                            break;
                    }
                    if (parameter.tabular_columns.empty())
                        failAt(parameter_token, fmt::format("tabular parameter '{}' declares no columns; write '(*)' for any schema", parameter.name));
                }
                expect(KQLTokenType::ClosingRoundBracket);
            }
            else
            {
                const KQLToken & type_token = current();
                const String type = Poco::toLower(String(expectIdentifierName()));
                resolveScalarType(type_token, type);
                parameter.type = type;
                seen_scalar = true;

                /// A default must be a literal, and defaulted parameters come last.
                if (consume(KQLTokenType::Equals))
                {
                    const KQLToken & default_token = current();
                    parameter.default_value = parseExpression();
                    if (!isLiteralExpression(parameter.default_value))
                        failAt(default_token, fmt::format("the default of parameter '{}' must be a literal", parameter.name));
                    seen_default = true;
                }
                else if (seen_default)
                {
                    failAt(parameter_token, fmt::format("parameter '{}' has no default but follows one that does", parameter.name));
                }
            }

            if (parameter.is_tabular && seen_scalar)
                failAt(parameter_token, "tabular parameters must come before scalar parameters");

            for (const auto & existing : definition.parameters)
                if (existing.name == parameter.name)
                    failAt(parameter_token, fmt::format("parameter '{}' is declared twice", parameter.name));

            definition.parameters.push_back(std::move(parameter));
        } while (consume(KQLTokenType::Comma));
    }
    expect(KQLTokenType::ClosingRoundBracket);

    expect(KQLTokenType::OpeningCurlyBrace);
    definition.body_begin = index;

    /// Record the body as a token range and skip past it; it is parsed at each call.
    int nesting = 1;
    while (nesting > 0)
    {
        if (at(KQLTokenType::EndOfStream) || at(KQLTokenType::Error))
            fail("unterminated function body");
        if (at(KQLTokenType::OpeningCurlyBrace))
            ++nesting;
        else if (at(KQLTokenType::ClosingCurlyBrace))
            --nesting;
        if (nesting > 0)
            ++index;
    }
    definition.body_end = index;
    expect(KQLTokenType::ClosingCurlyBrace);

    if (definition.body_end == definition.body_begin)
        fail("a function body cannot be empty");

    /// Classify the body while it is at hand: `let name = call(...);` has to decide how to
    /// bind long before any call parses the body. A tabular parameter can only feed a
    /// pipeline; otherwise the classification reads the body's tokens.
    for (const auto & parameter : definition.parameters)
        if (parameter.is_tabular)
            definition.body_looks_tabular = true;

    if (!definition.body_looks_tabular)
    {
        /// The names the body may stand on to be tabular without saying so: the enclosing
        /// tabular bindings and tabular-bodied functions it can see (`let F = () { Base };`).
        /// A parameter shadows anything of the same name outside; only scalar parameters
        /// reach this point, since a tabular one already classified the body.
        std::set<String> tabular_names = scopeTabularNames();
        std::set<String> scalar_names = scopeScalarNames();
        for (const auto & parameter : definition.parameters)
        {
            tabular_names.erase(parameter.name);
            scalar_names.insert(parameter.name);
        }

        definition.body_looks_tabular
            = bodyLooksTabular(definition.body_begin, definition.body_end, std::move(tabular_names), std::move(scalar_names));
    }

    scope.functions[name] = std::move(definition);
}

/// The first ';' outside any brackets ends a `let` statement; an inner function
/// definition hides its own ';'s inside `{ }`.
size_t KQLParser::statementEnd(size_t position, size_t end) const
{
    int nesting = 0;
    for (; position < end; ++position)
    {
        switch (tokens[position].type)
        {
            case KQLTokenType::OpeningRoundBracket:
            case KQLTokenType::OpeningSquareBracket:
            case KQLTokenType::OpeningCurlyBrace: ++nesting; break;
            case KQLTokenType::ClosingRoundBracket:
            case KQLTokenType::ClosingSquareBracket:
            case KQLTokenType::ClosingCurlyBrace: --nesting; break;
            case KQLTokenType::Semicolon:
                if (nesting <= 0)
                    return position;
                break;
            case KQLTokenType::EndOfStream: return position;
            default: break;
        }
    }
    return end;
}

size_t KQLParser::closingBracket(size_t position) const
{
    int nesting = 1;
    for (; position < tokens.size(); ++position)
    {
        switch (tokens[position].type)
        {
            case KQLTokenType::OpeningRoundBracket:
            case KQLTokenType::OpeningSquareBracket:
            case KQLTokenType::OpeningCurlyBrace: ++nesting; break;
            case KQLTokenType::ClosingRoundBracket:
            case KQLTokenType::ClosingSquareBracket:
            case KQLTokenType::ClosingCurlyBrace:
                if (--nesting == 0)
                    return position;
                break;
            case KQLTokenType::EndOfStream: return tokens.size();
            default: break;
        }
    }
    return tokens.size();
}

std::set<String> KQLParser::scopeTabularNames() const
{
    std::set<String> names;
    for (const auto & [tabular_name, _] : scope.tabulars)
        names.insert(tabular_name);
    for (const auto & [function_name, function] : scope.functions)
        if (function.body_looks_tabular)
            names.insert(function_name);
    return names;
}

std::set<String> KQLParser::scopeScalarNames() const
{
    std::set<String> names;
    for (const auto & [scalar_name, _] : scope.scalars)
        names.insert(scalar_name);
    for (const auto & [function_name, function] : scope.functions)
        if (!function.body_looks_tabular)
            names.insert(function_name);
    return names;
}

bool KQLParser::bodyLooksTabular(size_t begin, size_t end, std::set<String> tabular_names, std::set<String> scalar_names)
{
    DepthGuard guard(*this, "function body");

    /// Each leading `let` that binds something tabular adds a name the expression after them
    /// may stand on. Anything not of the `let <name> = <rhs>;` shape is left for the parse
    /// to reject.
    size_t probe = begin;
    while (probe < end && tokenIsKeyword(probe, "let"))
    {
        const size_t let_end = statementEnd(probe, end);

        if (probe + 3 < let_end && tokens[probe + 1].type == KQLTokenType::BareWord && tokens[probe + 2].type == KQLTokenType::Equals)
        {
            const String bound_name(tokens[probe + 1].text());
            const size_t rhs = probe + 3;

            /// A function definition - an optional `view`, a parenthesized parameter list, a
            /// braced body - binds a name that is tabular when its own body is.
            bool is_function_definition = false;
            bool bound_tabular = false;
            size_t parameters = rhs + (tokenIsKeyword(rhs, "view") ? 1 : 0);
            if (parameters < let_end && tokens[parameters].type == KQLTokenType::OpeningRoundBracket)
            {
                int nesting = 1;
                size_t body = parameters + 1;
                while (body < let_end && nesting > 0)
                {
                    if (tokens[body].type == KQLTokenType::OpeningRoundBracket)
                        ++nesting;
                    else if (tokens[body].type == KQLTokenType::ClosingRoundBracket)
                        --nesting;
                    ++body;
                }
                if (nesting == 0 && body < let_end && tokens[body].type == KQLTokenType::OpeningCurlyBrace)
                {
                    is_function_definition = true;
                    bound_tabular = tokens[let_end - 1].type == KQLTokenType::ClosingCurlyBrace
                        && bodyLooksTabular(body + 1, let_end - 1, tabular_names, scalar_names);
                }
            }

            if (!is_function_definition)
                bound_tabular = expressionLooksTabular(rhs, let_end, tabular_names, scalar_names);

            /// The binding shadows anything of the same name outside.
            if (bound_tabular)
            {
                tabular_names.insert(bound_name);
                scalar_names.erase(bound_name);
            }
            else
            {
                scalar_names.insert(bound_name);
                tabular_names.erase(bound_name);
            }
        }

        probe = let_end < end ? let_end + 1 : end;
    }

    return expressionLooksTabular(probe, end, tabular_names, scalar_names);
}

bool KQLParser::expressionLooksTabular(
    size_t begin,
    size_t end,
    const std::set<String> & tabular_names,
    const std::set<String> & scalar_names,
    bool unknown_name_is_table) const
{
    /// Where the bracket at `position` closes, or `end`.
    const auto matching_close = [&](size_t position)
    {
        int nesting = 0;
        for (; position < end; ++position)
        {
            switch (tokens[position].type)
            {
                case KQLTokenType::OpeningRoundBracket:
                case KQLTokenType::OpeningSquareBracket:
                case KQLTokenType::OpeningCurlyBrace: ++nesting; break;
                case KQLTokenType::ClosingRoundBracket:
                case KQLTokenType::ClosingSquareBracket:
                case KQLTokenType::ClosingCurlyBrace:
                    --nesting;
                    if (nesting == 0)
                        return position;
                    break;
                default: break;
            }
        }
        return end;
    };

    /// `(Events | take 1)` pipes behind parentheses; strip any number of full wraps.
    while (begin < end && tokens[begin].type == KQLTokenType::OpeningRoundBracket && matching_close(begin) == end - 1)
    {
        ++begin;
        --end;
    }

    if (begin >= end)
        return false;

    if (tokenIsKeyword(begin, "print") || tokenIsKeyword(begin, "datatable") || tokenIsKeyword(begin, "range")
        || tokenIsKeyword(begin, "union"))
        return true;

    int nesting = 0;
    for (size_t position = begin; position < end; ++position)
    {
        switch (tokens[position].type)
        {
            case KQLTokenType::OpeningRoundBracket:
            case KQLTokenType::OpeningSquareBracket:
            case KQLTokenType::OpeningCurlyBrace: ++nesting; break;
            case KQLTokenType::ClosingRoundBracket:
            case KQLTokenType::ClosingSquareBracket:
            case KQLTokenType::ClosingCurlyBrace: --nesting; break;
            case KQLTokenType::Pipe:
                if (nesting == 0)
                    return true;
                break;
            default: break;
        }
    }

    /// A name known to be tabular, standing alone (`Base`) or called (`F()`, `F(x)`).
    if (tokens[begin].type != KQLTokenType::BareWord)
        return false;
    const String name(tokens[begin].text());
    if (tabular_names.contains(name))
    {
        if (begin + 1 == end)
            return true;
        return tokens[begin + 1].type == KQLTokenType::OpeningRoundBracket && matching_close(begin + 1) == end - 1;
    }

    /// A bare name standing alone that is bound to nothing scalar reads as a physical table
    /// (`let T = StormEvents;`) - except where the caller says a column reference is possible
    /// too, as inside `in (...)`, where the column wins because the parser has no schema to
    /// tell them apart. `true` and `false` are literals, not names.
    const String lowered = Poco::toLower(name);
    if (lowered == "true" || lowered == "false")
        return false;
    if (scalar_names.contains(name))
        return false;
    if (begin + 1 == end)
        return unknown_name_is_table;

    /// `db.table` - the other source form `parseSource` accepts. Past an unbound name a `.` can
    /// mean nothing else here: a scalar binding was just excluded, and dynamic member access
    /// over anything else is not supported.
    if (tokens[begin + 1].type != KQLTokenType::Dot)
        return false;

    /// One identifier - a bare word or the `['quoted name']` form - and where it ends, or
    /// `position` when there is none.
    const auto identifier_end = [&](size_t position) -> size_t
    {
        if (position < end && tokens[position].type == KQLTokenType::BareWord)
            return position + 1;
        if (position + 2 < end && tokens[position].type == KQLTokenType::OpeningSquareBracket
            && tokens[position + 1].type == KQLTokenType::StringLiteral && tokens[position + 2].type == KQLTokenType::ClosingSquareBracket)
            return position + 3;
        return position;
    };

    return identifier_end(begin + 2) == end;
}

KQLParser::Scope KQLParser::bindArguments(const String & name, const FunctionDefinition & definition, const KQLToken & call_token)
{
    const size_t count = definition.parameters.size();
    std::vector<ASTPtr> scalar_arguments(count);
    std::vector<KQLTabularExpressionPtr> tabular_arguments(count);
    std::vector<bool> supplied(count, false);

    /// A zero-argument function may be called without parentheses.
    if (consume(KQLTokenType::OpeningRoundBracket))
    {
        size_t position = 0;
        if (!at(KQLTokenType::ClosingRoundBracket))
        {
            do
            {
                size_t target = position;

                /// `f(c = 7)` names its argument. `==` is a comparison, so only a single
                /// `=` after a bare word marks this form.
                if (at(KQLTokenType::BareWord) && lookahead().type == KQLTokenType::Equals)
                {
                    const KQLToken & argument_token = current();
                    const String argument_name(current().text());
                    index += 2;

                    const auto it = std::find_if(
                        definition.parameters.begin(),
                        definition.parameters.end(),
                        [&](const FunctionParameter & p) { return p.name == argument_name; });
                    if (it == definition.parameters.end())
                        failAt(argument_token, fmt::format("'{}' has no parameter named '{}'", name, argument_name));
                    target = static_cast<size_t>(it - definition.parameters.begin());
                }
                else if (position >= count)
                {
                    failAt(call_token, fmt::format("'{}' takes {} argument(s), and got more", name, count));
                }

                if (supplied[target])
                    failAt(call_token, fmt::format("argument '{}' of '{}' is given twice", definition.parameters[target].name, name));

                if (definition.parameters[target].is_tabular)
                {
                    if (at(KQLTokenType::OpeningRoundBracket))
                        tabular_arguments[target] = parseParenthesizedTabularExpression();
                    else
                        tabular_arguments[target] = parseTabularExpression();
                }
                else
                {
                    scalar_arguments[target] = parseExpression();
                }

                supplied[target] = true;
                position = target + 1;
            } while (consume(KQLTokenType::Comma));
        }
        expect(KQLTokenType::ClosingRoundBracket);
    }

    /// The body sees the enclosing bindings too, with its parameters on top.
    Scope inner = scope;
    for (size_t i = 0; i < count; ++i)
    {
        const FunctionParameter & parameter = definition.parameters[i];
        if (!supplied[i])
        {
            if (!parameter.default_value)
                failAt(call_token, fmt::format("'{}' needs an argument for '{}'", name, parameter.name));
            scalar_arguments[i] = parameter.default_value;
        }

        /// A parameter shadows anything of the same name outside.
        inner.scalars.erase(parameter.name);
        inner.tabulars.erase(parameter.name);
        inner.functions.erase(parameter.name);

        if (parameter.is_tabular)
            inner.tabulars[parameter.name] = restrictToDeclaredColumns(tabular_arguments[i], parameter, name);
        else
            inner.scalars[parameter.name] = enforceParameterType(
                scalar_arguments[i], parameter.type, fmt::format("Parameter '{}' of the KQL function '{}'", parameter.name, name));
    }

    return inner;
}

ASTPtr KQLParser::enforceParameterType(ASTPtr argument, const String & kql_type, String parameter_description)
{
    return makeASTFunction("kqlParameterCast", std::move(argument), makeLiteral(kql_type), makeLiteral(std::move(parameter_description)));
}

/// A tabular parameter declared `T: (a: long, ...)` names the columns its body may read, so
/// the argument is projected onto them: an undeclared column of the concrete argument stays
/// invisible inside the body, and a declared column the argument lacks is an error there.
/// Each declared column also carries a type, which is enforced on the argument's column.
/// `T: (*)` declares no columns and passes the argument through.
KQLTabularExpressionPtr
KQLParser::restrictToDeclaredColumns(const KQLTabularExpressionPtr & argument, const FunctionParameter & parameter, const String & function_name)
{
    if (parameter.tabular_columns.empty())
        return argument;

    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Project;
    op->name = "project";
    for (const TabularColumn & column : parameter.tabular_columns)
        op->expressions.push_back(KQLNamedExpression{
            column.name,
            enforceParameterType(
                makeIdentifier(column.name),
                column.type,
                fmt::format(
                    "Column '{}' of parameter '{}' of the KQL function '{}'", column.name, parameter.name, function_name))});

    auto restricted = std::make_shared<KQLTabularExpression>(*argument);
    restricted->operators.push_back(std::move(op));
    return restricted;
}

ASTPtr KQLParser::callScalarFunction(const String & name, const KQLToken & call_token)
{
    DepthGuard guard(*this, "function call");

    const FunctionDefinition definition = scope.functions.at(name);
    Scope inner = bindArguments(name, definition, call_token);

    if (!functions_in_progress.insert(name).second)
        failAt(call_token, fmt::format("'{}' calls itself, and KQL has no recursion", name));

    const size_t saved_index = index;
    Scope saved_scope = std::move(scope);
    scope = std::move(inner);
    index = definition.body_begin;

    /// A body is any number of `let` statements followed by one expression.
    while (atKeyword("let"))
    {
        parseLetStatement();
        if (!consume(KQLTokenType::Semicolon))
            fail("expected ';' after a 'let' statement in a function body");
    }
    ASTPtr result = parseExpression();

    if (index != definition.body_end)
        fail(fmt::format("unexpected text at the end of the body of '{}'", name));

    scope = std::move(saved_scope);
    index = saved_index;
    functions_in_progress.erase(name);
    return result;
}

KQLTabularExpressionPtr KQLParser::callTabularFunction(const String & name, const KQLToken & call_token)
{
    DepthGuard guard(*this, "function call");

    const FunctionDefinition definition = scope.functions.at(name);
    Scope inner = bindArguments(name, definition, call_token);

    if (!functions_in_progress.insert(name).second)
        failAt(call_token, fmt::format("'{}' calls itself, and KQL has no recursion", name));

    const size_t saved_index = index;
    Scope saved_scope = std::move(scope);
    scope = std::move(inner);
    index = definition.body_begin;

    while (atKeyword("let"))
    {
        parseLetStatement();
        if (!consume(KQLTokenType::Semicolon))
            fail("expected ';' after a 'let' statement in a function body");
    }
    KQLTabularExpressionPtr result = parseTabularExpression();

    if (index != definition.body_end)
        fail(fmt::format("unexpected text at the end of the body of '{}'", name));

    scope = std::move(saved_scope);
    index = saved_index;
    functions_in_progress.erase(name);
    return result;
}

void KQLParser::parseLetStatement()
{
    expectKeyword("let");
    const KQLToken & name_token = current();
    const String name = expectIdentifierName();
    expect(KQLTokenType::Equals);

    if (scope.scalars.contains(name) || scope.tabulars.contains(name) || scope.functions.contains(name))
        failAt(name_token, fmt::format("'{}' is already defined in this query", name));

    if (atFunctionDefinition())
    {
        parseFunctionDefinition(name);
        return;
    }

    /// A `let` may bind either a scalar or a whole tabular expression, and nothing at the `=`
    /// says which. The classifier that already reads function bodies decides: it knows
    /// pipelines, the source keywords (`datatable`, `union`, ...) under any parenthesizing,
    /// the names bound to something tabular, and bare physical table names - so
    /// `let T = (datatable (n: long) [1]);`, `let U = union A, B;` and `let T = Events;`
    /// bind tables, while `let x = (1 + 2);` stays a scalar.
    if (expressionLooksTabular(index, statementEnd(index, tokens.size()), scopeTabularNames(), scopeScalarNames()))
        scope.tabulars[name] = parseTabularExpression();
    else
        scope.scalars[name] = parseExpression();
}

KQLTabularExpressionPtr KQLParser::parseTabularExpression()
{
    DepthGuard guard(*this, "tabular expression");

    /// A tabular expression nested in an aggregation argument (`x in (T | ...)`) starts its
    /// own expression contexts; its operators may not aggregate any more than usual.
    const bool outer_in_aggregation = std::exchange(in_aggregation, false);

    auto result = std::make_shared<KQLTabularExpression>();
    result->source = parseSource();

    while (consume(KQLTokenType::Pipe))
        result->operators.push_back(parsePipelineOperator());

    in_aggregation = outer_in_aggregation;
    return result;
}

KQLSourcePtr KQLParser::parseSource()
{
    if (atKeyword("print"))
        return parsePrintSource();
    if (atKeyword("datatable"))
        return parseDataTableSource();
    if (atKeyword("range"))
        return parseRangeSource();

    if (at(KQLTokenType::OpeningRoundBracket))
    {
        ++index;
        auto source = std::make_shared<KQLSource>();
        source->kind = KQLSourceKind::Subquery;
        source->inputs.push_back(parseTabularExpression());
        expect(KQLTokenType::ClosingRoundBracket);
        return source;
    }

    if (atKeyword("union"))
    {
        ++index;
        auto source = std::make_shared<KQLSource>();
        source->kind = KQLSourceKind::Union;
        do
        {
            if (at(KQLTokenType::OpeningRoundBracket))
            {
                source->inputs.push_back(parseParenthesizedTabularExpression());
            }
            else
            {
                auto operand = std::make_shared<KQLTabularExpression>();
                operand->source = parseSource();
                source->inputs.push_back(operand);
            }
        } while (consume(KQLTokenType::Comma));

        if (source->inputs.size() < 2)
            fail("'union' needs at least two inputs");
        return source;
    }

    /// A bare name: either a `let`-bound tabular expression or a table.
    const KQLToken & name_token = current();
    const String name = expectIdentifierName();

    if (scope.functions.contains(name))
    {
        auto source = std::make_shared<KQLSource>();
        source->kind = KQLSourceKind::Subquery;
        source->inputs.push_back(callTabularFunction(name, name_token));
        return source;
    }

    if (auto it = scope.tabulars.find(name); it != scope.tabulars.end())
    {
        auto source = std::make_shared<KQLSource>();
        source->kind = KQLSourceKind::Subquery;
        source->inputs.push_back(it->second);
        return source;
    }

    /// An operator keyword here is not a table name. The hyphenated operators arrive as
    /// three tokens, so the check has to consider `<word>-<word>` too.
    String lowered_name = Poco::toLower(name);
    if (at(KQLTokenType::Minus) && lookahead().type == KQLTokenType::BareWord)
        lowered_name += "-" + Poco::toLower(String(lookahead().text()));

    if (pipelineOperatorNames().contains(lowered_name))
        failAt(name_token, fmt::format("'{}' is a pipeline operator, so it must follow a '|'", lowered_name));
    if (unsupportedOperatorNames().contains(lowered_name))
        failAt(name_token, fmt::format("'{}' is not a supported KQL operator", lowered_name));

    auto source = std::make_shared<KQLSource>();
    source->kind = KQLSourceKind::Table;
    source->table = name;

    /// `database.table`.
    if (consume(KQLTokenType::Dot))
    {
        source->database = source->table;
        source->table = expectIdentifierName();
    }

    return source;
}

KQLSourcePtr KQLParser::parsePrintSource()
{
    expectKeyword("print");
    auto source = std::make_shared<KQLSource>();
    source->kind = KQLSourceKind::Print;
    source->projections = parseNamedExpressionList();
    return source;
}

KQLSourcePtr KQLParser::parseDataTableSource()
{
    expectKeyword("datatable");
    auto source = std::make_shared<KQLSource>();
    source->kind = KQLSourceKind::DataTable;

    expect(KQLTokenType::OpeningRoundBracket);
    do
    {
        source->column_names.push_back(expectIdentifierName());
        expect(KQLTokenType::Colon);

        const KQLToken & type_token = current();
        const String kql_type = Poco::toLower(String(expectIdentifierName()));
        source->column_types.push_back(resolveScalarType(type_token, kql_type));
    } while (consume(KQLTokenType::Comma));
    expect(KQLTokenType::ClosingRoundBracket);

    expect(KQLTokenType::OpeningSquareBracket);
    if (!at(KQLTokenType::ClosingSquareBracket))
    {
        do
        {
            /// A trailing comma before `]` is allowed, as in Kusto.
            if (at(KQLTokenType::ClosingSquareBracket))
                break;
            source->values.push_back(parseExpression());
        } while (consume(KQLTokenType::Comma));
    }
    expect(KQLTokenType::ClosingSquareBracket);

    const size_t width = source->column_names.size();
    if (source->values.size() % width != 0)
        fail(fmt::format("datatable has {} columns but {} values, which is not a whole number of rows", width, source->values.size()));

    return source;
}

KQLSourcePtr KQLParser::parseRangeSource()
{
    expectKeyword("range");
    auto source = std::make_shared<KQLSource>();
    source->kind = KQLSourceKind::Range;
    source->range_column = expectIdentifierName();
    expectKeyword("from");
    source->range_from = parseExpression();
    expectKeyword("to");
    source->range_to = parseExpression();
    expectKeyword("step");
    source->range_step = parseExpression();
    return source;
}

KQLOperatorPtr KQLParser::parsePipelineOperator()
{
    DepthGuard guard(*this, "pipeline operator");

    if (!at(KQLTokenType::BareWord))
        fail("expected a pipeline operator after '|'");

    String name = Poco::toLower(String(current().text()));
    const KQLToken & name_token = current();

    /// `sort by` / `order by`, and the hyphenated operators the lexer splits on '-'.
    if ((name == "sort" || name == "order") && lookahead().type == KQLTokenType::BareWord)
    {
        /// consumed by parseSort, which expects the `by`
    }
    else if (lookahead().type == KQLTokenType::Minus && lookahead(2).type == KQLTokenType::BareWord)
    {
        name += "-";
        name += Poco::toLower(String(lookahead(2).text()));
    }

    auto it = pipelineOperatorNames().find(name);
    if (it == pipelineOperatorNames().end())
        failAt(name_token, fmt::format("'{}' is not a supported KQL operator", name));

    /// Consume the operator name (one token, or three for the hyphenated forms).
    if (name.contains('-'))
        index += 3;
    else
        ++index;

    KQLOperatorPtr result;
    switch (it->second)
    {
        case KQLOperatorKind::Where: result = parseWhere(); break;
        case KQLOperatorKind::Extend: result = parseExtend(); break;
        case KQLOperatorKind::Project: result = parseProject(); break;
        case KQLOperatorKind::ProjectAway: result = parseProjectAwayOrKeep(KQLOperatorKind::ProjectAway); break;
        case KQLOperatorKind::ProjectKeep: result = parseProjectAwayOrKeep(KQLOperatorKind::ProjectKeep); break;
        case KQLOperatorKind::ProjectRename: result = parseProjectRename(); break;
        case KQLOperatorKind::Summarize: result = parseSummarize(); break;
        case KQLOperatorKind::Sort: result = parseSort(); break;
        case KQLOperatorKind::Take: result = parseTake(); break;
        case KQLOperatorKind::Top: result = parseTop(); break;
        case KQLOperatorKind::Distinct: result = parseDistinct(); break;
        case KQLOperatorKind::Count: result = parseCount(); break;
        case KQLOperatorKind::MvExpand: result = parseMvExpand(); break;
        case KQLOperatorKind::Join: result = parseJoin(); break;
        case KQLOperatorKind::Union: result = parseUnion(); break;
        case KQLOperatorKind::As: result = parseAs(); break;
        case KQLOperatorKind::Render: result = parseRender(); break;
    }

    result->name = name;
    return result;
}

KQLOperatorPtr KQLParser::parseWhere()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Where;
    op->predicate = parseExpression();
    return op;
}

KQLOperatorPtr KQLParser::parseExtend()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Extend;
    op->expressions = parseNamedExpressionList();
    return op;
}

KQLOperatorPtr KQLParser::parseProject()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Project;
    op->expressions = parseNamedExpressionList();
    return op;
}

KQLOperatorPtr KQLParser::parseProjectAwayOrKeep(KQLOperatorKind kind)
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = kind;
    do
    {
        /// The argument is a column name possibly containing `*`, not an expression.
        if (at(KQLTokenType::Asterisk))
        {
            op->column_patterns.emplace_back("*");
            ++index;
            continue;
        }

        String pattern = expectIdentifierName();
        /// `Col*` lexes as an identifier followed by `*`.
        while (at(KQLTokenType::Asterisk))
        {
            pattern += "*";
            ++index;
            if (at(KQLTokenType::BareWord))
            {
                pattern += current().text();
                ++index;
            }
        }
        op->column_patterns.push_back(std::move(pattern));
    } while (consume(KQLTokenType::Comma));
    return op;
}

KQLOperatorPtr KQLParser::parseProjectRename()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::ProjectRename;
    do
    {
        String new_name = expectIdentifierName();
        expect(KQLTokenType::Equals);
        String old_name = expectIdentifierName();
        op->renames.emplace_back(std::move(new_name), std::move(old_name));
    } while (consume(KQLTokenType::Comma));
    return op;
}

KQLOperatorPtr KQLParser::parseSummarize()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Summarize;

    /// `summarize by X` aggregates nothing and just groups - the aggregation list is optional.
    /// Only its expressions may call aggregate functions; the `by` keys may not.
    if (!atKeyword("by"))
    {
        in_aggregation = true;
        do
        {
            op->expressions.push_back(parseNamedExpression());
        } while (consume(KQLTokenType::Comma));
        in_aggregation = false;
    }

    if (consumeKeyword("by"))
    {
        do
        {
            op->by_expressions.push_back(parseNamedExpression());
        } while (consume(KQLTokenType::Comma));
    }

    if (op->expressions.empty() && op->by_expressions.empty())
        fail("'summarize' needs an aggregation or a 'by' clause");

    return op;
}

std::vector<KQLSortItem> KQLParser::parseSortItems()
{
    std::vector<KQLSortItem> items;
    do
    {
        KQLSortItem item;
        item.expression = parseExpression();

        if (consumeKeyword("asc"))
            item.descending = false;
        else if (consumeKeyword("desc"))
            item.descending = true;

        /// Kusto puts nulls at the "small" end: first when ascending, last when descending.
        item.nulls_first = !item.descending;

        if (consumeKeyword("nulls"))
        {
            if (consumeKeyword("first"))
                item.nulls_first = true;
            else if (consumeKeyword("last"))
                item.nulls_first = false;
            else
                fail("expected 'first' or 'last' after 'nulls'");
        }

        items.push_back(std::move(item));
    } while (consume(KQLTokenType::Comma));
    return items;
}

KQLOperatorPtr KQLParser::parseSort()
{
    expectKeyword("by");
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Sort;
    op->sort_items = parseSortItems();
    return op;
}

KQLOperatorPtr KQLParser::parseTake()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Take;
    op->limit = parseExpression();
    return op;
}

KQLOperatorPtr KQLParser::parseTop()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Top;
    op->limit = parseExpression();
    expectKeyword("by");
    op->sort_items = parseSortItems();
    return op;
}

KQLOperatorPtr KQLParser::parseDistinct()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Distinct;

    if (at(KQLTokenType::Asterisk))
    {
        ++index;
        return op; /// Empty expression list means "all columns".
    }

    do
    {
        KQLNamedExpression named;
        named.expression = parseExpression();
        op->expressions.push_back(std::move(named));
    } while (consume(KQLTokenType::Comma));
    return op;
}

KQLOperatorPtr KQLParser::parseCount()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Count;
    op->alias = "Count";
    if (consumeKeyword("as"))
        op->alias = expectIdentifierName();
    return op;
}

KQLOperatorPtr KQLParser::parseMvExpand()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::MvExpand;
    do
    {
        op->expressions.push_back(parseNamedExpression());
    } while (consume(KQLTokenType::Comma));
    return op;
}

KQLTabularExpressionPtr KQLParser::parseParenthesizedTabularExpression()
{
    expect(KQLTokenType::OpeningRoundBracket);
    KQLTabularExpressionPtr result = parseTabularExpression();
    expect(KQLTokenType::ClosingRoundBracket);
    return result;
}

KQLOperatorPtr KQLParser::parseJoin()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Join;

    if (consumeKeyword("kind"))
    {
        expect(KQLTokenType::Equals);
        const KQLToken & kind_token = current();
        const String kind_name = Poco::toLower(String(expectIdentifierName()));
        auto it = joinKindNames().find(kind_name);
        if (it == joinKindNames().end())
            failAt(kind_token, fmt::format("'{}' is not a supported join kind", kind_name));
        op->join_kind = it->second;
    }

    /// `hint.*` options tune Kusto's distribution strategy and have no ClickHouse equivalent.
    /// Rejecting is better than accepting a hint we would silently ignore.
    if (atKeyword("hint"))
        fail("join hints are not supported");

    op->inputs.push_back(parseParenthesizedTabularExpression());

    expectKeyword("on");
    do
    {
        /// `on Key` or `on $left.A == $right.B`.
        if (at(KQLTokenType::BareWord) && current().text() == "$left")
        {
            index += 1;
            expect(KQLTokenType::Dot);
            String left = expectIdentifierName();
            expect(KQLTokenType::DoubleEquals);
            if (!(at(KQLTokenType::BareWord) && current().text() == "$right"))
                fail("expected '$right' on the right-hand side of a join condition");
            ++index;
            expect(KQLTokenType::Dot);
            String right = expectIdentifierName();
            op->join_keys.emplace_back(std::move(left), std::move(right));
        }
        else
        {
            String key = expectIdentifierName();
            op->join_keys.emplace_back(key, key);
        }
    } while (consume(KQLTokenType::Comma) || consumeKeyword("and"));

    return op;
}

KQLOperatorPtr KQLParser::parseUnion()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Union;

    if (consumeKeyword("kind"))
    {
        expect(KQLTokenType::Equals);
        const KQLToken & kind_token = current();
        const String kind_name = Poco::toLower(String(expectIdentifierName()));
        if (kind_name == "outer" || kind_name == "inner")
            failAt(kind_token, fmt::format("'union kind={}' is not supported", kind_name));
        else
            failAt(kind_token, fmt::format("'{}' is not a supported union kind", kind_name));
    }

    do
    {
        if (at(KQLTokenType::OpeningRoundBracket))
        {
            op->inputs.push_back(parseParenthesizedTabularExpression());
        }
        else
        {
            auto operand = std::make_shared<KQLTabularExpression>();
            operand->source = parseSource();
            op->inputs.push_back(operand);
        }
    } while (consume(KQLTokenType::Comma));

    return op;
}

KQLOperatorPtr KQLParser::parseAs()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::As;
    op->alias = expectIdentifierName();
    return op;
}

KQLOperatorPtr KQLParser::parseRender()
{
    auto op = std::make_shared<KQLOperator>();
    op->kind = KQLOperatorKind::Render;
    /// `render` is a client-side hint even in Kusto; the server returns the same rows.
    /// Consume the visualization name and any `with (...)` options, then do nothing.
    if (at(KQLTokenType::BareWord))
        ++index;
    if (consumeKeyword("with"))
    {
        expect(KQLTokenType::OpeningRoundBracket);
        int nesting = 1;
        while (nesting > 0)
        {
            if (at(KQLTokenType::EndOfStream) || at(KQLTokenType::Error))
                fail("unterminated 'with (' in 'render'");
            if (at(KQLTokenType::OpeningRoundBracket))
                ++nesting;
            else if (at(KQLTokenType::ClosingRoundBracket))
                --nesting;
            ++index;
        }
    }
    return op;
}

std::vector<KQLNamedExpression> KQLParser::parseNamedExpressionList()
{
    std::vector<KQLNamedExpression> result;
    do
    {
        result.push_back(parseNamedExpression());
    } while (consume(KQLTokenType::Comma));
    return result;
}

KQLNamedExpression KQLParser::parseNamedExpression()
{
    KQLNamedExpression named;

    /// `project *` selects every column. A wildcard is only itself - it cannot be named or
    /// take part in an expression - so it is recognized here rather than in `parsePrimary`.
    if (at(KQLTokenType::Asterisk))
    {
        ++index;
        named.expression = make_intrusive<ASTAsterisk>();
        return named;
    }

    /// `Name = expression`, distinguished from the comparison `a == b` by the single '='.
    const bool has_alias = (at(KQLTokenType::BareWord) && lookahead().type == KQLTokenType::Equals)
        || (at(KQLTokenType::OpeningSquareBracket) && lookahead().type == KQLTokenType::StringLiteral
            && lookahead(2).type == KQLTokenType::ClosingSquareBracket && lookahead(3).type == KQLTokenType::Equals);

    if (has_alias)
    {
        named.alias = expectIdentifierName();
        expect(KQLTokenType::Equals);
    }

    named.expression = parseExpression();
    return named;
}

/// Comparison and the logical connectives return `UInt8` in ClickHouse, which prints as
/// 0 and 1. KQL has a real `bool`, so a predicate is presented as one - otherwise
/// `print a > b` answers `1` where Kusto answers `true`.
namespace
{
bool producesBoolean(const ASTPtr & node)
{
    const auto * function = node->as<ASTFunction>();
    if (!function)
        return false;

    static const std::set<String> boolean_functions{
        /// Comparison and the connectives.
        "equals",
        "notEquals",
        "less",
        "lessOrEquals",
        "greater",
        "greaterOrEquals",
        "and",
        "or",
        "not",
        "in",
        "notIn",
        "match",
        "startsWith",
        "endsWith",
        /// The KQL predicates that are documented as returning `bool`.
        "isNull",
        "isNotNull",
        "isNaN",
        "isInfinite",
        "isFinite",
        "has",
        "empty",
        "notEmpty",
        "isIPAddressInRange",
        "isValidUTF8"};
    return boolean_functions.contains(function->name);
}
}

ASTPtr KQLParser::parseExpression()
{
    DepthGuard guard(*this, "expression");

    ASTPtr result = parseOr();
    if (producesBoolean(result))
        return makeASTFunction("toBool", result);
    return result;
}

ASTPtr KQLParser::parseOr()
{
    ASTPtr left = parseAnd();
    while (atKeyword("or"))
    {
        ++index;
        left = makeASTFunction("or", left, parseAnd());
    }
    return left;
}

ASTPtr KQLParser::parseAnd()
{
    ASTPtr left = parseComparison();
    while (atKeyword("and"))
    {
        ++index;
        left = makeASTFunction("and", left, parseComparison());
    }
    return left;
}

ASTPtr KQLParser::parseComparison()
{
    ASTPtr left = parseAdditive();

    /// Comparison does not chain in KQL: `a < b < c` is an error, not `(a < b) < c`.
    static const std::map<KQLTokenType, const char *> comparisons{
        {KQLTokenType::DoubleEquals, "equals"},
        {KQLTokenType::NotEquals, "notEquals"},
        {KQLTokenType::Less, "less"},
        {KQLTokenType::LessOrEquals, "lessOrEquals"},
        {KQLTokenType::Greater, "greater"},
        {KQLTokenType::GreaterOrEquals, "greaterOrEquals"},
    };

    if (auto it = comparisons.find(current().type); it != comparisons.end())
    {
        ++index;
        return makeASTFunction(it->second, left, parseAdditive());
    }

    /// `=~` and `!~` are case-insensitive string equality.
    if (at(KQLTokenType::TildeEquals) || at(KQLTokenType::NotTildeEquals))
    {
        const bool negated = at(KQLTokenType::NotTildeEquals);
        ++index;
        ASTPtr right = parseAdditive();
        ASTPtr equal = kqlCaseInsensitiveEquals(left, right);
        return negated ? ASTPtr(makeASTFunction("not", equal)) : equal;
    }

    if (at(KQLTokenType::Equals))
        fail("'=' is assignment in KQL; use '==' to compare");

    if (ASTPtr word_operator = tryParseWordOperator(left))
        return word_operator;

    return left;
}

ASTPtr KQLParser::parseAdditive()
{
    ASTPtr left = parseMultiplicative();
    while (at(KQLTokenType::Plus) || at(KQLTokenType::Minus))
    {
        const bool is_plus = at(KQLTokenType::Plus);
        ++index;
        left = makeASTFunction(is_plus ? "plus" : "minus", left, parseMultiplicative());
    }
    return left;
}

ASTPtr KQLParser::parseMultiplicative()
{
    ASTPtr left = parseUnary();
    while (at(KQLTokenType::Asterisk) || at(KQLTokenType::Slash) || at(KQLTokenType::Percent))
    {
        const KQLTokenType type = current().type;
        ++index;
        ASTPtr right = parseUnary();
        if (type == KQLTokenType::Asterisk)
            /// Kusto scales a timespan by a number (`2 * 1h`), which the ordinary `multiply`
            /// does not take; the operand types are only known at runtime.
            left = makeASTFunction("kqlMultiply", left, right);
        else if (type == KQLTokenType::Slash)
            /// KQL divides integers as integers: `7 / 2` is 3, not 3.5. Only the runtime
            /// knows the operand types, so the choice is made there.
            left = makeASTFunction("kqlDivide", left, right);
        else
            left = makeASTFunction("modulo", left, right);
    }
    return left;
}

ASTPtr KQLParser::parseUnary()
{
    if (at(KQLTokenType::Minus))
    {
        ++index;
        return makeASTFunction("negate", parseUnary());
    }
    if (at(KQLTokenType::Plus))
    {
        ++index;
        return parseUnary();
    }
    if (atKeyword("not"))
    {
        ++index;
        return makeASTFunction("not", parseUnary());
    }
    return parsePostfix(parsePrimary());
}

ASTPtr KQLParser::parsePostfix(ASTPtr operand)
{
    while (true)
    {
        if (at(KQLTokenType::OpeningSquareBracket))
        {
            const KQLToken & bracket = current();
            ++index;
            ASTPtr subscript = parseExpression();
            expect(KQLTokenType::ClosingSquareBracket);

            /// KQL's `dynamic` maps to a ClickHouse `Array`, so a subscript is `arrayElement`.
            /// KQL indexes from 0 and ClickHouse from 1. Object member lookup is a different
            /// thing that this mapping does not cover - see `parseDynamicLiteral`.
            if (const auto * literal = subscript->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
                failAt(bracket, "indexing a dynamic object by key is not supported");

            operand = makeASTFunction(
                "arrayElement",
                operand,
                makeASTFunction(
                    "if",
                    makeASTFunction("less", subscript, makeLiteral(static_cast<Int64>(0))),
                    subscript->clone(),
                    makeASTFunction("plus", subscript->clone(), makeLiteral(static_cast<Int64>(1)))));
            continue;
        }

        if (at(KQLTokenType::Dot) && lookahead().type == KQLTokenType::BareWord)
            fail("member access on a dynamic value is not supported");

        return operand;
    }
}

ASTPtr KQLParser::parseParenthesizedOrInList()
{
    expect(KQLTokenType::OpeningRoundBracket);
    ASTPtr inner = parseExpression();
    expect(KQLTokenType::ClosingRoundBracket);
    return inner;
}

ASTPtr KQLParser::parsePrimary()
{
    DepthGuard guard(*this, "expression");

    const KQLToken & token = current();

    switch (token.type)
    {
        case KQLTokenType::Error: fail("invalid token");

        case KQLTokenType::Number: {
            const std::string_view text = token.text();
            ++index;
            if (text.size() > 2 && text[0] == '0' && (text[1] == 'x' || text[1] == 'X'))
            {
                UInt64 value = 0;
                for (size_t i = 2; i < text.size(); ++i)
                    value = value * 16 + unhex(text[i]);
                return makeLiteral(value);
            }
            if (text.contains('.') || text.contains('e') || text.contains('E'))
                return makeLiteral(std::stod(String(text)));

            /// KQL integer literals are 64-bit signed (`long`).
            const String digits(text);
            errno = 0;
            char * parse_end = nullptr;
            const Int64 value = static_cast<Int64>(std::strtoll(digits.c_str(), &parse_end, 10));
            if (errno == ERANGE || parse_end != digits.c_str() + digits.size())
                failAt(token, "integer literal is out of range");
            return makeLiteral(value);
        }

        case KQLTokenType::StringLiteral: {
            /// The whole point of the rewrite: user text becomes a literal *node*. There is
            /// no representation in which it could be read back as SQL syntax.
            String value = token.inner;
            ++index;
            return makeLiteral(std::move(value));
        }

        case KQLTokenType::Timespan: {
            const Int64 ticks = token.timespan_ticks;
            ++index;
            /// A KQL timespan becomes a ClickHouse `Interval` in nanoseconds: the tick is
            /// 100 ns, which no coarser interval kind can represent exactly. As a real
            /// interval it both adds to a datetime and renders as `1.00:00:00` under
            /// `interval_output_format = 'kusto'`.
            if (ticks > std::numeric_limits<Int64>::max() / 100 || ticks < std::numeric_limits<Int64>::min() / 100)
                failAt(token, "timespan literal is too large to represent in nanoseconds");
            return makeASTFunction("toIntervalNanosecond", makeLiteral(ticks * 100));
        }

        case KQLTokenType::DateTimeLiteral: {
            String text = token.inner;
            ++index;
            if (Poco::toLower(text) == "null")
                return makeASTFunction("CAST", makeLiteral(Field()), makeLiteral(String("Nullable(DateTime64(7, 'UTC'))")));
            return makeASTFunction(
                "parseDateTime64BestEffortOrNull",
                makeLiteral(std::move(text)),
                makeLiteral(static_cast<UInt64>(7)),
                makeLiteral(String("UTC")));
        }

        case KQLTokenType::GuidLiteral: {
            String text = token.inner;
            ++index;
            return makeASTFunction("toUUIDOrNull", makeLiteral(std::move(text)));
        }

        case KQLTokenType::OpeningRoundBracket: return parseParenthesizedOrInList();

        case KQLTokenType::OpeningSquareBracket: {
            /// `['column name']` - a quoted column reference.
            if (lookahead().type == KQLTokenType::StringLiteral && lookahead(2).type == KQLTokenType::ClosingSquareBracket)
                return makeIdentifier(expectIdentifierName());
            fail("expected a quoted column name");
        }

        case KQLTokenType::BareWord: break;

        default: fail("expected an expression");
    }

    const String word(token.text());
    const String lowered = Poco::toLower(word);

    if (lowered == "true" || lowered == "false")
    {
        ++index;
        return makeLiteral(lowered == "true");
    }

    if (lowered == "null")
    {
        ++index;
        return makeLiteral(Field());
    }

    /// `dynamic([1, 2, 3])` / `dynamic({"a": 1})` - a literal of KQL's JSON-ish type.
    if (lowered == "dynamic" && lookahead().type == KQLTokenType::OpeningRoundBracket)
    {
        index += 2;
        ASTPtr value = parseDynamicLiteral();
        expect(KQLTokenType::ClosingRoundBracket);
        return value;
    }

    if (scope.functions.contains(word))
    {
        const KQLToken & call_token = current();
        /// A function whose body is a pipeline is a table, not a value. Re-parsing such a
        /// body as a scalar expression would silently read its leading name as a column of
        /// the current row, so a scalar site rejects it outright.
        if (scope.functions.at(word).body_looks_tabular)
            failAt(call_token, fmt::format("'{}' is a tabular function, and cannot be used in a scalar expression", word));
        ++index;
        return callScalarFunction(word, call_token);
    }

    /// `typeof(long)` names a type. It appears as the optional last argument of `extract`,
    /// and is passed on as the ClickHouse type name.
    if (lowered == "typeof" && lookahead().type == KQLTokenType::OpeningRoundBracket)
    {
        index += 2;
        const KQLToken & type_token = current();
        const String kql_type = Poco::toLower(String(expectIdentifierName()));
        const String & type = resolveScalarType(type_token, kql_type);
        expect(KQLTokenType::ClosingRoundBracket);
        return makeLiteral(type);
    }

    if (lookahead().type == KQLTokenType::OpeningRoundBracket)
    {
        if (ASTPtr typed = tryParseTypedLiteral(lowered))
            return typed;

        ++index;
        return parseFunctionCall(lowered);
    }

    /// A plain name: a `let`-bound scalar, or a column reference.
    ++index;
    if (auto it = scope.scalars.find(word); it != scope.scalars.end())
        return it->second->clone();

    return makeIdentifier(word);
}

ASTPtr KQLParser::parseDynamicLiteral()
{
    DepthGuard guard(*this, "dynamic literal");

    /// KQL `dynamic` holds JSON. We build the JSON text here, at parse time, from literal
    /// tokens only - so the result is a single `ASTLiteral(String)` that the JSON functions
    /// read at runtime. Nothing the user typed becomes SQL syntax.
    if (at(KQLTokenType::OpeningSquareBracket))
    {
        ++index;
        Array elements;
        if (!at(KQLTokenType::ClosingSquareBracket))
        {
            do
            {
                ASTPtr element = parseDynamicLiteral();
                std::optional<Field> value = tryFoldConstant(element);
                if (!value)
                    fail("a dynamic literal may only contain constants");
                elements.push_back(std::move(*value));
            } while (consume(KQLTokenType::Comma));
        }
        expect(KQLTokenType::ClosingSquareBracket);
        return makeLiteral(std::move(elements));
    }

    /// A dynamic *object* has no faithful ClickHouse counterpart under the array mapping
    /// used here, so it is rejected rather than approximated.
    if (at(KQLTokenType::OpeningCurlyBrace))
        fail("dynamic object literals are not supported");

    return parseExpression();
}

namespace
{
/// Ticks for a number written apart from its unit (`timespan(15 seconds)`) or with the unit
/// implied (`timespan(2)` is two days). Mirrors the lexer's rule for the glued form `15s`;
/// `std::nullopt` means the value does not fit a timespan.
std::optional<Int64> timespanTicksFromNumber(std::string_view number_text, std::string_view unit)
{
    const bool is_nanosecond = unit == "nanosecond" || unit == "nanoseconds";
    const Int64 unit_ticks = kqlTimespanUnitInTicks(unit);

    const bool is_floating_point = number_text.find_first_of(".eExX") != std::string_view::npos;
    if (is_floating_point)
    {
        const double mantissa = std::stod(String(number_text));
        /// A tick is 100 ns, so nanoseconds are a tenth of one.
        const double ticks = is_nanosecond ? mantissa / 100.0 : mantissa * static_cast<double>(unit_ticks);
        if (!std::isfinite(ticks) || std::abs(ticks) > 9.2e18)
            return {};
        return static_cast<Int64>(std::llround(ticks));
    }

    Int64 mantissa = 0;
    for (const char digit : number_text)
    {
        if (mantissa > (std::numeric_limits<Int64>::max() - (digit - '0')) / 10)
            return {};
        mantissa = mantissa * 10 + (digit - '0');
    }
    if (is_nanosecond)
        return mantissa / 100;
    if (unit_ticks != 0 && mantissa > std::numeric_limits<Int64>::max() / unit_ticks)
        return {};
    return mantissa * unit_ticks;
}
}

ASTPtr KQLParser::tryParseTypedLiteral(const String & name)
{
    enum class Kind : uint8_t
    {
        Boolean,
        Integer,
        Real,
        Decimal,
        Timespan,
        Text
    };

    static const std::map<String, Kind> kinds{
        {"bool", Kind::Boolean},
        {"boolean", Kind::Boolean},
        {"int", Kind::Integer},
        {"long", Kind::Integer},
        {"real", Kind::Real},
        {"double", Kind::Real},
        {"decimal", Kind::Decimal},
        {"timespan", Kind::Timespan},
        {"time", Kind::Timespan},
        {"string", Kind::Text},
    };

    auto it = kinds.find(name);
    if (it == kinds.end())
        return nullptr;
    const Kind kind = it->second;

    const KQLToken & name_token = current();
    const size_t saved_index = index;
    index += 2; /// The name and the '('.

    /// A typed literal takes a literal, not an expression: Kusto rejects `int('4')`.
    const auto bad = [&](const String & what) -> ASTPtr
    {
        const size_t offset = name_token.begin >= query_begin ? static_cast<size_t>(name_token.begin - query_begin) : 0;
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "KQL literal {}(...) at position {} {}", name, offset + 1, what);
    };

    bool negative = false;
    if (at(KQLTokenType::Minus))
    {
        negative = true;
        ++index;
    }
    else if (at(KQLTokenType::Plus))
    {
        ++index;
    }

    ASTPtr value;
    const KQLToken & token = current();

    if (token.type == KQLTokenType::BareWord)
    {
        const String word = Poco::toLower(String(token.text()));
        if (word == "null")
        {
            ++index;
            static const std::map<Kind, const char *> null_types{
                {Kind::Boolean, "Nullable(Bool)"},
                {Kind::Integer, "Nullable(Int64)"},
                {Kind::Real, "Nullable(Float64)"},
                {Kind::Decimal, "Nullable(Decimal128(20))"},
                {Kind::Timespan, "Nullable(IntervalNanosecond)"},
                {Kind::Text, "Nullable(String)"},
            };
            value = makeASTFunction("CAST", makeLiteral(Field()), makeLiteral(String(null_types.at(kind))));
        }
        else if (kind == Kind::Boolean && (word == "true" || word == "false"))
        {
            ++index;
            value = makeLiteral(word == "true");
        }
        else if (kind == Kind::Real && (word == "nan" || word == "inf" || word == "infinity"))
        {
            ++index;
            const double magnitude = word == "nan" ? std::numeric_limits<double>::quiet_NaN() : std::numeric_limits<double>::infinity();
            value = makeLiteral(negative && word != "nan" ? -magnitude : magnitude);
            negative = false;
        }
        else
        {
            index = saved_index;
            return bad("expects a literal");
        }
    }
    else if (token.type == KQLTokenType::Number)
    {
        if (kind == Kind::Decimal)
        {
            /// Keep the digits the user wrote; a Float64 round-trip would not be exact.
            String digits(token.text());
            ++index;
            value = makeLiteral(negative ? "-" + digits : digits);
            negative = false;
        }
        else if (kind == Kind::Timespan)
        {
            /// The constructor forms Kusto documents beyond the glued literal `timespan(15s)`:
            /// a bare number is days (`timespan(2)`), the unit may be a separate word
            /// (`timespan(15 seconds)`), and the printed form may come unquoted
            /// (`timespan(0.12:34:56.7)`).
            std::optional<Int64> ticks;
            if (lookahead().type == KQLTokenType::BareWord)
            {
                const KQLToken & unit_token = lookahead();
                const std::string_view unit = unit_token.text();
                if (kqlTimespanUnitInTicks(unit) == 0 && unit != "nanosecond" && unit != "nanoseconds")
                    return bad(fmt::format("does not know the timespan unit '{}'", unit));
                ticks = timespanTicksFromNumber(token.text(), unit);
                index += 2;
            }
            else if (lookahead().type == KQLTokenType::Colon)
            {
                /// `d.hh:mm:ss.fffffff` lexes as numbers and colons; the reading is defined
                /// on the raw text, so take the slice up to the closing parenthesis.
                size_t last = index;
                while (tokens[last].type != KQLTokenType::ClosingRoundBracket && !tokens[last].isEnd())
                    ++last;
                if (tokens[last].isEnd())
                    return bad("is missing its closing parenthesis");
                const std::string_view text{token.begin, static_cast<size_t>(tokens[last - 1].end - token.begin)};
                ticks = kqlParseTimespanText(text);
                if (!ticks)
                    return bad("could not read the timespan");
                index = last;
            }
            else
            {
                ticks = timespanTicksFromNumber(token.text(), "d");
                ++index;
            }
            if (!ticks)
                return bad("does not fit a timespan");
            Int64 nanoseconds = 0;
            if (common::mulOverflow<Int64>(*ticks, 100, nanoseconds))
                return bad("does not fit a timespan");
            value = makeASTFunction("toIntervalNanosecond", makeLiteral(nanoseconds));
        }
        else
        {
            value = parsePrimary();
        }
    }
    else if (token.type == KQLTokenType::Timespan && kind == Kind::Timespan)
    {
        value = parsePrimary();
    }
    else if (token.type == KQLTokenType::StringLiteral)
    {
        if (kind == Kind::Text)
        {
            value = parsePrimary();
        }
        else if (kind == Kind::Timespan)
        {
            const auto ticks = kqlParseTimespanText(token.inner);
            if (!ticks)
                return bad("could not read the timespan");
            ++index;
            Int64 nanoseconds = 0;
            if (common::mulOverflow<Int64>(*ticks, 100, nanoseconds))
                return bad("does not fit a timespan");
            value = makeASTFunction("toIntervalNanosecond", makeLiteral(nanoseconds));
        }
        else
        {
            /// `int('4')` is an error in Kusto: the constructor is not a cast.
            return bad("does not accept a string; use a cast such as toint()");
        }
    }
    else
    {
        return bad("expects a literal");
    }

    if (!consume(KQLTokenType::ClosingRoundBracket))
        return bad("is missing its closing parenthesis");

    if (negative)
        value = makeASTFunction("negate", value);

    /// Give the value the type the constructor names, so `int(1)` and `long(1)` differ.
    switch (kind)
    {
        case Kind::Boolean: return makeASTFunction("accurateCastOrNull", value, makeLiteral(String("Bool")));
        case Kind::Integer:
            return name == "int" ? ASTPtr(makeASTFunction("accurateCastOrNull", value, makeLiteral(String("Int32"))))
                                 : ASTPtr(makeASTFunction("accurateCastOrNull", value, makeLiteral(String("Int64"))));
        case Kind::Real: return makeASTFunction("accurateCastOrNull", value, makeLiteral(String("Float64")));
        case Kind::Decimal: return makeASTFunction("accurateCastOrNull", value, makeLiteral(String("Decimal128(20)")));
        case Kind::Timespan:
        case Kind::Text: return value;
    }
    return value;
}

ASTPtr KQLParser::parseFunctionCall(const String & name)
{
    DepthGuard guard(*this, "function call");

    const KQLToken & name_token = tokens[index - 1];
    expect(KQLTokenType::OpeningRoundBracket);

    ASTs arguments;
    if (!at(KQLTokenType::ClosingRoundBracket))
    {
        do
        {
            arguments.push_back(parseExpression());
        } while (consume(KQLTokenType::Comma));
    }
    expect(KQLTokenType::ClosingRoundBracket);

    String error;
    ASTPtr result = translateKQLFunction(name, String(name_token.text()), arguments, in_aggregation, error);
    if (!result)
        failAt(name_token, error);

    return result;
}

ASTPtr KQLParser::tryParseWordOperator(const ASTPtr & left)
{
    if (!at(KQLTokenType::BareWord))
        return nullptr;

    String op = Poco::toLower(String(current().text()));

    bool negated = false;
    if (op.starts_with('!'))
    {
        negated = true;
        op = op.substr(1);
    }

    const auto finish = [&](ASTPtr predicate) -> ASTPtr { return negated ? ASTPtr(makeASTFunction("not", predicate)) : predicate; };

    /// `between (low .. high)`
    if (op == "between")
    {
        ++index;
        expect(KQLTokenType::OpeningRoundBracket);
        ASTPtr low = parseAdditive();
        expect(KQLTokenType::DotDot);
        ASTPtr high = parseAdditive();
        expect(KQLTokenType::ClosingRoundBracket);
        return finish(
            makeASTFunction("and", makeASTFunction("greaterOrEquals", left, low), makeASTFunction("lessOrEquals", left->clone(), high)));
    }

    /// `in (a, b, c)` and its case-insensitive form `in~`.
    if (op == "in" || op == "in~")
    {
        const bool case_insensitive = op == "in~";
        const KQLToken & op_token = current();
        ++index;
        expect(KQLTokenType::OpeningRoundBracket);

        /// The right-hand side may also be a whole tabular expression whose first column
        /// supplies the values: `x in (T | project key)`. The classifier that already reads
        /// `let` right-hand sides decides, over the tokens up to the closing ')' - except
        /// that a lone name bound to nothing reads as a column here (`x in (y)`), not as a
        /// physical table: a `let`-bound tabular name or a `db.table` form is still a table.
        if (expressionLooksTabular(index, closingBracket(index), scopeTabularNames(), scopeScalarNames(), /*unknown_name_is_table=*/false))
        {
            /// There is no case-insensitive `IN`, and the spelled-out disjunction the list
            /// form uses needs the values at hand, which a subquery's are not.
            if (case_insensitive)
                failAt(op_token, "'in~' does not take a tabular expression; lowercase both sides and use 'in'");
            KQLTabularExpressionPtr table = parseTabularExpression();
            expect(KQLTokenType::ClosingRoundBracket);
            auto subquery = make_intrusive<ASTSubquery>(translateKQLQuery(*table));
            return finish(makeASTFunction("in", left, subquery));
        }

        ASTs elements;
        if (!at(KQLTokenType::ClosingRoundBracket))
        {
            do
            {
                elements.push_back(parseExpression());
            } while (consume(KQLTokenType::Comma));
        }
        expect(KQLTokenType::ClosingRoundBracket);

        if (case_insensitive)
        {
            /// There is no case-insensitive `IN`, so the membership test is spelled out as a
            /// disjunction of case-insensitive equalities.
            if (elements.empty())
                return finish(makeLiteral(false));

            ASTs comparisons;
            for (const auto & element : elements)
                comparisons.push_back(kqlCaseInsensitiveEquals(left->clone(), element));
            if (comparisons.size() == 1)
                return finish(comparisons.front());

            auto disjunction = makeASTFunction("or");
            disjunction->arguments->children = comparisons;
            return finish(ASTPtr(disjunction));
        }

        auto tuple = makeASTFunction("tuple");
        tuple->arguments->children = elements;
        return finish(makeASTFunction("in", left, tuple));
    }

    /// `has_any (a, b)` / `has_all (a, b)`
    if (op == "has_any" || op == "has_all")
    {
        const bool all = op == "has_all";
        const KQLToken & op_token = current();
        ++index;
        expect(KQLTokenType::OpeningRoundBracket);
        ASTs needles;
        do
        {
            needles.push_back(parseExpression());
        } while (consume(KQLTokenType::Comma));
        expect(KQLTokenType::ClosingRoundBracket);

        ASTPtr combined;
        for (const auto & needle : needles)
        {
            String error;
            ASTPtr term = buildKQLStringOperator("has", left->clone(), needle, error);
            if (!term)
                failAt(op_token, error);
            combined = combined ? ASTPtr(makeASTFunction(all ? "and" : "or", combined, term)) : term;
        }
        return finish(combined);
    }

    /// The substring and term operators. Each becomes a matching function whose semantics
    /// match Kusto's, never a LIKE pattern - `contains '50%'` must not match '50x'.
    {
        static const std::set<String> string_operators{
            "contains",
            "contains_cs",
            "startswith",
            "startswith_cs",
            "endswith",
            "endswith_cs",
            "has",
            "has_cs",
            "hasprefix",
            "hasprefix_cs",
            "hassuffix",
            "hassuffix_cs"};

        if (string_operators.contains(op))
        {
            const KQLToken & op_token = current();
            ++index;
            ASTPtr needle = parseAdditive();
            String error;
            ASTPtr predicate = buildKQLStringOperator(op, left, needle, error);
            if (!predicate)
                failAt(op_token, error);
            return finish(predicate);
        }
    }

    /// `matches regex "..."`
    if (op == "matches")
    {
        if (!(lookahead().type == KQLTokenType::BareWord && Poco::toLower(String(lookahead().text())) == "regex"))
            return nullptr;
        index += 2;
        ASTPtr pattern = parseAdditive();
        return finish(makeASTFunction("match", left, pattern));
    }

    return nullptr;
}

}
