#include <Parsers/LogsQL/LogsQLParser.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Common/Exception.h>
#include <Common/re2.h>
#include <Poco/String.h>

#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdio>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_DEEP_RECURSION;
}

namespace
{

using namespace LogsQLUtils;

/// Combines conditions with AND. A null condition means "match all" and is skipped.
ASTPtr makeAnd(ASTs conditions)
{
    ASTs filtered;
    for (auto & condition : conditions)
        if (condition)
            filtered.push_back(std::move(condition));

    if (filtered.empty())
        return nullptr;
    if (filtered.size() == 1)
        return filtered[0];
    auto function = makeASTFunction("and");
    function->arguments->children = std::move(filtered);
    return function;
}

/// Combines conditions with OR. A null condition means "match all", so the whole OR matches everything.
ASTPtr makeOr(ASTs conditions)
{
    for (const auto & condition : conditions)
        if (!condition)
            return nullptr;

    if (conditions.size() == 1)
        return conditions[0];
    auto function = makeASTFunction("or");
    function->arguments->children = std::move(conditions);
    return function;
}

ASTPtr makeNot(ASTPtr condition)
{
    if (!condition)
        return make_intrusive<ASTLiteral>(Field(static_cast<UInt8>(0)));
    return makeASTFunction("not", std::move(condition));
}

/// Word boundary patterns for RE2. Words consist of ASCII alphanumeric characters here.
/// This matches the tokenization of hasToken and of the tokenbf_v1 skip index
/// (VictoriaLogs additionally treats underscores and non-ASCII letters and digits as word characters).
constexpr const char * boundary_before = "(?:^|[^0-9A-Za-z])";
constexpr const char * boundary_after = "(?:$|[^0-9A-Za-z])";

bool isPlainASCIIToken(const String & text)
{
    if (text.empty())
        return false;
    for (char c : text)
    {
        bool is_token_char = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
        if (!is_token_char)
            return false;
    }
    return true;
}

/// True if the text is a plain decimal integer or float, so that it can be used
/// as a numeric literal in the resulting query.
bool isPlainNumber(const String & text)
{
    bool seen_digit = false;
    bool seen_dot = false;
    for (size_t i = 0; i < text.size(); ++i)
    {
        char c = text[i];
        if (c == '-' || c == '+')
        {
            if (i != 0)
                return false;
        }
        else if (c == '.')
        {
            if (seen_dot)
                return false;
            seen_dot = true;
        }
        else if (c >= '0' && c <= '9')
        {
            seen_digit = true;
        }
        else
        {
            return false;
        }
    }
    return seen_digit;
}

const std::vector<std::string_view> field_name_stop_tokens = {":"};

}

LogsQLParser::IncreaseDepth::IncreaseDepth(LogsQLParser & parser_) : parser(parser_)
{
    ++parser.depth;
    if (parser.context.max_depth && parser.depth > parser.context.max_depth)
        throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "Maximum parse depth ({}) exceeded in a LogsQL query", parser.context.max_depth);
}

LogsQLParser::IncreaseDepth::~IncreaseDepth()
{
    --parser.depth;
}

LogsQLParser::QueryScopeGuard::QueryScopeGuard(LogsQLParser & parser_)
    : parser(parser_)
    , saved_options_time_offset_ns(parser_.options_time_offset_ns)
    , saved_options_global_filter(parser_.options_global_filter)
    , saved_query_time_lower_bound_expr(parser_.query_time_lower_bound_expr)
    , saved_query_time_lower_bound_ns(parser_.query_time_lower_bound_ns)
    , saved_query_time_upper_bound_expr(parser_.query_time_upper_bound_expr)
    , saved_query_time_upper_bound_ns(parser_.query_time_upper_bound_ns)
    , saved_current_stats_time_bucket_ns(parser_.current_stats_time_bucket_ns)
    , saved_current_stats_time_bucket_seconds_expr(parser_.current_stats_time_bucket_seconds_expr)
    , saved_current_stats_time_bucket_is_calendar(parser_.current_stats_time_bucket_is_calendar)
{
}

LogsQLParser::QueryScopeGuard::~QueryScopeGuard()
{
    parser.options_time_offset_ns = saved_options_time_offset_ns;
    parser.options_global_filter = saved_options_global_filter;
    parser.query_time_lower_bound_expr = saved_query_time_lower_bound_expr;
    parser.query_time_lower_bound_ns = saved_query_time_lower_bound_ns;
    parser.query_time_upper_bound_expr = saved_query_time_upper_bound_expr;
    parser.query_time_upper_bound_ns = saved_query_time_upper_bound_ns;
    parser.current_stats_time_bucket_ns = saved_current_stats_time_bucket_ns;
    parser.current_stats_time_bucket_seconds_expr = saved_current_stats_time_bucket_seconds_expr;
    parser.current_stats_time_bucket_is_calendar = saved_current_stats_time_bucket_is_calendar;
}

LogsQLParser::LogsQLParser(const char * begin_, const char * end_, Context context_)
    : lex(begin_, end_, context_.truncated), context(std::move(context_))
{
}

void LogsQLParser::throwNotImplemented(const String & what) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not supported yet by the logsql dialect; context: [{}]", what, lex.context());
}

ASTPtr LogsQLParser::columnExpr(const String & field_name) const
{
    return make_intrusive<ASTIdentifier>(columnName(field_name));
}

String LogsQLParser::columnName(const String & field_name) const
{
    if (field_name.empty() || field_name == "_msg")
        return context.msg_column;
    if (field_name == "_time")
        return context.time_column;
    return field_name;
}

String LogsQLParser::parseFieldName()
{
    String name = lex.nextCompoundToken();
    if (name.empty())
        return "_msg";
    return name;
}

ASTPtr LogsQLParser::parse()
{
    Layer layer = parseQuery(/*is_subquery=*/ false);

    if (!lex.isEnd() && !lex.isKeyword(";"))
        throwSyntaxError(fmt::format("unexpected token {} after the query", lex.getToken()));

    parsed_end = lex.getTokenBegin();
    return buildSelectWithUnion(layer);
}

LogsQLParser::Layer LogsQLParser::parseQuery(bool is_subquery)
{
    IncreaseDepth depth_guard(*this);
    QueryScopeGuard scope_guard(*this);

    parseQueryOptions();

    if (lex.isQueryPartTrailer())
        throwSyntaxError("missing query");

    Layer layer;
    layer.where = parseFilterOr("");

    /// options(global_filter=(...)) is ANDed into the query and all its subqueries.
    if (options_global_filter && layer.where)
        layer.where = makeASTFunction("and", options_global_filter->clone(), layer.where);
    else if (options_global_filter)
        layer.where = options_global_filter->clone();

    if (lex.isKeyword("|"))
    {
        lex.nextToken();
        parsePipes(layer);
    }

    if (is_subquery)
    {
        /// An optional trailing semicolon is allowed: `in(q | fields x;)`.
        if (lex.isKeyword(";"))
            lex.nextToken();
        if (!lex.isKeyword(")"))
            throwSyntaxError(fmt::format("unexpected token {} in a subquery; expecting ')'", lex.getToken()));
    }

    return layer;
}

void LogsQLParser::parseQueryOptions()
{
    if (!lex.isKeyword("options"))
        return;

    lex.nextToken();
    if (!lex.isKeyword("("))
        throwSyntaxError("missing '(' after the 'options' keyword; wrap 'options' into quotes if you are searching for this word in the log message");
    lex.nextToken();

    while (true)
    {
        if (lex.isKeyword(")"))
        {
            lex.nextToken();
            return;
        }

        String option_name = lex.nextCompoundToken();
        if (!lex.isKeyword("="))
            throwSyntaxError(fmt::format("missing '=' after {} in options(...)", option_name));
        lex.nextToken();

        if (option_name == "concurrency" || option_name == "parallel_readers" || option_name == "allow_partial_response"
            || option_name == "ignore_global_time_filter")
        {
            /// Execution hints in VictoriaLogs, and `ignore_global_time_filter` refers to the global
            /// time filter of its HTTP API which does not exist here. Parse and ignore them.
            lex.nextCompoundToken();
        }
        else if (option_name == "time_offset")
        {
            String text = lex.nextCompoundToken();
            auto duration = tryParseDuration(text);
            if (!duration)
                throwSyntaxError(fmt::format("cannot parse the time_offset option {} as a duration", text));
            options_time_offset_ns = *duration;
        }
        else if (option_name == "global_filter")
        {
            if (!lex.isKeyword("("))
                throwSyntaxError("the global_filter option must have the following format: global_filter=(<filter>)");
            lex.nextToken();
            options_global_filter = parseFilterOr("");
            if (!lex.isKeyword(")"))
                throwSyntaxError(fmt::format("missing ')' after the global_filter option; got {}", lex.getToken()));
            lex.nextToken();
        }
        else
        {
            throwSyntaxError(fmt::format("unexpected option {} inside options(...)", option_name));
        }

        if (lex.isKeyword(","))
            lex.nextToken();
        else if (!lex.isKeyword(")"))
            throwSyntaxError(fmt::format("unexpected token {} inside options(...); expecting ',' or ')'", lex.getToken()));
    }
}

/// ---- Filters ----

ASTPtr LogsQLParser::parseFilterOr(const String & field_name)
{
    IncreaseDepth depth_guard(*this);

    ASTs conditions;
    while (true)
    {
        conditions.push_back(parseFilterAnd(field_name));
        if (lex.isKeyword("or"))
        {
            lex.nextToken();
            continue;
        }
        if (lex.isQueryPartTrailer())
            return makeOr(std::move(conditions));
    }
}

ASTPtr LogsQLParser::parseFilterAnd(const String & field_name)
{
    ASTs conditions;
    while (true)
    {
        conditions.push_back(parseFilterGeneric(field_name));
        if (lex.isKeyword("or") || lex.isQueryPartTrailer())
            return makeAnd(std::move(conditions));
        if (lex.isKeyword("and"))
            lex.nextToken();
    }
}

ASTPtr LogsQLParser::parseFilterGeneric(const String & field_name)
{
    IncreaseDepth depth_guard(*this);

    if (lex.isEnd())
        throwSyntaxError(fmt::format("unexpected end of query after {}; expecting a filter", lex.getPrevRawToken()));

    /// Filters must be separated from the previous token with whitespace or one of the explicit separators.
    if (lex.isKeyword("("))
        lex.checkPrevAdjacentToken({"|", ":", "(", "-", "not", "and", "or", "!"});
    else
        lex.checkPrevAdjacentToken({"|", ":", "(", "-", "!"});

    if (lex.isKeyword("{"))
    {
        if (!field_name.empty() && field_name != "_stream")
            throwSyntaxError(fmt::format("the stream filter {{...}} cannot be applied to the field {}", field_name));
        return parseFilterStream();
    }
    if (lex.isKeyword("*"))
        return parseFilterStar(field_name);
    if (lex.isKeyword("("))
        return parseFilterParens(field_name);
    if (lex.isKeyword(">"))
        return parseFilterGT(field_name);
    if (lex.isKeyword("<"))
        return parseFilterLT(field_name);
    if (lex.isKeyword("="))
        return parseFilterEQ(field_name, /*negative=*/ false);
    if (lex.isKeyword("!="))
        return parseFilterEQ(field_name, /*negative=*/ true);
    if (lex.isKeyword("~"))
        return parseFilterTilda(field_name, /*negative=*/ false);
    if (lex.isKeyword("!~"))
        return parseFilterTilda(field_name, /*negative=*/ true);
    if (lex.isKeyword("not") || lex.isKeyword("!") || lex.isKeyword("-"))
        return parseFilterNot(field_name);
    if (lex.isKeyword("contains_all"))
        return parseFilterContains(field_name, /*need_all=*/ true);
    if (lex.isKeyword("contains_any"))
        return parseFilterContains(field_name, /*need_all=*/ false);
    if (lex.isKeyword("exact"))
        return parseFilterExact(field_name);
    if (lex.isKeyword("i"))
        return parseFilterAnyCase(field_name);
    if (lex.isKeyword("in"))
        return parseFilterIn(field_name);
    if (lex.isKeyword("ipv4_range"))
        return parseFilterIPv4Range(field_name);
    if (lex.isKeyword("ipv6_range"))
        return parseFilterIPv6Range(field_name);
    if (lex.isKeyword("pattern_match") || lex.isKeyword("pattern_match_full")
        || lex.isKeyword("pattern_match_prefix") || lex.isKeyword("pattern_match_suffix"))
        return parseFilterPatternMatch(field_name, Poco::toLower(lex.getToken()));
    if (lex.isKeyword("contains_common_case"))
        return parseFilterCommonCase(field_name, /*equals=*/ false);
    if (lex.isKeyword("equals_common_case"))
        return parseFilterCommonCase(field_name, /*equals=*/ true);
    if (lex.isKeyword("json_array_contains_any"))
        return parseFilterJSONArrayContainsAny(field_name);
    if (lex.isKeyword("eq_field"))
        return parseFilterFieldComparison(field_name, "equals");
    if (lex.isKeyword("le_field"))
        return parseFilterFieldComparison(field_name, "lessOrEquals");
    if (lex.isKeyword("lt_field"))
        return parseFilterFieldComparison(field_name, "less");
    if (lex.isKeyword("len_range"))
        return parseFilterLenRange(field_name);
    if (lex.isKeyword("range"))
        return parseFilterRange(field_name);
    if (lex.isKeyword("re"))
        return parseFilterRegexpFunc(field_name);
    if (lex.isKeyword("seq"))
        return parseFilterSequence(field_name);
    if (lex.isKeyword("string_range"))
        return parseFilterStringRange(field_name);
    if (lex.isKeyword("_time") && field_name.empty())
    {
        auto state = lex.backupState();
        lex.nextToken();
        if (!lex.isKeyword(":"))
        {
            /// The word filter `_time`.
            lex.restoreState(state);
            return parseFilterPhrase(field_name);
        }
        lex.nextToken();
        return parseFilterTime();
    }
    if (lex.isKeyword("_stream") && field_name.empty())
    {
        auto state = lex.backupState();
        lex.nextToken();
        if (!lex.isKeyword(":"))
        {
            /// The word filter `_stream`.
            lex.restoreState(state);
            return parseFilterPhrase(field_name);
        }
        lex.nextToken();
        return parseFilterGeneric("_stream");
    }

    if (lex.isKeyword("_stream_id") && field_name.empty())
    {
        auto state = lex.backupState();
        lex.nextToken();
        if (!lex.isKeyword(":"))
        {
            /// The word filter `_stream_id`.
            lex.restoreState(state);
            return parseFilterPhrase(field_name);
        }
        lex.nextToken();
        return parseFilterStreamId();
    }

    if (!lex.isQuoted() && lex.isKeyword("value_type"))
    {
        /// This may still be a plain word filter when not followed by '('.
        auto state = lex.backupState();
        String name = lex.getToken();
        lex.nextToken();
        if (!lex.skippedSpace() && lex.isKeyword("("))
            throwNotImplemented(fmt::format("The filter '{}' (it inspects the internal storage format of VictoriaLogs)", name));
        lex.restoreState(state);
    }

    return parseFilterPhrase(field_name);
}

ASTPtr LogsQLParser::parseFilterPhrase(const String & field_name)
{
    bool was_quoted = lex.isQuoted();
    String phrase = lex.nextCompoundToken(field_name.empty() ? field_name_stop_tokens : std::vector<std::string_view>{});

    if (!lex.skippedSpace() && lex.isKeyword("*"))
    {
        lex.nextToken();
        if (field_name.empty() && lex.isKeyword(":"))
            throwNotImplemented("Filtering over multiple fields selected by a name prefix");
        return makePrefixFilter(field_name, phrase);
    }

    if (field_name.empty() && lex.isKeyword(":"))
    {
        /// The phrase is a field name.
        lex.nextToken();

        if (phrase == "_time" && !was_quoted)
            return parseFilterTime();
        if (phrase == "_stream" && !was_quoted)
            return parseFilterGeneric("_stream");
        if (phrase == "_stream_id" && !was_quoted)
            return parseFilterStreamId();

        return parseFilterGeneric(phrase);
    }

    if (was_quoted && phrase.empty())
    {
        /// The empty phrase filter: matches logs where the field is empty or missing.
        return makeASTFunction("equals", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(String())));
    }

    return makePhraseFilter(field_name, phrase);
}

ASTPtr LogsQLParser::parseFilterParens(const String & field_name)
{
    lex.nextToken();
    ASTPtr result = parseFilterOr(field_name);
    if (!lex.isKeyword(")"))
        throwSyntaxError(fmt::format("missing ')'; got {}", lex.getToken()));
    lex.nextToken();
    return result;
}

ASTPtr LogsQLParser::parseFilterNot(const String & field_name)
{
    lex.nextToken();
    ASTPtr condition = parseFilterGeneric(field_name);

    /// not(not(x)) -> x
    if (const auto * function = condition ? condition->as<ASTFunction>() : nullptr; function && function->name == "not")
        return function->arguments->children[0];

    return makeNot(condition);
}

ASTPtr LogsQLParser::parseFilterStar(const String & field_name)
{
    lex.nextToken();

    if (field_name.empty() && lex.isKeyword(":"))
        throwNotImplemented("Filtering over all fields with the '*:' prefix");

    if (lex.skippedSpace() || lex.isQueryPartTrailer())
    {
        if (field_name.empty())
            return nullptr;  /// `*` matches all logs.

        /// `field:*` matches logs with a non-empty field.
        return makeASTFunction("notEquals", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(String())));
    }

    /// The `*substr*` filter.
    String substring = lex.nextCompoundToken();
    if (lex.skippedSpace() || !lex.isKeyword("*"))
        throwSyntaxError(fmt::format("missing ending '*' in the *{}* filter", substring));
    lex.nextToken();
    if (!lex.skippedSpace() && !lex.isQueryPartTrailer())
        throwSyntaxError(fmt::format("missing whitespace after the *{}* filter", substring));

    return makeASTFunction("greater",
        makeASTFunction("position", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(substring))),
        make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(0))));
}

ASTPtr LogsQLParser::parseFilterTilda(const String & field_name, bool negative)
{
    lex.nextToken();

    if (lex.isKeyword("-"))
        throwSyntaxError("regexps starting with '-' must be put in quotes");

    if (lex.skippedSpace() && field_name.empty())
        throwSyntaxError("missing ':' in front of '~'");

    String regexp = lex.nextCompoundToken();
    ASTPtr condition = makeRegexpFilter(field_name, regexp);
    return negative ? makeNot(condition) : condition;
}

ASTPtr LogsQLParser::parseFilterEQ(const String & field_name, bool negative)
{
    lex.nextToken();
    if (lex.skippedSpace() && field_name.empty())
        throwSyntaxError("missing ':' in front of '='");

    bool quoted = lex.isQuoted();
    String value = lex.nextCompoundToken();

    ASTPtr condition;
    if (!lex.skippedSpace() && lex.isKeyword("*"))
    {
        lex.nextToken();
        condition = makeASTFunction("startsWith", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(value)));
    }
    else
    {
        condition = makeComparisonFilter(field_name, "equals", value, quoted);
    }
    return negative ? makeNot(condition) : condition;
}

ASTPtr LogsQLParser::parseFilterGT(const String & field_name)
{
    lex.nextToken();

    bool inclusive = false;
    if (!lex.skippedSpace() && lex.isKeyword("="))
    {
        lex.nextToken();
        inclusive = true;
    }

    if (lex.skippedSpace() && field_name.empty())
        throwSyntaxError("missing ':' in front of the comparison");

    bool quoted = lex.isQuoted();
    String value = lex.nextCompoundToken();
    return makeComparisonFilter(field_name, inclusive ? "greaterOrEquals" : "greater", value, quoted);
}

ASTPtr LogsQLParser::parseFilterLT(const String & field_name)
{
    lex.nextToken();

    bool inclusive = false;
    if (!lex.skippedSpace() && lex.isKeyword("="))
    {
        lex.nextToken();
        inclusive = true;
    }

    if (lex.skippedSpace() && field_name.empty())
        throwSyntaxError("missing ':' in front of the comparison");

    bool quoted = lex.isQuoted();
    String value = lex.nextCompoundToken();
    return makeComparisonFilter(field_name, inclusive ? "lessOrEquals" : "less", value, quoted);
}

ASTPtr LogsQLParser::parseFilterRange(const String & field_name)
{
    auto state = lex.backupState();
    lex.nextToken();

    bool include_min = false;
    if (lex.isKeyword("["))
        include_min = true;
    else if (!lex.isKeyword("("))
    {
        /// Not the range() filter - fall back to the phrase filter (`range` is a normal word).
        lex.restoreState(state);
        return parseFilterPhrase(field_name);
    }
    lex.nextToken();

    String min_text = lex.nextCompoundToken();
    auto min_value = tryParseNumberField(min_text);
    if (!min_value)
        throwSyntaxError(fmt::format("cannot parse {} as a number in range()", min_text));

    if (!lex.isKeyword(","))
        throwSyntaxError(fmt::format("unexpected token {} in range(); expecting ','", lex.getToken()));
    lex.nextToken();

    String max_text = lex.nextCompoundToken();
    auto max_value = tryParseNumberField(max_text);
    if (!max_value)
        throwSyntaxError(fmt::format("cannot parse {} as a number in range()", max_text));

    bool include_max = false;
    if (lex.isKeyword("]"))
        include_max = true;
    else if (!lex.isKeyword(")"))
        throwSyntaxError(fmt::format("unexpected closing token {} in range(); expecting ')' or ']'", lex.getToken()));
    lex.nextToken();

    auto is_infinite = [](const Field & field)
    {
        return field.getType() == Field::Types::Float64 && std::isinf(field.safeGet<Float64>());
    };

    ASTs conditions;
    if (!is_infinite(*min_value))
        conditions.push_back(makeNumericComparison(field_name, include_min ? "greaterOrEquals" : "greater",
            make_intrusive<ASTLiteral>(*min_value), min_text));
    if (!is_infinite(*max_value))
        conditions.push_back(makeNumericComparison(field_name, include_max ? "lessOrEquals" : "less",
            make_intrusive<ASTLiteral>(*max_value), max_text));

    return makeAnd(std::move(conditions));
}

std::vector<String> LogsQLParser::parseArgsInParens(bool * wildcard)
{
    if (!lex.isKeyword("("))
        throwSyntaxError(fmt::format("missing '('; got {}", lex.getToken()));
    lex.nextToken();

    std::vector<String> args;
    while (true)
    {
        if (lex.isKeyword(")"))
        {
            lex.nextToken();
            return args;
        }
        if (lex.isKeyword(","))
            throwSyntaxError("unexpected ','");

        if (wildcard != nullptr && lex.isKeyword("*"))
        {
            *wildcard = true;
            lex.nextToken();
        }
        else
        {
            args.push_back(lex.nextCompoundToken());
            if (wildcard != nullptr && !lex.skippedSpace() && lex.isKeyword("*"))
                throwNotImplemented("A prefix argument inside a filter function");
        }

        if (lex.isKeyword(","))
            lex.nextToken();
        else if (!lex.isKeyword(")"))
            throwSyntaxError(fmt::format("unexpected token {}; expecting ',' or ')'", lex.getToken()));
    }
}

ASTPtr LogsQLParser::parseFilterIn(const String & field_name)
{
    lex.nextToken();

    /// Unlike most filter functions, a bare `in` word cannot be used as a word filter - it must be quoted.
    if (!lex.isKeyword("("))
        throwSyntaxError("missing '(' after 'in'; put 'in' into quotes if you are searching for this word");

    /// Try in(value1, ..., valueN) first.
    bool wildcard = false;
    std::vector<std::pair<String, bool>> values;
    bool simple_values = true;
    {
        auto args_state = lex.backupState();
        lex.nextToken();
        while (true)
        {
            if (lex.isKeyword(")"))
            {
                lex.nextToken();
                break;
            }
            if (lex.isKeyword("*"))
            {
                wildcard = true;
                lex.nextToken();
            }
            else if (lex.isQuoted() || (!lex.getToken().empty() && !lex.isKeyword("|") && !lex.isKeyword("(")))
            {
                bool quoted = lex.isQuoted();
                auto token_state = lex.backupState();
                try
                {
                    values.emplace_back(lex.nextCompoundToken(), quoted);
                }
                catch (const Exception &)
                {
                    lex.restoreState(token_state);
                    simple_values = false;
                    break;
                }
            }
            else
            {
                simple_values = false;
                break;
            }

            if (lex.isKeyword(","))
                lex.nextToken();
            else if (!lex.isKeyword(")"))
            {
                simple_values = false;
                break;
            }
        }

        if (!simple_values)
            lex.restoreState(args_state);
    }

    if (simple_values)
    {
        if (wildcard)
            return nullptr;  /// in(*) matches all logs.
        if (values.empty())
            return make_intrusive<ASTLiteral>(Field(static_cast<UInt8>(0)));  /// in() matches nothing.

        ASTs elements;
        for (const auto & [value, quoted] : values)
            elements.push_back(makeValueLiteral(value, quoted));
        auto tuple = makeASTFunction("tuple");
        tuple->arguments->children = std::move(elements);
        return makeASTFunction("in", columnExpr(field_name), tuple);
    }

    /// in(<subquery>)
    lex.nextToken();
    Layer subquery_layer = parseQuery(/*is_subquery=*/ true);
    lex.nextToken();  /// Skip ')'.

    if (subquery_layer.select.size() != 1)
        throwSyntaxError("a subquery inside in() must return exactly one field: it must end with '| fields <field>' or '| uniq by (<field>)'");

    auto subquery = make_intrusive<ASTSubquery>(buildSelectWithUnion(subquery_layer));
    return makeASTFunction("in", columnExpr(field_name), subquery);
}

ASTPtr LogsQLParser::parseFilterContains(const String & field_name, bool need_all)
{
    auto state = lex.backupState();
    String func_name = lex.getToken();
    lex.nextToken();

    /// A bare `contains_any`/`contains_all` word cannot be used as a word filter - it must be quoted.
    if (!lex.isKeyword("("))
        throwSyntaxError(fmt::format("missing '(' after '{}'; put it into quotes if you are searching for this word", func_name));

    bool wildcard = false;
    std::vector<String> args;
    try
    {
        args = parseArgsInParens(&wildcard);
    }
    catch (const Exception &)
    {
        /// The argument may also be a subquery: `contains_any(<query> | fields <field>)`.
        /// Unlike in(<subquery>), it cannot be translated into an IN clause, because it means
        /// containment rather than equality: the returned values are matched as substrings
        /// (VictoriaLogs additionally checks word boundaries).
        auto failure_state = lex.backupState();
        lex.restoreState(state);
        lex.nextToken();
        lex.nextToken();
        Layer subquery_layer;
        try
        {
            subquery_layer = parseQuery(/*is_subquery=*/ true);
        }
        catch (const Exception &)
        {
            lex.restoreState(failure_state);
            throw;
        }
        lex.nextToken();  /// Skip ')'.

        if (subquery_layer.select.size() != 1)
            throwSyntaxError("a subquery inside contains_any() and contains_all() must return exactly one field: "
                "it must end with '| fields <field>' or '| uniq by (<field>)'");

        String output_name = subquery_layer.select[0]->tryGetAlias();
        if (output_name.empty())
        {
            const auto * identifier = subquery_layer.select[0]->as<ASTIdentifier>();
            if (identifier == nullptr)
                throwSyntaxError("cannot determine the output field of the subquery inside contains_any()/contains_all()");
            output_name = identifier->shortName();
        }

        /// (SELECT groupUniqArray(<field>) FROM (<subquery>))
        Layer values_layer;
        values_layer.source_subquery = buildSelectWithUnion(subquery_layer);
        auto values_aggregate = makeASTFunction("groupUniqArray", make_intrusive<ASTIdentifier>(output_name));
        values_layer.select = {values_aggregate};
        values_layer.has_aggregation = true;
        values_layer.has_projection = true;
        auto values_subquery = make_intrusive<ASTSubquery>(buildSelectWithUnion(values_layer));

        /// arrayExists(v -> position(col, v) > 0, <values>) / arrayAll(...) for contains_all.
        auto lambda_argument = make_intrusive<ASTIdentifier>("__logsql_value");
        auto lambda_body = makeASTFunction("greater",
            makeASTFunction("position", columnExpr(field_name), lambda_argument),
            make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(0))));
        auto lambda = makeASTFunction("lambda", makeASTFunction("tuple", lambda_argument->clone()), lambda_body);
        return makeASTFunction(need_all ? "arrayAll" : "arrayExists", lambda, values_subquery);
    }
    if (wildcard)
        return nullptr;  /// contains_any(*) and contains_all(*) match all logs.

    if (args.empty())
        return make_intrusive<ASTLiteral>(Field(static_cast<UInt8>(0)));

    ASTs conditions;
    for (const auto & arg : args)
        conditions.push_back(makePhraseFilter(field_name, arg));

    return need_all ? makeAnd(std::move(conditions)) : makeOr(std::move(conditions));
}

ASTPtr LogsQLParser::parseFilterSequence(const String & field_name)
{
    auto state = lex.backupState();
    lex.nextToken();

    if (!lex.isKeyword("(") || lex.skippedSpace())
    {
        lex.restoreState(state);
        return parseFilterPhrase(field_name);
    }

    std::vector<String> args = parseArgsInParens();

    /// seq(p1, ..., pN) requires the phrases to be present in the given order.
    /// This is translated into a chain of position() calls, each starting the search
    /// after the end of the previous phrase. Unlike VictoriaLogs, word boundaries
    /// around the phrases are not checked.
    ASTs conditions;
    ASTPtr search_start;
    for (const auto & arg : args)
    {
        if (arg.empty())
            continue;
        ASTPtr position;
        if (search_start)
            position = makeASTFunction("position", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(arg)), search_start);
        else
            position = makeASTFunction("position", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(arg)));
        conditions.push_back(makeASTFunction("greater", position, make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(0)))));
        search_start = makeASTFunction("plus", position->clone(), make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(arg.size()))));
    }

    return makeAnd(std::move(conditions));
}

ASTPtr LogsQLParser::parseFilterExact(const String & field_name)
{
    auto state = lex.backupState();
    lex.nextToken();

    if (!lex.isKeyword("(") || lex.skippedSpace())
    {
        lex.restoreState(state);
        return parseFilterPhrase(field_name);
    }
    lex.nextToken();

    if (lex.isKeyword("*"))
    {
        lex.nextToken();
        if (!lex.isKeyword(")"))
            throwSyntaxError(fmt::format("missing ')' after exact(*); got {}", lex.getToken()));
        lex.nextToken();
        return makeASTFunction("notEquals", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(String())));
    }

    bool quoted = lex.isQuoted();
    String value = lex.nextCompoundToken();

    bool is_prefix = false;
    if (!lex.skippedSpace() && lex.isKeyword("*"))
    {
        is_prefix = true;
        lex.nextToken();
    }

    if (!lex.isKeyword(")"))
        throwSyntaxError(fmt::format("missing ')' for exact(); got {}", lex.getToken()));
    lex.nextToken();

    if (is_prefix)
        return makeASTFunction("startsWith", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(value)));
    return makeComparisonFilter(field_name, "equals", value, quoted);
}

ASTPtr LogsQLParser::parseFilterRegexpFunc(const String & field_name)
{
    auto state = lex.backupState();
    lex.nextToken();

    if (!lex.isKeyword("(") || lex.skippedSpace())
    {
        lex.restoreState(state);
        return parseFilterPhrase(field_name);
    }

    std::vector<String> args = parseArgsInParens();
    if (args.size() != 1)
        throwSyntaxError(fmt::format("unexpected number of args for re(); got {}; want 1", args.size()));

    return makeRegexpFilter(field_name, args[0]);
}

ASTPtr LogsQLParser::parseFilterAnyCase(const String & field_name)
{
    auto state = lex.backupState();
    lex.nextToken();

    if (!lex.isKeyword("(") || lex.skippedSpace())
    {
        lex.restoreState(state);
        return parseFilterPhrase(field_name);
    }
    lex.nextToken();

    if (lex.isKeyword("*"))
    {
        lex.nextToken();
        if (!lex.isKeyword(")"))
            throwSyntaxError(fmt::format("missing ')' after i(*); got {}", lex.getToken()));
        lex.nextToken();
        return makeASTFunction("notEquals", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(String())));
    }

    String phrase = lex.nextCompoundToken();
    bool is_prefix = false;
    if (!lex.skippedSpace() && lex.isKeyword("*"))
    {
        is_prefix = true;
        lex.nextToken();
    }

    if (!lex.isKeyword(")"))
        throwSyntaxError(fmt::format("missing ')' for i(); got {}", lex.getToken()));
    lex.nextToken();

    if (phrase.empty())
        return makeASTFunction("equals", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(String())));

    if (is_prefix)
        return makePrefixFilter(field_name, phrase, /*case_insensitive=*/ true);
    return makePhraseFilter(field_name, phrase, /*case_insensitive=*/ true);
}

ASTPtr LogsQLParser::parseFilterStringRange(const String & field_name)
{
    auto state = lex.backupState();
    lex.nextToken();

    if (!lex.isKeyword("(") || lex.skippedSpace())
    {
        lex.restoreState(state);
        return parseFilterPhrase(field_name);
    }

    std::vector<String> args = parseArgsInParens();
    if (args.size() != 2)
        throwSyntaxError(fmt::format("unexpected number of args for string_range(); got {}; want 2", args.size()));

    /// string_range(min, max) includes the lower bound and excludes the upper bound.
    return makeAnd({
        makeASTFunction("greaterOrEquals", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(args[0]))),
        makeASTFunction("less", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(args[1])))});
}

ASTPtr LogsQLParser::parseFilterLenRange(const String & field_name)
{
    auto state = lex.backupState();
    lex.nextToken();

    if (!lex.isKeyword("(") || lex.skippedSpace())
    {
        lex.restoreState(state);
        return parseFilterPhrase(field_name);
    }

    std::vector<String> args = parseArgsInParens();
    if (args.size() != 2)
        throwSyntaxError(fmt::format("unexpected number of args for len_range(); got {}; want 2", args.size()));

    auto min_value = tryParseNumber(args[0]);
    auto max_value = tryParseNumber(args[1]);
    if (!min_value || !max_value)
        throwSyntaxError("cannot parse len_range() bounds as numbers");
    if ((!std::isinf(*min_value) && *min_value != std::floor(*min_value)) || (!std::isinf(*max_value) && *max_value != std::floor(*max_value)))
        throwSyntaxError("len_range() bounds must be integers");

    /// Both bounds are inclusive.
    ASTs conditions;
    if (!std::isinf(*min_value))
        conditions.push_back(makeASTFunction("greaterOrEquals",
            makeASTFunction("length", columnExpr(field_name)),
            make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(*min_value)))));
    if (!std::isinf(*max_value))
        conditions.push_back(makeASTFunction("lessOrEquals",
            makeASTFunction("length", columnExpr(field_name)),
            make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(*max_value)))));
    return makeAnd(std::move(conditions));
}

ASTPtr LogsQLParser::parseFilterIPv4Range(const String & field_name)
{
    auto state = lex.backupState();
    lex.nextToken();

    if (!lex.isKeyword("(") || lex.skippedSpace())
    {
        lex.restoreState(state);
        return parseFilterPhrase(field_name);
    }

    std::vector<String> args = parseArgsInParens();

    UInt32 range_start = 0;
    UInt32 range_end = 0;
    if (args.size() == 1)
    {
        /// A single address or a CIDR subnet.
        if (auto slash = args[0].find('/'); slash != String::npos)
        {
            auto address = tryParseIPv4(args[0].substr(0, slash));
            UInt32 bits = 0;
            auto [end, ec] = std::from_chars(args[0].data() + slash + 1, args[0].data() + args[0].size(), bits);
            if (!address || ec != std::errc() || end != args[0].data() + args[0].size() || bits > 32)
                throwSyntaxError(fmt::format("cannot parse {} as an IPv4 subnet in ipv4_range()", args[0]));
            UInt32 mask = bits == 0 ? 0 : (~UInt32(0) << (32 - bits));
            range_start = *address & mask;
            range_end = range_start | ~mask;
        }
        else
        {
            auto address = tryParseIPv4(args[0]);
            if (!address)
                throwSyntaxError(fmt::format("cannot parse {} as an IPv4 address in ipv4_range()", args[0]));
            range_start = *address;
            range_end = *address;
        }
    }
    else if (args.size() == 2)
    {
        auto start_address = tryParseIPv4(args[0]);
        auto end_address = tryParseIPv4(args[1]);
        if (!start_address || !end_address)
            throwSyntaxError("cannot parse ipv4_range() bounds as IPv4 addresses");
        range_start = *start_address;
        range_end = *end_address;
    }
    else
    {
        throwSyntaxError(fmt::format("unexpected number of args for ipv4_range(); got {}; want 1 or 2", args.size()));
    }

    auto format_address = [](UInt32 address)
    {
        return fmt::format("{}.{}.{}.{}", address >> 24, (address >> 16) & 0xFF, (address >> 8) & 0xFF, address & 0xFF);
    };

    ASTPtr parsed = makeASTFunction("toIPv4OrNull", columnExpr(field_name));
    return makeAnd({
        makeASTFunction("greaterOrEquals", parsed,
            makeASTFunction("toIPv4", make_intrusive<ASTLiteral>(Field(format_address(range_start))))),
        makeASTFunction("lessOrEquals", parsed->clone(),
            makeASTFunction("toIPv4", make_intrusive<ASTLiteral>(Field(format_address(range_end)))))});
}

ASTPtr LogsQLParser::parseFilterIPv6Range(const String & field_name)
{
    auto state = lex.backupState();
    lex.nextToken();

    if (!lex.isKeyword("(") || lex.skippedSpace())
    {
        lex.restoreState(state);
        return parseFilterPhrase(field_name);
    }

    std::vector<String> args = parseArgsInParens();

    ASTPtr parsed = makeASTFunction("toIPv6OrNull", columnExpr(field_name));
    if (args.size() == 1)
    {
        if (auto slash = args[0].find('/'); slash != String::npos)
        {
            UInt32 bits = 0;
            auto [end, ec] = std::from_chars(args[0].data() + slash + 1, args[0].data() + args[0].size(), bits);
            if (ec != std::errc() || end != args[0].data() + args[0].size() || bits > 128)
                throwSyntaxError(fmt::format("cannot parse {} as an IPv6 subnet in ipv6_range()", args[0]));

            /// IPv6CIDRToRange returns a (min, max) tuple of the subnet.
            ASTPtr range = makeASTFunction("IPv6CIDRToRange",
                makeASTFunction("toIPv6", make_intrusive<ASTLiteral>(Field(args[0].substr(0, slash)))),
                make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(bits))));
            return makeAnd({
                makeASTFunction("greaterOrEquals", parsed, makeASTFunction("tupleElement", range, make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(1))))),
                makeASTFunction("lessOrEquals", parsed->clone(), makeASTFunction("tupleElement", range->clone(), make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(2)))))});
        }
        return makeASTFunction("equals", parsed, makeASTFunction("toIPv6", make_intrusive<ASTLiteral>(Field(args[0]))));
    }
    if (args.size() == 2)
    {
        return makeAnd({
            makeASTFunction("greaterOrEquals", parsed, makeASTFunction("toIPv6", make_intrusive<ASTLiteral>(Field(args[0])))),
            makeASTFunction("lessOrEquals", parsed->clone(), makeASTFunction("toIPv6", make_intrusive<ASTLiteral>(Field(args[1]))))});
    }
    throwSyntaxError(fmt::format("unexpected number of args for ipv6_range(); got {}; want 1 or 2", args.size()));
}

ASTPtr LogsQLParser::parseFilterStreamId()
{
    /// `_stream_id` is treated as an ordinary column with exact-match semantics:
    /// `_stream_id:<id>`, `_stream_id:in(...)` and other filters work on it as on any other field.
    if (lex.isKeyword("in"))
        return parseFilterIn("_stream_id");

    if (lex.isQueryPartTrailer())
        throwSyntaxError("missing the value of the _stream_id filter");

    bool quoted = lex.isQuoted();
    String value = lex.nextCompoundToken();
    return makeASTFunction("equals", columnExpr("_stream_id"), makeValueLiteral(value, quoted));
}

ASTPtr LogsQLParser::parseFilterCommonCase(const String & field_name, bool equals)
{
    auto state = lex.backupState();
    lex.nextToken();

    if (!lex.isKeyword("(") || lex.skippedSpace())
    {
        lex.restoreState(state);
        return parseFilterPhrase(field_name);
    }

    std::vector<String> args = parseArgsInParens();

    /// The "common case" variants of a phrase: the phrase itself, its all-uppercase form,
    /// and every variant where each originally-uppercase ASCII letter is independently lowercased.
    std::set<String> variants;
    for (const auto & phrase : args)
    {
        std::vector<size_t> upper_positions;
        for (size_t i = 0; i < phrase.size(); ++i)
            if (phrase[i] >= 'A' && phrase[i] <= 'Z')
                upper_positions.push_back(i);
        if (upper_positions.size() > 10)
            throwSyntaxError(fmt::format("too many common_case combinations for {}; reduce the number of uppercase letters", phrase));

        String upper = phrase;
        for (auto & c : upper)
            if (c >= 'a' && c <= 'z')
                c = static_cast<char>(c - 'a' + 'A');
        variants.insert(upper);

        for (size_t mask = 0; mask < (1ULL << upper_positions.size()); ++mask)
        {
            String variant = phrase;
            for (size_t bit = 0; bit < upper_positions.size(); ++bit)
                if (mask & (1ULL << bit))
                    variant[upper_positions[bit]] = static_cast<char>(variant[upper_positions[bit]] - 'A' + 'a');
            variants.insert(variant);
        }
    }

    if (equals)
    {
        ASTs elements;
        for (const auto & variant : variants)
            elements.push_back(make_intrusive<ASTLiteral>(Field(variant)));
        auto tuple = makeASTFunction("tuple");
        tuple->arguments->children = std::move(elements);
        return makeASTFunction("in", columnExpr(field_name), tuple);
    }

    ASTs conditions;
    for (const auto & variant : variants)
        conditions.push_back(makePhraseFilter(field_name, variant));
    return makeOr(std::move(conditions));
}

ASTPtr LogsQLParser::parseFilterJSONArrayContainsAny(const String & field_name)
{
    auto state = lex.backupState();
    lex.nextToken();

    if (!lex.isKeyword("(") || lex.skippedSpace())
    {
        lex.restoreState(state);
        return parseFilterPhrase(field_name);
    }

    std::vector<String> args = parseArgsInParens();

    /// The field holds a JSON array; the filter matches if any of the listed values is its element.
    /// String elements are compared decoded, other scalar elements by their JSON text.
    ASTs elements;
    for (const auto & arg : args)
        elements.push_back(make_intrusive<ASTLiteral>(Field(arg)));
    auto values = makeASTFunction("tuple");
    values->arguments->children = std::move(elements);

    auto element_argument = make_intrusive<ASTIdentifier>("__logsql_element");
    ASTPtr decoded = makeASTFunction("if",
        makeASTFunction("startsWith", element_argument, make_intrusive<ASTLiteral>(Field(String("\"")))),
        makeASTFunction("JSONExtractString", element_argument->clone()),
        element_argument->clone());
    auto lambda = makeASTFunction("lambda",
        makeASTFunction("tuple", element_argument->clone()),
        makeASTFunction("in", decoded, values));

    return makeASTFunction("arrayExists", lambda, makeASTFunction("JSONExtractArrayRaw", columnExpr(field_name)));
}

ASTPtr LogsQLParser::parseFilterFieldComparison(const String & field_name, const String & func)
{
    auto state = lex.backupState();
    lex.nextToken();

    if (!lex.isKeyword("(") || lex.skippedSpace())
    {
        lex.restoreState(state);
        return parseFilterPhrase(field_name);
    }

    std::vector<String> args = parseArgsInParens();
    if (args.size() != 1)
        throwSyntaxError(fmt::format("unexpected number of args; got {}; want 1", args.size()));

    return makeASTFunction(func, columnExpr(field_name), columnExpr(args[0]));
}

ASTPtr LogsQLParser::parseFilterStream()
{
    /// {label1="value1", label2=~"regexp" or label3!="value3", ...}
    lex.nextToken();

    ASTs or_groups;
    ASTs current_group;

    while (true)
    {
        if (lex.isKeyword("}"))
        {
            lex.nextToken();
            break;
        }

        String label = lex.nextCompoundToken();

        ASTPtr condition;
        if (lex.isKeyword("=") || lex.isKeyword("!="))
        {
            bool negative = lex.isKeyword("!=");
            lex.nextToken();
            String value;
            if (lex.isQuoted())
            {
                value = lex.getToken();
                lex.nextToken();
            }
            else
            {
                value = lex.nextCompoundToken();
            }
            condition = makeASTFunction(negative ? "notEquals" : "equals", columnExpr(label), make_intrusive<ASTLiteral>(Field(value)));
        }
        else if (lex.isKeyword("=~") || lex.isKeyword("!~"))
        {
            bool negative = lex.isKeyword("!~");
            lex.nextToken();
            String value;
            if (lex.isQuoted())
            {
                value = lex.getToken();
                lex.nextToken();
            }
            else
            {
                value = lex.nextCompoundToken();
            }

            re2::RE2 checked_regexp(value, re2::RE2::Quiet);
            if (!checked_regexp.ok())
                throwSyntaxError(fmt::format("invalid regexp {} for the stream label {}: {}", value, label, checked_regexp.error()));

            /// Stream label regexps are anchored, like in PromQL.
            condition = makeASTFunction("match", columnExpr(label), make_intrusive<ASTLiteral>(Field("^(?:" + value + ")$")));
            if (negative)
                condition = makeNot(condition);
        }
        else if (lex.isKeyword("in") || lex.isKeyword("not_in"))
        {
            bool negative = lex.isKeyword("not_in");
            lex.nextToken();
            bool wildcard = false;
            std::vector<String> values = parseArgsInParens(&wildcard);
            if (wildcard)
            {
                condition = negative ? make_intrusive<ASTLiteral>(Field(static_cast<UInt8>(0))) : nullptr;
            }
            else
            {
                ASTs elements;
                for (const auto & value : values)
                    elements.push_back(make_intrusive<ASTLiteral>(Field(value)));
                auto tuple = makeASTFunction("tuple");
                tuple->arguments->children = std::move(elements);
                condition = makeASTFunction(negative ? "notIn" : "in", columnExpr(label), tuple);
            }
        }
        else
        {
            throwSyntaxError(fmt::format("unexpected token {} after the stream label {}; expecting '=', '!=', '=~', '!~', 'in' or 'not_in'",
                lex.getToken(), label));
        }

        current_group.push_back(std::move(condition));

        if (lex.isKeyword(","))
        {
            lex.nextToken();
        }
        else if (lex.isKeyword("or"))
        {
            lex.nextToken();
            if (lex.isKeyword("}"))
                throwSyntaxError("missing a label filter after 'or' in the stream filter");
            or_groups.push_back(makeAnd(std::move(current_group)));
            current_group = {};
        }
        else if (!lex.isKeyword("}"))
        {
            throwSyntaxError(fmt::format("unexpected token {} inside the stream filter; expecting ',', 'or' or '}}'", lex.getToken()));
        }
    }

    or_groups.push_back(makeAnd(std::move(current_group)));
    return makeOr(std::move(or_groups));
}

/// ---- Primitive filter builders ----

ASTPtr LogsQLParser::makePhraseFilter(const String & field_name, const String & phrase, bool case_insensitive)
{
    if (phrase.empty())
        return nullptr;

    if (isPlainASCIIToken(phrase))
    {
        /// A single word: match it with token boundaries. hasToken can use tokenbf skip indexes.
        return makeASTFunction(case_insensitive ? "hasTokenCaseInsensitive" : "hasToken",
            columnExpr(field_name), make_intrusive<ASTLiteral>(Field(phrase)));
    }

    /// A phrase: match it with word boundaries on both sides.
    String pattern = fmt::format("{}{}{}{}", case_insensitive ? "(?i)" : "", boundary_before, escapeRegexp(phrase), boundary_after);
    return makeASTFunction("match", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(pattern)));
}

ASTPtr LogsQLParser::makePrefixFilter(const String & field_name, const String & prefix, bool case_insensitive)
{
    if (prefix.empty())
    {
        /// `field:*` matches logs with a non-empty field.
        if (field_name.empty())
            return nullptr;
        return makeASTFunction("notEquals", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(String())));
    }

    /// A word prefix: a word boundary before the prefix, anything after it.
    String pattern = fmt::format("{}{}{}", case_insensitive ? "(?i)" : "", boundary_before, escapeRegexp(prefix));
    return makeASTFunction("match", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(pattern)));
}

ASTPtr LogsQLParser::makeRegexpFilter(const String & field_name, const String & regexp)
{
    /// Optimizations for typical regexps, following VictoriaLogs.
    if (regexp.empty() || regexp == ".*")
        return nullptr;
    if (regexp == ".+")
        return makeASTFunction("notEquals", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(String())));

    /// Validate the regexp here, so that invalid regexps are reported as parse errors.
    re2::RE2 checked_regexp(regexp, re2::RE2::Quiet);
    if (!checked_regexp.ok())
        throwSyntaxError(fmt::format("invalid regexp {}: {}", regexp, checked_regexp.error()));

    return makeASTFunction("match", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(regexp)));
}

ASTPtr LogsQLParser::makeNumericComparison(const String & field_name, const String & function_name, ASTPtr literal, const String & original_text) const
{
    /// In VictoriaLogs every field is a string, and a numeric comparison filter compares
    /// the numeric value of the field, skipping rows where the field is not a number.
    /// The casts make such comparisons work for string columns as well as for numeric ones:
    /// a non-numeric value becomes NULL, and the comparison filters it out.
    ///
    /// The value is compared exactly through `Decimal256(38)` first: it is rendered to its
    /// exact decimal text, because a direct cast to `Decimal` is lossy for `Float64`, while
    /// `toString` of a numeric column is exact. This branch covers integers up to the full
    /// 64-bit range, decimals, floats, and numeric text in `String` columns. Values whose
    /// text does not parse as a decimal (dates, enum names, huge `Int128` values) are
    /// compared exactly through `Int128` when they are integral; this branch must come
    /// after the decimal one, because `accurateCastOrNull(x, 'Int128')` silently truncates
    /// fractional `Decimal` values instead of returning NULL. Everything else falls back
    /// to the lossy `Float64` comparison, which rounds values above 2^53.
    const Field & literal_value = literal->as<ASTLiteral>()->value;
    /// `inf` and `nan` have no decimal representation, and values with more than 38 integer
    /// digits do not fit `Decimal256(38)`; such literals keep the two-branch form.
    /// (Integral literals always fit: they are bounded by the 64-bit range.)
    bool literal_fits_decimal = literal_value.getType() != Field::Types::Float64
        || (std::isfinite(literal_value.safeGet<Float64>()) && std::abs(literal_value.safeGet<Float64>()) < 1e38);

    ASTPtr exact = makeASTFunction("accurateCastOrNull", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(String("Int128"))));
    ASTPtr lossy = makeASTFunction("accurateCastOrNull", columnExpr(field_name), make_intrusive<ASTLiteral>(Field(String("Float64"))));
    ASTPtr exact_comparison = makeASTFunction(function_name, exact, literal->clone());
    ASTPtr result = makeASTFunction("if",
        makeASTFunction("isNotNull", exact->clone()),
        std::move(exact_comparison),
        makeASTFunction(function_name, lossy, literal->clone()));

    if (literal_fits_decimal)
    {
        ASTPtr decimal = makeASTFunction("toDecimal256OrNull",
            makeASTFunction("toString", columnExpr(field_name)),
            make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(38))));
        /// A plain decimal literal is compared with its exact original text: high-precision
        /// values like `10.50000000000000000002` would otherwise be rounded through the
        /// `Float64` literal field. For rich forms (`10.5KiB`) the shortest round-trip
        /// formatting of the `Float64` value restores its exact decimal text.
        String literal_text;
        if (!original_text.empty() && isPlainNumber(original_text))
            literal_text = original_text[0] == '+' ? original_text.substr(1) : original_text;
        else if (literal_value.getType() == Field::Types::Float64)
            literal_text = fmt::format("{}", literal_value.safeGet<Float64>());
        else if (literal_value.getType() == Field::Types::Int64)
            literal_text = fmt::format("{}", literal_value.safeGet<Int64>());
        else
            literal_text = fmt::format("{}", literal_value.safeGet<UInt64>());
        ASTPtr decimal_literal = makeASTFunction("toDecimal256",
            make_intrusive<ASTLiteral>(Field(literal_text)),
            make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(38))));
        ASTPtr decimal_comparison = makeASTFunction(function_name, decimal, std::move(decimal_literal));
        result = makeASTFunction("if",
            makeASTFunction("isNotNull", decimal->clone()),
            std::move(decimal_comparison),
            std::move(result));
    }

    return result;
}

ASTPtr LogsQLParser::makeComparisonFilter(const String & field_name, const String & function_name, const String & value, bool quoted)
{
    ASTPtr literal = makeValueLiteral(value, quoted);

    /// Comparison and equality filters accept the same numeric grammar as range()
    /// and len_range(): besides plain numbers, also underscores ("1_000"), base
    /// prefixes ("0x10"), durations, byte sizes ("10.5M"), and "inf". The prefix
    /// check keeps words that merely start like a number ("inflight") textual.
    bool is_numeric = literal->as<ASTLiteral>()->value.getType() != Field::Types::String;
    if (!is_numeric && !quoted && isNumberPrefix(value))
    {
        if (auto number = tryParseNumberField(value))
        {
            literal = make_intrusive<ASTLiteral>(*number);
            is_numeric = true;
        }
    }

    if (is_numeric)
        return makeNumericComparison(field_name, function_name, std::move(literal), value);
    return makeASTFunction(function_name, columnExpr(field_name), std::move(literal));
}

ASTPtr LogsQLParser::makeValueLiteral(const String & text, bool quoted)
{
    /// Unquoted numbers are emitted as numeric literals, so that comparisons
    /// with numeric columns work naturally. Everything else is a string literal -
    /// ClickHouse converts string literals to the column type in comparisons.
    if (!quoted && isPlainNumber(text))
    {
        if (text.find('.') == String::npos)
        {
            std::string_view digits = text;
            if (digits.starts_with('+'))
                digits.remove_prefix(1);
            if (digits.starts_with('-'))
            {
                Int64 int_value = 0;
                auto [end, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), int_value);
                if (ec == std::errc() && end == digits.data() + digits.size())
                    return make_intrusive<ASTLiteral>(Field(int_value));
            }
            else
            {
                /// Parsed as unsigned to keep integers above Int64::max exact.
                UInt64 uint_value = 0;
                auto [end, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), uint_value);
                if (ec == std::errc() && end == digits.data() + digits.size())
                    return make_intrusive<ASTLiteral>(Field(uint_value));
            }
        }
        if (auto value = tryParseNumber(text))
            return make_intrusive<ASTLiteral>(Field(*value));
    }
    return make_intrusive<ASTLiteral>(Field(text));
}

/// ---- Time filters ----

ASTPtr LogsQLParser::makeIntervalAST(Int64 ns)
{
    if (ns % 1'000'000'000 == 0)
        return makeASTFunction("toIntervalSecond", make_intrusive<ASTLiteral>(Field(static_cast<Int64>(ns / 1'000'000'000))));
    if (ns % 1'000'000 == 0)
        return makeASTFunction("toIntervalMillisecond", make_intrusive<ASTLiteral>(Field(static_cast<Int64>(ns / 1'000'000))));
    if (ns % 1'000 == 0)
        return makeASTFunction("toIntervalMicrosecond", make_intrusive<ASTLiteral>(Field(static_cast<Int64>(ns / 1'000))));
    return makeASTFunction("toIntervalNanosecond", make_intrusive<ASTLiteral>(Field(ns)));
}

ASTPtr LogsQLParser::shiftTime(ASTPtr expr, Int64 offset_ns)
{
    if (offset_ns == 0)
        return expr;
    if (offset_ns > 0)
        return makeASTFunction("minus", expr, makeIntervalAST(offset_ns));
    return makeASTFunction("plus", expr, makeIntervalAST(-offset_ns));
}

std::optional<Int64> LogsQLParser::parseOptionalTimeOffset()
{
    if (!lex.isKeyword("offset"))
        return {};
    lex.nextToken();
    String text = lex.nextCompoundToken();
    auto duration = tryParseDuration(text);
    if (!duration)
        throwSyntaxError(fmt::format("cannot parse offset {} as a duration", text));
    return duration;
}

LogsQLParser::TimeBound LogsQLParser::parseTimeBound()
{
    String text = lex.nextCompoundToken();
    String lower_text = Poco::toLower(text);

    /// now, now-5m, now+1h
    if (lower_text == "now" || lower_text.starts_with("now-") || lower_text.starts_with("now+"))
    {
        ASTPtr instant = makeASTFunction("now");
        if (lower_text.size() > 3)
        {
            auto duration = tryParseDuration(lower_text.substr(4));
            if (!duration)
                throwSyntaxError(fmt::format("cannot parse {} as now() with a duration offset", text));
            if (lower_text[3] == '-')
                instant = makeASTFunction("minus", instant, makeIntervalAST(*duration));
            else
                instant = makeASTFunction("plus", instant, makeIntervalAST(*duration));
        }
        TimeBound bound;
        bound.start = instant;
        bound.end = instant;
        return bound;
    }

    /// A bare duration means an offset from the current time: `_time:[-5m, now)`.
    if (auto duration = tryParseDuration(text))
    {
        ASTPtr instant;
        if (*duration <= 0)
            instant = makeASTFunction("minus", makeASTFunction("now"), makeIntervalAST(-*duration));
        else
            instant = makeASTFunction("plus", makeASTFunction("now"), makeIntervalAST(*duration));
        TimeBound bound;
        bound.start = instant;
        bound.end = instant;
        return bound;
    }

    auto timestamp = tryParseTimestamp(text);
    if (!timestamp)
        throwSyntaxError(fmt::format("cannot parse {} as a timestamp", text));

    auto make_instant = [&](Int64 ns, const String & civil)
    {
        if (timestamp->has_timezone)
            return makeASTFunction("toDateTime64",
                make_intrusive<ASTLiteral>(Field(formatTimestampUTC(ns))),
                make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(9))),
                make_intrusive<ASTLiteral>(Field(String("UTC"))));
        return makeASTFunction("toDateTime64",
            make_intrusive<ASTLiteral>(Field(civil)),
            make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(9))));
    };

    TimeBound result;
    result.start = make_instant(timestamp->start_ns, timestamp->start_civil);
    result.end = make_instant(timestamp->end_ns, timestamp->end_civil);
    if (timestamp->has_timezone)
    {
        result.start_ns = timestamp->start_ns;
        result.end_ns = timestamp->end_ns;
    }
    return result;
}

ASTPtr LogsQLParser::makeTimeRangeSecondsExpr(ASTPtr lower, ASTPtr upper)
{
    /// The bounds may be DateTime (`now()` arithmetic) or DateTime64 (absolute timestamps);
    /// normalize to nanosecond precision before subtracting.
    auto to_ns = [](const ASTPtr & instant)
    {
        return makeASTFunction("toUnixTimestamp64Nano",
            makeASTFunction("toDateTime64", instant->clone(), make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(9)))));
    };
    return makeASTFunction("divide",
        makeASTFunction("minus", to_ns(upper), to_ns(lower)),
        make_intrusive<ASTLiteral>(Field(1e9)));
}

void LogsQLParser::recordTimeLowerBound(ASTPtr expr, std::optional<Int64> ns, Int64 offset_ns)
{
    offset_ns += options_time_offset_ns;
    ASTPtr shifted = shiftTime(expr->clone(), offset_ns);
    std::optional<Int64> shifted_ns;
    if (ns)
        shifted_ns = *ns - offset_ns;

    if (!query_time_lower_bound_expr)
    {
        query_time_lower_bound_expr = shifted;
        query_time_lower_bound_ns = shifted_ns;
        return;
    }
    /// Several lower bounds intersect into the largest one.
    if (query_time_lower_bound_ns && shifted_ns)
    {
        if (*shifted_ns > *query_time_lower_bound_ns)
        {
            query_time_lower_bound_expr = shifted;
            query_time_lower_bound_ns = shifted_ns;
        }
        return;
    }
    /// The bounds are not comparable at parse time (e.g. `now()` arithmetic against
    /// an absolute timestamp): intersect them at runtime.
    query_time_lower_bound_expr = makeASTFunction("greatest", query_time_lower_bound_expr, shifted);
    query_time_lower_bound_ns.reset();
}

void LogsQLParser::recordTimeUpperBound(ASTPtr expr, std::optional<Int64> ns, Int64 offset_ns)
{
    offset_ns += options_time_offset_ns;
    ASTPtr shifted = shiftTime(expr->clone(), offset_ns);
    std::optional<Int64> shifted_ns;
    if (ns)
        shifted_ns = *ns - offset_ns;

    if (!query_time_upper_bound_expr)
    {
        query_time_upper_bound_expr = shifted;
        query_time_upper_bound_ns = shifted_ns;
        return;
    }
    /// Several upper bounds intersect into the smallest one.
    if (query_time_upper_bound_ns && shifted_ns)
    {
        if (*shifted_ns < *query_time_upper_bound_ns)
        {
            query_time_upper_bound_expr = shifted;
            query_time_upper_bound_ns = shifted_ns;
        }
        return;
    }
    query_time_upper_bound_expr = makeASTFunction("least", query_time_upper_bound_expr, shifted);
    query_time_upper_bound_ns.reset();
}

ASTPtr LogsQLParser::makeTimeCondition(ASTPtr lower, bool lower_inclusive, ASTPtr upper, bool upper_inclusive, Int64 offset_ns)
{
    offset_ns += options_time_offset_ns;
    ASTs conditions;
    if (lower)
        conditions.push_back(makeASTFunction(lower_inclusive ? "greaterOrEquals" : "greater",
            columnExpr("_time"), shiftTime(lower, offset_ns)));
    if (upper)
        conditions.push_back(makeASTFunction(upper_inclusive ? "lessOrEquals" : "less",
            columnExpr("_time"), shiftTime(upper, offset_ns)));
    return makeAnd(std::move(conditions));
}

ASTPtr LogsQLParser::parseFilterTime()
{
    if (lex.isKeyword("day_range"))
        return parseFilterDayRange();
    if (lex.isKeyword("week_range"))
        return parseFilterWeekRange();

    /// _time:offset 1h - all logs up to now-1h.
    if (lex.isKeyword("offset"))
    {
        auto offset = parseOptionalTimeOffset();
        recordTimeUpperBound(makeASTFunction("now"), {}, *offset);
        return makeTimeCondition(nullptr, false, makeASTFunction("now"), true, *offset);
    }

    if (lex.isKeyword(">") || lex.isKeyword("<"))
    {
        bool is_greater = lex.isKeyword(">");
        lex.nextToken();
        bool inclusive = false;
        if (!lex.skippedSpace() && lex.isKeyword("="))
        {
            inclusive = true;
            lex.nextToken();
        }

        /// A duration operand means "older/newer than the given age".
        auto state = lex.backupState();
        String text = lex.nextCompoundToken();
        if (auto duration = tryParseDuration(text))
        {
            auto offset = parseOptionalTimeOffset();
            ASTPtr instant = makeASTFunction("minus", makeASTFunction("now"), makeIntervalAST(*duration));
            if (is_greater)
            {
                recordTimeUpperBound(instant, {}, offset.value_or(0));
                return makeTimeCondition(nullptr, false, instant, inclusive, offset.value_or(0));
            }
            recordTimeLowerBound(instant, {}, offset.value_or(0));
            return makeTimeCondition(instant, inclusive, nullptr, false, offset.value_or(0));
        }
        lex.restoreState(state);

        TimeBound bound = parseTimeBound();
        auto offset = parseOptionalTimeOffset();
        if (is_greater)
        {
            /// _time:>2023-04-25Z means "after the whole day", _time:>=2023-04-25Z means "from the start of the day".
            ASTPtr lower = inclusive ? bound.start : bound.end;
            recordTimeLowerBound(lower, inclusive ? bound.start_ns : bound.end_ns, offset.value_or(0));
            return makeTimeCondition(lower, true, nullptr, false, offset.value_or(0));
        }
        ASTPtr upper = inclusive ? bound.end : bound.start;
        recordTimeUpperBound(upper, inclusive ? bound.end_ns : bound.start_ns, offset.value_or(0));
        return makeTimeCondition(nullptr, false, upper, false, offset.value_or(0));
    }

    if (lex.isKeyword("[") || lex.isKeyword("("))
    {
        bool include_start = lex.isKeyword("[");
        lex.nextToken();
        TimeBound start_bound = parseTimeBound();
        if (!lex.isKeyword(","))
            throwSyntaxError(fmt::format("unexpected token {} in the _time range; expecting ','", lex.getToken()));
        lex.nextToken();
        TimeBound end_bound = parseTimeBound();
        bool include_end = false;
        if (lex.isKeyword("]"))
            include_end = true;
        else if (!lex.isKeyword(")"))
            throwSyntaxError(fmt::format("unexpected closing token {} in the _time range", lex.getToken()));
        lex.nextToken();
        auto offset = parseOptionalTimeOffset();

        /// An inclusive start means the start of the period; an exclusive start skips the whole period.
        /// An inclusive end includes the whole period.
        ASTPtr lower = include_start ? start_bound.start : start_bound.end;
        ASTPtr upper = include_end ? end_bound.end : end_bound.start;

        /// Remember the window bounds for the rate() and rate_sum() stats functions:
        /// all top-level `_time` filters intersect into the effective query time range.
        auto lower_ns = include_start ? start_bound.start_ns : start_bound.end_ns;
        auto upper_ns = include_end ? end_bound.end_ns : end_bound.start_ns;
        recordTimeLowerBound(lower, lower_ns, offset.value_or(0));
        recordTimeUpperBound(upper, upper_ns, offset.value_or(0));

        return makeTimeCondition(lower, true, upper, false, offset.value_or(0));
    }

    /// The `_time:=<timestamp>` form is equivalent to `_time:<timestamp>`.
    if (lex.isKeyword("="))
        lex.nextToken();

    /// _time:5m or _time:2023-04-25Z
    auto state = lex.backupState();
    String text = lex.nextCompoundToken();
    if (auto duration = tryParseDuration(text))
    {
        auto offset = parseOptionalTimeOffset();
        ASTPtr lower = makeASTFunction("minus", makeASTFunction("now"), makeIntervalAST(*duration));
        recordTimeLowerBound(lower, {}, offset.value_or(0));
        recordTimeUpperBound(makeASTFunction("now"), {}, offset.value_or(0));
        return makeTimeCondition(lower, true, makeASTFunction("now"), false, offset.value_or(0));
    }
    lex.restoreState(state);

    TimeBound bound = parseTimeBound();
    auto offset = parseOptionalTimeOffset();
    /// An instant bound (`now`, a duration) has `start` and `end` pointing to the same
    /// expression: it is not a period, so it does not define a range for rate().
    if (bound.start && bound.end && bound.start != bound.end)
    {
        recordTimeLowerBound(bound.start, bound.start_ns, offset.value_or(0));
        recordTimeUpperBound(bound.end, bound.end_ns, offset.value_or(0));
    }
    return makeTimeCondition(bound.start, true, bound.end, false, offset.value_or(0));
}

ASTPtr LogsQLParser::parseFilterDayRange()
{
    lex.nextToken();

    bool include_start = false;
    if (lex.isKeyword("["))
        include_start = true;
    else if (!lex.isKeyword("("))
        throwSyntaxError("missing '[' or '(' after day_range");
    lex.nextToken();

    auto parse_time_of_day = [&]() -> Int64
    {
        String text = lex.nextCompoundToken();
        unsigned hour = 0;
        unsigned minute = 0;
        if (sscanf(text.c_str(), "%u:%u", &hour, &minute) != 2 || hour > 23 || minute > 59)  /// NOLINT(cert-err34-c)
            throwSyntaxError(fmt::format("cannot parse {} as hh:mm in day_range", text));
        return static_cast<Int64>(hour) * 3600 + minute * 60;
    };

    Int64 start_seconds = parse_time_of_day();
    if (!lex.isKeyword(","))
        throwSyntaxError("expecting ',' in day_range");
    lex.nextToken();
    Int64 end_seconds = parse_time_of_day();

    bool include_end = false;
    if (lex.isKeyword("]"))
        include_end = true;
    else if (!lex.isKeyword(")"))
        throwSyntaxError("missing ']' or ')' in day_range");
    lex.nextToken();

    auto offset = parseOptionalTimeOffset();

    /// Seconds since the start of the day.
    /// The global options(time_offset=...) applies here as well, same as in makeTimeCondition.
    Int64 total_offset_ns = offset.value_or(0) + options_time_offset_ns;
    ASTPtr time_expr = columnExpr("_time");
    if (total_offset_ns)
        time_expr = shiftTime(time_expr, -total_offset_ns);
    ASTPtr seconds_of_day = makeASTFunction("plus",
        makeASTFunction("plus",
            makeASTFunction("multiply", makeASTFunction("toHour", time_expr), make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(3600)))),
            makeASTFunction("multiply", makeASTFunction("toMinute", time_expr->clone()), make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(60))))),
        makeASTFunction("toSecond", time_expr->clone()));

    return makeAnd({
        makeASTFunction(include_start ? "greaterOrEquals" : "greater", seconds_of_day, make_intrusive<ASTLiteral>(Field(start_seconds))),
        makeASTFunction(include_end ? "lessOrEquals" : "less", seconds_of_day->clone(), make_intrusive<ASTLiteral>(Field(end_seconds)))});
}

ASTPtr LogsQLParser::parseFilterWeekRange()
{
    lex.nextToken();

    bool include_start = false;
    if (lex.isKeyword("["))
        include_start = true;
    else if (!lex.isKeyword("("))
        throwSyntaxError("missing '[' or '(' after week_range");
    lex.nextToken();

    auto parse_day = [&]() -> Int64
    {
        String text = Poco::toLower(lex.nextCompoundToken());
        /// Day numbers as in VictoriaLogs: Sunday = 0.
        if (text == "sun" || text == "sunday") return 0;
        if (text == "mon" || text == "monday") return 1;
        if (text == "tue" || text == "tuesday") return 2;
        if (text == "wed" || text == "wednesday") return 3;
        if (text == "thu" || text == "thursday") return 4;
        if (text == "fri" || text == "friday") return 5;
        if (text == "sat" || text == "saturday") return 6;
        throwSyntaxError(fmt::format("cannot parse {} as a day of week in week_range", text));
    };

    Int64 start_day = parse_day();
    if (!lex.isKeyword(","))
        throwSyntaxError("expecting ',' in week_range");
    lex.nextToken();
    Int64 end_day = parse_day();

    bool include_end = false;
    if (lex.isKeyword("]"))
        include_end = true;
    else if (!lex.isKeyword(")"))
        throwSyntaxError("missing ']' or ')' in week_range");
    lex.nextToken();

    auto offset = parseOptionalTimeOffset();

    /// The global options(time_offset=...) applies here as well, same as in makeTimeCondition.
    Int64 total_offset_ns = offset.value_or(0) + options_time_offset_ns;
    ASTPtr time_expr = columnExpr("_time");
    if (total_offset_ns)
        time_expr = shiftTime(time_expr, -total_offset_ns);

    /// toDayOfWeek returns 1 for Monday ... 7 for Sunday; convert to Sunday = 0.
    ASTPtr day_of_week = makeASTFunction("modulo", makeASTFunction("toDayOfWeek", time_expr), make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(7))));

    return makeAnd({
        makeASTFunction(include_start ? "greaterOrEquals" : "greater", day_of_week, make_intrusive<ASTLiteral>(Field(start_day))),
        makeASTFunction(include_end ? "lessOrEquals" : "less", day_of_week->clone(), make_intrusive<ASTLiteral>(Field(end_day)))});
}

}
