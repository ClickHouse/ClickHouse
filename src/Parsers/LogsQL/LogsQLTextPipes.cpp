/// The text-processing pipes of the LogsQL parser (`| extract`, `| format`, `| unpack_json`, ...),
/// the join/window pipes, and the pattern_match filters.

#include <Parsers/LogsQL/LogsQLParser.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTWindowDefinition.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/re2.h>
#include <Poco/String.h>

#include <charconv>

namespace DB
{

namespace
{

using namespace LogsQLUtils;

ASTPtr makeString(const String & value)
{
    return make_intrusive<ASTLiteral>(Field(value));
}

ASTPtr makeNumber(UInt64 value)
{
    return make_intrusive<ASTLiteral>(Field(value));
}

String trimCopy(const char * begin, const char * end)
{
    while (begin < end && isWhitespaceASCII(*begin))
        ++begin;
    while (end > begin && isWhitespaceASCII(end[-1]))
        --end;
    return String(begin, end);
}

/// Unescapes the common HTML entities in the literal parts of extract/format patterns,
/// so that `&lt;` can be used for a literal '<'. VictoriaLogs unescapes the full HTML5 table;
/// here only the common entities and numeric references are supported.
String unescapeHTMLEntities(const String & text)
{
    String result;
    result.reserve(text.size());
    size_t i = 0;
    while (i < text.size())
    {
        if (text[i] != '&')
        {
            result += text[i];
            ++i;
            continue;
        }

        auto try_entity = [&](std::string_view entity, char replacement)
        {
            if (text.size() - i >= entity.size() && std::string_view(text).substr(i, entity.size()) == entity)
            {
                result += replacement;
                i += entity.size();
                return true;
            }
            return false;
        };

        if (try_entity("&lt;", '<') || try_entity("&gt;", '>') || try_entity("&quot;", '"')
            || try_entity("&#39;", '\'') || try_entity("&amp;", '&'))
            continue;

        result += text[i];
        ++i;
    }
    return result;
}

}

std::vector<LogsQLParser::PatternStep> LogsQLParser::parsePatternSteps(const String & pattern)
{
    std::vector<PatternStep> steps;
    size_t pos = 0;
    String prefix;
    while (pos < pattern.size())
    {
        size_t open = pattern.find('<', pos);
        if (open == String::npos)
        {
            prefix += pattern.substr(pos);
            break;
        }
        size_t close = pattern.find('>', open + 1);
        if (close == String::npos)
        {
            prefix += pattern.substr(pos);
            break;
        }

        prefix += pattern.substr(pos, open - pos);
        String field = pattern.substr(open + 1, close - open - 1);
        pos = close + 1;

        PatternStep step;
        if (field == "_" || field == "*")
            field.clear();
        if (auto colon = field.find(':'); colon != String::npos)
        {
            String option = field.substr(0, colon);
            Poco::trimInPlace(option);
            step.plain = option == "plain";
            field = field.substr(colon + 1);
        }
        Poco::trimInPlace(field);

        step.prefix = unescapeHTMLEntities(prefix);
        step.field = field;
        steps.push_back(std::move(step));
        prefix.clear();
    }

    if (!prefix.empty())
    {
        PatternStep trailing;
        trailing.prefix = unescapeHTMLEntities(prefix);
        steps.push_back(std::move(trailing));
    }

    return steps;
}

void LogsQLParser::applyComputedFields(Layer & layer, const std::vector<std::pair<String, ASTPtr>> & fields, bool use_replace)
{
    if (!use_replace)
    {
        for (const auto & [name, expression] : fields)
            appendComputedColumn(layer, expression, name);
        return;
    }

    wrapLayerIf(layer, !layer.select.empty());

    /// The replaced expressions reference the original column (through `if (...)`,
    /// `keep_original_fields`, or `skip_empty_results`), so the column must exist.
    /// The strict transformer reports a missing column explicitly, while a non-strict
    /// one would silently drop the computed field.
    auto replace = make_intrusive<ASTColumnsReplaceTransformer>();
    replace->is_strict = true;
    for (const auto & [name, expression] : fields)
    {
        auto replacement = make_intrusive<ASTColumnsReplaceTransformer::Replacement>();
        replacement->name = name;
        replacement->children.push_back(expression);
        replace->children.push_back(replacement);
    }

    auto transformers = make_intrusive<ASTColumnsTransformerList>();
    transformers->children.push_back(replace);

    auto asterisk = make_intrusive<ASTAsterisk>();
    asterisk->transformers = transformers;
    asterisk->children.push_back(transformers);
    layer.select = {asterisk};
    layer.has_projection = true;
}

/// ---- The extract and extract_regexp pipes ----

void LogsQLParser::parsePipeExtract(Layer & layer, bool is_regexp)
{
    lex.nextToken();

    ASTPtr condition = parseOptionalIfCondition();

    if (!lex.isQuoted())
        throwSyntaxError("the extract pattern must be quoted");
    String pattern_text = lex.getToken();
    lex.nextToken();

    String source = "_msg";
    if (lex.isKeyword("from"))
    {
        lex.nextToken();
        source = parseFieldName();
    }

    bool keep_original_fields = false;
    bool skip_empty_results = false;
    if (lex.isKeyword("keep_original_fields"))
    {
        keep_original_fields = true;
        lex.nextToken();
    }
    else if (lex.isKeyword("skip_empty_results"))
    {
        skip_empty_results = true;
        lex.nextToken();
    }

    /// Build the regular expression with a capture group per named placeholder.
    String regexp = "(?s)";
    std::vector<std::pair<String, size_t>> group_of_field;

    if (is_regexp)
    {
        regexp += "(?:" + pattern_text + ")";
        re2::RE2 compiled(regexp, re2::RE2::Quiet);
        if (!compiled.ok())
            throwSyntaxError(fmt::format("invalid regexp {} in the extract_regexp pipe: {}", pattern_text, compiled.error()));
        for (const auto & [index, name] : compiled.CapturingGroupNames())
            group_of_field.emplace_back(name, index);
        if (group_of_field.empty())
            throwSyntaxError("the extract_regexp pattern must contain at least one named group (?P<name>...)");
        std::sort(group_of_field.begin(), group_of_field.end(), [](const auto & a, const auto & b) { return a.second < b.second; });
    }
    else
    {
        std::vector<PatternStep> steps = parsePatternSteps(pattern_text);

        size_t group = 0;
        size_t named_fields = 0;
        for (size_t i = 0; i < steps.size(); ++i)
        {
            if (i > 0 && steps[i].prefix.empty() && !steps[i].field.empty() && !steps[i - 1].field.empty())
                throwSyntaxError(fmt::format("missing delimiter between <{}> and <{}> in the extract pattern", steps[i - 1].field, steps[i].field));
            if (steps[i].field.ends_with('*'))
                throwSyntaxError(fmt::format("the extracted field name {} cannot end with '*'", steps[i].field));

            regexp += escapeRegexp(steps[i].prefix);
            if (i + 1 >= steps.size() && steps[i].field.empty())
                break;
            if (i + 1 < steps.size() || !steps[i].field.empty())
            {
                bool is_last = i + 1 == steps.size() || (i + 2 == steps.size() && steps[i + 1].field.empty() && steps[i + 1].prefix.empty());
                const char * capture = is_last ? "(.*)" : "(.*?)";
                if (steps[i].field.empty())
                {
                    regexp += is_last ? "(?:.*)" : "(?:.*?)";
                }
                else
                {
                    regexp += capture;
                    ++group;
                    group_of_field.emplace_back(steps[i].field, group);
                    ++named_fields;
                }
            }
        }
        if (named_fields == 0)
            throwSyntaxError("the extract pattern must contain at least one named placeholder like <field>");
    }

    /// The inner layer computes the capture groups once; the outer layer projects them into fields.
    wrapLayerIf(layer, !layer.select.empty());
    ASTPtr groups = makeASTFunction("extractGroups", columnExpr(source), makeString(regexp));
    layer.select.push_back(make_intrusive<ASTAsterisk>());
    groups->setAlias("__logsql_extract");
    layer.select.push_back(groups);
    layer.has_projection = true;
    wrapLayer(layer);

    bool use_replace = keep_original_fields || skip_empty_results || condition != nullptr;

    std::vector<std::pair<String, ASTPtr>> computed;
    for (const auto & [field, group_index] : group_of_field)
    {
        ASTPtr value = makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>("__logsql_extract"), makeNumber(group_index));
        ASTPtr original = columnExpr(field);
        if (skip_empty_results)
            value = makeASTFunction("if", makeASTFunction("notEquals", value, makeString("")), value->clone(), original->clone());
        else if (keep_original_fields)
            value = makeASTFunction("if", makeASTFunction("notEquals", original, makeString("")), original->clone(), value);
        if (condition)
            value = makeASTFunction("if", condition->clone(), value, original->clone());
        computed.emplace_back(columnName(field), value);
    }

    /// Drop the temporary column with the capture groups. The plain path also drops
    /// the original columns overwritten by the extracted fields, so that extracting
    /// into an existing field replaces it instead of producing a duplicate column
    /// (the EXCEPT transformer is not strict, so names absent from the schema are fine).
    auto except = make_intrusive<ASTColumnsExceptTransformer>();
    except->children.push_back(make_intrusive<ASTIdentifier>("__logsql_extract"));
    if (!use_replace)
        for (const auto & [name, expression] : computed)
            except->children.push_back(make_intrusive<ASTIdentifier>(name));
    auto transformers = make_intrusive<ASTColumnsTransformerList>();
    transformers->children.push_back(except);

    if (use_replace)
    {
        /// See the comment about the strict transformer in `applyComputedFields`.
        auto replace = make_intrusive<ASTColumnsReplaceTransformer>();
        replace->is_strict = true;
        for (const auto & [name, expression] : computed)
        {
            auto replacement = make_intrusive<ASTColumnsReplaceTransformer::Replacement>();
            replacement->name = name;
            replacement->children.push_back(expression);
            replace->children.push_back(replacement);
        }
        transformers->children.push_back(replace);
    }

    auto asterisk = make_intrusive<ASTAsterisk>();
    asterisk->transformers = transformers;
    asterisk->children.push_back(transformers);
    layer.select = {asterisk};
    if (!use_replace)
    {
        for (auto & [name, expression] : computed)
        {
            expression->setAlias(name);
            layer.select.push_back(expression);
        }
    }
    layer.has_projection = true;
}

/// ---- The format pipe ----

void LogsQLParser::parsePipeFormat(Layer & layer)
{
    lex.nextToken();

    ASTPtr condition = parseOptionalIfCondition();

    String pattern_text;
    if (lex.isQuoted())
    {
        pattern_text = lex.getToken();
        lex.nextToken();
    }
    else
    {
        pattern_text = lex.nextCompoundToken();
    }

    String result_field = "_msg";
    if (lex.isKeyword("as"))
    {
        lex.nextToken();
        result_field = parseFieldName();
    }

    bool keep_original_fields = false;
    bool skip_empty_results = false;
    if (lex.isKeyword("keep_original_fields"))
    {
        keep_original_fields = true;
        lex.nextToken();
    }
    else if (lex.isKeyword("skip_empty_results"))
    {
        skip_empty_results = true;
        lex.nextToken();
    }

    /// Re-parse the pattern keeping the transformer options.
    std::vector<PatternStep> steps;
    {
        size_t pos = 0;
        String prefix;
        while (pos < pattern_text.size())
        {
            size_t open = pattern_text.find('<', pos);
            size_t close = open == String::npos ? String::npos : pattern_text.find('>', open + 1);
            if (open == String::npos || close == String::npos)
            {
                prefix += pattern_text.substr(pos);
                break;
            }
            prefix += pattern_text.substr(pos, open - pos);
            PatternStep step;
            step.prefix = unescapeHTMLEntities(prefix);
            step.field = pattern_text.substr(open + 1, close - open - 1);
            prefix.clear();
            pos = close + 1;
            steps.push_back(std::move(step));
        }
        if (!prefix.empty())
        {
            PatternStep trailing;
            trailing.prefix = unescapeHTMLEntities(prefix);
            steps.push_back(std::move(trailing));
        }
    }

    auto transform_value = [&](const String & placeholder) -> ASTPtr
    {
        String option;
        String field = placeholder;
        if (auto colon = placeholder.find(':'); colon != String::npos)
        {
            option = placeholder.substr(0, colon);
            Poco::trimInPlace(option);
            field = placeholder.substr(colon + 1);
        }
        Poco::trimInPlace(field);
        if (field == "_" || field == "*" || field.empty())
            return nullptr;

        ASTPtr value = makeASTFunction("toString", columnExpr(field));
        if (option.empty())
            return value;
        if (option == "q")
            return makeASTFunction("toJSONString", columnExpr(field));
        if (option == "uc")
            return makeASTFunction("upperUTF8", value);
        if (option == "lc")
            return makeASTFunction("lowerUTF8", value);
        if (option == "urlencode")
            return makeASTFunction("encodeURLFormComponent", value);
        if (option == "urldecode")
            return makeASTFunction("decodeURLFormComponent", value);
        if (option == "hexencode")
            return makeASTFunction("upper", makeASTFunction("hex", value));
        if (option == "hexdecode")
            return makeASTFunction("unhex", value);
        if (option == "base64encode")
            return makeASTFunction("base64Encode", value);
        if (option == "base64decode")
        {
            /// Invalid input falls back to the raw value, as in VictoriaLogs.
            ASTPtr decoded = makeASTFunction("tryBase64Decode", value);
            return makeASTFunction("if", makeASTFunction("notEquals", decoded, makeString("")), decoded->clone(), value->clone());
        }
        if (option == "ipv4")
        {
            /// The value must be a uint32 number; otherwise the raw value is kept.
            ASTPtr parsed = makeASTFunction("toUInt64OrNull", value);
            return makeASTFunction("if",
                makeASTFunction("and",
                    makeASTFunction("isNotNull", parsed),
                    makeASTFunction("lessOrEquals", parsed->clone(), makeNumber(4294967295ULL))),
                makeASTFunction("IPv4NumToString", makeASTFunction("toUInt32OrZero", value->clone())),
                value->clone());
        }
        if (option == "hexnumencode")
        {
            /// A uint64 number encoded as 16 uppercase hex digits; non-numbers keep the raw value.
            /// `hex` trims leading zero bytes, so pad the result to the fixed width.
            ASTPtr parsed = makeASTFunction("toUInt64OrNull", value);
            return makeASTFunction("if",
                makeASTFunction("isNotNull", parsed),
                makeASTFunction("leftPad",
                    makeASTFunction("hex", makeASTFunction("toUInt64OrZero", value->clone())),
                    makeNumber(16),
                    makeString("0")),
                value->clone());
        }
        if (option == "plain")
            return value;
        throwNotImplemented(fmt::format("The format transformer '{}:'", option));
    };

    auto concat = makeASTFunction("concat");
    for (const auto & step : steps)
    {
        if (!step.prefix.empty())
            concat->arguments->children.push_back(makeString(step.prefix));
        if (!step.field.empty())
        {
            if (ASTPtr value = transform_value(step.field))
                concat->arguments->children.push_back(value);
        }
    }
    ASTPtr formatted = concat;
    if (concat->arguments->children.empty())
        formatted = makeString("");
    else if (concat->arguments->children.size() == 1)
        formatted = makeASTFunction("concat", concat->arguments->children[0], makeString(""));

    String result_column = columnName(result_field);
    bool result_exists_for_sure = result_column == context.msg_column || result_column == context.time_column;
    bool use_replace = keep_original_fields || skip_empty_results || condition != nullptr || result_exists_for_sure;

    ASTPtr original = make_intrusive<ASTIdentifier>(result_column);
    ASTPtr value = formatted;
    if (skip_empty_results)
        value = makeASTFunction("if", makeASTFunction("notEquals", value, makeString("")), value->clone(), original->clone());
    else if (keep_original_fields)
        value = makeASTFunction("if", makeASTFunction("notEquals", original, makeString("")), original->clone(), value);
    if (condition)
        value = makeASTFunction("if", condition, value, original->clone());

    applyComputedFields(layer, {{result_column, value}}, use_replace);
}

/// ---- The unpack_json and unpack_logfmt pipes ----

void LogsQLParser::parsePipeUnpack(Layer & layer, bool is_logfmt)
{
    lex.nextToken();

    ASTPtr condition = parseOptionalIfCondition();

    String source = "_msg";
    if (lex.isKeyword("from"))
    {
        lex.nextToken();
        source = parseFieldName();
    }
    else if (!lex.isQueryPartTrailer() && !lex.isKeyword("fields") && !lex.isKeyword("result_prefix")
        && !lex.isKeyword("keep_original_fields") && !lex.isKeyword("skip_empty_results") && !lex.isKeyword("preserve_keys"))
    {
        source = parseFieldName();
    }

    std::vector<String> fields;
    if (lex.isKeyword("fields"))
    {
        lex.nextToken();
        if (!lex.isKeyword("("))
            throwSyntaxError("missing '(' after 'fields'");
        lex.nextToken();
        while (!lex.isKeyword(")"))
        {
            fields.push_back(parseFieldName());
            if (!lex.skippedSpace() && lex.isKeyword("*"))
                throwNotImplemented("Field name prefixes in the unpack pipes");
            if (lex.isKeyword(","))
                lex.nextToken();
            else if (!lex.isKeyword(")"))
                throwSyntaxError(fmt::format("unexpected token {}; expecting ',' or ')'", lex.getToken()));
        }
        lex.nextToken();
    }

    std::vector<String> preserve_keys;
    if (!is_logfmt && lex.isKeyword("preserve_keys"))
    {
        lex.nextToken();
        if (!lex.isKeyword("("))
            throwSyntaxError("missing '(' after 'preserve_keys'");
        lex.nextToken();
        while (!lex.isKeyword(")"))
        {
            preserve_keys.push_back(parseFieldName());
            if (lex.isKeyword(","))
                lex.nextToken();
            else if (!lex.isKeyword(")"))
                throwSyntaxError(fmt::format("unexpected token {}; expecting ',' or ')'", lex.getToken()));
        }
        lex.nextToken();
    }

    String result_prefix;
    if (lex.isKeyword("result_prefix"))
    {
        lex.nextToken();
        result_prefix = lex.getToken();
        if (!lex.isQuoted())
            result_prefix = lex.nextCompoundToken();
        else
            lex.nextToken();
    }

    bool keep_original_fields = false;
    bool skip_empty_results = false;
    if (lex.isKeyword("keep_original_fields"))
    {
        keep_original_fields = true;
        lex.nextToken();
    }
    else if (lex.isKeyword("skip_empty_results"))
    {
        skip_empty_results = true;
        lex.nextToken();
    }

    if (fields.empty())
        throwNotImplemented(fmt::format(
            "The {} pipe without an explicit 'fields' list (the set of fields of a ClickHouse table is fixed)",
            is_logfmt ? "unpack_logfmt" : "unpack_json"));

    bool use_replace = keep_original_fields || skip_empty_results || condition != nullptr;

    std::vector<std::pair<String, ASTPtr>> computed;
    for (const auto & field : fields)
    {
        ASTPtr value;
        if (is_logfmt)
        {
            /// key=value with optionally double-quoted values (approximated with JSON unquoting).
            String key_pattern = fmt::format(R"re((?:^|[ ]){}=("(?:[^"\\]|\\.)*"|[^ ]*))re", escapeRegexp(field));
            ASTPtr token = makeASTFunction("extract", columnExpr(source), makeString(key_pattern));
            value = makeASTFunction("if",
                makeASTFunction("startsWith", token, makeString("\"")),
                makeASTFunction("JSONExtractString", token->clone()),
                token->clone());
        }
        else
        {
            /// JSON: strings are decoded, other values keep their JSON form, null and missing keys become ''.
            ASTs path_arguments;
            path_arguments.push_back(columnExpr(source));
            std::string_view rest = field;
            while (!rest.empty())
            {
                size_t dot = rest.find('.');
                path_arguments.push_back(makeString(String(rest.substr(0, dot))));
                if (dot == std::string_view::npos)
                    break;
                rest.remove_prefix(dot + 1);
            }

            auto with_path = [&](const String & function)
            {
                auto call = makeASTFunction(function);
                for (const auto & argument : path_arguments)
                    call->arguments->children.push_back(argument->clone());
                return ASTPtr(call);
            };

            bool preserved = std::find(preserve_keys.begin(), preserve_keys.end(), field) != preserve_keys.end();
            if (preserved)
            {
                value = with_path("JSONExtractRaw");
            }
            else
            {
                ASTPtr type = with_path("JSONType");
                value = makeASTFunction("multiIf",
                    makeASTFunction("equals", type, makeString("String")), with_path("JSONExtractString"),
                    makeASTFunction("equals", type->clone(), makeString("Null")), makeString(""),
                    with_path("JSONExtractRaw"));
            }
        }

        String output_name = result_prefix + field;
        ASTPtr original = make_intrusive<ASTIdentifier>(columnName(output_name));
        if (skip_empty_results)
            value = makeASTFunction("if", makeASTFunction("notEquals", value, makeString("")), value->clone(), original->clone());
        else if (keep_original_fields)
            value = makeASTFunction("if", makeASTFunction("notEquals", original, makeString("")), original->clone(), value);
        if (condition)
            value = makeASTFunction("if", condition->clone(), value, original->clone());

        computed.emplace_back(columnName(output_name), value);
    }

    applyComputedFields(layer, computed, use_replace);
}

/// ---- The join pipe ----

void LogsQLParser::parsePipeJoin(Layer & layer)
{
    lex.nextToken();

    if (lex.isKeyword("by") || lex.isKeyword("on"))
        lex.nextToken();

    if (!lex.isKeyword("("))
        throwSyntaxError("missing '(' with the 'by' fields of the join pipe");
    lex.nextToken();
    std::vector<String> by_fields;
    while (!lex.isKeyword(")"))
    {
        by_fields.push_back(parseFieldName());
        if (lex.isKeyword(","))
            lex.nextToken();
        else if (!lex.isKeyword(")"))
            throwSyntaxError(fmt::format("unexpected token {}; expecting ',' or ')'", lex.getToken()));
    }
    lex.nextToken();
    if (by_fields.empty())
        throwSyntaxError("missing fields in the 'by' clause of the join pipe");

    if (lex.isKeyword("rows"))
        throwNotImplemented("The static rows(...) form of the join pipe");

    if (!lex.isKeyword("("))
        throwSyntaxError("missing '(' with the subquery of the join pipe");
    lex.nextToken();
    Layer other = parseQuery(/*is_subquery=*/ true);
    lex.nextToken();  /// Skip ')'.

    bool inner = false;
    while (true)
    {
        if (lex.isKeyword("inner"))
        {
            inner = true;
            lex.nextToken();
        }
        else if (lex.isKeyword("prefix"))
        {
            throwNotImplemented("The 'prefix' clause of the join pipe");
        }
        else
        {
            break;
        }
    }

    wrapLayer(layer);
    layer.join_subquery = buildSelectWithUnion(other);
    for (const auto & field : by_fields)
        layer.join_using.push_back(columnName(field));
    layer.join_inner = inner;
}

/// ---- The running_stats and total_stats pipes, and window helpers ----

namespace
{

/// A window function call: name(args) OVER (PARTITION BY ... [ORDER BY ... ROWS ...]).
ASTPtr makeWindowCall(
    const String & name,
    ASTs arguments,
    ASTs partition_by,
    ASTs order_by,
    bool running_frame)
{
    auto function = makeASTFunction(name);
    function->arguments->children = std::move(arguments);
    function->setIsWindowFunction(true);

    auto window = make_intrusive<ASTWindowDefinition>();
    if (!partition_by.empty())
    {
        auto list = make_intrusive<ASTExpressionList>();
        list->children = std::move(partition_by);
        window->partition_by = list;
        window->children.push_back(window->partition_by);
    }
    if (!order_by.empty())
    {
        auto list = make_intrusive<ASTExpressionList>();
        list->children = std::move(order_by);
        window->order_by = list;
        window->children.push_back(window->order_by);
    }

    /// ROWS BETWEEN UNBOUNDED PRECEDING AND (CURRENT ROW | UNBOUNDED FOLLOWING).
    window->frame_is_default = false;
    window->frame_type = WindowFrame::FrameType::ROWS;
    window->frame_begin_type = WindowFrame::BoundaryType::Unbounded;
    window->frame_begin_preceding = true;
    if (running_frame)
    {
        window->frame_end_type = WindowFrame::BoundaryType::Current;
    }
    else
    {
        window->frame_end_type = WindowFrame::BoundaryType::Unbounded;
        window->frame_end_preceding = false;
    }

    function->window_definition = window;
    function->children.push_back(window);
    return function;
}

ASTPtr makeOrderElement(ASTPtr expression, bool is_desc)
{
    auto element = make_intrusive<ASTOrderByElement>();
    element->direction = is_desc ? -1 : 1;
    element->nulls_direction = element->direction;
    element->children.push_back(std::move(expression));
    return element;
}

}

void LogsQLParser::parsePipeRunningStats(Layer & layer, bool is_total)
{
    lex.nextToken();

    if (lex.isKeyword("by"))
        lex.nextToken();

    std::vector<String> by_fields;
    if (lex.isKeyword("("))
    {
        lex.nextToken();
        while (!lex.isKeyword(")"))
        {
            by_fields.push_back(parseFieldName());
            if (lex.isKeyword(","))
                lex.nextToken();
            else if (!lex.isKeyword(")"))
                throwSyntaxError(fmt::format("unexpected token {}; expecting ',' or ')'", lex.getToken()));
        }
        lex.nextToken();
    }

    struct Entry
    {
        ASTPtr expression;
        String result_name;
    };
    std::vector<Entry> entries;

    while (true)
    {
        if (lex.isQuoted())
            throwSyntaxError(fmt::format("unknown running_stats function {}", lex.getToken()));
        String name = Poco::toLower(lex.getToken());
        const char * canonical_begin = lex.getTokenBegin();

        String aggregate;
        ASTs arguments;
        std::optional<UInt64> offset;
        if (name == "count" || name == "sum" || name == "min" || name == "max")
        {
            lex.nextToken();
            bool wildcard = false;
            std::vector<String> args = parseArgsInParens(&wildcard);
            if (wildcard || (args.empty() && name != "count"))
                throwNotImplemented(fmt::format("The running_stats function {}() over all fields", name));
            if (args.size() > 1)
                throwNotImplemented(fmt::format("The running_stats function {}() over multiple fields", name));
            aggregate = name;
            if (!args.empty())
            {
                /// `sum` is numeric, so it takes the parsed numeric value of the field
                /// and skips the non-numeric values; `count`, `min` and `max` take the value itself.
                arguments.push_back(name == "sum" ? numericValueExpr(args[0]) : columnExpr(args[0]));
            }
        }
        else if (name == "first" || name == "last")
        {
            lex.nextToken();
            std::vector<String> args = parseArgsInParens();
            if (args.size() != 1)
                throwSyntaxError(fmt::format("{}() of running_stats requires exactly one field", name));
            aggregate = name;
            arguments.push_back(columnExpr(args[0]));
            if (lex.isKeyword("offset"))
            {
                lex.nextToken();
                offset = parseLimitValue();
            }
        }
        else
        {
            throwSyntaxError(fmt::format("unknown {} function {}", is_total ? "total_stats" : "running_stats", lex.getToken()));
        }
        String canonical = trimCopy(canonical_begin, lex.getTokenBegin());

        ASTs partition_by;
        for (const auto & field : by_fields)
            partition_by.push_back(columnExpr(field));

        ASTs order_by;
        bool needs_order = !is_total || aggregate == "first" || aggregate == "last";
        if (needs_order)
            order_by.push_back(makeOrderElement(columnExpr("_time"), /*is_desc=*/ aggregate == "last"));

        ASTPtr expression;
        if (aggregate == "first" || aggregate == "last")
        {
            /// nth_value over the _time order; `last` uses the reversed order.
            /// For running_stats the frame ends at the current row, which yields ""
            /// until enough rows are seen - the same as in VictoriaLogs.
            expression = makeWindowCall("nth_value",
                {arguments[0], makeNumber(offset.value_or(0) + 1)},
                std::move(partition_by), std::move(order_by),
                /*running_frame=*/ !is_total && aggregate != "last");
            if (!is_total && aggregate == "last")
            {
                /// The running last(f) offset N is the value N rows back in the _time order.
                ASTs asc_order;
                asc_order.push_back(makeOrderElement(columnExpr("_time"), false));
                ASTs partition_again;
                for (const auto & field : by_fields)
                    partition_again.push_back(columnExpr(field));
                expression = makeWindowCall("lagInFrame",
                    {arguments[0]->clone(), makeNumber(offset.value_or(0))},
                    std::move(partition_again), std::move(asc_order),
                    /*running_frame=*/ true);
            }
        }
        else
        {
            expression = makeWindowCall(aggregate, std::move(arguments), std::move(partition_by), std::move(order_by), !is_total);
        }

        Entry entry;
        entry.expression = expression;
        entry.result_name = canonical;

        if (!lex.isKeyword(",") && !lex.isQueryPartTrailer())
        {
            if (lex.isKeyword("as"))
                lex.nextToken();
            entry.result_name = parseFieldName();
        }
        entries.push_back(std::move(entry));

        if (lex.isQueryPartTrailer())
            break;
        if (!lex.isKeyword(","))
            throwSyntaxError(fmt::format("unexpected token {} after a {} function", lex.getToken(), is_total ? "total_stats" : "running_stats"));
        lex.nextToken();
    }

    wrapLayerIf(layer, !layer.select.empty() || layer.has_aggregation || layer.limit.has_value() || layer.offset.has_value()
        || !layer.order_by.empty() || layer.order_by_all);

    /// `* EXCEPT (...)` overwrites existing same-named columns instead of duplicating them
    /// (see the comment in `appendComputedColumn`).
    auto except = make_intrusive<ASTColumnsExceptTransformer>();
    for (const auto & entry : entries)
        except->children.push_back(make_intrusive<ASTIdentifier>(columnName(entry.result_name)));
    auto transformers = make_intrusive<ASTColumnsTransformerList>();
    transformers->children.push_back(except);
    auto asterisk = make_intrusive<ASTAsterisk>();
    asterisk->transformers = transformers;
    asterisk->children.push_back(transformers);
    layer.select.push_back(asterisk);
    for (auto & entry : entries)
    {
        entry.expression->setAlias(columnName(entry.result_name));
        layer.select.push_back(entry.expression);
    }
    layer.has_projection = true;

    /// VictoriaLogs emits the rows sorted by the group key and then by `_time`.
    for (const auto & field : by_fields)
        layer.order_by.push_back(makeOrderElement(columnExpr(field), false));
    layer.order_by.push_back(makeOrderElement(columnExpr("_time"), false));
}

/// ---- The pattern_match filters ----

ASTPtr LogsQLParser::parseFilterPatternMatch(const String & field_name, const String & func_name)
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
        throwSyntaxError(fmt::format("unexpected number of args for {}(); got {}; want 1", func_name, args.size()));

    /// The pattern placeholders are approximated with regular expressions
    /// (VictoriaLogs matches them with a hand-written greedy matcher with extra boundary rules).
    static constexpr const char * number = "(?:[0-9]+|[0-9a-fA-F]{4}(?:[0-9a-fA-F]{2})*)";
    const String uuid = fmt::format("{0}-{0}-{0}-{0}-{0}", number);
    const String ip4 = fmt::format(R"({0}\.{0}\.{0}\.{0})", number);
    const String time_re = fmt::format("{0}:{0}:{0}(?:[.,]{0})?", number);
    const String date_re = fmt::format("(?:{0}-{0}-{0}|{0}/{0}/{0})", number);
    const String datetime_re = fmt::format("{0}[T ]{1}(?:Z|[+-]{2}:{2})?", date_re, time_re, number);
    static constexpr const char * word = R"re((?:"(?:[^"\\]|\\.)*"|`[^`]*`|'(?:[^'\\]|\\.)*'|[0-9A-Za-z_]*))re";

    const String & pattern = args[0];
    String regexp = "(?s)";
    size_t pos = 0;
    while (pos < pattern.size())
    {
        size_t open = pattern.find('<', pos);
        size_t close = open == String::npos ? String::npos : pattern.find('>', open + 1);
        if (open == String::npos || close == String::npos)
        {
            regexp += escapeRegexp(pattern.substr(pos));
            break;
        }

        String placeholder = pattern.substr(open, close - open + 1);
        String replacement;
        if (placeholder == "<N>")
            replacement = number;
        else if (placeholder == "<UUID>")
            replacement = uuid;
        else if (placeholder == "<IP4>")
            replacement = ip4;
        else if (placeholder == "<TIME>")
            replacement = time_re;
        else if (placeholder == "<DATE>")
            replacement = date_re;
        else if (placeholder == "<DATETIME>")
            replacement = datetime_re;
        else if (placeholder == "<W>")
            replacement = word;

        if (replacement.empty())
        {
            /// Unknown placeholders are matched literally.
            regexp += escapeRegexp(pattern.substr(pos, close - pos + 1));
        }
        else
        {
            regexp += escapeRegexp(pattern.substr(pos, open - pos));
            regexp += replacement;
        }
        pos = close + 1;
    }

    String core = regexp.substr(4);
    if (func_name == "pattern_match_full")
        regexp = "(?s)^(?:" + core + ")$";
    else if (func_name == "pattern_match_prefix")
        regexp = "(?s)^(?:" + core + ")";
    else if (func_name == "pattern_match_suffix")
        regexp = "(?s)(?:" + core + ")$";

    re2::RE2 checked(regexp, re2::RE2::Quiet);
    if (!checked.ok())
        throwSyntaxError(fmt::format("cannot compile the {} pattern {}: {}", func_name, pattern, checked.error()));

    return makeASTFunction("match", columnExpr(field_name), makeString(regexp));
}

}
