/// The pipe part of the LogsQL parser: `| fields ...`, `| stats ...`, `| sort by (...)` etc.,
/// and the assembly of the resulting ASTSelectQuery.

#include <Parsers/LogsQL/LogsQLParser.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTColumnsTransformers.h>
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
#include <Poco/String.h>

#include <cctype>
#include <cmath>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

namespace
{

using namespace LogsQLUtils;

const std::unordered_set<String> stats_func_names = {
    "any", "avg", "count", "count_empty", "count_uniq", "count_uniq_hash", "field_max", "field_min",
    "histogram", "json_values", "max", "median", "min", "quantile", "rate", "rate_sum",
    "row_any", "row_max", "row_min", "stddev", "sum", "sum_len", "uniq_values", "values"};

/// Pipes which exist in LogsQL but are not translated yet.
const std::unordered_set<String> unsupported_pipes = {
    "block_stats", "blocks_count", "coalesce", "collapse_nums", "decolorize", "drop_empty_fields",
    "extract", "extract_regexp", "facets", "field_names", "field_values", "format", "generate_sequence",
    "hash", "join", "json_array_concat", "json_array_len", "len", "pack_json", "pack_logfmt",
    "query_stats", "replace", "replace_regexp", "running_stats", "sample", "set_stream_fields",
    "split", "stream_context", "time_add", "top_stats", "total_stats", "union",
    "unpack_json", "unpack_logfmt", "unpack_syslog", "unpack_words", "unroll"};

const std::vector<std::string_view> field_name_stop_tokens = {":"};

ASTPtr makeUInt64Literal(UInt64 value)
{
    return make_intrusive<ASTLiteral>(Field(value));
}

ASTPtr makeStringLiteral(const String & value)
{
    return make_intrusive<ASTLiteral>(Field(value));
}

/// An aggregate function with an optional -If combinator and optional parameters.
ASTPtr makeAggregate(const String & name, ASTs arguments, ASTPtr condition, ASTs parameters = {})
{
    auto function = make_intrusive<ASTFunction>();
    function->name = condition ? name + "If" : name;
    if (condition)
        arguments.push_back(condition);
    function->arguments = make_intrusive<ASTExpressionList>();
    function->arguments->children = std::move(arguments);
    function->children.push_back(function->arguments);
    if (!parameters.empty())
    {
        function->parameters = make_intrusive<ASTExpressionList>();
        function->parameters->children = std::move(parameters);
        function->children.push_back(function->parameters);
    }
    return function;
}

ASTPtr makeOrderByElement(ASTPtr expression, bool is_desc)
{
    auto element = make_intrusive<ASTOrderByElement>();
    element->direction = is_desc ? -1 : 1;
    element->nulls_direction = element->direction;
    element->children.push_back(std::move(expression));
    return element;
}

String trimText(const char * begin, const char * end)
{
    while (begin < end && isspace(static_cast<unsigned char>(*begin)))
        ++begin;
    while (end > begin && isspace(static_cast<unsigned char>(end[-1])))
        --end;
    return String(begin, end);
}

}

/// ---- Pipes ----

void LogsQLParser::parsePipes(Layer & layer)
{
    while (true)
    {
        parsePipe(layer);

        if (lex.isKeyword("|"))
        {
            lex.nextToken();
            continue;
        }
        if (lex.isQueryPartTrailer())
            return;
        throwSyntaxError(fmt::format("unexpected token {} after a pipe; expecting '|', ';' or ')'", lex.getToken()));
    }
}

void LogsQLParser::parsePipe(Layer & layer)
{
    IncreaseDepth depth_guard(*this);

    if (!lex.isQuoted())
    {
        String name = Poco::toLower(lex.getToken());

        if (name == "fields" || name == "keep")
        {
            parsePipeFields(layer);
            return;
        }
        if (name == "delete" || name == "del" || name == "drop" || name == "rm")
        {
            parsePipeDelete(layer);
            return;
        }
        if (name == "copy" || name == "cp")
        {
            parsePipeCopy(layer);
            return;
        }
        if (name == "rename" || name == "mv")
        {
            parsePipeRename(layer);
            return;
        }
        if (name == "limit" || name == "head")
        {
            parsePipeLimit(layer);
            return;
        }
        if (name == "offset" || name == "skip")
        {
            parsePipeOffset(layer);
            return;
        }
        if (name == "sort" || name == "order")
        {
            parsePipeSort(layer);
            return;
        }
        if (name == "stats" || name == "stats_remote")
        {
            parsePipeStats(layer, /*need_keyword=*/ true);
            return;
        }
        if (name == "where" || name == "filter")
        {
            parsePipeWhere(layer, /*need_keyword=*/ true);
            return;
        }
        if (name == "uniq")
        {
            parsePipeUniq(layer);
            return;
        }
        if (name == "top")
        {
            parsePipeTop(layer);
            return;
        }
        if (name == "first" || name == "last")
        {
            parsePipeFirstLast(layer, /*is_last=*/ name == "last");
            return;
        }
        if (name == "math" || name == "eval")
        {
            parsePipeMath(layer);
            return;
        }

        if (unsupported_pipes.contains(name))
            throwNotImplemented(fmt::format("The pipe '{}'", name));
    }

    if (isLikelyStatsPipe())
    {
        parsePipeStats(layer, /*need_keyword=*/ false);
        return;
    }

    if (isLikelyFilterPipe())
    {
        parsePipeWhere(layer, /*need_keyword=*/ false);
        return;
    }

    throwSyntaxError(fmt::format("unexpected pipe name {}; probably, 'filter' is missing in front of it", lex.getToken()));
}

bool LogsQLParser::isLikelyStatsPipe()
{
    if (lex.isQuoted())
        return false;
    return stats_func_names.contains(Poco::toLower(lex.getToken())) || lex.isKeyword("by") || lex.isKeyword("(");
}

bool LogsQLParser::isLikelyFilterPipe()
{
    if (lex.isQuoted())
        return true;
    if (!LogsQLLexer::isWord(lex.getToken()))
        return true;
    if (lex.isKeyword("not"))
        return true;

    /// A filter pipe without the `filter` keyword must start with `field_name:`.
    auto state = lex.backupState();
    bool result = false;
    try
    {
        lex.nextCompoundToken(field_name_stop_tokens);
        result = lex.isKeyword(":");
    }
    catch (const Exception &)
    {
    }
    lex.restoreState(state);
    return result;
}

void LogsQLParser::parsePipeFields(Layer & layer)
{
    lex.nextToken();

    ASTs entries;
    while (true)
    {
        if (lex.isKeyword("*"))
        {
            lex.nextToken();
            entries.push_back(make_intrusive<ASTAsterisk>());
        }
        else
        {
            String name = lex.nextCompoundToken();
            if (!lex.skippedSpace() && lex.isKeyword("*"))
            {
                /// A field name prefix: `fields foo*`.
                lex.nextToken();
                auto matcher = make_intrusive<ASTColumnsRegexpMatcher>();
                matcher->setPattern("^" + escapeRegexp(name));
                entries.push_back(matcher);
            }
            else
            {
                entries.push_back(columnExpr(name));
            }
        }

        if (!lex.isKeyword(","))
            break;
        lex.nextToken();
    }

    wrapLayerIf(layer, layer.has_projection || layer.has_aggregation);
    layer.select = std::move(entries);
    layer.has_projection = true;
}

void LogsQLParser::parsePipeDelete(Layer & layer)
{
    lex.nextToken();

    std::vector<String> names;
    while (true)
    {
        String name = lex.nextCompoundToken();
        if (!lex.skippedSpace() && lex.isKeyword("*"))
            throwNotImplemented("Deleting fields selected by a name prefix");
        names.push_back(columnName(name));

        if (!lex.isKeyword(","))
            break;
        lex.nextToken();
    }

    wrapLayerIf(layer, layer.has_projection || layer.has_aggregation);

    auto except = make_intrusive<ASTColumnsExceptTransformer>();
    for (const auto & name : names)
        except->children.push_back(make_intrusive<ASTIdentifier>(name));

    auto transformers = make_intrusive<ASTColumnsTransformerList>();
    transformers->children.push_back(except);

    auto asterisk = make_intrusive<ASTAsterisk>();
    asterisk->transformers = transformers;
    asterisk->children.push_back(transformers);

    layer.select = {asterisk};
    layer.has_projection = true;
}

void LogsQLParser::parsePipeCopy(Layer & layer)
{
    lex.nextToken();

    if (layer.select.empty())
        layer.select.push_back(make_intrusive<ASTAsterisk>());

    while (true)
    {
        String source = lex.nextCompoundToken();
        if (!lex.skippedSpace() && lex.isKeyword("*"))
            throwNotImplemented("Copying fields selected by a name prefix");
        if (lex.isKeyword("as"))
            lex.nextToken();
        String target = parseFieldName();

        auto expression = columnExpr(source);
        expression->setAlias(columnName(target));
        layer.select.push_back(expression);

        if (!lex.isKeyword(","))
            break;
        lex.nextToken();
    }

    layer.has_projection = true;
}

void LogsQLParser::parsePipeRename(Layer & layer)
{
    lex.nextToken();

    std::vector<std::pair<String, String>> renames;
    while (true)
    {
        String source = lex.nextCompoundToken();
        if (!lex.skippedSpace() && lex.isKeyword("*"))
            throwNotImplemented("Renaming fields selected by a name prefix");
        if (lex.isKeyword("as"))
            lex.nextToken();
        String target = parseFieldName();
        renames.emplace_back(columnName(source), columnName(target));

        if (!lex.isKeyword(","))
            break;
        lex.nextToken();
    }

    wrapLayerIf(layer, layer.has_projection || layer.has_aggregation);

    auto except = make_intrusive<ASTColumnsExceptTransformer>();
    for (const auto & [source, target] : renames)
        except->children.push_back(make_intrusive<ASTIdentifier>(source));

    auto transformers = make_intrusive<ASTColumnsTransformerList>();
    transformers->children.push_back(except);

    auto asterisk = make_intrusive<ASTAsterisk>();
    asterisk->transformers = transformers;
    asterisk->children.push_back(transformers);

    layer.select = {asterisk};
    for (const auto & [source, target] : renames)
    {
        auto expression = make_intrusive<ASTIdentifier>(source);
        expression->setAlias(target);
        layer.select.push_back(expression);
    }
    layer.has_projection = true;
}

UInt64 LogsQLParser::parseLimitValue()
{
    String text = lex.nextCompoundToken();
    auto value = tryParseNumber(text);
    if (!value || *value < 0 || std::isinf(*value) || std::isnan(*value) || *value != std::floor(*value))
        throwSyntaxError(fmt::format("cannot parse {} as a non-negative integer", text));
    return static_cast<UInt64>(*value);
}

void LogsQLParser::parsePipeLimit(Layer & layer)
{
    lex.nextToken();

    UInt64 limit = 10;
    if (!lex.isQueryPartTrailer())
        limit = parseLimitValue();

    /// SQL applies OFFSET before LIMIT, which matches the `| offset N | limit M` pipe order.
    wrapLayerIf(layer, layer.limit.has_value());
    layer.limit = limit;
}

void LogsQLParser::parsePipeOffset(Layer & layer)
{
    lex.nextToken();
    UInt64 offset = parseLimitValue();

    wrapLayerIf(layer, layer.limit.has_value() || layer.offset.has_value());
    layer.offset = offset;
}

std::vector<LogsQLParser::SortField> LogsQLParser::parseSortFields()
{
    std::vector<SortField> fields;
    if (!lex.isKeyword("("))
        throwSyntaxError("missing '('");

    lex.nextToken();
    while (true)
    {
        if (lex.isKeyword(")"))
        {
            lex.nextToken();
            return fields;
        }

        SortField field;
        field.name = parseFieldName();
        if (lex.isKeyword("desc"))
        {
            field.is_desc = true;
            lex.nextToken();
        }
        else if (lex.isKeyword("asc"))
        {
            lex.nextToken();
        }
        fields.push_back(std::move(field));

        if (lex.isKeyword(","))
            lex.nextToken();
        else if (!lex.isKeyword(")"))
            throwSyntaxError(fmt::format("unexpected token {}; expecting ',' or ')'", lex.getToken()));
    }
}

ASTPtr LogsQLParser::sortKeyExpr(const Layer & layer, const String & field_name) const
{
    String name = columnName(field_name);
    for (const auto & entry : layer.select)
    {
        if (entry->tryGetAlias() == name)
        {
            ASTPtr expression = entry->clone();
            expression->setAlias("");
            return expression;
        }
    }
    return make_intrusive<ASTIdentifier>(name);
}

void LogsQLParser::parsePipeSort(Layer & layer)
{
    lex.nextToken();

    std::vector<SortField> fields;
    bool has_fields = false;
    if (lex.isKeyword("by") || lex.isKeyword("("))
    {
        if (lex.isKeyword("by"))
            lex.nextToken();
        fields = parseSortFields();
        has_fields = true;
    }

    bool global_desc = false;
    if (lex.isKeyword("desc"))
    {
        global_desc = true;
        lex.nextToken();
    }
    else if (lex.isKeyword("asc"))
    {
        lex.nextToken();
    }

    std::optional<UInt64> sort_limit;
    std::optional<UInt64> sort_offset;
    while (true)
    {
        if (lex.isKeyword("offset"))
        {
            lex.nextToken();
            if (sort_offset)
                throwSyntaxError("duplicate 'offset' in the sort pipe");
            sort_offset = parseLimitValue();
        }
        else if (lex.isKeyword("limit"))
        {
            lex.nextToken();
            if (sort_limit)
                throwSyntaxError("duplicate 'limit' in the sort pipe");
            sort_limit = parseLimitValue();
        }
        else if (lex.isKeyword("rank"))
        {
            throwNotImplemented("The 'rank' clause of the sort pipe");
        }
        else if (lex.isKeyword("partition"))
        {
            throwNotImplemented("The 'partition by' clause of the sort pipe");
        }
        else
        {
            break;
        }
    }

    /// Aggregation and projection layers are wrapped into a subquery: sorting by an output field
    /// which shadows a source column (e.g. the bucketed `_time` of `stats by (_time:1d)`)
    /// is unambiguous only across a subquery boundary.
    wrapLayerIf(layer,
        !layer.order_by.empty() || layer.order_by_all || layer.limit.has_value() || layer.offset.has_value()
        || layer.has_aggregation || layer.has_projection);

    if (has_fields && !fields.empty())
    {
        for (const auto & field : fields)
            layer.order_by.push_back(makeOrderByElement(sortKeyExpr(layer, field.name), field.is_desc != global_desc));
    }
    else
    {
        layer.order_by_all = true;
        layer.order_by.push_back(makeOrderByElement(make_intrusive<ASTIdentifier>("all"), global_desc));
    }

    if (sort_limit)
        layer.limit = sort_limit;
    if (sort_offset)
        layer.offset = sort_offset;
}

void LogsQLParser::parsePipeWhere(Layer & layer, bool need_keyword)
{
    if (need_keyword)
        lex.nextToken();

    if (lex.isQueryPartTrailer())
        throwSyntaxError("missing filters after 'where'");

    ASTPtr condition = parseFilterOr("");

    if (layer.has_projection || layer.has_aggregation || layer.limit || layer.offset)
    {
        wrapLayer(layer);
        layer.where = condition;
    }
    else if (!layer.where)
    {
        layer.where = condition;
    }
    else if (condition)
    {
        layer.where = makeASTFunction("and", layer.where, condition);
    }
}

void LogsQLParser::parsePipeUniq(Layer & layer)
{
    lex.nextToken();

    bool need_fields = false;
    if (lex.isKeyword("by"))
    {
        lex.nextToken();
        need_fields = true;
    }

    std::vector<String> fields;
    if (lex.isKeyword("("))
    {
        lex.nextToken();
        while (!lex.isKeyword(")"))
        {
            fields.push_back(parseFieldName());
            if (lex.isKeyword(","))
                lex.nextToken();
            else if (!lex.isKeyword(")"))
                throwSyntaxError(fmt::format("unexpected token {}; expecting ',' or ')'", lex.getToken()));
        }
        lex.nextToken();
    }
    else if (!lex.isKeyword("filter") && !lex.isKeyword("with") && !lex.isKeyword("hits") && !lex.isKeyword("limit") && !lex.isQueryPartTrailer())
    {
        while (true)
        {
            fields.push_back(parseFieldName());
            if (!lex.isKeyword(","))
                break;
            lex.nextToken();
        }
    }
    else if (need_fields)
    {
        throwSyntaxError("missing fields after 'by'");
    }

    if (fields.empty())
        throwSyntaxError("missing fields inside 'by(...)' of the uniq pipe");

    if (lex.isKeyword("filter"))
        throwNotImplemented("The 'filter' clause of the uniq pipe");

    bool with_hits = false;
    if (lex.isKeyword("with"))
    {
        lex.nextToken();
        if (!lex.isKeyword("hits"))
            throwSyntaxError("missing 'hits' after 'with'");
    }
    if (lex.isKeyword("hits"))
    {
        with_hits = true;
        lex.nextToken();
    }

    std::optional<UInt64> limit;
    if (lex.isKeyword("limit"))
    {
        lex.nextToken();
        limit = parseLimitValue();
    }

    wrapLayerIf(layer,
        layer.has_projection || layer.has_aggregation || layer.limit.has_value() || layer.offset.has_value()
        || !layer.order_by.empty() || layer.order_by_all);

    for (const auto & field : fields)
    {
        layer.select.push_back(columnExpr(field));
        layer.group_by.push_back(columnExpr(field));
    }
    if (with_hits)
    {
        auto hits = makeAggregate("count", {}, nullptr);
        hits->setAlias("hits");
        layer.select.push_back(hits);
    }
    if (limit)
        layer.limit = limit;
    layer.has_aggregation = true;
    layer.has_projection = true;
}

void LogsQLParser::parsePipeTop(Layer & layer)
{
    lex.nextToken();

    UInt64 limit = 10;
    if (!lex.isQuoted() && isNumberPrefix(lex.getToken()))
        limit = parseLimitValue();

    bool need_fields = false;
    if (lex.isKeyword("by"))
    {
        lex.nextToken();
        need_fields = true;
    }

    std::vector<String> fields;
    if (lex.isKeyword("("))
    {
        lex.nextToken();
        while (!lex.isKeyword(")"))
        {
            fields.push_back(parseFieldName());
            if (lex.isKeyword(","))
                lex.nextToken();
            else if (!lex.isKeyword(")"))
                throwSyntaxError(fmt::format("unexpected token {}; expecting ',' or ')'", lex.getToken()));
        }
        lex.nextToken();
    }
    else if (!lex.isKeyword("hits") && !lex.isKeyword("rank") && !lex.isQueryPartTrailer())
    {
        while (true)
        {
            fields.push_back(parseFieldName());
            if (!lex.isKeyword(","))
                break;
            lex.nextToken();
        }
    }
    else if (need_fields)
    {
        throwSyntaxError("missing fields after 'by'");
    }

    if (fields.empty())
        throwSyntaxError("expecting at least a single field in 'by(...)' of the top pipe");

    String hits_name = "hits";
    while (true)
    {
        if (lex.isKeyword("hits"))
        {
            lex.nextToken();
            if (lex.isKeyword("as"))
                lex.nextToken();
            hits_name = lex.nextCompoundToken();
        }
        else if (lex.isKeyword("rank"))
        {
            throwNotImplemented("The 'rank' clause of the top pipe");
        }
        else
        {
            break;
        }
    }

    wrapLayerIf(layer,
        layer.has_projection || layer.has_aggregation || layer.limit.has_value() || layer.offset.has_value()
        || !layer.order_by.empty() || layer.order_by_all);

    for (const auto & field : fields)
    {
        layer.select.push_back(columnExpr(field));
        layer.group_by.push_back(columnExpr(field));
    }

    auto hits = makeAggregate("count", {}, nullptr);
    hits->setAlias(hits_name);
    layer.select.push_back(hits);

    /// The most frequent values first; the field values are a tiebreaker to make the order deterministic.
    layer.order_by.push_back(makeOrderByElement(sortKeyExpr(layer, hits_name), /*is_desc=*/ true));
    for (const auto & field : fields)
        layer.order_by.push_back(makeOrderByElement(columnExpr(field), /*is_desc=*/ false));

    layer.limit = limit;
    layer.has_aggregation = true;
    layer.has_projection = true;
}

void LogsQLParser::parsePipeFirstLast(Layer & layer, bool is_last)
{
    lex.nextToken();

    UInt64 limit = 10;
    if (!lex.isQuoted() && isNumberPrefix(lex.getToken()))
        limit = parseLimitValue();

    if (lex.isKeyword("by"))
        lex.nextToken();

    std::vector<SortField> fields;
    if (lex.isKeyword("("))
        fields = parseSortFields();

    if (lex.isKeyword("partition"))
        throwNotImplemented("The 'partition by' clause of the first/last pipes");
    if (lex.isKeyword("rank"))
        throwNotImplemented("The 'rank' clause of the first/last pipes");

    wrapLayerIf(layer,
        !layer.order_by.empty() || layer.order_by_all || layer.limit.has_value() || layer.offset.has_value()
        || layer.has_aggregation || layer.has_projection);

    if (!fields.empty())
    {
        for (const auto & field : fields)
            layer.order_by.push_back(makeOrderByElement(sortKeyExpr(layer, field.name), field.is_desc != is_last));
    }
    else
    {
        layer.order_by_all = true;
        layer.order_by.push_back(makeOrderByElement(make_intrusive<ASTIdentifier>("all"), is_last));
    }
    layer.limit = limit;
}

/// ---- The stats pipe ----

void LogsQLParser::parsePipeStats(Layer & layer, bool need_keyword)
{
    if (need_keyword)
        lex.nextToken();

    ASTs by_select;
    ASTs by_keys;

    if (lex.isKeyword("by") || lex.isKeyword("("))
    {
        if (lex.isKeyword("by"))
            lex.nextToken();
        if (!lex.isKeyword("("))
            throwSyntaxError("missing '(' after 'by'");
        lex.nextToken();

        while (!lex.isKeyword(")"))
        {
            String name = lex.nextCompoundToken(field_name_stop_tokens);
            if (name.empty())
                name = "_msg";

            ASTPtr key;
            if (lex.isKeyword(":"))
            {
                /// A bucket specification: by (_time:1h), by (request_size:10KB).
                lex.nextToken();
                String bucket = lex.nextCompoundToken();
                if (bucket.starts_with('/'))
                    throwNotImplemented("IPv4 subnet buckets in the 'by' clause");

                std::optional<String> bucket_offset;
                if (lex.isKeyword("offset"))
                {
                    lex.nextToken();
                    bucket_offset = lex.nextCompoundToken();
                }

                if (name == "_time")
                {
                    if (bucket_offset)
                        throwNotImplemented("A bucket offset for the _time field");

                    static const std::unordered_map<String, String> named_steps = {
                        {"nanosecond", "toIntervalNanosecond"}, {"microsecond", "toIntervalMicrosecond"},
                        {"millisecond", "toIntervalMillisecond"}, {"second", "toIntervalSecond"},
                        {"minute", "toIntervalMinute"}, {"hour", "toIntervalHour"}, {"day", "toIntervalDay"},
                        {"week", "toIntervalWeek"}, {"month", "toIntervalMonth"}, {"year", "toIntervalYear"}};

                    ASTPtr interval;
                    if (auto it = named_steps.find(Poco::toLower(bucket)); it != named_steps.end())
                        interval = makeASTFunction(it->second, makeUInt64Literal(1));
                    else if (auto duration = tryParseDuration(bucket))
                        interval = makeIntervalAST(*duration);
                    else
                        throwSyntaxError(fmt::format("cannot parse the time bucket step {}", bucket));

                    key = makeASTFunction("toStartOfInterval", columnExpr(name), interval);
                }
                else
                {
                    auto step = tryParseNumber(bucket);
                    if (!step || *step <= 0)
                        throwSyntaxError(fmt::format("cannot parse the bucket step {} for the field {}", bucket, name));

                    std::optional<Float64> offset_value;
                    if (bucket_offset)
                    {
                        offset_value = tryParseNumber(*bucket_offset);
                        if (!offset_value)
                            throwSyntaxError(fmt::format("cannot parse the bucket offset {} for the field {}", *bucket_offset, name));
                    }

                    ASTPtr value = columnExpr(name);
                    if (offset_value)
                        value = makeASTFunction("minus", value, make_intrusive<ASTLiteral>(Field(*offset_value)));
                    key = makeASTFunction("multiply",
                        makeASTFunction("floor", makeASTFunction("divide", value, make_intrusive<ASTLiteral>(Field(*step)))),
                        make_intrusive<ASTLiteral>(Field(*step)));
                    if (offset_value)
                        key = makeASTFunction("plus", key, make_intrusive<ASTLiteral>(Field(*offset_value)));
                }
            }
            else
            {
                key = columnExpr(name);
            }

            ASTPtr select_expr = key->clone();
            if (!select_expr->as<ASTIdentifier>())
                select_expr->setAlias(columnName(name));
            by_select.push_back(select_expr);
            /// GROUP BY references the output name, like the canonical hand-written form
            /// `SELECT toStartOfDay(t) AS t ... GROUP BY t`. Repeating the expression instead
            /// would re-resolve the field inside it to the select alias and fail.
            by_keys.push_back(make_intrusive<ASTIdentifier>(columnName(name)));

            if (lex.isKeyword(","))
                lex.nextToken();
            else if (!lex.isKeyword(")"))
                throwSyntaxError(fmt::format("unexpected token {} in the 'by' list; expecting ',' or ')'", lex.getToken()));
        }
        lex.nextToken();
    }

    ASTs aggregates;
    while (true)
    {
        StatsFunc stats_func = parseStatsFunc();

        if (lex.isKeyword("switch"))
            throwNotImplemented("The 'switch' clause of the stats pipe");

        ASTPtr condition;
        String condition_text;
        if (lex.isKeyword("if"))
        {
            const char * if_begin = lex.getTokenBegin();
            lex.nextToken();
            if (!lex.isKeyword("("))
                throwSyntaxError("missing '(' after 'if'");
            lex.nextToken();
            if (!lex.isKeyword(")"))
            {
                /// An empty `if ()` matches all rows, so no condition is needed for it.
                condition = parseFilterOr("");
                if (lex.isKeyword(";"))
                    lex.nextToken();
                if (condition == nullptr)
                    condition = make_intrusive<ASTLiteral>(Field(static_cast<UInt8>(1)));
            }
            if (!lex.isKeyword(")"))
                throwSyntaxError(fmt::format("missing ')' after the 'if' filter; got {}", lex.getToken()));
            lex.nextToken();
            condition_text = trimText(if_begin, lex.getTokenBegin());
        }

        String result_name;
        if (lex.isKeyword(",") || lex.isQueryPartTrailer())
        {
            result_name = stats_func.canonical;
            if (!condition_text.empty())
                result_name += " " + condition_text;
        }
        else
        {
            if (lex.isKeyword("as"))
                lex.nextToken();
            result_name = parseFieldName();
        }

        ASTPtr aggregate = stats_func.build(condition);
        aggregate->setAlias(result_name);
        aggregates.push_back(aggregate);

        if (lex.isQueryPartTrailer())
            break;
        if (!lex.isKeyword(","))
            throwSyntaxError(fmt::format("unexpected token {} after a stats function; expecting ',', '|', ';' or ')'", lex.getToken()));
        lex.nextToken();
    }

    wrapLayerIf(layer,
        layer.has_projection || layer.has_aggregation || layer.limit.has_value() || layer.offset.has_value()
        || !layer.order_by.empty() || layer.order_by_all);

    layer.select = by_select;
    layer.select.insert(layer.select.end(), aggregates.begin(), aggregates.end());
    layer.group_by = by_keys;
    layer.has_aggregation = true;
    layer.has_projection = true;
}

LogsQLParser::StatsFunc LogsQLParser::parseStatsFunc()
{
    if (lex.isQuoted())
        throwSyntaxError(fmt::format("unknown stats function {}", lex.getToken()));

    String name = Poco::toLower(lex.getToken());
    if (!stats_func_names.contains(name))
        throwSyntaxError(fmt::format("unknown stats function {}", lex.getToken()));
    lex.nextToken();

    if (name == "rate" || name == "rate_sum" || name == "histogram" || name == "json_values"
        || name == "row_any" || name == "row_max" || name == "row_min" || name == "field_max" || name == "field_min")
        throwNotImplemented(fmt::format("The stats function '{}'", name));

    bool wildcard = false;
    std::vector<String> args = parseArgsInParens(&wildcard);

    std::optional<UInt64> limit;
    if ((name == "count_uniq" || name == "uniq_values" || name == "values") && lex.isKeyword("limit"))
    {
        lex.nextToken();
        limit = parseLimitValue();
    }

    String canonical;
    if (name == "count" && (args.empty() || (args.size() == 1 && wildcard)))
        canonical = "count(*)";
    else
    {
        canonical = name + "(";
        for (size_t i = 0; i < args.size(); ++i)
        {
            if (i > 0)
                canonical += ", ";
            canonical += args[i];
        }
        canonical += ")";
    }

    auto column_args = [this](const std::vector<String> & arg_names)
    {
        ASTs columns;
        for (const auto & arg_name : arg_names)
            columns.push_back(columnExpr(arg_name));
        return columns;
    };

    if (name == "count")
    {
        if (args.empty() || wildcard)
            return {canonical, [](ASTPtr condition) { return makeAggregate("count", {}, condition); }};
        if (args.size() > 1)
            throwNotImplemented("count() over multiple fields");
        ASTPtr column = columnExpr(args[0]);
        return {canonical, [column](ASTPtr condition) { return makeAggregate("count", {column}, condition); }};
    }

    if (wildcard || args.empty())
        throwNotImplemented(fmt::format("The stats function {}() over all fields", name));

    for (const auto & arg : args)
        if (arg.ends_with('*'))
            throwNotImplemented("Field name prefixes in stats functions");

    if (name == "count_empty")
    {
        ASTs empty_checks;
        for (const auto & arg : args)
            empty_checks.push_back(makeASTFunction("equals", columnExpr(arg), makeStringLiteral("")));
        ASTPtr all_empty = empty_checks.size() == 1 ? empty_checks[0] : [&]
        {
            auto function = makeASTFunction("and");
            function->arguments->children = std::move(empty_checks);
            return ASTPtr(function);
        }();
        return {canonical, [all_empty](ASTPtr condition)
        {
            ASTPtr full_condition = condition ? makeASTFunction("and", all_empty, condition) : all_empty;
            return makeAggregate("count", {}, full_condition);
        }};
    }

    if (name == "count_uniq" || name == "count_uniq_hash")
    {
        ASTs columns = column_args(args);
        String aggregate_name = name == "count_uniq" ? "uniqExact" : "uniq";
        return {canonical, [aggregate_name, columns, limit](ASTPtr condition)
        {
            ASTPtr result = makeAggregate(aggregate_name, columns, condition);
            if (limit)
                result = makeASTFunction("least", result, makeUInt64Literal(*limit));
            return result;
        }};
    }

    if (name == "quantile")
    {
        if (args.size() != 2)
            throwNotImplemented("quantile() over all fields or over multiple fields");
        auto phi = tryParseNumber(args[0]);
        if (!phi || *phi < 0 || *phi > 1)
            throwSyntaxError(fmt::format("cannot parse the quantile level {}", args[0]));
        ASTPtr column = columnExpr(args[1]);
        Float64 phi_value = *phi;
        return {canonical, [column, phi_value](ASTPtr condition)
        {
            return makeAggregate("quantile", {column}, condition, {make_intrusive<ASTLiteral>(Field(phi_value))});
        }};
    }

    if (name == "uniq_values" || name == "values")
    {
        if (args.size() > 1)
            throwNotImplemented(fmt::format("{}() over multiple fields", name));
        ASTPtr column = columnExpr(args[0]);
        String aggregate_name = name == "uniq_values" ? "groupUniqArray" : "groupArray";
        bool sorted = name == "uniq_values";
        return {canonical, [aggregate_name, column, limit, sorted](ASTPtr condition)
        {
            ASTs parameters;
            if (limit)
                parameters.push_back(makeUInt64Literal(*limit));
            ASTPtr result = makeAggregate(aggregate_name, {column}, condition, parameters);
            if (sorted)
                result = makeASTFunction("arraySort", result);
            return result;
        }};
    }

    if (name == "sum_len")
    {
        if (args.size() > 1)
            throwNotImplemented("sum_len() over multiple fields");
        ASTPtr length = makeASTFunction("length", columnExpr(args[0]));
        return {canonical, [length](ASTPtr condition) { return makeAggregate("sum", {length}, condition); }};
    }

    static const std::unordered_map<String, String> simple_aggregates = {
        {"any", "any"}, {"avg", "avg"}, {"max", "max"}, {"median", "median"},
        {"min", "min"}, {"stddev", "stddevPop"}, {"sum", "sum"}};

    if (auto it = simple_aggregates.find(name); it != simple_aggregates.end())
    {
        if (args.size() > 1)
            throwNotImplemented(fmt::format("{}() over multiple fields", name));
        ASTPtr column = columnExpr(args[0]);
        String aggregate_name = it->second;
        return {canonical, [aggregate_name, column](ASTPtr condition) { return makeAggregate(aggregate_name, {column}, condition); }};
    }

    throwNotImplemented(fmt::format("The stats function '{}'", name));
}

/// ---- The math pipe ----

void LogsQLParser::parsePipeMath(Layer & layer)
{
    lex.nextToken();

    if (layer.select.empty())
        layer.select.push_back(make_intrusive<ASTAsterisk>());

    while (true)
    {
        const char * expression_begin = lex.getTokenBegin();
        ASTPtr expression = parseMathExpr(100);
        String default_name = trimText(expression_begin, lex.getTokenBegin());

        String result_name;
        if (lex.isKeyword(",") || lex.isQueryPartTrailer())
        {
            result_name = default_name;
        }
        else
        {
            if (lex.isKeyword("as"))
                lex.nextToken();
            result_name = parseFieldName();
        }

        expression->setAlias(result_name);
        layer.select.push_back(expression);

        if (!lex.isKeyword(","))
            break;
        lex.nextToken();
    }

    layer.has_projection = true;
}

ASTPtr LogsQLParser::parseMathExpr(int max_priority)
{
    IncreaseDepth depth_guard(*this);

    ASTPtr left = parseMathExprOperand();

    while (true)
    {
        if (lex.isQuoted())
            return left;

        struct BinaryOp { int priority; const char * function; };
        static const std::unordered_map<String, BinaryOp> binary_ops = {
            {"^", {1, "pow"}}, {"*", {2, "multiply"}}, {"/", {2, "divide"}}, {"%", {2, "modulo"}},
            {"+", {3, "plus"}}, {"-", {3, "minus"}}, {"&", {4, "bitAnd"}},
            {"xor", {5, "bitXor"}}, {"or", {6, "bitOr"}}, {"default", {10, "__default"}}};

        auto it = binary_ops.find(Poco::toLower(lex.getToken()));
        if (it == binary_ops.end() || it->second.priority > max_priority)
            return left;

        lex.nextToken();
        ASTPtr right = parseMathExpr(it->second.priority - 1);

        if (String(it->second.function) == "__default")
        {
            /// `a default b` returns b when a is not a finite number.
            left = makeASTFunction("if", makeASTFunction("isNaN", left->clone()), right, left);
        }
        else
        {
            left = makeASTFunction(it->second.function, left, right);
        }
    }
}

ASTPtr LogsQLParser::parseMathExprOperand()
{
    IncreaseDepth depth_guard(*this);

    if (lex.isKeyword("("))
    {
        lex.nextToken();
        ASTPtr result = parseMathExpr(100);
        if (!lex.isKeyword(")"))
            throwSyntaxError(fmt::format("missing ')' in a math expression; got {}", lex.getToken()));
        lex.nextToken();
        return result;
    }

    if (lex.isKeyword("-"))
    {
        lex.nextToken();
        return makeASTFunction("negate", parseMathExprOperand());
    }
    if (lex.isKeyword("+"))
    {
        lex.nextToken();
        return parseMathExprOperand();
    }

    if (lex.isQuoted())
    {
        /// A quoted literal: a timestamp, an IPv4 address or a number.
        String text = lex.getToken();
        lex.nextToken();
        if (auto timestamp = tryParseTimestamp(text); timestamp && timestamp->has_timezone)
            return make_intrusive<ASTLiteral>(Field(timestamp->start_ns));
        if (auto address = tryParseIPv4(text))
            return make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(*address)));
        if (auto number = tryParseNumber(text))
            return make_intrusive<ASTLiteral>(Field(*number));
        throwSyntaxError(fmt::format("cannot parse the math literal {}", text));
    }

    /// Function calls.
    if (!lex.isQuoted())
    {
        static const std::unordered_map<String, String> math_functions = {
            {"abs", "abs"}, {"ceil", "ceil"}, {"exp", "exp"}, {"floor", "floor"},
            {"ln", "log"}, {"max", "greatest"}, {"min", "least"}};

        String name = Poco::toLower(lex.getToken());
        auto state = lex.backupState();

        if (math_functions.contains(name) || name == "round" || name == "now" || name == "rand")
        {
            lex.nextToken();
            if (lex.isKeyword("(") && !lex.skippedSpace())
            {
                lex.nextToken();
                ASTs arguments;
                if (!lex.isKeyword(")"))
                {
                    while (true)
                    {
                        arguments.push_back(parseMathExpr(100));
                        if (lex.isKeyword(","))
                        {
                            lex.nextToken();
                            continue;
                        }
                        break;
                    }
                }
                if (!lex.isKeyword(")"))
                    throwSyntaxError(fmt::format("missing ')' in a math function call; got {}", lex.getToken()));
                lex.nextToken();

                if (name == "now")
                    return makeASTFunction("toUnixTimestamp64Nano", makeASTFunction("now64", makeUInt64Literal(9)));
                if (name == "rand")
                    return makeASTFunction("randCanonical");
                if (name == "round")
                {
                    if (arguments.size() == 1)
                        return makeASTFunction("round", arguments[0]);
                    if (arguments.size() == 2)
                        return makeASTFunction("multiply", makeASTFunction("round", makeASTFunction("divide", arguments[0], arguments[1])), arguments[1]->clone());
                    throwSyntaxError("round() requires one or two arguments");
                }
                auto function = makeASTFunction(math_functions.at(name));
                function->arguments->children = std::move(arguments);
                return function;
            }
            lex.restoreState(state);
        }
    }

    /// A number literal or a field name.
    static const std::vector<std::string_view> math_stop_tokens = {"+", "-", "/"};
    String token = lex.nextCompoundToken(math_stop_tokens);
    if (isNumberPrefix(token) || Poco::toLower(token) == "inf" || Poco::toLower(token) == "nan")
    {
        if (auto number = tryParseNumber(token))
        {
            if (*number == std::floor(*number) && std::abs(*number) < 9.007199254740992e15 && !std::isinf(*number))
                return make_intrusive<ASTLiteral>(Field(static_cast<Int64>(*number)));
            return make_intrusive<ASTLiteral>(Field(*number));
        }
    }
    return columnExpr(token);
}

/// ---- Assembling the resulting AST ----

ASTPtr LogsQLParser::buildSelect(Layer & layer) const
{
    auto select = make_intrusive<ASTSelectQuery>();

    auto select_list = make_intrusive<ASTExpressionList>();
    if (layer.select.empty())
        select_list->children.push_back(make_intrusive<ASTAsterisk>());
    else
        select_list->children = layer.select;
    select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list));

    auto table_expression = make_intrusive<ASTTableExpression>();
    if (layer.source_subquery)
    {
        auto subquery = make_intrusive<ASTSubquery>(layer.source_subquery);
        table_expression->subquery = subquery;
        table_expression->children.push_back(subquery);
    }
    else
    {
        ASTPtr table_identifier;
        if (context.database.empty())
            table_identifier = make_intrusive<ASTTableIdentifier>(context.table);
        else
            table_identifier = make_intrusive<ASTTableIdentifier>(context.database, context.table);
        table_expression->database_and_table_name = table_identifier;
        table_expression->children.push_back(table_identifier);
    }

    auto table_element = make_intrusive<ASTTablesInSelectQueryElement>();
    table_element->table_expression = table_expression;
    table_element->children.push_back(table_expression);

    auto tables = make_intrusive<ASTTablesInSelectQuery>();
    tables->children.push_back(table_element);
    select->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));

    if (layer.where)
        select->setExpression(ASTSelectQuery::Expression::WHERE, ASTPtr(layer.where));

    if (!layer.group_by.empty())
    {
        auto group_by_list = make_intrusive<ASTExpressionList>();
        group_by_list->children = layer.group_by;
        select->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_by_list));
    }

    if (!layer.order_by.empty())
    {
        auto order_by_list = make_intrusive<ASTExpressionList>();
        order_by_list->children = layer.order_by;
        select->setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_by_list));
        select->order_by_all = layer.order_by_all;
    }

    if (layer.limit)
        select->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, makeUInt64Literal(*layer.limit));
    if (layer.offset)
        select->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, makeUInt64Literal(*layer.offset));

    return select;
}

ASTPtr LogsQLParser::buildSelectWithUnion(Layer & layer) const
{
    auto list_of_selects = make_intrusive<ASTExpressionList>();
    list_of_selects->children.push_back(buildSelect(layer));

    auto select_with_union = make_intrusive<ASTSelectWithUnionQuery>();
    select_with_union->list_of_selects = list_of_selects;
    select_with_union->children.push_back(list_of_selects);
    return select_with_union;
}

void LogsQLParser::wrapLayer(Layer & layer) const
{
    ASTPtr inner = buildSelectWithUnion(layer);
    layer = Layer{};
    layer.source_subquery = inner;
}

void LogsQLParser::wrapLayerIf(Layer & layer, bool condition) const
{
    if (condition)
        wrapLayer(layer);
}

}
