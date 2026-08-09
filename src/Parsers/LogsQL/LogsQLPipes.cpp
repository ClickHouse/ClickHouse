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
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/SelectUnionMode.h>

#include <Common/re2.h>

#include <Common/Exception.h>
#include <Poco/String.h>

#include <cctype>
#include <charconv>
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

/// Pipes which exist in LogsQL but are not translated: they either introspect the internal
/// storage of VictoriaLogs or require the dynamic set of fields, which does not exist
/// for a ClickHouse table with a fixed schema.
const std::unordered_set<String> unsupported_pipes = {
    "block_stats", "blocks_count", "collapse_nums",
    "facets", "field_names",
    "query_stats", "set_stream_fields",
    "stream_context", "top_stats",
    "unpack_syslog"};

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
        if (name == "len")
        {
            parsePipeLen(layer);
            return;
        }
        if (name == "coalesce")
        {
            parsePipeCoalesce(layer);
            return;
        }
        if (name == "decolorize")
        {
            parsePipeDecolorize(layer);
            return;
        }
        if (name == "split")
        {
            parsePipeSplit(layer);
            return;
        }
        if (name == "unpack_words")
        {
            parsePipeUnpackWords(layer);
            return;
        }
        if (name == "time_add")
        {
            parsePipeTimeAdd(layer);
            return;
        }
        if (name == "sample")
        {
            parsePipeSample(layer);
            return;
        }
        if (name == "generate_sequence")
        {
            parsePipeGenerateSequence(layer);
            return;
        }
        if (name == "field_values")
        {
            parsePipeFieldValues(layer);
            return;
        }
        if (name == "json_array_len")
        {
            parsePipeJSONArrayLen(layer);
            return;
        }
        if (name == "replace" || name == "replace_regexp")
        {
            parsePipeReplace(layer, /*is_regexp=*/ name == "replace_regexp");
            return;
        }
        if (name == "union")
        {
            parsePipeUnion(layer);
            return;
        }
        if (name == "hash")
        {
            parsePipeHash(layer);
            return;
        }
        if (name == "unroll")
        {
            parsePipeUnroll(layer);
            return;
        }
        if (name == "pack_json" || name == "pack_logfmt")
        {
            parsePipePack(layer, /*is_logfmt=*/ name == "pack_logfmt");
            return;
        }
        if (name == "extract" || name == "extract_regexp")
        {
            parsePipeExtract(layer, /*is_regexp=*/ name == "extract_regexp");
            return;
        }
        if (name == "format")
        {
            parsePipeFormat(layer);
            return;
        }
        if (name == "unpack_json" || name == "unpack_logfmt")
        {
            parsePipeUnpack(layer, /*is_logfmt=*/ name == "unpack_logfmt");
            return;
        }
        if (name == "join")
        {
            parsePipeJoin(layer);
            return;
        }
        if (name == "running_stats" || name == "total_stats")
        {
            parsePipeRunningStats(layer, /*is_total=*/ name == "total_stats");
            return;
        }
        if (name == "json_array_concat")
        {
            /// Joins the elements of a JSON array into a string with the given delimiter.
            lex.nextToken();
            String delimiter;
            if (!lex.isQueryPartTrailer() && !lex.isKeyword("from") && !lex.isKeyword("as"))
            {
                delimiter = lex.getToken();
                if (!lex.isQuoted())
                    delimiter = lex.nextCompoundToken();
                else
                    lex.nextToken();
            }
            String source = "_msg";
            if (lex.isKeyword("from"))
            {
                lex.nextToken();
                source = parseFieldName();
            }
            else if (!lex.isQueryPartTrailer() && !lex.isKeyword("as"))
            {
                source = parseFieldName();
            }
            String target = source;
            if (lex.isKeyword("as"))
            {
                lex.nextToken();
                target = parseFieldName();
            }
            else if (!lex.isQueryPartTrailer())
            {
                target = parseFieldName();
            }
            auto element = make_intrusive<ASTIdentifier>("__logsql_element");
            auto decode = makeASTFunction("if",
                makeASTFunction("startsWith", element, makeStringLiteral("\"")),
                makeASTFunction("JSONExtractString", element->clone()),
                element->clone());
            auto lambda = makeASTFunction("lambda", makeASTFunction("tuple", element->clone()), decode);
            ASTPtr expression = makeASTFunction("arrayStringConcat",
                makeASTFunction("arrayMap", lambda, makeASTFunction("JSONExtractArrayRaw", columnExpr(source))),
                makeStringLiteral(delimiter));
            if (columnName(target) == columnName(source))
                applyColumnReplacement(layer, columnName(target), expression);
            else
                appendComputedColumn(layer, expression, columnName(target));
            return;
        }
        if (name == "drop_empty_fields")
        {
            /// Drops the fields with empty values from each row. With a fixed table schema
            /// there is nothing to drop, so this is a no-op (rows are never dropped by this pipe).
            lex.nextToken();
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
    catch (const Exception &) // NOLINT(bugprone-empty-catch)
    {
        /// Not a field name (e.g. malformed input): this is not a shorthand filter pipe.
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

    std::vector<std::pair<String, String>> copies;
    while (true)
    {
        String source = lex.nextCompoundToken();
        if (!lex.skippedSpace() && lex.isKeyword("*"))
            throwNotImplemented("Copying fields selected by a name prefix");
        if (lex.isKeyword("as"))
            lex.nextToken();
        String target = parseFieldName();
        copies.emplace_back(columnName(source), columnName(target));

        if (!lex.isKeyword(","))
            break;
        lex.nextToken();
    }

    wrapLayerIf(layer, !layer.select.empty());

    /// `* EXCEPT (...)` overwrites existing same-named columns instead of duplicating them
    /// (see the comment in `appendComputedColumn`).
    auto except = make_intrusive<ASTColumnsExceptTransformer>();
    for (const auto & copy : copies)
        except->children.push_back(make_intrusive<ASTIdentifier>(copy.second));
    auto transformers = make_intrusive<ASTColumnsTransformerList>();
    transformers->children.push_back(except);
    auto asterisk = make_intrusive<ASTAsterisk>();
    asterisk->transformers = transformers;
    asterisk->children.push_back(transformers);
    layer.select.push_back(asterisk);

    for (const auto & [source, target] : copies)
    {
        auto expression = make_intrusive<ASTIdentifier>(source);
        expression->setAlias(target);
        layer.select.push_back(expression);
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

    /// Both the source and the target are excluded from `*`: the source is renamed away,
    /// and an already-existing target column is overwritten rather than duplicated.
    /// The non-strict EXCEPT is a no-op for names that do not exist.
    auto except = make_intrusive<ASTColumnsExceptTransformer>();
    std::unordered_set<String> excluded;
    for (const auto & [source, target] : renames)
        for (const auto & name : {source, target})
            if (excluded.insert(name).second)
                except->children.push_back(make_intrusive<ASTIdentifier>(name));

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
    String rank_name;
    std::vector<String> partition_fields;
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
            lex.nextToken();
            if (lex.isKeyword("as"))
                lex.nextToken();
            rank_name = "rank";
            if (!lex.isQueryPartTrailer() && !lex.isKeyword("offset") && !lex.isKeyword("limit") && !lex.isKeyword("partition"))
                rank_name = parseFieldName();
        }
        else if (lex.isKeyword("partition"))
        {
            lex.nextToken();
            if (lex.isKeyword("by"))
                lex.nextToken();
            if (!lex.isKeyword("("))
                throwSyntaxError("missing '(' after 'partition by'");
            lex.nextToken();
            while (!lex.isKeyword(")"))
            {
                partition_fields.push_back(parseFieldName());
                if (lex.isKeyword(","))
                    lex.nextToken();
                else if (!lex.isKeyword(")"))
                    throwSyntaxError(fmt::format("unexpected token {}; expecting ',' or ')'", lex.getToken()));
            }
            lex.nextToken();
            if (partition_fields.empty())
                throwSyntaxError("missing fields in 'partition by' of the sort pipe");
        }
        else
        {
            break;
        }
    }

    if (!partition_fields.empty() && !sort_limit)
        throwSyntaxError("missing 'limit' for 'partition by' in the sort pipe");
    if ((!rank_name.empty() || !partition_fields.empty()) && (!has_fields || fields.empty()))
        throwNotImplemented("The 'rank' and 'partition by' clauses of a sort pipe without explicit 'by' fields");

    applySortWithExtras(layer, fields, global_desc, partition_fields, rank_name, sort_limit, sort_offset);
}

void LogsQLParser::applySortWithExtras(
    Layer & layer,
    const std::vector<SortField> & fields,
    bool global_desc,
    const std::vector<String> & partition_fields,
    const String & rank_name,
    std::optional<UInt64> sort_limit,
    std::optional<UInt64> sort_offset)
{
    /// Aggregation and projection layers are wrapped into a subquery: sorting by an output field
    /// which shadows a source column (e.g. the bucketed `_time` of `stats by (_time:1d)`)
    /// is unambiguous only across a subquery boundary.
    wrapLayerIf(layer,
        !layer.order_by.empty() || layer.order_by_all || layer.limit.has_value() || layer.offset.has_value()
        || layer.has_aggregation || layer.has_projection);

    auto make_order_elements = [&]
    {
        ASTs elements;
        for (const auto & field : fields)
            elements.push_back(makeOrderByElement(sortKeyExpr(layer, field.name), field.is_desc != global_desc));
        return elements;
    };

    if (!partition_fields.empty())
    {
        /// Top-N per partition: an inner layer computes the per-partition row number,
        /// the outer layer filters by it and restores the ordering.
        auto window = make_intrusive<ASTWindowDefinition>();
        auto partition_list = make_intrusive<ASTExpressionList>();
        for (const auto & field : partition_fields)
            partition_list->children.push_back(columnExpr(field));
        window->partition_by = partition_list;
        window->children.push_back(window->partition_by);
        auto order_list = make_intrusive<ASTExpressionList>();
        order_list->children = make_order_elements();
        window->order_by = order_list;
        window->children.push_back(window->order_by);

        auto row_number = makeASTFunction("row_number");
        row_number->setIsWindowFunction(true);
        row_number->window_definition = window;
        row_number->children.push_back(window);

        layer.select.push_back(make_intrusive<ASTAsterisk>());
        row_number->setAlias("__logsql_rank");
        layer.select.push_back(row_number);
        layer.has_projection = true;
        wrapLayer(layer);

        auto rank_column = make_intrusive<ASTIdentifier>("__logsql_rank");
        ASTs conditions;
        UInt64 offset = sort_offset.value_or(0);
        if (offset > 0)
            conditions.push_back(makeASTFunction("greater", rank_column, makeUInt64Literal(offset)));
        conditions.push_back(makeASTFunction("lessOrEquals", rank_column->clone(), makeUInt64Literal(offset + *sort_limit)));
        layer.where = conditions.size() == 1 ? conditions[0] : [&]
        {
            auto conjunction = makeASTFunction("and");
            conjunction->arguments->children = std::move(conditions);
            return ASTPtr(conjunction);
        }();

        auto except = make_intrusive<ASTColumnsExceptTransformer>();
        except->children.push_back(make_intrusive<ASTIdentifier>("__logsql_rank"));
        /// The rank overwrites an existing same-named column (see `appendComputedColumn`).
        if (!rank_name.empty())
            except->children.push_back(make_intrusive<ASTIdentifier>(columnName(rank_name)));
        auto transformers = make_intrusive<ASTColumnsTransformerList>();
        transformers->children.push_back(except);
        auto asterisk = make_intrusive<ASTAsterisk>();
        asterisk->transformers = transformers;
        asterisk->children.push_back(transformers);
        layer.select = {asterisk};
        if (!rank_name.empty())
        {
            auto rank_alias = make_intrusive<ASTIdentifier>("__logsql_rank");
            rank_alias->setAlias(columnName(rank_name));
            layer.select.push_back(rank_alias);
        }
        layer.has_projection = true;

        /// Partitions in the order of their keys, sorted rows inside each partition.
        for (const auto & field : partition_fields)
            layer.order_by.push_back(makeOrderByElement(columnExpr(field), false));
        for (auto & element : make_order_elements())
            layer.order_by.push_back(element);
        return;
    }

    if (!rank_name.empty())
    {
        /// The 1-based position in the sort order, computed before offset and limit are applied.
        auto window = make_intrusive<ASTWindowDefinition>();
        auto order_list = make_intrusive<ASTExpressionList>();
        order_list->children = make_order_elements();
        window->order_by = order_list;
        window->children.push_back(window->order_by);

        auto row_number = makeASTFunction("row_number");
        row_number->setIsWindowFunction(true);
        row_number->window_definition = window;
        row_number->children.push_back(window);

        /// The rank overwrites an existing same-named column (see `appendComputedColumn`).
        auto except = make_intrusive<ASTColumnsExceptTransformer>();
        except->children.push_back(make_intrusive<ASTIdentifier>(columnName(rank_name)));
        auto transformers = make_intrusive<ASTColumnsTransformerList>();
        transformers->children.push_back(except);
        auto asterisk = make_intrusive<ASTAsterisk>();
        asterisk->transformers = transformers;
        asterisk->children.push_back(transformers);
        layer.select.push_back(asterisk);
        row_number->setAlias(columnName(rank_name));
        layer.select.push_back(row_number);
        layer.has_projection = true;
    }

    if (!fields.empty())
        layer.order_by = make_order_elements();
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

    UInt64 limit = 1;
    if (!lex.isQuoted() && isNumberPrefix(lex.getToken()))
        limit = parseLimitValue();

    if (lex.isKeyword("by"))
        lex.nextToken();

    std::vector<SortField> fields;
    if (lex.isKeyword("("))
        fields = parseSortFields();

    std::vector<String> partition_fields;
    String rank_name;
    while (true)
    {
        if (lex.isKeyword("partition"))
        {
            lex.nextToken();
            if (lex.isKeyword("by"))
                lex.nextToken();
            if (!lex.isKeyword("("))
                throwSyntaxError("missing '(' after 'partition by'");
            lex.nextToken();
            while (!lex.isKeyword(")"))
            {
                partition_fields.push_back(parseFieldName());
                if (lex.isKeyword(","))
                    lex.nextToken();
                else if (!lex.isKeyword(")"))
                    throwSyntaxError(fmt::format("unexpected token {}; expecting ',' or ')'", lex.getToken()));
            }
            lex.nextToken();
        }
        else if (lex.isKeyword("rank"))
        {
            lex.nextToken();
            if (lex.isKeyword("as"))
                lex.nextToken();
            rank_name = "rank";
            if (!lex.isQueryPartTrailer() && !lex.isKeyword("partition"))
                rank_name = parseFieldName();
        }
        else
        {
            break;
        }
    }

    if ((!rank_name.empty() || !partition_fields.empty()) && fields.empty())
        throwNotImplemented("The 'rank' and 'partition by' clauses of a first/last pipe without explicit 'by' fields");

    std::vector<SortField> effective = fields;
    if (is_last)
        for (auto & field : effective)
            field.is_desc = !field.is_desc;

    if (fields.empty())
    {
        wrapLayerIf(layer,
            !layer.order_by.empty() || layer.order_by_all || layer.limit.has_value() || layer.offset.has_value()
            || layer.has_aggregation || layer.has_projection);
        layer.order_by_all = true;
        layer.order_by.push_back(makeOrderByElement(make_intrusive<ASTIdentifier>("all"), is_last));
        layer.limit = limit;
        return;
    }

    applySortWithExtras(layer, effective, /*global_desc=*/ false, partition_fields, rank_name, limit, /*sort_offset=*/ {});
}

/// ---- Helpers for the expression pipes ----

ASTPtr LogsQLParser::parseParenthesizedFilter()
{
    if (!lex.isKeyword("("))
        throwSyntaxError("missing '(' with the filter");
    lex.nextToken();

    ASTPtr condition;
    if (!lex.isKeyword(")"))
    {
        condition = parseFilterOr("");
        if (lex.isKeyword(";"))
            lex.nextToken();
    }
    if (!lex.isKeyword(")"))
        throwSyntaxError(fmt::format("missing ')' after the filter; got {}", lex.getToken()));
    lex.nextToken();
    return condition;
}

ASTPtr LogsQLParser::parseOptionalIfCondition()
{
    if (!lex.isKeyword("if"))
        return nullptr;
    lex.nextToken();
    return parseParenthesizedFilter();
}

void LogsQLParser::applyColumnReplacement(Layer & layer, const String & column, ASTPtr expression)
{
    wrapLayerIf(layer, !layer.select.empty());

    auto replacement = make_intrusive<ASTColumnsReplaceTransformer::Replacement>();
    replacement->name = column;
    replacement->children.push_back(std::move(expression));

    /// The replacement expression transforms the original column in place, so the column
    /// must exist. The strict transformer reports a missing column explicitly, while a
    /// non-strict one would silently skip the transformation.
    auto replace = make_intrusive<ASTColumnsReplaceTransformer>();
    replace->is_strict = true;
    replace->children.push_back(replacement);

    auto transformers = make_intrusive<ASTColumnsTransformerList>();
    transformers->children.push_back(replace);

    auto asterisk = make_intrusive<ASTAsterisk>();
    asterisk->transformers = transformers;
    asterisk->children.push_back(transformers);

    layer.select = {asterisk};
    layer.has_projection = true;
}

void LogsQLParser::appendComputedColumn(Layer & layer, ASTPtr expression, const String & alias)
{
    wrapLayerIf(layer, !layer.select.empty());

    /// `SELECT * EXCEPT (alias), expression AS alias`: as in LogsQL, setting a field
    /// overwrites it when the column already exists (a plain `SELECT *, expression AS alias`
    /// would produce two same-named columns, which the old analyzer rejects when their
    /// types differ), and creates the field otherwise (the EXCEPT transformer is not
    /// strict, so a missing name is fine).
    auto except = make_intrusive<ASTColumnsExceptTransformer>();
    except->children.push_back(make_intrusive<ASTIdentifier>(alias));
    auto transformers = make_intrusive<ASTColumnsTransformerList>();
    transformers->children.push_back(except);
    auto asterisk = make_intrusive<ASTAsterisk>();
    asterisk->transformers = transformers;
    asterisk->children.push_back(transformers);
    layer.select.push_back(asterisk);

    expression->setAlias(alias);
    layer.select.push_back(expression);
    layer.has_projection = true;
}

/// ---- Expression pipes ----

void LogsQLParser::parsePipeLen(Layer & layer)
{
    lex.nextToken();

    bool with_parens = lex.isKeyword("(");
    if (with_parens)
        lex.nextToken();
    String field = parseFieldName();
    if (with_parens)
    {
        if (!lex.isKeyword(")"))
            throwSyntaxError(fmt::format("missing ')' in the len pipe; got {}", lex.getToken()));
        lex.nextToken();
    }

    String result_name = "_msg";
    if (!lex.isQueryPartTrailer())
    {
        if (lex.isKeyword("as"))
            lex.nextToken();
        result_name = parseFieldName();
    }

    appendComputedColumn(layer, makeASTFunction("length", columnExpr(field)), columnName(result_name));
}

void LogsQLParser::parsePipeCoalesce(Layer & layer)
{
    lex.nextToken();
    if (!lex.isKeyword("("))
        throwSyntaxError("missing '(' after 'coalesce'");
    lex.nextToken();

    std::vector<String> fields;
    while (!lex.isKeyword(")"))
    {
        fields.push_back(parseFieldName());
        if (!lex.skippedSpace() && lex.isKeyword("*"))
            throwNotImplemented("Field name prefixes in the coalesce pipe");
        if (lex.isKeyword(","))
            lex.nextToken();
        else if (!lex.isKeyword(")"))
            throwSyntaxError(fmt::format("unexpected token {} in the coalesce pipe; expecting ',' or ')'", lex.getToken()));
    }
    lex.nextToken();
    if (fields.empty())
        throwSyntaxError("missing fields in the coalesce pipe");

    ASTPtr default_value = makeStringLiteral("");
    if (lex.isKeyword("default"))
    {
        lex.nextToken();
        bool quoted = lex.isQuoted();
        default_value = makeValueLiteral(lex.nextCompoundToken(), quoted);
    }

    String result_name = "_msg";
    if (!lex.isQueryPartTrailer())
    {
        if (lex.isKeyword("as"))
            lex.nextToken();
        result_name = parseFieldName();
    }

    /// The first non-empty value: if(f1 != '', f1, if(f2 != '', f2, ..., default)).
    ASTPtr expression = default_value;
    for (auto it = fields.rbegin(); it != fields.rend(); ++it)
    {
        expression = makeASTFunction("if",
            makeASTFunction("notEquals", columnExpr(*it), makeStringLiteral("")),
            columnExpr(*it),
            expression);
    }

    appendComputedColumn(layer, expression, columnName(result_name));
}

void LogsQLParser::parsePipeDecolorize(Layer & layer)
{
    lex.nextToken();

    String field = "_msg";
    if (!lex.isQueryPartTrailer())
        field = parseFieldName();

    /// Strips ANSI CSI escape sequences, like VictoriaLogs does.
    ASTPtr expression = makeASTFunction("replaceRegexpAll",
        columnExpr(field), makeStringLiteral("\x1b\\[[\x30-\x3f]*[\x20-\x2f]*[\x30-\x7e]?"), makeStringLiteral(""));
    applyColumnReplacement(layer, columnName(field), expression);
}

void LogsQLParser::parsePipeSplit(Layer & layer)
{
    lex.nextToken();

    String separator = lex.getToken();
    if (!lex.isQuoted())
        separator = lex.nextCompoundToken();
    else
        lex.nextToken();

    String source = "_msg";
    if (lex.isKeyword("from"))
    {
        lex.nextToken();
        source = parseFieldName();
    }
    else if (!lex.isQueryPartTrailer() && !lex.isKeyword("as"))
    {
        source = parseFieldName();
    }
    String target = source;
    if (lex.isKeyword("as"))
    {
        lex.nextToken();
        target = parseFieldName();
    }
    else if (!lex.isQueryPartTrailer())
    {
        target = parseFieldName();
    }

    /// The result is a JSON array of strings, as in VictoriaLogs.
    ASTPtr expression = makeASTFunction("toJSONString",
        makeASTFunction("splitByString", makeStringLiteral(separator), columnExpr(source)));

    if (columnName(target) == columnName(source))
        applyColumnReplacement(layer, columnName(target), expression);
    else
        appendComputedColumn(layer, expression, columnName(target));
}

void LogsQLParser::parsePipeUnpackWords(Layer & layer)
{
    lex.nextToken();

    String source = "_msg";
    if (lex.isKeyword("from"))
    {
        lex.nextToken();
        source = parseFieldName();
    }
    else if (!lex.isQueryPartTrailer() && !lex.isKeyword("as") && !lex.isKeyword("drop_duplicates"))
    {
        source = parseFieldName();
    }
    String target = source;
    if (lex.isKeyword("as"))
    {
        lex.nextToken();
        target = parseFieldName();
    }
    else if (!lex.isQueryPartTrailer() && !lex.isKeyword("drop_duplicates"))
    {
        target = parseFieldName();
    }
    bool drop_duplicates = false;
    if (lex.isKeyword("drop_duplicates"))
    {
        drop_duplicates = true;
        lex.nextToken();
    }

    ASTPtr words = makeASTFunction("extractAll", columnExpr(source), makeStringLiteral("[0-9A-Za-z_]+"));
    if (drop_duplicates)
        words = makeASTFunction("arrayDistinct", words);
    ASTPtr expression = makeASTFunction("toJSONString", words);

    if (columnName(target) == columnName(source))
        applyColumnReplacement(layer, columnName(target), expression);
    else
        appendComputedColumn(layer, expression, columnName(target));
}

void LogsQLParser::parsePipeTimeAdd(Layer & layer)
{
    lex.nextToken();

    String text = lex.nextCompoundToken();
    auto duration = tryParseDuration(text);
    if (!duration)
        throwSyntaxError(fmt::format("cannot parse {} as a duration in the time_add pipe", text));

    String field = "_time";
    if (lex.isKeyword("at"))
    {
        lex.nextToken();
        field = parseFieldName();
    }

    ASTPtr expression = shiftTime(columnExpr(field), -*duration);
    applyColumnReplacement(layer, columnName(field), expression);
}

void LogsQLParser::parsePipeSample(Layer & layer)
{
    lex.nextToken();
    UInt64 step = parseLimitValue();
    if (step == 0)
        throwSyntaxError("the sample step must be a positive integer");

    /// Returns roughly 1/N of the input rows.
    ASTPtr condition = makeASTFunction("equals",
        makeASTFunction("modulo", makeASTFunction("rand"), makeUInt64Literal(step)),
        makeUInt64Literal(0));

    if (layer.has_projection || layer.has_aggregation || layer.limit || layer.offset)
    {
        wrapLayer(layer);
        layer.where = condition;
    }
    else if (!layer.where)
        layer.where = condition;
    else
        layer.where = makeASTFunction("and", layer.where, condition);
}

void LogsQLParser::parsePipeGenerateSequence(Layer & layer)
{
    lex.nextToken();
    UInt64 count = parseLimitValue();

    /// Replaces the input with `count` rows whose message field is 0 .. count-1.
    auto select = make_intrusive<ASTSelectQuery>();
    auto select_list = make_intrusive<ASTExpressionList>();
    ASTPtr value = makeASTFunction("toString", make_intrusive<ASTIdentifier>("number"));
    value->setAlias(context.msg_column);
    select_list->children.push_back(value);
    select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list));

    auto table_expression = make_intrusive<ASTTableExpression>();
    auto table_function = makeASTFunction("numbers", makeUInt64Literal(count));
    table_expression->table_function = table_function;
    table_expression->children.push_back(table_function);

    auto table_element = make_intrusive<ASTTablesInSelectQueryElement>();
    table_element->table_expression = table_expression;
    table_element->children.push_back(table_expression);

    auto tables = make_intrusive<ASTTablesInSelectQuery>();
    tables->children.push_back(table_element);
    select->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));

    auto list_of_selects = make_intrusive<ASTExpressionList>();
    list_of_selects->children.push_back(select);
    auto select_with_union = make_intrusive<ASTSelectWithUnionQuery>();
    select_with_union->list_of_selects = list_of_selects;
    select_with_union->children.push_back(list_of_selects);

    layer = Layer{};
    layer.source_subquery = select_with_union;
}

void LogsQLParser::parsePipeFieldValues(Layer & layer)
{
    lex.nextToken();

    bool with_parens = lex.isKeyword("(");
    if (with_parens)
        lex.nextToken();
    String field = parseFieldName();
    if (!lex.skippedSpace() && lex.isKeyword("*"))
        throwNotImplemented("Field name prefixes in the field_values pipe");
    if (with_parens)
    {
        if (!lex.isKeyword(")"))
            throwSyntaxError(fmt::format("missing ')' in the field_values pipe; got {}", lex.getToken()));
        lex.nextToken();
    }

    if (lex.isKeyword("filter"))
        throwNotImplemented("The 'filter' clause of the field_values pipe");

    std::optional<UInt64> limit;
    if (lex.isKeyword("limit"))
    {
        lex.nextToken();
        limit = parseLimitValue();
    }

    wrapLayerIf(layer,
        layer.has_projection || layer.has_aggregation || layer.limit.has_value() || layer.offset.has_value()
        || !layer.order_by.empty() || layer.order_by_all);

    layer.select.push_back(columnExpr(field));
    layer.group_by.push_back(columnExpr(field));
    auto hits = makeAggregate("count", {}, nullptr);
    hits->setAlias("hits");
    layer.select.push_back(hits);
    layer.order_by.push_back(makeOrderByElement(columnExpr(field), /*is_desc=*/ false));
    if (limit)
        layer.limit = limit;
    layer.has_aggregation = true;
    layer.has_projection = true;
}

void LogsQLParser::parsePipeJSONArrayLen(Layer & layer)
{
    lex.nextToken();

    bool with_parens = lex.isKeyword("(");
    if (with_parens)
        lex.nextToken();
    String field = parseFieldName();
    if (with_parens)
    {
        if (!lex.isKeyword(")"))
            throwSyntaxError(fmt::format("missing ')' in the json_array_len pipe; got {}", lex.getToken()));
        lex.nextToken();
    }

    String result_name = "_msg";
    if (!lex.isQueryPartTrailer())
    {
        if (lex.isKeyword("as"))
            lex.nextToken();
        result_name = parseFieldName();
    }

    appendComputedColumn(layer,
        makeASTFunction("length", makeASTFunction("JSONExtractArrayRaw", columnExpr(field))),
        columnName(result_name));
}

void LogsQLParser::parsePipeReplace(Layer & layer, bool is_regexp)
{
    lex.nextToken();

    ASTPtr condition = parseOptionalIfCondition();

    if (!lex.isKeyword("("))
        throwSyntaxError("missing '(' with the ('old', 'new') arguments of the replace pipe");
    std::vector<String> args = parseArgsInParens();
    if (args.size() != 2)
        throwSyntaxError(fmt::format("unexpected number of arguments of the replace pipe; got {}; want 2", args.size()));

    String field = "_msg";
    if (lex.isKeyword("at"))
    {
        lex.nextToken();
        field = parseFieldName();
    }

    std::optional<UInt64> limit;
    if (lex.isKeyword("limit"))
    {
        lex.nextToken();
        limit = parseLimitValue();
    }
    if (limit && *limit > 1)
        throwNotImplemented("A replacement limit greater than 1 in the replace pipe");

    if (is_regexp)
    {
        re2::RE2 checked_regexp(args[0], re2::RE2::Quiet);
        if (!checked_regexp.ok())
            throwSyntaxError(fmt::format("invalid regexp {} in the replace_regexp pipe: {}", args[0], checked_regexp.error()));
    }

    String function = is_regexp
        ? (limit == 1 ? "replaceRegexpOne" : "replaceRegexpAll")
        : (limit == 1 ? "replaceOne" : "replaceAll");

    ASTPtr expression = makeASTFunction(function, columnExpr(field), makeStringLiteral(args[0]), makeStringLiteral(args[1]));
    if (condition)
        expression = makeASTFunction("if", condition, expression, columnExpr(field));

    applyColumnReplacement(layer, columnName(field), expression);
}

void LogsQLParser::parsePipeUnion(Layer & layer)
{
    lex.nextToken();
    if (lex.isKeyword("rows"))
        throwNotImplemented("The static rows(...) form of the union pipe");
    if (!lex.isKeyword("("))
        throwSyntaxError("missing '(' with the subquery of the union pipe");
    lex.nextToken();

    Layer other = parseQuery(/*is_subquery=*/ true);
    lex.nextToken();  /// Skip ')'.

    wrapLayer(layer);

    /// layer.source_subquery is a fresh ASTSelectWithUnionQuery with a single select - append the other one.
    auto * select_with_union = layer.source_subquery->as<ASTSelectWithUnionQuery>();
    select_with_union->list_of_selects->children.push_back(buildSelect(other));
    select_with_union->union_mode = SelectUnionMode::UNION_ALL;
    select_with_union->list_of_modes = {SelectUnionMode::UNION_ALL};
}

void LogsQLParser::parsePipeHash(Layer & layer)
{
    lex.nextToken();

    bool with_parens = lex.isKeyword("(");
    if (with_parens)
        lex.nextToken();
    String field = parseFieldName();
    if (with_parens)
    {
        if (!lex.isKeyword(")"))
            throwSyntaxError(fmt::format("missing ')' in the hash pipe; got {}", lex.getToken()));
        lex.nextToken();
    }

    String result_name = "_msg";
    if (!lex.isQueryPartTrailer())
    {
        if (lex.isKeyword("as"))
            lex.nextToken();
        result_name = parseFieldName();
    }

    /// VictoriaLogs computes xxHash64 of the value, truncated to 53 bits.
    ASTPtr expression = makeASTFunction("bitAnd",
        makeASTFunction("xxHash64", columnExpr(field)),
        makeUInt64Literal((1ULL << 53) - 1));
    appendComputedColumn(layer, expression, columnName(result_name));
}

void LogsQLParser::parsePipeUnroll(Layer & layer)
{
    lex.nextToken();

    if (lex.isKeyword("if"))
        throwNotImplemented("The 'if' clause of the unroll pipe");

    if (lex.isKeyword("by"))
        lex.nextToken();

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
                throwSyntaxError(fmt::format("unexpected token {} in the unroll pipe; expecting ',' or ')'", lex.getToken()));
        }
        lex.nextToken();
    }
    else
    {
        while (true)
        {
            fields.push_back(parseFieldName());
            if (!lex.isKeyword(","))
                break;
            lex.nextToken();
        }
    }
    if (fields.empty())
        throwSyntaxError("missing fields in the unroll pipe");

    /// Each listed field holds a JSON array; a row is unrolled into max(array lengths) rows
    /// (shorter arrays are padded with ""), or into a single row with empty fields
    /// when no field holds a non-empty array.
    auto array_of = [&](const String & field)
    {
        return makeASTFunction("JSONExtractArrayRaw", columnExpr(field));
    };

    ASTPtr rows_count = makeASTFunction("length", array_of(fields[0]));
    for (size_t i = 1; i < fields.size(); ++i)
        rows_count = makeASTFunction("greatest", rows_count, makeASTFunction("length", array_of(fields[i])));

    /// The inner layer adds the unrolling index; arrayJoin must appear exactly once.
    /// A pending `limit` / `offset` must also stay in an inner layer: it counts the
    /// source rows, while `arrayJoin` in the same SELECT would count the unrolled rows.
    wrapLayerIf(layer, !layer.select.empty() || layer.limit.has_value() || layer.offset.has_value());
    ASTPtr index = makeASTFunction("arrayJoin",
        makeASTFunction("range", makeASTFunction("greatest", makeUInt64Literal(1), rows_count)));
    layer.select.push_back(make_intrusive<ASTAsterisk>());
    index->setAlias("__logsql_unroll_index");
    layer.select.push_back(index);
    layer.has_projection = true;
    wrapLayer(layer);

    /// The outer layer replaces the fields with the decoded array elements and drops the index.
    auto except = make_intrusive<ASTColumnsExceptTransformer>();
    except->children.push_back(make_intrusive<ASTIdentifier>("__logsql_unroll_index"));

    auto transformers = make_intrusive<ASTColumnsTransformerList>();
    transformers->children.push_back(except);

    /// The unrolled fields are read from the source table, so they must exist;
    /// the strict transformer reports a missing column explicitly.
    auto replace = make_intrusive<ASTColumnsReplaceTransformer>();
    replace->is_strict = true;
    for (const auto & field : fields)
    {
        /// String elements are decoded; other elements keep their JSON form.
        ASTPtr element = makeASTFunction("arrayElement", array_of(field),
            makeASTFunction("plus", make_intrusive<ASTIdentifier>("__logsql_unroll_index"), makeUInt64Literal(1)));
        ASTPtr decoded = makeASTFunction("if",
            makeASTFunction("startsWith", element, makeStringLiteral("\"")),
            makeASTFunction("JSONExtractString", element->clone()),
            element->clone());

        auto replacement = make_intrusive<ASTColumnsReplaceTransformer::Replacement>();
        replacement->name = columnName(field);
        replacement->children.push_back(decoded);
        replace->children.push_back(replacement);
    }
    transformers->children.push_back(replace);

    auto asterisk = make_intrusive<ASTAsterisk>();
    asterisk->transformers = transformers;
    asterisk->children.push_back(transformers);
    layer.select = {asterisk};
    layer.has_projection = true;
}

void LogsQLParser::parsePipePack(Layer & layer, bool is_logfmt)
{
    lex.nextToken();

    if (!lex.isKeyword("fields"))
        throwNotImplemented(fmt::format("The {} pipe without an explicit 'fields' list", is_logfmt ? "pack_logfmt" : "pack_json"));
    lex.nextToken();

    if (!lex.isKeyword("("))
        throwSyntaxError("missing '(' after 'fields'");
    lex.nextToken();
    std::vector<String> fields;
    while (!lex.isKeyword(")"))
    {
        fields.push_back(parseFieldName());
        if (!lex.skippedSpace() && lex.isKeyword("*"))
            throwNotImplemented("Field name prefixes in the pack pipes");
        if (lex.isKeyword(","))
            lex.nextToken();
        else if (!lex.isKeyword(")"))
            throwSyntaxError(fmt::format("unexpected token {}; expecting ',' or ')'", lex.getToken()));
    }
    lex.nextToken();
    if (fields.empty())
        throwSyntaxError("missing fields in the pack pipe");

    String result_name = "_msg";
    if (!lex.isQueryPartTrailer())
    {
        if (lex.isKeyword("as"))
            lex.nextToken();
        result_name = parseFieldName();
    }

    ASTPtr expression;
    if (is_logfmt)
    {
        /// Space-separated name=value pairs. A value containing a space, '=', a quote,
        /// a backslash, or a control character would make the output ambiguous or invalid
        /// logfmt, so it is quoted and escaped as a JSON string - the same quoting
        /// the unpack_logfmt pipe decodes, so such values round-trip.
        auto concat = makeASTFunction("concat");
        for (size_t i = 0; i < fields.size(); ++i)
        {
            ASTPtr value = makeASTFunction("toString", columnExpr(fields[i]));
            ASTPtr needs_quoting = makeASTFunction("match", value->clone(), makeStringLiteral(R"re([ ="\\[:cntrl:]])re"));
            value = makeASTFunction("if", needs_quoting, makeASTFunction("toJSONString", value->clone()), value);
            concat->arguments->children.push_back(makeStringLiteral((i == 0 ? "" : " ") + fields[i] + "="));
            concat->arguments->children.push_back(value);
        }
        expression = concat;
    }
    else
    {
        /// A JSON object; fields with empty values are omitted, as in VictoriaLogs.
        auto entries = makeASTFunction("map");
        for (const auto & field : fields)
        {
            entries->arguments->children.push_back(makeStringLiteral(field));
            entries->arguments->children.push_back(makeASTFunction("toString", columnExpr(field)));
        }
        auto key_argument = make_intrusive<ASTIdentifier>("__logsql_key");
        auto value_argument = make_intrusive<ASTIdentifier>("__logsql_value");
        auto lambda = makeASTFunction("lambda",
            makeASTFunction("tuple", key_argument, value_argument),
            makeASTFunction("notEquals", value_argument->clone(), makeStringLiteral("")));
        expression = makeASTFunction("toJSONString", makeASTFunction("mapFilter", lambda, entries));
    }

    appendComputedColumn(layer, expression, columnName(result_name));
}

/// ---- The stats pipe ----

void LogsQLParser::parsePipeStats(Layer & layer, bool need_keyword)
{
    if (need_keyword)
        lex.nextToken();

    current_stats_time_bucket_ns.reset();
    current_stats_time_bucket_seconds_expr = nullptr;
    current_stats_time_bucket_is_calendar = false;

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

                std::optional<String> bucket_offset;
                if (lex.isKeyword("offset"))
                {
                    lex.nextToken();
                    bucket_offset = lex.nextCompoundToken();
                }

                if (name == "_time")
                {
                    static const std::unordered_map<String, String> named_steps = {
                        {"nanosecond", "toIntervalNanosecond"}, {"microsecond", "toIntervalMicrosecond"},
                        {"millisecond", "toIntervalMillisecond"}, {"second", "toIntervalSecond"},
                        {"minute", "toIntervalMinute"}, {"hour", "toIntervalHour"}, {"day", "toIntervalDay"},
                        {"week", "toIntervalWeek"}, {"month", "toIntervalMonth"}, {"year", "toIntervalYear"}};

                    static const std::unordered_map<String, Int64> named_step_ns = {
                        {"nanosecond", 1LL}, {"microsecond", 1000LL}, {"millisecond", 1000000LL},
                        {"second", 1000000000LL}, {"minute", 60000000000LL}, {"hour", 3600000000000LL}};

                    ASTPtr interval;
                    if (auto it = named_steps.find(Poco::toLower(bucket)); it != named_steps.end())
                    {
                        String step_name = Poco::toLower(bucket);
                        interval = makeASTFunction(it->second, makeUInt64Literal(1));
                        if (auto it_ns = named_step_ns.find(step_name); it_ns != named_step_ns.end())
                        {
                            current_stats_time_bucket_ns = it_ns->second;
                        }
                        else if (step_name == "day")
                        {
                            /// A civil day is not fixed-length (the day of a DST transition is
                            /// an hour shorter or longer), so the rate() denominator is the
                            /// length of each bucket, computed at runtime from the bucket key
                            /// of the group. This works for the day step because its key keeps
                            /// the DateTime type (and so the timezone) of the column; the week
                            /// step yields a timezone-less Date and is treated as a calendar
                            /// bucket instead.
                            ASTPtr bucket_start = make_intrusive<ASTIdentifier>(columnName(name));
                            ASTPtr bucket_end = makeASTFunction("plus", bucket_start->clone(), makeASTFunction(it->second, makeUInt64Literal(1)));
                            current_stats_time_bucket_seconds_expr = makeTimeRangeSecondsExpr(std::move(bucket_start), std::move(bucket_end));
                        }
                        else
                        {
                            current_stats_time_bucket_is_calendar = true;
                        }
                    }
                    else if (auto duration = tryParseDuration(bucket))
                    {
                        interval = makeIntervalAST(*duration);
                        current_stats_time_bucket_ns = duration;
                    }
                    else
                        throwSyntaxError(fmt::format("cannot parse the time bucket step {}", bucket));

                    if (bucket_offset)
                    {
                        /// A timezone-like shift of the bucket boundaries.
                        auto offset_duration = tryParseDuration(*bucket_offset);
                        if (!offset_duration)
                            throwSyntaxError(fmt::format("cannot parse the bucket offset {} for the _time field", *bucket_offset));
                        key = shiftTime(
                            makeASTFunction("toStartOfInterval", shiftTime(columnExpr(name), *offset_duration), interval),
                            -*offset_duration);
                    }
                    else
                    {
                        key = makeASTFunction("toStartOfInterval", columnExpr(name), interval);
                    }
                }
                else if (bucket.starts_with('/'))
                {
                    /// An IPv4 subnet bucket: by (ip:/24).
                    UInt32 bits = 0;
                    auto [end, ec] = std::from_chars(bucket.data() + 1, bucket.data() + bucket.size(), bits);
                    if (ec != std::errc() || end != bucket.data() + bucket.size() || bits > 32)
                        throwSyntaxError(fmt::format("cannot parse the IPv4 subnet bucket {} for the field {}", bucket, name));
                    if (bucket_offset)
                        throwSyntaxError("bucket offsets are not applicable to IPv4 subnet buckets");
                    UInt32 mask = bits == 0 ? 0 : (~UInt32(0) << (32 - bits));
                    key = makeASTFunction("IPv4NumToString",
                        makeASTFunction("bitAnd",
                            makeASTFunction("IPv4StringToNumOrDefault", columnExpr(name)),
                            makeUInt64Literal(mask)));
                }
                else
                {
                    auto step = tryParseNumberField(bucket);
                    auto to_float = [](const Field & field)
                    {
                        if (field.getType() == Field::Types::UInt64)
                            return static_cast<Float64>(field.safeGet<UInt64>());
                        if (field.getType() == Field::Types::Int64)
                            return static_cast<Float64>(field.safeGet<Int64>());
                        return field.safeGet<Float64>();
                    };
                    if (!step || to_float(*step) <= 0)
                        throwSyntaxError(fmt::format("cannot parse the bucket step {} for the field {}", bucket, name));

                    std::optional<Field> offset_value;
                    if (bucket_offset)
                    {
                        offset_value = tryParseNumberField(*bucket_offset);
                        if (!offset_value)
                            throwSyntaxError(fmt::format("cannot parse the bucket offset {} for the field {}", *bucket_offset, name));
                    }

                    auto is_integral = [](const Field & field)
                    {
                        return field.getType() == Field::Types::UInt64 || field.getType() == Field::Types::Int64;
                    };

                    if (is_integral(*step) && (!offset_value || is_integral(*offset_value)))
                    {
                        /// An integral step keeps integer values exact across the full 64-bit
                        /// range: `floor(value / step) * step` would round the value through
                        /// `Float64` and merge adjacent buckets above 2^53. The step and the
                        /// offset are widened to `Int128` so that the arithmetic cannot wrap
                        /// around, and `positiveModulo` keeps the floor semantics for
                        /// negative values.
                        auto make_int128 = [](const Field & field)
                        {
                            return makeASTFunction("toInt128", make_intrusive<ASTLiteral>(field));
                        };
                        ASTPtr value = columnExpr(name);
                        if (offset_value)
                            value = makeASTFunction("minus", value, make_int128(*offset_value));
                        key = makeASTFunction("minus", value,
                            makeASTFunction("positiveModulo", value->clone(), make_int128(*step)));
                        if (offset_value)
                            key = makeASTFunction("plus", key, make_int128(*offset_value));
                    }
                    else
                    {
                        ASTPtr step_literal = make_intrusive<ASTLiteral>(Field(to_float(*step)));
                        ASTPtr offset_literal;
                        if (offset_value)
                            offset_literal = make_intrusive<ASTLiteral>(Field(to_float(*offset_value)));
                        ASTPtr value = columnExpr(name);
                        if (offset_literal)
                            value = makeASTFunction("minus", value, offset_literal);
                        key = makeASTFunction("multiply",
                            makeASTFunction("floor", makeASTFunction("divide", value, step_literal)),
                            step_literal->clone());
                        if (offset_literal)
                            key = makeASTFunction("plus", key, offset_literal->clone());
                    }
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
        {
            /// func() switch(case (<filter>) as name1, ..., default as nameN):
            /// each row is counted into the first matching case.
            lex.nextToken();
            if (!lex.isKeyword("("))
                throwSyntaxError("missing '(' after 'switch'");
            lex.nextToken();

            /// Each case is an independent conditional aggregate (a row may be counted into several
            /// matching cases), and `default` matches the rows matching none of the `case` filters.
            struct SwitchCase
            {
                ASTPtr condition;
                bool is_default = false;
                String result_name;
            };
            std::vector<SwitchCase> switch_cases;
            bool seen_default = false;
            while (!lex.isKeyword(")"))
            {
                SwitchCase entry;
                if (lex.isKeyword("case") || lex.isKeyword("if"))
                {
                    lex.nextToken();
                    entry.condition = parseParenthesizedFilter();
                }
                else if (lex.isKeyword("default"))
                {
                    if (seen_default)
                        throwSyntaxError("switch(...) cannot contain more than one 'default'");
                    seen_default = true;
                    entry.is_default = true;
                    lex.nextToken();
                }
                else
                {
                    throwSyntaxError(fmt::format("unexpected token {} inside switch(...); expecting 'case' or 'default'", lex.getToken()));
                }

                if (lex.isKeyword("as"))
                    lex.nextToken();
                entry.result_name = parseFieldName();
                switch_cases.push_back(std::move(entry));

                if (lex.isKeyword(","))
                    lex.nextToken();
                else if (!lex.isKeyword(")"))
                    throwSyntaxError(fmt::format("unexpected token {} inside switch(...); expecting ',' or ')'", lex.getToken()));
            }
            lex.nextToken();
            if (switch_cases.empty())
                throwSyntaxError("switch(...) must contain at least a single 'case' or 'default'");

            for (const auto & entry : switch_cases)
            {
                ASTPtr combined = entry.condition;
                if (entry.is_default)
                {
                    /// The rows matching none of the case filters (all rows if there are no cases).
                    ASTs case_conditions;
                    for (const auto & other : switch_cases)
                        if (other.condition)
                            case_conditions.push_back(other.condition->clone());
                    if (case_conditions.size() == 1)
                        combined = makeASTFunction("not", case_conditions[0]);
                    else if (!case_conditions.empty())
                    {
                        auto disjunction = makeASTFunction("or");
                        disjunction->arguments->children = std::move(case_conditions);
                        combined = makeASTFunction("not", disjunction);
                    }
                }
                ASTPtr case_aggregate = stats_func.build(combined ? combined->clone() : nullptr);
                case_aggregate->setAlias(entry.result_name);
                aggregates.push_back(case_aggregate);
            }

            if (lex.isQueryPartTrailer())
                break;
            if (!lex.isKeyword(","))
                throwSyntaxError(fmt::format("unexpected token {} after a stats function; expecting ',', '|', ';' or ')'", lex.getToken()));
            lex.nextToken();
            continue;
        }

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

    if (name == "histogram")
        throwNotImplemented("The stats function 'histogram' (its VictoriaMetrics bucket format has no ClickHouse equivalent)");

    bool wildcard = false;
    std::vector<String> args = parseArgsInParens(&wildcard);

    std::optional<UInt64> limit;
    if ((name == "json_values") && (lex.isKeyword("sort") || lex.isKeyword("order")))
        throwNotImplemented("The 'sort' clause of the json_values stats function");
    if ((name == "count_uniq" || name == "uniq_values" || name == "values" || name == "json_values") && lex.isKeyword("limit"))
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
        if (args.size() == 1)
        {
            ASTPtr column = columnExpr(args[0]);
            return {canonical, [column](ASTPtr condition) { return makeAggregate("count", {column->clone()}, condition); }};
        }

        /// count(f1, ..., fN): rows where at least one of the fields has a value.
        auto any_present = makeASTFunction("or");
        for (const auto & arg : args)
            any_present->arguments->children.push_back(makeASTFunction("isNotNull", columnExpr(arg)));
        ASTPtr present = any_present;
        return {canonical, [present](ASTPtr condition)
        {
            ASTPtr full_condition = condition ? makeASTFunction("and", present->clone(), condition) : present->clone();
            return makeAggregate("count", {}, full_condition);
        }};
    }

    if (name == "rate" || name == "rate_sum")
    {
        /// The number of matching logs (or the sum of the field) per second of the query time range.
        /// The range is taken from the query's `_time` filter; without a known range,
        /// `rate()` is the same as `count()` and `rate_sum(x)` the same as `sum(x)`.
        if (name == "rate" && !args.empty())
            throwSyntaxError("rate() does not accept arguments");
        if (name == "rate_sum" && (wildcard || args.size() != 1))
            throwNotImplemented("rate_sum() over all fields or over multiple fields");
        /// Week, month and year buckets have variable lengths (weeks because of DST
        /// transitions), so a constant denominator would be wrong; their bucket keys
        /// are timezone-less Dates, so the length cannot be derived at runtime either.
        if (current_stats_time_bucket_is_calendar)
            throwNotImplemented(fmt::format("{}() over week, month, or year buckets", name));

        ASTPtr column = args.empty() ? nullptr : columnExpr(args[0]);
        std::optional<Float64> range_seconds;
        ASTPtr range_seconds_expr;
        bool range_may_be_empty = false;
        /// A `_time` bucket of the same stats pipe takes precedence over the whole query range.
        if (current_stats_time_bucket_ns && *current_stats_time_bucket_ns > 0)
            range_seconds = static_cast<Float64>(*current_stats_time_bucket_ns) / 1e9;
        else if (current_stats_time_bucket_seconds_expr)
            range_seconds_expr = current_stats_time_bucket_seconds_expr;
        else if (query_time_lower_bound_ns && query_time_upper_bound_ns)
        {
            /// The intersection of all top-level `_time` filters, known at parse time.
            if (*query_time_upper_bound_ns > *query_time_lower_bound_ns)
                range_seconds = static_cast<Float64>(*query_time_upper_bound_ns - *query_time_lower_bound_ns) / 1e9;
        }
        else if (query_time_lower_bound_expr && query_time_upper_bound_expr)
        {
            /// Unlike a bucket length, the intersection of the `_time` filters may turn out
            /// empty at runtime; the guard below then falls back to the plain aggregate,
            /// the same way the parse-time branch above does.
            range_seconds_expr = makeTimeRangeSecondsExpr(query_time_lower_bound_expr, query_time_upper_bound_expr);
            range_may_be_empty = true;
        }
        return {canonical, [column, range_seconds, range_seconds_expr, range_may_be_empty](ASTPtr condition)
        {
            ASTPtr result = column
                ? makeAggregate("sum", {column->clone()}, condition)
                : makeAggregate("count", {}, condition);
            if (range_seconds)
            {
                result = makeASTFunction("divide", result, make_intrusive<ASTLiteral>(Field(*range_seconds)));
            }
            else if (range_seconds_expr)
            {
                ASTPtr divided = makeASTFunction("divide", result->clone(), range_seconds_expr->clone());
                if (range_may_be_empty)
                    result = makeASTFunction("if",
                        makeASTFunction("greater", range_seconds_expr->clone(), make_intrusive<ASTLiteral>(Field(0.0))),
                        std::move(divided), std::move(result));
                else
                    result = std::move(divided);
            }
            return result;
        }};
    }

    if (wildcard || args.empty())
        throwNotImplemented(fmt::format("The stats function {}() over all fields", name));

    for (const auto & arg : args)
        if (arg.ends_with('*'))
            throwNotImplemented("Field name prefixes in stats functions");

    if (name == "row_any" || name == "row_min" || name == "row_max" || name == "json_values")
    {
        /// These return rows as JSON objects. The set of fields must be explicit,
        /// because the schema is not known at parse time; empty values are omitted.
        std::vector<String> row_fields = args;
        String tracked;
        if (name == "row_min" || name == "row_max")
        {
            tracked = args[0];
            row_fields.erase(row_fields.begin());
        }
        if (row_fields.empty())
            throwNotImplemented(fmt::format("The stats function {}() without an explicit list of returned fields", name));

        auto entries = makeASTFunction("map");
        for (const auto & field : row_fields)
        {
            entries->arguments->children.push_back(makeStringLiteral(field));
            entries->arguments->children.push_back(makeASTFunction("toString", columnExpr(field)));
        }
        auto key_argument = make_intrusive<ASTIdentifier>("__logsql_key");
        auto value_argument = make_intrusive<ASTIdentifier>("__logsql_value");
        auto filter_lambda = makeASTFunction("lambda",
            makeASTFunction("tuple", key_argument, value_argument),
            makeASTFunction("notEquals", value_argument->clone(), makeStringLiteral("")));
        ASTPtr row_json = makeASTFunction("toJSONString", makeASTFunction("mapFilter", filter_lambda, entries));

        if (name == "row_any")
            return {canonical, [row_json](ASTPtr condition) { return makeAggregate("any", {row_json->clone()}, condition); }};
        if (name == "json_values")
        {
            return {canonical, [row_json, limit](ASTPtr condition)
            {
                ASTs parameters;
                if (limit)
                    parameters.push_back(makeUInt64Literal(*limit));
                return makeAggregate("groupArray", {row_json->clone()}, condition, parameters);
            }};
        }

        ASTPtr tracked_column = columnExpr(tracked);
        String aggregate_name = name == "row_min" ? "argMin" : "argMax";
        return {canonical, [aggregate_name, row_json, tracked_column](ASTPtr condition)
        {
            return makeAggregate(aggregate_name, {row_json->clone(), tracked_column->clone()}, condition);
        }};
    }

    if (name == "field_min" || name == "field_max")
    {
        /// field_max(maxField, field) returns the value of `field` from the row with the maximum `maxField`.
        if (args.size() != 2)
            throwSyntaxError(fmt::format("{}() requires two arguments: ({}Field, field)", name, name == "field_min" ? "min" : "max"));
        ASTPtr tracked = columnExpr(args[0]);
        ASTPtr returned = columnExpr(args[1]);
        String aggregate_name = name == "field_min" ? "argMin" : "argMax";
        return {canonical, [aggregate_name, returned, tracked](ASTPtr condition)
        {
            return makeAggregate(aggregate_name, {returned->clone(), tracked->clone()}, condition);
        }};
    }

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
            ASTPtr full_condition = condition ? makeASTFunction("and", all_empty->clone(), condition) : all_empty->clone();
            return makeAggregate("count", {}, full_condition);
        }};
    }

    if (name == "count_uniq" || name == "count_uniq_hash")
    {
        ASTs columns = column_args(args);
        String aggregate_name = name == "count_uniq" ? "uniqExact" : "uniq";
        return {canonical, [aggregate_name, columns, limit](ASTPtr condition)
        {
            ASTs cloned;
            cloned.reserve(columns.size());
            for (const auto & column : columns)
                cloned.push_back(column->clone());
            ASTPtr result = makeAggregate(aggregate_name, std::move(cloned), condition);
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
            return makeAggregate("quantile", {column->clone()}, condition, {make_intrusive<ASTLiteral>(Field(phi_value))});
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
            ASTPtr result = makeAggregate(aggregate_name, {column->clone()}, condition, parameters);
            if (sorted)
                result = makeASTFunction("arraySort", result);
            return result;
        }};
    }

    if (name == "sum_len")
    {
        /// The total length of the values across all listed fields.
        ASTPtr total = makeASTFunction("length", columnExpr(args[0]));
        for (size_t i = 1; i < args.size(); ++i)
            total = makeASTFunction("plus", total, makeASTFunction("length", columnExpr(args[i])));
        return {canonical, [total](ASTPtr condition) { return makeAggregate("sum", {total->clone()}, condition); }};
    }

    if (name == "min" || name == "max")
    {
        /// Over multiple fields, the extremum is taken across all their values.
        ASTPtr value = columnExpr(args[0]);
        if (args.size() > 1)
        {
            auto pooled = makeASTFunction(name == "min" ? "least" : "greatest");
            for (const auto & arg : args)
                pooled->arguments->children.push_back(columnExpr(arg));
            value = pooled;
        }
        return {canonical, [aggregate_name = name, value](ASTPtr condition) { return makeAggregate(aggregate_name, {value->clone()}, condition); }};
    }

    if (name == "sum" || name == "avg")
    {
        std::vector<ASTPtr> columns;
        columns.reserve(args.size());
        for (const auto & arg : args)
            columns.push_back(columnExpr(arg));
        bool is_avg = name == "avg";
        return {canonical, [columns, is_avg](ASTPtr condition) -> ASTPtr
        {
            if (columns.size() == 1 && !is_avg)
                return makeAggregate("sum", {columns[0]->clone()}, condition);
            if (columns.size() == 1)
                return makeAggregate("avg", {columns[0]->clone()}, condition);

            if (!is_avg)
            {
                /// The values of all listed fields are pooled together.
                ASTPtr total = makeAggregate("sum", {columns[0]->clone()}, condition);
                for (size_t i = 1; i < columns.size(); ++i)
                    total = makeASTFunction("plus", total, makeAggregate("sum", {columns[i]->clone()}, condition ? condition->clone() : ASTPtr{}));
                return total;
            }

            /// The pooled average is the sum of the present values divided by their number:
            /// a NULL slot contributes to neither, so it does not dilute the average, and
            /// `sum` of a fully-NULL column (which is NULL) does not poison the numerator.
            ASTPtr total;
            ASTPtr values;
            for (const auto & column : columns)
            {
                ASTPtr column_sum = makeASTFunction("ifNull",
                    makeAggregate("sum", {column->clone()}, condition ? condition->clone() : ASTPtr{}),
                    makeUInt64Literal(0));
                ASTPtr column_values = makeAggregate("count", {column->clone()}, condition ? condition->clone() : ASTPtr{});
                total = total ? makeASTFunction("plus", total, column_sum) : column_sum;
                values = values ? makeASTFunction("plus", values, column_values) : column_values;
            }
            /// With no present values at all the average is NULL, like for a single field.
            return makeASTFunction("if",
                makeASTFunction("greater", values->clone(), makeUInt64Literal(0)),
                makeASTFunction("divide", total, values),
                make_intrusive<ASTLiteral>(Field()));
        }};
    }

    static const std::unordered_map<String, String> simple_aggregates = {
        {"any", "any"}, {"median", "median"}, {"stddev", "stddevPop"}};

    if (auto it = simple_aggregates.find(name); it != simple_aggregates.end())
    {
        if (args.size() > 1)
            throwNotImplemented(fmt::format("{}() over multiple fields", name));
        ASTPtr column = columnExpr(args[0]);
        String aggregate_name = it->second;
        return {canonical, [aggregate_name, column](ASTPtr condition) { return makeAggregate(aggregate_name, {column->clone()}, condition); }};
    }

    throwNotImplemented(fmt::format("The stats function '{}'", name));
}

/// ---- The math pipe ----

void LogsQLParser::parsePipeMath(Layer & layer)
{
    lex.nextToken();

    std::vector<ASTPtr> computed;
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
        computed.push_back(expression);

        if (!lex.isKeyword(","))
            break;
        lex.nextToken();
    }

    wrapLayerIf(layer, !layer.select.empty());

    /// `* EXCEPT (...)` overwrites existing same-named columns instead of duplicating them
    /// (see the comment in `appendComputedColumn`).
    auto except = make_intrusive<ASTColumnsExceptTransformer>();
    for (const auto & expression : computed)
        except->children.push_back(make_intrusive<ASTIdentifier>(expression->tryGetAlias()));
    auto transformers = make_intrusive<ASTColumnsTransformerList>();
    transformers->children.push_back(except);
    auto asterisk = make_intrusive<ASTAsterisk>();
    asterisk->transformers = transformers;
    asterisk->children.push_back(transformers);
    layer.select.push_back(asterisk);

    for (const auto & expression : computed)
        layer.select.push_back(expression);

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
            /// `a default b` returns b when a is not a finite number (NaN or infinity).
            left = makeASTFunction("if", makeASTFunction("isFinite", left->clone()), left, right);
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
        if (auto number = tryParseNumberField(text))
            return make_intrusive<ASTLiteral>(*number);
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
        /// Integral literals keep their exact 64-bit value; only genuinely
        /// fractional values (and inf/nan) become Float64.
        if (auto number = tryParseNumberField(token))
            return make_intrusive<ASTLiteral>(*number);
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
        if (layer.join_subquery)
            subquery->setAlias("__logsql_left");
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

    if (layer.join_subquery)
    {
        auto join = make_intrusive<ASTTableJoin>();
        join->kind = layer.join_inner ? JoinKind::Inner : JoinKind::Left;
        join->strictness = JoinStrictness::All;
        auto using_list = make_intrusive<ASTExpressionList>();
        for (const auto & name : layer.join_using)
            using_list->children.push_back(make_intrusive<ASTIdentifier>(name));
        join->using_expression_list = using_list;
        join->children.push_back(using_list);

        auto right_expression = make_intrusive<ASTTableExpression>();
        auto right_subquery = make_intrusive<ASTSubquery>(layer.join_subquery);
        right_subquery->setAlias("__logsql_right");
        right_expression->subquery = right_subquery;
        right_expression->children.push_back(right_subquery);

        auto right_element = make_intrusive<ASTTablesInSelectQueryElement>();
        right_element->table_join = join;
        right_element->children.push_back(join);
        right_element->table_expression = right_expression;
        right_element->children.push_back(right_expression);

        tables->children.push_back(right_element);
    }

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
