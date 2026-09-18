#include <TableFunctions/TableFunctionTraceView.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/BlockIO.h>
#include <Storages/StorageValues.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/quoteString.h>

#include <fmt/format.h>

#include <array>
#include <functional>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Arguments are positional (trace_id, timeline_width, cluster), and any of them can instead
/// be given as `name = value`. `query_id` exists only in the named form: a query id cannot be
/// told apart from a trace id positionally, because server-generated query ids are UUIDs too.
constexpr std::array<std::string_view, 3> positional_names{"trace_id", "timeline_width", "cluster"};

/// `name = value` -> (name, value); anything else -> (the name of the position, the argument).
std::pair<String, ASTPtr> splitNamedArgument(const ASTPtr & arg, size_t position)
{
    const auto * equals = arg->as<ASTFunction>();
    if (!equals || equals->name != "equals")
        return {String(positional_names[position]), arg};

    const auto * identifier = equals->arguments->children.at(0)->as<ASTIdentifier>();
    if (!identifier)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table function 'traceView': the left side of a named argument must be an identifier, got '{}'",
            arg->formatForErrorMessage());

    return {identifier->name(), equals->arguments->children.at(1)};
}

UUID parseTraceId(const ASTPtr & value)
{
    const auto * literal = value->as<ASTLiteral>();

    if (literal && literal->value.getType() == Field::Types::UUID)
        return literal->value.safeGet<UUID>();

    if (literal && literal->value.getType() == Field::Types::String)
    {
        const auto & text = literal->value.safeGet<String>();
        ReadBufferFromString buf(text);
        UUID uuid;
        readUUIDText(uuid, buf);
        if (!buf.eof())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Table function 'traceView': cannot parse '{}' as a trace_id UUID", text);
        return uuid;
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Table function 'traceView' requires a String or UUID trace_id, got '{}'", value->formatForErrorMessage());
}

}

void TableFunctionTraceView::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (!function || !function->arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments", getName());

    const auto & args = function->arguments->children;
    if (args.empty() || args.size() > positional_names.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table function '{}' requires 1 to {} arguments: trace_id|query_id [, timeline_width [, cluster]], got {}",
            getName(), positional_names.size(), args.size());

    bool has_trace_id = false;

    /// One parser per parameter; each owns the validation of its value.
    using Parser = std::function<void(const ASTPtr &)>;
    const UnorderedMapWithMemoryTracking<std::string_view, Parser> parsers
    {
        {"trace_id", [&](const ASTPtr & value)
        {
            trace_id = parseTraceId(value);
            has_trace_id = true;
        }},
        {"query_id", [&](const ASTPtr & value)
        {
            query_id = checkAndGetLiteralArgument<String>(value, "query_id");
            if (query_id.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}': query_id must not be empty", getName());
        }},
        {"timeline_width", [&](const ASTPtr & value)
        {
            timeline_width = checkAndGetLiteralArgument<UInt64>(value, "timeline_width");
            if (timeline_width == 0 || timeline_width > max_timeline_width)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Table function '{}': timeline_width must be in [1, {}], got {}", getName(), max_timeline_width, timeline_width);
        }},
        {"cluster", [&](const ASTPtr & value)
        {
            cluster = checkAndGetLiteralArgument<String>(value, "cluster");
            /// Fail early with a clear error instead of a confusing one from the internal query.
            context->getCluster(cluster);
        }},
    };

    UnorderedSetWithMemoryTracking<String> seen;
    for (size_t i = 0; i < args.size(); ++i)
    {
        auto [param_name, value] = splitNamedArgument(args[i], i);

        auto parser = parsers.find(param_name);
        if (parser == parsers.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Table function '{}': unknown argument '{}'; expected trace_id, query_id, timeline_width or cluster",
                getName(), param_name);

        if (!seen.insert(param_name).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}': argument '{}' is given twice", getName(), param_name);

        parser->second(evaluateConstantExpressionOrIdentifierAsLiteral(value, context));
    }

    if (has_trace_id == !query_id.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table function '{}' requires exactly one of trace_id and query_id", getName());
}

ColumnsDescription TableFunctionTraceView::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    return ColumnsDescription{
        {"span", std::make_shared<DataTypeString>()},
        {"kind", std::make_shared<DataTypeString>()},
        {"status", std::make_shared<DataTypeString>()},
        {"status_message", std::make_shared<DataTypeString>()},
        {"start_offset_us", std::make_shared<DataTypeUInt64>()},
        {"duration_us", std::make_shared<DataTypeUInt64>()},
        {"duration", std::make_shared<DataTypeString>()},
        {"self_pct", std::make_shared<DataTypeFloat64>()},
        {"timeline", std::make_shared<DataTypeString>()},
        {"attribute", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())},
    };
}

namespace
{

Block pullMonoBlock(QueryPipeline & pipeline)
{
    if (!pipeline.pulling())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected pulling pipeline");

    PullingPipelineExecutor pulling_executor(pipeline);
    Blocks blocks;
    while (true)
    {
        Block block;
        if (pulling_executor.pull(block))
            blocks.push_back(std::move(block));
        else
            break;
    }

    return concatenateBlocks(blocks);
}

String formatDurationUs(UInt64 us)
{
    if (us >= 1000000)
        return fmt::format("{:.2f} s", static_cast<double>(us) / 1e6);
    if (us >= 1000)
        return fmt::format("{:.2f} ms", static_cast<double>(us) / 1e3);
    return fmt::format("{} us", us);
}

Block executeInternalQuery(const String & query, ContextPtr context)
{
    auto query_context = Context::createCopy(context);
    query_context->makeQueryContext();
    /// The copied context carries the enclosing query's id; the internal query must
    /// register under its own, or the process list rejects it as already running.
    query_context->setCurrentQueryId("");
    auto io = executeQuery(query, query_context, QueryFlags{.internal = true}).second;
    return pullMonoBlock(io.pipeline);
}

/// The spans of one trace, ordered by (start_time_us, span_id) so that sibling order is deterministic.
Block loadSpans(const String & source, const UUID & trace_id, ContextPtr context)
{
    /// LowCardinality columns are converted to plain types so that the rendering code and the
    /// declared structure of the `attribute` result column need no special cases.
    Block spans = executeInternalQuery(
        fmt::format(
            "SELECT span_id, parent_span_id, toString(operation_name) AS operation_name,"
            " toString(kind) AS kind, toString(status_code) AS status, toString(status_message) AS status_message,"
            " start_time_us, finish_time_us,"
            " toString(attribute['clickhouse.shard_num']) AS shard_num,"
            " CAST(attribute, 'Map(String, String)') AS attribute"
            " FROM {} WHERE trace_id = toUUID('{}') ORDER BY start_time_us, span_id",
            source, toString(trace_id)),
        context);

    if (spans.rows() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "No spans found for trace_id '{}'. Spans are flushed to the log in background:"
            " run SYSTEM FLUSH LOGS opentelemetry_span_log and retry", toString(trace_id));

    return spans;
}

/// The columns of `loadSpans` by name; `rows` is the number of spans.
struct SpanColumns
{
    explicit SpanColumns(const Block & spans)
        : rows(spans.rows())
        , span_id(*spans.getByName("span_id").column)
        , parent_span_id(*spans.getByName("parent_span_id").column)
        , operation_name(*spans.getByName("operation_name").column)
        , kind(*spans.getByName("kind").column)
        , status(*spans.getByName("status").column)
        , status_message(*spans.getByName("status_message").column)
        , start_time_us(*spans.getByName("start_time_us").column)
        , finish_time_us(*spans.getByName("finish_time_us").column)
        , shard_num(*spans.getByName("shard_num").column)
        , attribute(*spans.getByName("attribute").column)
    {
    }

    size_t rows;
    const IColumn & span_id;
    const IColumn & parent_span_id;
    const IColumn & operation_name;
    const IColumn & kind;
    const IColumn & status;
    const IColumn & status_message;
    const IColumn & start_time_us;
    const IColumn & finish_time_us;
    const IColumn & shard_num;
    const IColumn & attribute;

    UInt64 start(size_t row) const { return start_time_us.getUInt(row); }
    /// A finish before the start is a clock artifact: such a span is treated as instantaneous.
    UInt64 finish(size_t row) const { return std::max(finish_time_us.getUInt(row), start(row)); }
};

/// The parent links of a trace resolved into a forest of row indexes.
struct SpanForest
{
    /// The children of every row, in row order.
    VectorWithMemoryTracking<VectorWithMemoryTracking<size_t>> children;
    /// The rows whose parent is not part of the trace.
    VectorWithMemoryTracking<size_t> roots;
    /// The rows repeating a span id seen earlier; they are not part of the forest.
    VectorWithMemoryTracking<bool> duplicate;
};

SpanForest buildSpanForest(const SpanColumns & spans)
{
    SpanForest forest{.children = {}, .roots = {}, .duplicate = {}};
    forest.children.resize(spans.rows);
    forest.duplicate.resize(spans.rows, false);

    /// A span id read twice keeps its first row only: with `cluster`, replicas that resolve
    /// to the same node return the same log rows.
    UnorderedMapWithMemoryTracking<UInt64, size_t> row_by_span_id;
    for (size_t i = 0; i < spans.rows; ++i)
        if (!row_by_span_id.emplace(spans.span_id.getUInt(i), i).second)
            forest.duplicate[i] = true;

    for (size_t i = 0; i < spans.rows; ++i)
    {
        if (forest.duplicate[i])
            continue;

        auto parent = row_by_span_id.find(spans.parent_span_id.getUInt(i));
        /// A span whose parent is not part of the trace is shown as a root: this keeps
        /// subtrees visible when their parent span was lost or not instrumented.
        if (parent == row_by_span_id.end() || parent->second == i)
            forest.roots.push_back(i);
        else
            forest.children[parent->second].push_back(i);
    }

    return forest;
}

/// The time span of the whole trace: the earliest start and the duration up to the latest finish.
struct TraceBounds
{
    UInt64 start;
    /// At least 1: it is the divisor of every relative measure.
    UInt64 duration;
};

TraceBounds traceBounds(const SpanColumns & spans)
{
    UInt64 start = std::numeric_limits<UInt64>::max();
    UInt64 finish = 0;
    for (size_t i = 0; i < spans.rows; ++i)
    {
        start = std::min(start, spans.start(i));
        finish = std::max(finish, spans.finish_time_us.getUInt(i));
    }
    return {.start = start, .duration = std::max<UInt64>(1, finish - start)};
}

/// The time a span spent by itself: its duration minus the union of its children's intervals.
/// Children can overlap (e.g. parallel shard reads), so plain summing would overcount.
UInt64 selfTimeUs(const SpanColumns & spans, const SpanForest & forest, size_t row)
{
    const UInt64 start = spans.start(row);
    const UInt64 finish = spans.finish(row);

    UInt64 covered = 0;
    UInt64 cursor = start;
    for (size_t child : forest.children[row])
    {
        const UInt64 child_start = std::clamp(spans.start(child), cursor, finish);
        const UInt64 child_finish = std::clamp(spans.finish_time_us.getUInt(child), cursor, finish);
        if (child_finish > child_start)
            covered += child_finish - child_start;
        cursor = std::max(cursor, child_finish);
    }

    const UInt64 duration = finish - start;
    return duration - std::min(duration, covered);
}

/// A bar of `width` cells: its position is the span's offset within the trace, its length is
/// proportional to the span's duration, and at least one cell so that every span is visible.
String renderTimeline(UInt64 offset, UInt64 duration, const TraceBounds & trace, UInt64 width)
{
    const size_t pad = std::min(static_cast<size_t>(offset * width / trace.duration), static_cast<size_t>(width - 1));
    const size_t len = std::min(
        std::max<size_t>(1, static_cast<size_t>((duration * width + trace.duration / 2) / trace.duration)),
        static_cast<size_t>(width - pad));

    String timeline;
    for (size_t i = 0; i < pad; ++i)
        timeline += "·";
    for (size_t i = 0; i < len; ++i)
        timeline += "█";
    for (size_t i = pad + len; i < width; ++i)
        timeline += "·";
    return timeline;
}

/// The `span` column: the tree drawing, the operation name, and the shard number when the span has one.
String renderSpanText(const SpanColumns & spans, size_t row, const String & tree_prefix)
{
    String text = tree_prefix + String(spans.operation_name.getDataAt(row));
    if (String shard_num = String(spans.shard_num.getDataAt(row)); !shard_num.empty())
        text += fmt::format("  shard {}", shard_num);
    return text;
}

/// Visits every span of the forest once, depth-first, children in row order. Duplicates are
/// skipped. The roots go first; whatever is left afterwards is visited as a root too, so that no span of the trace is lost.
/// `visit(row, prefix, connector)` receives the tree drawing of the row:
/// `prefix` is the indentation inherited from the ancestors, `connector` the branch to the row itself.
template <typename Visit>
void walkDepthFirst(const SpanForest & forest, Visit && visit)
{
    const size_t rows = forest.children.size();

    /// Explicit DFS stack: a trace is unbounded in depth, recursion is not.
    struct Frame
    {
        size_t row;
        String prefix;
        String connector;
    };
    VectorWithMemoryTracking<Frame> stack;
    for (size_t i = forest.roots.size(); i > 0; --i)
        stack.push_back({forest.roots[i - 1], "", ""});

    /// Every span is visited once. The guard also ends the walk of a cycle in the parent links.
    VectorWithMemoryTracking<bool> visited(rows, false);

    size_t next_unvisited = 0;
    auto push_next_unvisited = [&]
    {
        for (; next_unvisited < rows; ++next_unvisited)
        {
            if (!visited[next_unvisited] && !forest.duplicate[next_unvisited])
            {
                stack.push_back({next_unvisited, "", ""});
                return true;
            }
        }
        return false;
    };

    while (!stack.empty() || push_next_unvisited())
    {
        auto [row, prefix, connector] = std::move(stack.back());
        stack.pop_back();

        if (visited[row])
            continue;
        visited[row] = true;

        visit(row, prefix, connector);

        String child_prefix = prefix;
        if (connector == "├─ ")
            child_prefix += "│  ";
        else if (connector == "└─ ")
            child_prefix += "   ";

        const auto & child_rows = forest.children[row];
        for (size_t i = child_rows.size(); i > 0; --i)
            stack.push_back({child_rows[i - 1], child_prefix, i == child_rows.size() ? "└─ " : "├─ "});
    }
}

/// The result rows, one per span in depth-first order, in the columns of `result_structure`.
Block renderTrace(const SpanColumns & spans, UInt64 timeline_width, const NamesAndTypesList & result_structure)
{
    const SpanForest forest = buildSpanForest(spans);
    const TraceBounds trace = traceBounds(spans);

    MutableColumns res;
    for (const auto & column : result_structure)
        res.push_back(column.type->createColumn());

    walkDepthFirst(forest, [&](size_t row, const String & prefix, const String & connector)
    {
        const UInt64 offset = spans.start(row) - trace.start;
        const UInt64 duration = spans.finish(row) - spans.start(row);
        const UInt64 self = selfTimeUs(spans, forest, row);

        res[0]->insert(renderSpanText(spans, row, prefix + connector));
        res[1]->insert(String(spans.kind.getDataAt(row)));
        res[2]->insert(String(spans.status.getDataAt(row)));
        res[3]->insert(String(spans.status_message.getDataAt(row)));
        res[4]->insert(offset);
        res[5]->insert(duration);
        res[6]->insert(formatDurationUs(duration));
        res[7]->insert(100.0 * static_cast<Float64>(self) / static_cast<Float64>(trace.duration));
        res[8]->insert(renderTimeline(offset, duration, trace, timeline_width));
        res[9]->insertFrom(spans.attribute, row);
    });

    Block block;
    size_t i = 0;
    for (const auto & column : result_structure)
        block.insert({std::move(res[i++]), column.type, column.name});
    return block;
}

}

String TableFunctionTraceView::spanLogSource() const
{
    /// In a cluster the spans of each node are written to that node's own span log,
    /// so an explicitly given cluster reads the log of every replica.
    if (cluster.empty())
        return "system.opentelemetry_span_log";
    return fmt::format("clusterAllReplicas({}, system.opentelemetry_span_log)", quoteString(cluster));
}

UUID TableFunctionTraceView::resolveTraceId(const String & source, ContextPtr context) const
{
    if (query_id.empty())
        return trace_id;

    /// The query's root span ('query') carries its id in the `clickhouse.query_id`
    /// attribute. A custom query id can be reused across runs, so several traces may
    /// match: take the most recent one - that is what a debugging session wants.
    Block lookup = executeInternalQuery(
        fmt::format(
            "SELECT trace_id FROM {} WHERE operation_name = 'query'"
            " AND attribute['clickhouse.query_id'] = {} ORDER BY finish_time_us DESC LIMIT 1",
            source, quoteString(query_id)),
        context);

    if (lookup.rows() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "No trace found for query_id '{}'. The query must run with tracing enabled"
            " (a traceparent or opentelemetry_start_trace_probability); spans are flushed"
            " to the log in background: run SYSTEM FLUSH LOGS opentelemetry_span_log and retry",
            query_id);

    return (*lookup.getByPosition(0).column)[0].safeGet<UUID>();
}

StoragePtr TableFunctionTraceView::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    const String source = spanLogSource();
    const UUID effective_trace_id = resolveTraceId(source, context);
    const Block spans = loadSpans(source, effective_trace_id, context);

    const ColumnsDescription structure = getActualTableStructure(context, is_insert_query);
    Block rendered = renderTrace(SpanColumns(spans), timeline_width, structure.getAllPhysical());

    auto storage = std::make_shared<StorageValues>(StorageID(getDatabaseName(), table_name), structure, std::move(rendered));
    storage->startup();
    return storage;
}

void registerTableFunctionTraceView(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionTraceView>({
        .description = R"(
Renders the spans of one OpenTelemetry trace from `system.opentelemetry_span_log` as a call tree with a timeline.

Returns one row per span of the trace, in depth-first tree order:
- `span` - the operation name indented by its depth in the call tree, with `clickhouse.shard_num` appended when present;
- `kind`, `status`, `status_message` - from the span log;
- `start_offset_us`, `duration_us`, `duration` - timing relative to the trace start;
- `self_pct` - the span's own time (its duration minus the union of its children's intervals) as a percentage of the whole trace: a phase that is slow by itself, not merely a container of a slow child;
- `timeline` - a fixed-width bar: the position is the span's start offset within the trace, the length is proportional to its duration;
- `attribute` - the span attributes.

Arguments: `trace_id` (String or UUID), optional `timeline_width` (default 40, at most 1024), optional `cluster` - read `clusterAllReplicas(cluster, system.opentelemetry_span_log)` instead of the local span log, because in a cluster every node writes its spans to its own log. Arguments can also be passed by name (`name = value`). Instead of `trace_id`, the named argument `query_id` selects the most recent trace of that query - named only, because a server-generated query id is itself a UUID and cannot be told apart from a trace id positionally: `traceView(query_id = '<query id>')`.

Spans are flushed to the log in background: run `SYSTEM FLUSH LOGS opentelemetry_span_log` first.
Example:
[example:trace_view]
)",
        .examples = {{"trace_view", "SELECT span, status, duration, timeline FROM traceView('5c9e4a3b-2f61-4d6e-8b7a-90c1d2e3f405')", ""}},
        .category = FunctionDocumentation::Category::TableFunction});
}

}
