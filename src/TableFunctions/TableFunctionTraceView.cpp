#include <TableFunctions/TableFunctionTraceView.h>

#include <Columns/ColumnBLOB.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageValues.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/getStructureOfRemoteTable.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/quoteString.h>

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <functional>
#include <memory>

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
/// `since` and `until` are named only as well: a date has no natural position.
constexpr std::array<std::string_view, 3> positional_names{"trace_id", "timeline_width", "cluster"};

struct Argument
{
    String name;
    ASTPtr value;
    bool named;
};

/// `name = value` -> (name, value).
Argument splitNamedArgument(const ASTPtr & arg, size_t position, bool after_named)
{
    const auto * equals = arg->as<ASTFunction>();
    /// A malformed `equals` (wrong arity) is not a named argument: it is reported as a bad positional value.
    if (!equals || equals->name != "equals" || !equals->arguments || equals->arguments->children.size() != 2)
    {
        if (after_named)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Table function 'traceView': positional argument {} after a named argument; give it by name (`name = value`)",
                arg->formatForErrorMessage());
        if (position >= positional_names.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Table function 'traceView': argument {} must be given by name (`name = value`), got '{}'",
                position + 1, arg->formatForErrorMessage());
        return {.name = String(positional_names[position]), .value = arg, .named = false};
    }

    const auto * identifier = equals->arguments->children[0]->as<ASTIdentifier>();
    if (!identifier)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table function 'traceView': the left side of a named argument must be an identifier, got '{}'",
            arg->formatForErrorMessage());

    return {.name = identifier->name(), .value = equals->arguments->children[1], .named = true};
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
        if (!tryReadUUIDText(uuid, buf) || !buf.eof())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Table function 'traceView': cannot parse '{}' as a trace_id UUID", text);
        return uuid;
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Table function 'traceView' requires a String or UUID trace_id, got '{}'", value->formatForErrorMessage());
}

/// A `YYYY-MM-DD` date, returned as written once it is known to parse.
String parseDate(const ASTPtr & value, const String & arg_name)
{
    const auto text = checkAndGetLiteralArgument<String>(value, arg_name);
    ReadBufferFromString buf(text);
    LocalDate date;
    if (!tryReadDateText(date, buf) || !buf.eof())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table function 'traceView': cannot parse '{}' as a YYYY-MM-DD date for {}", text, arg_name);
    return text;
}

}

void TableFunctionTraceView::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (!function || !function->arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments", getName());

    const auto & args = function->arguments->children;
    if (args.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table function '{}' requires arguments: trace_id [, timeline_width [, cluster]] [, since = '...'] [, until = '...'],"
            " or query_id = '...' in place of trace_id",
            getName());

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
        {"since", [&](const ASTPtr & value) { since = parseDate(value, "since"); }},
        {"until", [&](const ASTPtr & value) { until = parseDate(value, "until"); }},
    };

    UnorderedSetWithMemoryTracking<String> seen;
    bool seen_named = false;
    for (size_t i = 0; i < args.size(); ++i)
    {
        const Argument argument = splitNamedArgument(args[i], i, seen_named);
        seen_named |= argument.named;

        auto parser = parsers.find(argument.name);
        if (parser == parsers.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Table function '{}': unknown argument '{}'; expected trace_id, query_id, timeline_width, cluster, since or until",
                getName(), argument.name);

        if (!seen.insert(argument.name).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}': argument '{}' is given twice", getName(), argument.name);

        parser->second(evaluateConstantExpressionOrIdentifierAsLiteral(argument.value, context));
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
        if (!pulling_executor.pull(block))
            break;
        /// No empty blocks allowed.
        if (!block.empty())
            blocks.push_back(std::move(block));
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
Block loadSpans(const String & source, const String & time_filter, const UUID & trace_id, ContextPtr context)
{
    /// LowCardinality columns are converted to plain types
    Block spans = executeInternalQuery(
        fmt::format(
            "SELECT span_id, parent_span_id, toString(operation_name) AS operation_name,"
            " toString(kind) AS kind, toString(status_code) AS status, toString(status_message) AS status_message,"
            " start_time_us, finish_time_us,"
            " toString(attribute['clickhouse.shard_num']) AS shard_num,"
            " CAST(attribute, 'Map(String, String)') AS attribute"
            " FROM {} WHERE trace_id = toUUID('{}'){} ORDER BY start_time_us, span_id",
            source, toString(trace_id), time_filter),
        context);

    if (spans.rows() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "No spans found for trace_id '{}'{}. Spans are flushed to the log in background:"
            " run SYSTEM FLUSH LOGS opentelemetry_span_log and retry",
            toString(trace_id), time_filter.empty() ? "" : " within the since/until window");

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
        finish = std::max(finish, spans.finish(i));
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
        const UInt64 child_finish = std::clamp(spans.finish(child), cursor, finish);
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

/// Visits every span of the forest once, depth-first, children in row order. Duplicates are skipped
/// `visit(row, prefix, connector)` receives the tree drawing of the row:
/// `prefix` is the indentation inherited from the ancestors, `connector` the branch to the row itself.
template <typename Visit>
void walkDepthFirst(const SpanForest & forest, Visit && visit)
{
    const size_t rows = forest.children.size();

    /// Explicit DFS stack: a trace is unbounded in depth, recursion is not.
    struct Frame
    {
        size_t row = 0;
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
    UnorderedMapWithMemoryTracking<String, IColumn *> by_name;
    for (const auto & column : result_structure)
    {
        res.push_back(column.type->createColumn());
        by_name[column.name] = res.back().get();
    }

    /// The columns are found by name, so the order of `getActualTableStructure` is free to change.
    const auto column = [&](const String & name) -> IColumn &
    {
        auto it = by_name.find(name);
        if (it == by_name.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function 'traceView': no result column '{}'", name);
        return *it->second;
    };
    IColumn & span = column("span");
    IColumn & kind = column("kind");
    IColumn & status = column("status");
    IColumn & status_message = column("status_message");
    IColumn & start_offset_us = column("start_offset_us");
    IColumn & duration_us = column("duration_us");
    IColumn & duration_text = column("duration");
    IColumn & self_pct = column("self_pct");
    IColumn & timeline = column("timeline");
    IColumn & attribute = column("attribute");

    walkDepthFirst(forest, [&](size_t row, const String & prefix, const String & connector)
    {
        const UInt64 offset = spans.start(row) - trace.start;
        const UInt64 duration = spans.finish(row) - spans.start(row);
        const UInt64 self = selfTimeUs(spans, forest, row);

        span.insert(renderSpanText(spans, row, prefix + connector));
        kind.insert(String(spans.kind.getDataAt(row)));
        status.insert(String(spans.status.getDataAt(row)));
        status_message.insert(String(spans.status_message.getDataAt(row)));
        start_offset_us.insert(offset);
        duration_us.insert(duration);
        duration_text.insert(formatDurationUs(duration));
        self_pct.insert(100.0 * static_cast<Float64>(self) / static_cast<Float64>(trace.duration));
        timeline.insert(renderTimeline(offset, duration, trace, timeline_width));
        attribute.insertFrom(spans.attribute, row);
    });

    Block block;
    size_t i = 0;
    for (const auto & column_desc : result_structure)
        block.insert({std::move(res[i++]), column_desc.type, column_desc.name});
    return block;
}

/// The positions in `replicas` of the replicas that have the table `table_id`. Every remote replica
/// is asked with `EXISTS TABLE`, which needs SHOW TABLES on the table only - a privilege SELECT on
/// it implies - and the replicas that are this server are looked up in the catalog. The queries are
/// sent to every replica before the first answer is read, so the replicas answer in parallel.
///
/// Under `skip_unavailable_shards` an unreachable replica answers nothing and is left out, as the
/// read of its log would leave it out too; without the setting, it fails the call, as it would the read.
std::vector<size_t> replicasWithTable(const Cluster & replicas, const StorageID & table_id, ContextPtr context)
{
    const auto & shards = replicas.getShardsInfo();

    auto probe_context = ClusterProxy::updateSettingsForCluster(replicas, context, context->getSettingsRef(), table_id);
    const String query = "EXISTS TABLE " + table_id.getFullTableName();
    /// The result of `EXISTS TABLE`.
    auto header = std::make_shared<const Block>(Block{{ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "result"}});

    std::vector<std::unique_ptr<RemoteQueryExecutor>> probes(shards.size());
    for (size_t i = 0; i < shards.size(); ++i)
    {
        if (shards[i].isLocal())
            continue;
        /// No main table for the probe: with one, the connection asks the replica for the status of
        /// the table before the query, and a replica without the table is rejected instead of asked.
        probes[i] = std::make_unique<RemoteQueryExecutor>(shards[i].pool, query, header, probe_context);
        probes[i]->setPoolMode(PoolMode::GET_ONE);
        probes[i]->sendQuery();
    }

    std::vector<size_t> with_table;
    for (size_t i = 0; i < shards.size(); ++i)
    {
        bool has_table = false;
        if (shards[i].isLocal())
        {
            has_table = DatabaseCatalog::instance().isTableExist(table_id, context);
        }
        else
        {
            for (Block answer = probes[i]->readBlock(); !answer.empty(); answer = probes[i]->readBlock())
                has_table = convertBLOBColumns(answer).getByPosition(0).column->getBool(0);
            probes[i]->finish();
        }
        if (has_table)
            with_table.push_back(i);
    }
    return with_table;
}

}

String TableFunctionTraceView::spanLogSource(ContextMutablePtr context) const
{
    const StorageID span_log_id{"system", "opentelemetry_span_log"};

    if (cluster.empty())
    {
        /// The span log is created on its first flush: a server that never wrote a span has no table.
        if (!DatabaseCatalog::instance().tryGetTable(span_log_id, context))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The table system.opentelemetry_span_log does not exist yet: it is created by the first flush of spans."
                " Run a query with tracing enabled, then SYSTEM FLUSH LOGS opentelemetry_span_log and retry");
        return span_log_id.getFullTableName();
    }

    /// In a cluster the spans of each node are written to that node's own span log, so an explicitly
    /// given cluster reads the log of every replica. A replica that never flushed a span has no log
    /// table yet, and `clusterAllReplicas(cluster, system.opentelemetry_span_log)` fails on the first
    /// such replica even when the trace is on the others. So the read goes to the replicas that have
    /// the table only, and it needs no privilege beyond those of that read: `clusterAllReplicas` is
    /// not used because it takes the structure of the table from one replica, which may have none.
    /// The read reaches other servers all the same, so it needs the grant `clusterAllReplicas` needs.
    context->getAccess()->checkAccessWithFilter(AccessType::READ, toStringSource(AccessTypeObjects::Source::REMOTE), /* filter */ "");
    const ClusterPtr all_replicas = context->getCluster(cluster)->getClusterWithReplicasAsShards(context->getSettingsRef());

    /// The replicas that are this server read their log in this process: the caller's SELECT on it is
    /// checked here, as `clusterAllReplicas` does, and before the log is looked up.
    if (std::ranges::any_of(all_replicas->getShardsInfo(), [](const auto & shard) { return shard.isLocal(); }))
        context->checkAccess(AccessType::SELECT, span_log_id);

    const std::vector<size_t> indices = replicasWithTable(*all_replicas, span_log_id, context);
    if (indices.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "No replica of cluster '{}' has the table system.opentelemetry_span_log yet: it is created by the first flush of spans."
            " Run a query with tracing enabled, then SYSTEM FLUSH LOGS opentelemetry_span_log on the nodes that ran it and retry",
            cluster);
    const ClusterPtr span_log_replicas = all_replicas->getClusterWithMultipleShards(indices);

    /// A Distributed table over those replicas only, visible to the internal queries of `context`
    /// under this name. It lives as long as `context`, which is private to this call.
    const String source = "_trace_view_span_log";
    context->addExternalTable(source, TemporaryTableHolder(context, [&](const StorageID & table_id) -> StoragePtr
    {
        auto storage = std::make_shared<StorageDistributed>(
            table_id,
            getStructureOfRemoteTable(*span_log_replicas, span_log_id, context),
            ConstraintsDescription{},
            /* comment */ String{},
            span_log_id.database_name,
            span_log_id.table_name,
            /* cluster_name */ String{},
            context,
            /* sharding_key */ nullptr,
            /* storage_policy_name */ String{},
            /* relative_data_path */ String{},
            DistributedSettings{},
            LoadingStrictnessLevel::CREATE,
            span_log_replicas);
        storage->startup();
        return storage;
    }));
    return source;
}

String TableFunctionTraceView::spanLogTimeFilter() const
{
    /// The span log is partitioned and ordered by `finish_date`, and neither `trace_id` nor the
    /// attributes are in the key: without this window every call scans the whole log.
    String filter;
    if (!since.empty())
        filter += fmt::format(" AND finish_date >= {}", quoteString(since));
    if (!until.empty())
        filter += fmt::format(" AND finish_date <= {}", quoteString(until));
    return filter;
}

UUID TableFunctionTraceView::resolveTraceId(const String & source, const String & time_filter, ContextPtr context) const
{
    if (query_id.empty())
        return trace_id;

    /// The query's root span ('query') carries its id in the `clickhouse.query_id`
    /// attribute. A custom query id can be reused across runs, so several traces may
    /// match: take the most recent one - that is what a debugging session wants.
    Block lookup = executeInternalQuery(
        fmt::format(
            "SELECT trace_id FROM {} WHERE operation_name = 'query'"
            " AND attribute['clickhouse.query_id'] = {}{} ORDER BY finish_time_us DESC LIMIT 1",
            source, quoteString(query_id), time_filter),
        context);

    if (lookup.rows() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "No trace found for query_id '{}'{}. The query must run with tracing enabled"
            " (a traceparent or opentelemetry_start_trace_probability); spans are flushed"
            " to the log in background: run SYSTEM FLUSH LOGS opentelemetry_span_log and retry",
            query_id, time_filter.empty() ? "" : " within the since/until window");

    return (*lookup.getByPosition(0).column)[0].safeGet<UUID>();
}

StoragePtr TableFunctionTraceView::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    /// The internal queries run in a context of their own: with `cluster`, the table they read
    /// from is registered in it, and the caller's context must not see it.
    ContextMutablePtr internal_context = Context::createCopy(context);

    const String source = spanLogSource(internal_context);
    const String time_filter = spanLogTimeFilter();
    const UUID effective_trace_id = resolveTraceId(source, time_filter, internal_context);
    const Block spans = loadSpans(source, time_filter, effective_trace_id, internal_context);

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

Arguments: `trace_id` (String or UUID), optional `timeline_width` (default 40, at most 1024), optional `cluster` - read the span log of every replica of the cluster instead of the local one, because in a cluster every node writes its spans to its own log; a replica that has no `system.opentelemetry_span_log` yet, because it never flushed a span, is skipped. Arguments can also be passed by name (`name = value`). Instead of `trace_id`, the named argument `query_id` selects the most recent trace of that query - named only, because a server-generated query id is itself a UUID and cannot be told apart from a trace id positionally: `traceView(query_id = '<query id>')`.

The named arguments `since` and `until` (`'YYYY-MM-DD'` strings, e.g. `since = toString(today() - 7)`) restrict the search to spans whose `finish_date` is within the window, inclusive. The span log is partitioned and ordered by `finish_date` and has no TTL by default, so without a window every call scans the whole log, and the cost grows with the age of the server. The `event_date` of the query in `system.query_log` is a good value for both.

Spans are flushed to the log in background: run `SYSTEM FLUSH LOGS opentelemetry_span_log` first.
Example:
[example:trace_view]
)",
        .examples = {{"trace_view", "SELECT span, status, duration, timeline FROM traceView('5c9e4a3b-2f61-4d6e-8b7a-90c1d2e3f405')", ""}},
        .category = FunctionDocumentation::Category::TableFunction},
        /// A read-only wrapper over the span log: the caller's SELECT access is checked by the
        /// query it runs, no CREATE TEMPORARY TABLE is needed, and it works under readonly = 1.
        {.allow_readonly = true});
}

}
