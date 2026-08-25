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
#include <Parsers/ASTLiteral.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/BlockIO.h>
#include <Storages/StorageValues.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Common/quoteString.h>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

void TableFunctionTraceView::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (!function || !function->arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments", getName());

    auto & args = function->arguments->children;
    if (args.empty() || args.size() > 3)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table function '{}' requires 1 to 3 arguments: trace_id [, timeline_width [, cluster]], got {}",
            getName(), args.size());

    args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context);
    const auto * trace_id_literal = args[0]->as<ASTLiteral>();
    if (!trace_id_literal)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table function '{}' requires a constant trace_id as the first argument", getName());

    if (trace_id_literal->value.getType() == Field::Types::UUID)
    {
        trace_id = trace_id_literal->value.safeGet<UUID>();
    }
    else if (trace_id_literal->value.getType() == Field::Types::String)
    {
        const auto & trace_id_str = trace_id_literal->value.safeGet<String>();
        ReadBufferFromString buf(trace_id_str);
        readUUIDText(trace_id, buf);
        if (!buf.eof())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Table function '{}': cannot parse '{}' as a trace_id UUID", getName(), trace_id_str);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table function '{}' requires a String or UUID trace_id as the first argument, got '{}'",
            getName(), args[0]->formatForErrorMessage());
    }

    if (args.size() >= 2)
    {
        args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], context);
        timeline_width = checkAndGetLiteralArgument<UInt64>(args[1], "timeline_width");
        if (timeline_width == 0 || timeline_width > 1024)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Table function '{}': timeline_width must be in [1, 1024], got {}", getName(), timeline_width);
    }

    if (args.size() == 3)
    {
        args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], context);
        cluster = checkAndGetLiteralArgument<String>(args[2], "cluster");
        /// Fail early with a clear error instead of a confusing one from the internal query.
        context->getCluster(cluster);
    }
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

}

StoragePtr TableFunctionTraceView::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    /// In a cluster the spans of each node are written to that node's own span log,
    /// so an explicitly given cluster reads the log of every replica.
    String source = cluster.empty()
        ? "system.opentelemetry_span_log"
        : fmt::format("clusterAllReplicas({}, system.opentelemetry_span_log)", quoteString(cluster));

    /// LowCardinality columns are converted to plain types so that the code below and the
    /// declared structure of the `attribute` result column need no special cases.
    String query = fmt::format(
        "SELECT span_id, parent_span_id, toString(operation_name) AS operation_name,"
        " toString(kind) AS kind, toString(status_code) AS status, toString(status_message) AS status_message,"
        " start_time_us, finish_time_us,"
        " toString(attribute['clickhouse.shard_num']) AS shard_num,"
        " CAST(attribute, 'Map(String, String)') AS attribute"
        " FROM {} WHERE trace_id = toUUID('{}') ORDER BY start_time_us, span_id",
        source, toString(trace_id));

    auto query_context = Context::createCopy(context);
    query_context->makeQueryContext();
    /// The copied context carries the enclosing query's id; the internal query must
    /// register under its own, or the process list rejects it as already running.
    query_context->setCurrentQueryId("");
    auto io = executeQuery(query, query_context, QueryFlags{.internal = true}).second;
    Block spans = pullMonoBlock(io.pipeline);

    size_t rows = spans.rows();
    if (rows == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "No spans found for trace_id '{}'. Spans are flushed to the log in background:"
            " run SYSTEM FLUSH LOGS opentelemetry_span_log and retry", toString(trace_id));

    const auto & col_span_id = *spans.getByName("span_id").column;
    const auto & col_parent = *spans.getByName("parent_span_id").column;
    const auto & col_name = *spans.getByName("operation_name").column;
    const auto & col_kind = *spans.getByName("kind").column;
    const auto & col_status = *spans.getByName("status").column;
    const auto & col_message = *spans.getByName("status_message").column;
    const auto & col_start = *spans.getByName("start_time_us").column;
    const auto & col_finish = *spans.getByName("finish_time_us").column;
    const auto & col_shard_num = *spans.getByName("shard_num").column;
    const auto & col_attribute = *spans.getByName("attribute").column;

    /// Children follow the ORDER BY of the query (start time, then span id), so sibling
    /// order and the DFS below are deterministic.
    std::unordered_map<UInt64, size_t> row_by_span_id;
    for (size_t i = 0; i < rows; ++i)
        row_by_span_id.emplace(col_span_id.getUInt(i), i);

    std::vector<std::vector<size_t>> children(rows);
    std::vector<size_t> roots;
    for (size_t i = 0; i < rows; ++i)
    {
        auto parent = row_by_span_id.find(col_parent.getUInt(i));
        /// A span whose parent is not part of the trace is shown as a root: this keeps
        /// subtrees visible when their parent span was lost or not instrumented.
        if (parent == row_by_span_id.end() || parent->second == i)
            roots.push_back(i);
        else
            children[parent->second].push_back(i);
    }

    UInt64 trace_start = std::numeric_limits<UInt64>::max();
    UInt64 trace_finish = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        trace_start = std::min(trace_start, col_start.getUInt(i));
        trace_finish = std::max(trace_finish, col_finish.getUInt(i));
    }
    const UInt64 trace_duration = std::max<UInt64>(1, trace_finish - trace_start);

    auto result_columns = getActualTableStructure(context, is_insert_query).getAllPhysical();
    MutableColumns res;
    for (const auto & column : result_columns)
        res.push_back(column.type->createColumn());

    /// Explicit DFS stack: a trace is unbounded in depth, recursion is not.
    struct Frame
    {
        size_t row;
        String prefix;
        String connector;
    };
    std::vector<Frame> stack;
    for (size_t i = roots.size(); i > 0; --i)
        stack.push_back({roots[i - 1], "", ""});

    while (!stack.empty())
    {
        auto [row, prefix, connector] = std::move(stack.back());
        stack.pop_back();

        const UInt64 start = col_start.getUInt(row);
        const UInt64 finish = std::max(col_finish.getUInt(row), start);
        const UInt64 duration = finish - start;
        const UInt64 offset = start - trace_start;

        /// Self time: the span's duration minus the union of its children's intervals
        /// (children can overlap, e.g. parallel shard reads, so plain summing is wrong).
        UInt64 covered = 0;
        {
            UInt64 cursor = start;
            for (size_t child : children[row])
            {
                UInt64 child_start = std::clamp(col_start.getUInt(child), cursor, finish);
                UInt64 child_finish = std::clamp(col_finish.getUInt(child), cursor, finish);
                if (child_finish > child_start)
                    covered += child_finish - child_start;
                cursor = std::max(cursor, child_finish);
            }
        }
        const UInt64 self = duration - std::min(duration, covered);

        String timeline;
        {
            size_t pad = std::min(static_cast<size_t>(offset * timeline_width / trace_duration), static_cast<size_t>(timeline_width - 1));
            size_t len = std::max<size_t>(1, static_cast<size_t>((duration * timeline_width + trace_duration / 2) / trace_duration));
            len = std::min(len, timeline_width - pad);
            for (size_t i = 0; i < pad; ++i)
                timeline += "·";
            for (size_t i = 0; i < len; ++i)
                timeline += "█";
            for (size_t i = pad + len; i < timeline_width; ++i)
                timeline += "·";
        }

        String span_text = prefix + connector + String(col_name.getDataAt(row));
        if (String shard_num = String(col_shard_num.getDataAt(row)); !shard_num.empty())
            span_text += fmt::format("  shard {}", shard_num);

        res[0]->insert(span_text);
        res[1]->insert(String(col_kind.getDataAt(row)));
        res[2]->insert(String(col_status.getDataAt(row)));
        res[3]->insert(String(col_message.getDataAt(row)));
        res[4]->insert(offset);
        res[5]->insert(duration);
        res[6]->insert(formatDurationUs(duration));
        res[7]->insert(100.0 * static_cast<Float64>(self) / static_cast<Float64>(trace_duration));
        res[8]->insert(timeline);
        res[9]->insertFrom(col_attribute, row);

        String child_prefix = prefix;
        if (connector == "├─ ")
            child_prefix += "│  ";
        else if (connector == "└─ ")
            child_prefix += "   ";

        const auto & child_rows = children[row];
        for (size_t i = child_rows.size(); i > 0; --i)
            stack.push_back({child_rows[i - 1], child_prefix, i == child_rows.size() ? "└─ " : "├─ "});
    }

    Block block;
    {
        size_t i = 0;
        for (const auto & column : result_columns)
        {
            block.insert({std::move(res[i]), column.type, column.name});
            ++i;
        }
    }

    StorageID storage_id(getDatabaseName(), table_name);
    auto storage = std::make_shared<StorageValues>(storage_id, getActualTableStructure(context, is_insert_query), std::move(block));
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

Arguments: `trace_id` (String or UUID), optional `timeline_width` (default 40, at most 1024), optional `cluster` - read `clusterAllReplicas(cluster, system.opentelemetry_span_log)` instead of the local span log, because in a cluster every node writes its spans to its own log.

Spans are flushed to the log in background: run `SYSTEM FLUSH LOGS opentelemetry_span_log` first.
Example:
[example:trace_view]
)",
        .examples = {{"trace_view", "SELECT span, status, duration, timeline FROM traceView('5c9e4a3b-2f61-4d6e-8b7a-90c1d2e3f405')", ""}},
        .category = FunctionDocumentation::Category::TableFunction});
}

}
