#include <Interpreters/QueryOracleChecker.h>

#include <Common/ProfileEvents.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/GetAggregatesVisitor.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Core/Joins.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Common/quoteString.h>
#include <Common/thread_local_rng.h>

#include <algorithm>
#include <unordered_set>


namespace ProfileEvents
{
extern const Event ASTFuzzerOracleChecks;
extern const Event ASTFuzzerOracleMismatches;
}

namespace DB
{

namespace Setting
{
extern const SettingsBool ast_fuzzer_oracle;
}

namespace ErrorCodes
{
extern const int AST_FUZZER_ORACLE_MISMATCH;
extern const int TOO_MANY_ROWS;
extern const int TOO_MANY_BYTES;
}


namespace
{

/// Set of known non-deterministic function names that would invalidate oracle checks.
const std::unordered_set<String> non_deterministic_functions = {
    "rand", "rand32", "rand64", "randConstant", "randUniform", "randNormal",
    "randBernoulli", "randExponential", "randChiSquared", "randStudentT",
    "randFisherF", "randLogNormal", "randPoisson",
    "generateUUIDv4", "generateUUIDv7", "generateSnowflakeID",
    "now", "now64", "today", "yesterday",
    "rowNumberInBlock", "blockNumber", "blockSize",
    "runningDifference", "runningDifferenceStartingWithFirstValue",
    "currentDatabase", "queryID", "serverUUID",
    "getSetting", "fuzzBits", "throwIf",
    "file", "url", "s3", "hdfs", "input",
    "numbers", "zeros", "generateRandom",
    "randomPrintableASCII", "randomString", "randomFixedString",
    "fuzzQuery",
    "materialize",
    /// Non-deterministic or approximate aggregate functions.
    "any", "anyLast", "anyHeavy",
    "anyRespectNulls", "anyLastRespectNulls",
    "first_value", "last_value",
    "topK", "topKWeighted",
    "uniqHLL12", "uniqCombined", "uniqCombined64", "uniqTheta",
    /// Approximate quantile/median functions: State/Merge gives different results
    /// than direct computation due to approximate merging algorithms.
    "median", "quantile", "quantiles",
    "quantileTDigest", "quantileTDigestWeighted",
    "quantileGK", "quantileBFloat16", "quantileDD",
    "quantileTiming", "quantileTimingWeighted",
    "quantileDeterministic",
    /// Order-dependent or floating-point aggregates whose State/Merge path
    /// can differ from direct computation. `sum` / `sumWithOverflow` are
    /// blocked because floating-point addition is non-associative — the
    /// metamorphic `sumState`/`sumMerge` rewrite can legitimately produce a
    /// different rounded value than direct `sum` over Float32/Float64
    /// arguments. We don't try to inspect argument types here, so we exclude
    /// `sum` family unconditionally — that costs some integer-sum coverage
    /// but eliminates a flaky-mismatch source.
    "deltaSum", "deltaSumTimestamp",
    "stddevPop", "stddevSamp", "stddevPopStable", "stddevSampStable",
    "varPop", "varSamp", "varPopStable", "varSampStable",
    "covarPop", "covarSamp", "covarPopStable", "covarSampStable", "corr", "corrStable",
    "avg", "avgWeighted",
    "skewPop", "skewSamp", "kurtPop", "kurtSamp",
    "sum", "sumWithOverflow", "sumKahan",
    "stochasticLinearRegression", "stochasticLogisticRegression",
    "initializeAggregation",
    /// Order-dependent aggregate functions.
    "groupArray", "groupUniqArray", "groupArrayInsertAt",
    "groupArrayMovingSum", "groupArrayMovingAvg",
    "groupArraySorted", "groupArrayLast",
    /// Approximate/formatting-dependent aggregates.
    "entropy", "exponentialMovingAverage", "exponentialTimeDecayedAvg",
    "simpleLinearRegression", "sparkBar", "histogram",
    "retentionState",
};

/// Maximum formatted query length for oracle sub-queries.
constexpr size_t MAX_ORACLE_QUERY_LENGTH = 10000;

/// Maximum total output size from an oracle sub-query (bytes).
constexpr size_t MAX_ORACLE_OUTPUT_SIZE = 10 * 1024 * 1024;

/// Maximum row count for an oracle sub-query result. Caps memory before the
/// post-execution size check in `executeAndCollect*` triggers.
constexpr size_t MAX_ORACLE_RESULT_ROWS = 10'000'000;

/// Strip known aggregate-function combinator suffixes from the right of `name`
/// repeatedly, so e.g. `first_valueOrNullDistinct` becomes `first_value`.
/// Combinators are checked against the same set ClickHouse recognises in
/// `AggregateFunctionCombinatorFactory`. Cheap and safe: we never strip into
/// an empty string and stop as soon as no suffix matches.
String stripAggregateCombinators(String name)
{
    static const std::vector<String> suffixes = {
        "If",
        "Array",
        "Map",
        "ForEach",
        "Distinct",
        "OrDefault",
        "OrFill",
        "OrNull",
        "Resample",
        "ArgMin",
        "ArgMax",
        "MergeState",
        "State",
        "Merge",
        "SimpleState",
        "Tuple",
        "RespectNulls",
        "IgnoreNulls",
        "Null",
    };
    bool stripped = true;
    while (stripped)
    {
        stripped = false;
        for (const auto & suffix : suffixes)
        {
            if (name.size() > suffix.size() && name.ends_with(suffix))
            {
                name.resize(name.size() - suffix.size());
                stripped = true;
                break;
            }
        }
    }
    return name;
}

/// Walk an AST tree and check if any ASTFunction has a name in the given set.
/// We strip aggregate combinator suffixes before the lookup so e.g.
/// `first_valueOrNull` matches the `first_value` entry in the blocklist —
/// the fuzzer routinely mutates aggregates into `*OrNull`/`*Distinct`/`*State`
/// chains and exact-name matching would miss those.
bool hasNonDeterministicFunctionsImpl(const ASTPtr & ast)
{
    if (!ast)
        return false;

    if (const auto * func = ast->as<ASTFunction>())
    {
        if (non_deterministic_functions.contains(func->name)
            || non_deterministic_functions.contains(stripAggregateCombinators(func->name)))
            return true;
    }

    for (const auto & child : ast->children)
    {
        if (hasNonDeterministicFunctionsImpl(child))
            return true;
    }
    return false;
}

/// Split a string by newline into individual rows, ignoring trailing empty line.
std::vector<String> splitIntoRows(const String & output)
{
    /// Split TabSeparated output into rows. ClickHouse always terminates a TSV
    /// row with `\n`, so the canonical form is `<row1>\n<row2>\n...\n<rowN>\n`.
    /// Strip exactly one trailing `\n` if present (it terminates the last row,
    /// not a separator), then split the rest on `\n`. This produces the right
    /// number of rows even when many of them are empty strings (e.g. unmatched
    /// LEFT JOIN rows for a `String` right-side column come out as empty).
    std::vector<String> rows;
    if (output.empty())
        return rows;

    std::string_view sv{output};
    if (sv.back() == '\n')
        sv.remove_suffix(1);

    size_t start = 0;
    while (true)
    {
        size_t end = sv.find('\n', start);
        if (end == std::string_view::npos)
        {
            rows.emplace_back(sv.substr(start));
            break;
        }
        rows.emplace_back(sv.substr(start, end - start));
        start = end + 1;
    }

    return rows;
}

/// ARRAY JOIN multiplies rows (one input → many output), breaking partition identity.
bool hasArrayJoin(const ASTSelectQuery & select)
{
    ASTPtr tables = select.tables();
    if (!tables)
        return false;
    for (const auto & child : tables->children)
    {
        const auto * elem = child->as<ASTTablesInSelectQueryElement>();
        if (elem && elem->array_join)
            return true;
    }
    return false;
}

/// Recursively walks `ast` looking for any call to `arrayJoin(...)`
/// — the function form, distinct from the ARRAY JOIN clause caught by
/// `hasArrayJoin` above. Both forms multiply rows, so any of them in a
/// SELECT list breaks oracle invariants like NoREC's
/// `count(SELECT ... arrayJoin ...) == countIf(WHERE)`.
bool hasArrayJoinFunction(const ASTPtr & ast)
{
    if (!ast)
        return false;
    if (const auto * func = ast->as<ASTFunction>())
    {
        if (func->name == "arrayJoin")
            return true;
    }
    for (const auto & child : ast->children)
        if (hasArrayJoinFunction(child))
            return true;
    return false;
}

/// Recursively walks `ast` looking for any window-function call whose
/// `OVER (...)` definition lacks an `ORDER BY`, or that references a named
/// window (which we can't verify without scope resolution).
///
/// `row_number()`, `rank()`, etc. over a partition without an explicit ORDER BY
/// produce values in implementation-defined row order — re-running the same
/// query (or wrapping it in `SELECT * FROM (...)`) is permitted to assign
/// different row numbers. The Subquery-wrap oracle's row-set comparison
/// correctly detects this divergence but the divergence is allowed, so we
/// must skip these queries (issue #105743).
bool hasWindowFunctionWithoutOrderBy(const ASTPtr & ast)
{
    if (!ast)
        return false;
    if (const auto * func = ast->as<ASTFunction>())
    {
        /// Inline OVER (...) definition.
        if (func->window_definition)
        {
            const auto * def = func->window_definition->as<ASTWindowDefinition>();
            if (!def || !def->order_by)
                return true;
        }
        /// Named window reference `OVER w` — we can't verify w's ORDER BY
        /// without resolving the SELECT's WINDOW clause; conservatively reject.
        else if (!func->window_name.empty())
        {
            return true;
        }
    }
    for (const auto & child : ast->children)
        if (hasWindowFunctionWithoutOrderBy(child))
            return true;
    return false;
}

/// PASTE JOIN pairs rows by position — WHERE filtering changes positions, breaking the invariant.
bool hasPasteJoin(const ASTSelectQuery & select)
{
    ASTPtr tables = select.tables();
    if (!tables)
        return false;
    for (const auto & child : tables->children)
    {
        const auto * elem = child->as<ASTTablesInSelectQueryElement>();
        if (elem && elem->table_join)
        {
            const auto * join = elem->table_join->as<ASTTableJoin>();
            if (join && isPaste(join->kind))
                return true;
        }
    }
    return false;
}

/// True if the FROM clause uses a subquery that itself contains UNION ALL /
/// UNION DISTINCT / INTERSECT / EXCEPT. We skip those: when the union
/// branches have mismatched column types, the subquery output column ends up
/// as `Variant(...)`, and the same predicate can produce different rows
/// depending on whether it is applied directly to the union output, through
/// an intermediate `SELECT *`, or pushed into individual branches before the
/// type unification. The oracle's reference-vs-rewrite comparison cannot
/// distinguish those alternative semantics from real correctness bugs.
bool fromContainsUnionSubquery(const ASTSelectQuery & select)
{
    ASTPtr tables = select.tables();
    if (!tables)
        return false;
    for (const auto & table_child : tables->children)
    {
        const auto * tables_element = table_child->as<ASTTablesInSelectQueryElement>();
        if (!tables_element || !tables_element->table_expression)
            continue;
        const auto * table_expr = tables_element->table_expression->as<ASTTableExpression>();
        if (!table_expr || !table_expr->subquery)
            continue;
        /// table_expr->subquery is an ASTSubquery whose child is the SELECT
        /// (often wrapped in an ASTSelectWithUnionQuery).
        for (const auto & sub_child : table_expr->subquery->children)
        {
            if (const auto * union_query = sub_child->as<ASTSelectWithUnionQuery>())
            {
                if (union_query->list_of_selects && union_query->list_of_selects->children.size() > 1)
                    return true;
            }
        }
    }
    return false;
}

/// True if any table in the FROM clause refers to a database whose contents
/// are not stable across two adjacent reads. This includes the `system`
/// database (snapshots of live server state like `processes`, `merges`,
/// `metric_log`, etc.) and `INFORMATION_SCHEMA`. Even when the rows look
/// identical between calls, the underlying values (timings, counters,
/// thread ids) drift, which causes spurious oracle mismatches between the
/// reference and rewritten queries.
bool referencesNonDeterministicDatabase(const ASTSelectQuery & select)
{
    ASTPtr tables = select.tables();
    if (!tables)
        return false;
    for (const auto & table_child : tables->children)
    {
        const auto * tables_element = table_child->as<ASTTablesInSelectQueryElement>();
        if (!tables_element || !tables_element->table_expression)
            continue;
        const auto * table_expr = tables_element->table_expression->as<ASTTableExpression>();
        if (!table_expr)
            continue;

        /// Subquery / table function as the source — let the per-oracle logic decide.
        if (!table_expr->database_and_table_name)
            continue;

        const auto * table_id = table_expr->database_and_table_name->as<ASTTableIdentifier>();
        if (!table_id)
            continue;

        String database = table_id->getDatabaseName();
        if (database == "system" || database == "INFORMATION_SCHEMA" || database == "information_schema")
            return true;
    }
    return false;
}

/// Safer replacement for `GetAggregatesVisitor` when scanning fuzzer-mutated
/// ASTs. The default visitor calls `node.getColumnName()` for deduplication,
/// which recursively invokes `appendColumnName` on every child. Some AST node
/// types (e.g. ones produced by certain fuzz mutations) do not implement
/// `appendColumnNameImpl` and the default base `IAST::appendColumnName`
/// throws `LOGICAL_ERROR`. In sanitizer / debug builds, a `LOGICAL_ERROR`
/// triggers `abortOnFailedAssertion` from inside the `Exception` constructor
/// — before any catch block runs — and the server is killed.
///
/// We only need the list of aggregate / window function nodes, not their
/// canonical column names, so do a direct tree walk that mirrors
/// `GetAggregatesMatcher::needChildVisit` but never calls `getColumnName`.
struct SafeAggregateScan
{
    ASTs aggregates;
    ASTs window_functions;
};

void scanAggregatesSafe(const ASTPtr & ast, SafeAggregateScan & out)
{
    if (!ast)
        return;
    if (ast->as<ASTSubquery>() || ast->as<ASTSelectQuery>())
        return; /// Don't descend into nested subqueries — they're not part of *this* SELECT's aggregates.

    if (const auto * func = ast->as<ASTFunction>())
    {
        /// Match `GetAggregatesMatcher::isAggregateFunction` semantics: a window
        /// function does not count as an aggregate even if its underlying function
        /// has an aggregate name.
        if (!func->isWindowFunction() && AggregateUtils::isAggregateFunction(*func))
        {
            out.aggregates.push_back(ast);
            return; /// Don't recurse into the aggregate's own arguments.
        }
        if (func->isWindowFunction())
            out.window_functions.push_back(ast);
    }
    for (const auto & child : ast->children)
        scanAggregatesSafe(child, out);
}

}


const ASTSelectQuery * QueryOracleChecker::extractSimpleSelect(const ASTPtr & ast)
{
    if (const auto * select = ast->as<ASTSelectQuery>())
        return select;

    if (const auto * union_query = ast->as<ASTSelectWithUnionQuery>())
    {
        if (union_query->list_of_selects && union_query->list_of_selects->children.size() == 1)
            return union_query->list_of_selects->children[0]->as<ASTSelectQuery>();
    }

    return nullptr;
}


bool QueryOracleChecker::isSafeForOracle(const ASTSelectQuery & select)
{
    /// Regular JOINs (INNER, LEFT, RIGHT, FULL, CROSS) are safe — the FROM clause
    /// stays identical across all TLP partitions, only WHERE changes.
    /// ARRAY JOIN clause and PASTE JOIN are NOT safe. Neither is the `arrayJoin()`
    /// *function* appearing anywhere in the query: it multiplies rows, breaking
    /// `count(Q) == countIf(WHERE)` (NoREC) and the partitioned-vs-whole-table
    /// row-count equality the TLP oracles depend on.
    if (hasArrayJoin(select) || hasPasteJoin(select))
        return false;
    if (select.select() && hasArrayJoinFunction(select.select()))
        return false;
    if (select.where() && hasArrayJoinFunction(select.where()))
        return false;
    /// `system.*` / `INFORMATION_SCHEMA.*` views are non-deterministic.
    if (referencesNonDeterministicDatabase(select))
        return false;
    if (select.distinct)
        return false;
    if (select.limitLength())
        return false;
    if (select.limitBy())
        return false;
    /// PREWHERE can interact unpredictably with WHERE partitioning — the fuzzer
    /// may produce PREWHERE expressions with suspicious type coercions that
    /// silently succeed in some contexts but fail in others, causing false
    /// positive mismatches. Skip queries with PREWHERE.
    if (select.prewhere())
        return false;
    if (select.qualify())
        return false;
    if (!select.tables())
        return false;
    if (select.group_by_with_rollup || select.group_by_with_cube
        || select.group_by_with_totals || select.group_by_with_grouping_sets)
        return false;

    /// Window functions are never safe for oracle testing.
    if (select.select())
    {
        SafeAggregateScan data;
        scanAggregatesSafe(select.select(), data);
        if (!data.window_functions.empty())
            return false;
    }

    return true;
}


bool QueryOracleChecker::hasAggregates(const ASTSelectQuery & select)
{
    if (!select.select())
        return false;
    SafeAggregateScan data;
    scanAggregatesSafe(select.select(), data);
    return !data.aggregates.empty();
}


bool QueryOracleChecker::hasNonDeterministicFunctions(const ASTPtr & ast)
{
    return hasNonDeterministicFunctionsImpl(ast);
}


void QueryOracleChecker::stripOrderAndLimit(ASTSelectQuery & select)
{
    select.setExpression(ASTSelectQuery::Expression::ORDER_BY, {});
    select.setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, {});
    select.setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, {});
    select.setExpression(ASTSelectQuery::Expression::LIMIT_BY, {});
    select.setExpression(ASTSelectQuery::Expression::LIMIT_BY_LENGTH, {});
    select.setExpression(ASTSelectQuery::Expression::LIMIT_BY_OFFSET, {});
    select.setExpression(ASTSelectQuery::Expression::INTERPOLATE, {});
    select.setExpression(ASTSelectQuery::Expression::SETTINGS, {});
    select.order_by_all = false;
    select.limit_with_ties = false;
    select.limit_by_all = false;
}


String QueryOracleChecker::formatAST(const ASTPtr & ast)
{
    WriteBufferFromOwnString buf;
    ast->format(buf, IAST::FormatSettings(/*one_line=*/true));
    return buf.str();
}


ContextMutablePtr QueryOracleChecker::makeOracleContext(const ContextMutablePtr & base_context)
{
    auto session_context = Context::createCopy(base_context);
    session_context->makeSessionContext();

    auto oracle_context = Context::createCopy(session_context);
    oracle_context->makeQueryContext();
    oracle_context->setSetting("ast_fuzzer_runs", Field(Float64(0)));
    oracle_context->setSetting("ast_fuzzer_oracle", Field(false));
    oracle_context->setSetting("max_execution_time", Field(UInt64(10)));
    /// Prevent the optimizer from pushing TLP predicates across subquery/JOIN boundaries.
    oracle_context->setSetting("enable_optimize_predicate_expression", Field(false));
    /// Cap result size so oracle sub-queries (especially TLP's UNION ALL of three
    /// partitions) cannot allocate unbounded memory. We use `result_overflow_mode=throw`
    /// — `break` would silently truncate the result before the cap fires, and the
    /// caller's post-check on `output.size() > MAX_ORACLE_OUTPUT_SIZE` would then never
    /// trip (the output sits at exactly the cap). The caller catches the resulting
    /// `TOO_MANY_ROWS` / `TOO_MANY_BYTES` exception and treats the query as skipped.
    oracle_context->setSetting("max_result_rows", Field(UInt64(MAX_ORACLE_RESULT_ROWS)));
    oracle_context->setSetting("max_result_bytes", Field(UInt64(MAX_ORACLE_OUTPUT_SIZE)));
    oracle_context->setSetting("result_overflow_mode", String("throw"));

    /// Run oracle sub-queries single-threaded so the nested pipeline cannot have
    /// background-pool workers running while the outer call site (this thread)
    /// is tearing the pipeline down. TSan caught a heap-use-after-free on
    /// `shared_ptr<FunctionToExecutableFunctionAdaptor>::__on_zero_shared` that
    /// fired exactly when a global-pool worker was still in
    /// `FilterTransform::doTransform → castColumn` for the nested oracle query
    /// at the moment `executeQuery(...)` returned and started destroying the
    /// pipeline. Constraining the nested execution to the caller thread closes
    /// that race (worker tasks run inline, finish before the executor returns).
    oracle_context->setSetting("max_threads", Field(UInt64(1)));
    oracle_context->setSetting("max_insert_threads", Field(UInt64(1)));
    oracle_context->setSetting("output_format_parallel_formatting", Field(false));

    oracle_context->setCurrentQueryId("");
    return oracle_context;
}


std::optional<std::vector<String>>
QueryOracleChecker::executeAndCollectSortedRows(const String & query, const ContextMutablePtr & context)
{
    auto oracle_context = makeOracleContext(context);
    oracle_context->setDefaultFormat("TabSeparated");

    /// Use the ReadBuffer/WriteBuffer executeQuery API — this is crash-safe because
    /// ClickHouse handles all column serialization within the pipeline internally,
    /// writing formatted text directly to the output buffer.
    ReadBufferFromString istr(query);
    WriteBufferFromOwnString ostr;

    try
    {
        executeQuery(istr, ostr, oracle_context, {}, QueryFlags{.internal = true});
    }
    catch (const Exception & e)
    {
        /// `result_overflow_mode=throw` makes oracle sub-queries that exceed
        /// `max_result_rows` / `max_result_bytes` throw rather than silently
        /// truncate. Catch the size-cap exceptions here and signal "skipped"
        /// so the oracle never compares partial results.
        if (e.code() == ErrorCodes::TOO_MANY_ROWS || e.code() == ErrorCodes::TOO_MANY_BYTES)
            return std::nullopt;
        throw;
    }

    String output = ostr.str();
    if (output.size() > MAX_ORACLE_OUTPUT_SIZE)
        return std::nullopt; /// Belt-and-braces: still cap the formatted output.

    auto rows = splitIntoRows(output);
    std::sort(rows.begin(), rows.end());
    return rows;
}


Field QueryOracleChecker::executeScalar(const String & query, const ContextMutablePtr & context)
{
    auto oracle_context = makeOracleContext(context);

    auto result = executeQuery(query, oracle_context, QueryFlags{.internal = true});

    if (!result.second.pipeline.initialized() || !result.second.pipeline.pulling())
        return Field();

    PullingPipelineExecutor executor(result.second.pipeline);
    Block block;

    Field scalar;
    bool found = false;

    while (executor.pull(block))
    {
        if (block.rows() > 0 && block.columns() > 0 && !found)
        {
            block.getByPosition(0).column->get(0, scalar);
            found = true;
        }
    }

    return scalar;
}


std::optional<std::vector<String>>
QueryOracleChecker::executeAndCollectSortedUniqueRows(const String & query, const ContextMutablePtr & context)
{
    auto rows_opt = executeAndCollectSortedRows(query, context);
    if (!rows_opt)
        return std::nullopt;
    auto & rows = *rows_opt;
    rows.erase(std::unique(rows.begin(), rows.end()), rows.end());
    return rows_opt;
}


std::optional<std::vector<String>>
QueryOracleChecker::executeWithSettings(
    const String & query, const ContextMutablePtr & context,
    const std::vector<std::pair<String, Field>> & settings)
{
    auto oracle_context = makeOracleContext(context);
    oracle_context->setDefaultFormat("TabSeparated");
    for (const auto & [name, value] : settings)
        oracle_context->setSetting(name, value);

    ReadBufferFromString istr(query);
    WriteBufferFromOwnString ostr;
    try
    {
        executeQuery(istr, ostr, oracle_context, {}, QueryFlags{.internal = true});
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::TOO_MANY_ROWS || e.code() == ErrorCodes::TOO_MANY_BYTES)
            return std::nullopt;
        throw;
    }

    String output = ostr.str();
    if (output.size() > MAX_ORACLE_OUTPUT_SIZE)
        return std::nullopt;

    auto rows = splitIntoRows(output);
    std::sort(rows.begin(), rows.end());
    return rows;
}


bool QueryOracleChecker::checkTLPWhere(const ASTSelectQuery & select, const ContextMutablePtr & context)
{
    if (!select.where())
        return false;

    if (!isSafeForOracle(select))
        return false;

    /// TLP WHERE requires no aggregates, no GROUP BY, no HAVING.
    /// GROUP BY produces independent groups per partition — UNION ALL duplicates them.
    /// (GROUP BY with aggregates is handled by TLP Aggregate via State/Merge.)
    if (hasAggregates(select) || select.groupBy() || select.having())
        return false;

    if (hasNonDeterministicFunctions(select.clone()))
        return false;

    ASTPtr predicate = select.where()->clone();

    /// Build reference query: original query without WHERE (and without ORDER BY/LIMIT).
    /// JOINs, GROUP BY, and other clauses are preserved.
    auto ref_ast = select.clone();
    auto & ref_select = ref_ast->as<ASTSelectQuery &>();
    ref_select.setExpression(ASTSelectQuery::Expression::WHERE, {});
    stripOrderAndLimit(ref_select);

    String ref_sql = formatAST(ref_ast);
    if (ref_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    /// Build 3 partitioned queries.
    /// Clone 1: WHERE p
    auto clone1_ast = select.clone();
    auto & clone1 = clone1_ast->as<ASTSelectQuery &>();
    stripOrderAndLimit(clone1);

    /// Clone 2: WHERE NOT(p)
    auto clone2_ast = select.clone();
    auto & clone2 = clone2_ast->as<ASTSelectQuery &>();
    clone2.setExpression(ASTSelectQuery::Expression::WHERE, makeASTFunction("not", predicate->clone()));
    stripOrderAndLimit(clone2);

    /// Clone 3: WHERE isNull(p)
    auto clone3_ast = select.clone();
    auto & clone3 = clone3_ast->as<ASTSelectQuery &>();
    clone3.setExpression(ASTSelectQuery::Expression::WHERE, makeASTFunction("isNull", predicate->clone()));
    stripOrderAndLimit(clone3);

    /// Build UNION ALL of the three.
    auto list = make_intrusive<ASTExpressionList>();
    list->children.push_back(clone1_ast);
    list->children.push_back(clone2_ast);
    list->children.push_back(clone3_ast);

    auto union_query = make_intrusive<ASTSelectWithUnionQuery>();
    union_query->union_mode = SelectUnionMode::UNION_ALL;
    union_query->is_normalized = true;
    union_query->list_of_selects = list;
    union_query->children.push_back(list);

    ASTPtr union_ast = union_query;
    String union_sql = formatAST(union_ast);
    if (union_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleChecks);

    LOG_TRACE(logger, "TLP WHERE oracle: reference query: {}", ref_sql);
    LOG_TRACE(logger, "TLP WHERE oracle: partitioned query: {}", union_sql);

    /// Execute both and collect sorted rows for full content comparison.
    auto ref_rows_opt = executeAndCollectSortedRows(ref_sql, context);
    auto part_rows_opt = executeAndCollectSortedRows(union_sql, context);
    if (!ref_rows_opt || !part_rows_opt)
        return false; /// Output exceeded MAX_ORACLE_OUTPUT_SIZE — skip rather than false-pass on truncation.
    auto & ref_rows = *ref_rows_opt;
    auto & part_rows = *part_rows_opt;

    if (ref_rows != part_rows)
    {
        ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleMismatches);

        String message = fmt::format(
            "TLP WHERE oracle mismatch!\n"
            "Reference query ({} rows): {}\n"
            "Partitioned query ({} rows): {}\n",
            ref_rows.size(), ref_sql,
            part_rows.size(), union_sql);

        /// Show first few differing rows for diagnostics.
        size_t max_diff = 5;
        size_t shown = 0;
        size_t ri = 0;
        size_t pi = 0;
        while ((ri < ref_rows.size() || pi < part_rows.size()) && shown < max_diff)
        {
            if (ri < ref_rows.size() && (pi >= part_rows.size() || ref_rows[ri] < part_rows[pi]))
            {
                message += fmt::format("  Only in reference: {}\n", ref_rows[ri]);
                ++ri;
                ++shown;
            }
            else if (pi < part_rows.size() && (ri >= ref_rows.size() || part_rows[pi] < ref_rows[ri]))
            {
                message += fmt::format("  Only in partitioned: {}\n", part_rows[pi]);
                ++pi;
                ++shown;
            }
            else
            {
                ++ri;
                ++pi;
            }
        }

        throw Exception(ErrorCodes::AST_FUZZER_ORACLE_MISMATCH, "{}", message);
    }

    LOG_TRACE(logger, "TLP WHERE oracle passed ({} rows)", ref_rows.size());
    return true;
}


bool QueryOracleChecker::checkNoREC(const ASTSelectQuery & select, const ContextMutablePtr & context)
{
    if (!select.where())
        return false;

    if (!isSafeForOracle(select))
        return false;

    /// NoREC requires no aggregates, no GROUP BY, no HAVING
    /// (its count comparison is per-query, not per-group).
    if (hasAggregates(select) || select.groupBy() || select.having())
        return false;

    if (hasNonDeterministicFunctions(select.clone()))
        return false;

    ASTPtr predicate = select.where()->clone();

    /// Optimized: SELECT count() FROM (<original_with_where>)
    auto opt_ast = select.clone();
    auto & opt_select = opt_ast->as<ASTSelectQuery &>();
    stripOrderAndLimit(opt_select);
    String opt_inner_sql = formatAST(opt_ast);

    String opt_sql = fmt::format("SELECT count() FROM ({})", opt_inner_sql);
    if (opt_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    /// Unoptimized: SELECT countIf(<cond>) FROM (<original_without_where>)
    auto unopt_ast = select.clone();
    auto & unopt_select = unopt_ast->as<ASTSelectQuery &>();
    unopt_select.setExpression(ASTSelectQuery::Expression::WHERE, {});
    stripOrderAndLimit(unopt_select);

    /// Replace SELECT list with countIf(<predicate>)
    auto count_if = makeASTFunction("countIf", predicate->clone());
    auto new_select_list = make_intrusive<ASTExpressionList>();
    new_select_list->children.push_back(std::move(count_if));
    unopt_select.setExpression(ASTSelectQuery::Expression::SELECT, std::move(new_select_list));

    String unopt_sql = formatAST(unopt_ast);
    if (unopt_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleChecks);

    LOG_TRACE(logger, "NoREC oracle: optimized query: {}", opt_sql);
    LOG_TRACE(logger, "NoREC oracle: unoptimized query: {}", unopt_sql);

    Field opt_count = executeScalar(opt_sql, context);
    Field unopt_count = executeScalar(unopt_sql, context);

    if (opt_count != unopt_count)
    {
        ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleMismatches);

        throw Exception(ErrorCodes::AST_FUZZER_ORACLE_MISMATCH,
            "NoREC oracle mismatch!\n"
            "Optimized query (count={}): {}\n"
            "Unoptimized query (count={}): {}",
            opt_count, opt_sql,
            unopt_count, unopt_sql);
    }

    LOG_TRACE(logger, "NoREC oracle passed (count={})", opt_count);
    return true;
}


bool QueryOracleChecker::checkTLPDistinct(const ASTSelectQuery & select, const ContextMutablePtr & context)
{
    /// TLP DISTINCT: for queries with DISTINCT, use UNION (not UNION ALL) to deduplicate partitions.
    /// Reference: SELECT DISTINCT ... FROM t (no WHERE)
    /// Partitioned: SELECT DISTINCT ... WHERE p UNION SELECT DISTINCT ... WHERE NOT p UNION SELECT DISTINCT ... WHERE isNull(p)
    if (!select.where())
        return false;

    /// This oracle specifically requires DISTINCT and no GROUP BY/aggregates.
    if (!select.distinct)
        return false;

    /// Use the common safety checks but skip the distinct check (we want it).
    /// The `arrayJoin(...)` *function* multiplies rows just like the ARRAY JOIN
    /// clause; partitioning by WHERE then breaks the row-count invariant the
    /// oracle relies on. `isSafeForOracle` rejects both — mirror that here.
    if (hasArrayJoin(select) || hasPasteJoin(select))
        return false;
    if (select.select() && hasArrayJoinFunction(select.select()))
        return false;
    if (select.where() && hasArrayJoinFunction(select.where()))
        return false;
    if (select.limitLength() || select.limitBy() || select.prewhere() || select.qualify())
        return false;
    if (!select.tables())
        return false;
    if (select.group_by_with_rollup || select.group_by_with_cube
        || select.group_by_with_totals || select.group_by_with_grouping_sets)
        return false;
    if (hasAggregates(select) || select.groupBy() || select.having())
        return false;

    /// Window functions are never safe for oracle testing.
    if (select.select())
    {
        SafeAggregateScan data;
        scanAggregatesSafe(select.select(), data);
        if (!data.window_functions.empty())
            return false;
    }

    if (hasNonDeterministicFunctions(select.clone()))
        return false;

    ASTPtr predicate = select.where()->clone();

    /// Reference: remove WHERE, keep DISTINCT.
    auto ref_ast = select.clone();
    auto & ref_select = ref_ast->as<ASTSelectQuery &>();
    ref_select.setExpression(ASTSelectQuery::Expression::WHERE, {});
    stripOrderAndLimit(ref_select);
    String ref_sql = formatAST(ref_ast);
    if (ref_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    /// Build 3 partitioned queries — each keeps DISTINCT.
    auto clone1_ast = select.clone();
    stripOrderAndLimit(clone1_ast->as<ASTSelectQuery &>());

    auto clone2_ast = select.clone();
    clone2_ast->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::WHERE, makeASTFunction("not", predicate->clone()));
    stripOrderAndLimit(clone2_ast->as<ASTSelectQuery &>());

    auto clone3_ast = select.clone();
    clone3_ast->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::WHERE, makeASTFunction("isNull", predicate->clone()));
    stripOrderAndLimit(clone3_ast->as<ASTSelectQuery &>());

    /// Use UNION DISTINCT (not UNION ALL) to deduplicate across partitions.
    auto list = make_intrusive<ASTExpressionList>();
    list->children.push_back(clone1_ast);
    list->children.push_back(clone2_ast);
    list->children.push_back(clone3_ast);

    auto union_query = make_intrusive<ASTSelectWithUnionQuery>();
    union_query->union_mode = SelectUnionMode::UNION_DISTINCT;
    union_query->is_normalized = true;
    union_query->list_of_selects = list;
    union_query->children.push_back(list);

    ASTPtr union_ast = union_query;
    String union_sql = formatAST(union_ast);
    if (union_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleChecks);
    LOG_TRACE(logger, "TLP DISTINCT oracle: reference: {}", ref_sql);
    LOG_TRACE(logger, "TLP DISTINCT oracle: partitioned: {}", union_sql);

    auto ref_rows_opt = executeAndCollectSortedRows(ref_sql, context);
    auto part_rows_opt = executeAndCollectSortedRows(union_sql, context);
    if (!ref_rows_opt || !part_rows_opt)
        return false; /// Output exceeded MAX_ORACLE_OUTPUT_SIZE — skip.
    auto & ref_rows = *ref_rows_opt;
    auto & part_rows = *part_rows_opt;

    if (ref_rows != part_rows)
    {
        ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleMismatches);
        throw Exception(ErrorCodes::AST_FUZZER_ORACLE_MISMATCH,
            "TLP DISTINCT oracle mismatch!\n"
            "Reference query ({} rows): {}\n"
            "Partitioned query ({} rows): {}",
            ref_rows.size(), ref_sql,
            part_rows.size(), union_sql);
    }

    LOG_TRACE(logger, "TLP DISTINCT oracle passed ({} rows)", ref_rows.size());
    return true;
}


bool QueryOracleChecker::checkTLPGroupBy(const ASTSelectQuery & select, const ContextMutablePtr & context)
{
    /// TLP GROUP BY: for queries with GROUP BY and no aggregates in SELECT,
    /// the SELECT list equals the GROUP BY columns (like DISTINCT).
    /// We deduplicate both sides and compare as sets.
    if (!select.where())
        return false;
    if (!select.groupBy())
        return false;
    if (hasAggregates(select) || select.having())
        return false;

    if (!isSafeForOracle(select))
        return false;

    if (hasNonDeterministicFunctions(select.clone()))
        return false;

    ASTPtr predicate = select.where()->clone();

    /// Reference: remove WHERE, keep GROUP BY.
    auto ref_ast = select.clone();
    auto & ref_select = ref_ast->as<ASTSelectQuery &>();
    ref_select.setExpression(ASTSelectQuery::Expression::WHERE, {});
    stripOrderAndLimit(ref_select);
    String ref_sql = formatAST(ref_ast);
    if (ref_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    /// Build 3 partitioned queries — each keeps GROUP BY.
    auto clone1_ast = select.clone();
    stripOrderAndLimit(clone1_ast->as<ASTSelectQuery &>());

    auto clone2_ast = select.clone();
    clone2_ast->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::WHERE, makeASTFunction("not", predicate->clone()));
    stripOrderAndLimit(clone2_ast->as<ASTSelectQuery &>());

    auto clone3_ast = select.clone();
    clone3_ast->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::WHERE, makeASTFunction("isNull", predicate->clone()));
    stripOrderAndLimit(clone3_ast->as<ASTSelectQuery &>());

    auto list = make_intrusive<ASTExpressionList>();
    list->children.push_back(clone1_ast);
    list->children.push_back(clone2_ast);
    list->children.push_back(clone3_ast);

    auto union_query = make_intrusive<ASTSelectWithUnionQuery>();
    union_query->union_mode = SelectUnionMode::UNION_ALL;
    union_query->is_normalized = true;
    union_query->list_of_selects = list;
    union_query->children.push_back(list);

    ASTPtr union_ast = union_query;
    String union_sql = formatAST(union_ast);
    if (union_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleChecks);
    LOG_TRACE(logger, "TLP GROUP BY oracle: reference: {}", ref_sql);
    LOG_TRACE(logger, "TLP GROUP BY oracle: partitioned: {}", union_sql);

    /// Compare as sets — deduplicate both sides since each partition produces its own groups.
    auto ref_rows_opt = executeAndCollectSortedUniqueRows(ref_sql, context);
    auto part_rows_opt = executeAndCollectSortedUniqueRows(union_sql, context);
    if (!ref_rows_opt || !part_rows_opt)
        return false; /// Output exceeded MAX_ORACLE_OUTPUT_SIZE — skip.
    auto & ref_rows = *ref_rows_opt;
    auto & part_rows = *part_rows_opt;

    if (ref_rows != part_rows)
    {
        ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleMismatches);
        throw Exception(ErrorCodes::AST_FUZZER_ORACLE_MISMATCH,
            "TLP GROUP BY oracle mismatch!\n"
            "Reference query ({} unique rows): {}\n"
            "Partitioned query ({} unique rows): {}",
            ref_rows.size(), ref_sql,
            part_rows.size(), union_sql);
    }

    LOG_TRACE(logger, "TLP GROUP BY oracle passed ({} unique rows)", ref_rows.size());
    return true;
}


bool QueryOracleChecker::checkTLPHaving(const ASTSelectQuery & select, const ContextMutablePtr & context)
{
    /// TLP HAVING: for queries with GROUP BY and HAVING, partition on HAVING instead of WHERE.
    /// Reference: SELECT ... GROUP BY g (no HAVING)
    /// Partitioned: SELECT ... GROUP BY g HAVING p UNION ALL ... HAVING NOT p UNION ALL ... HAVING isNull(p)
    /// Compare as sets (deduplicated) since each partition independently groups.
    if (!select.having())
        return false;
    if (!select.groupBy())
        return false;

    if (!isSafeForOracle(select))
        return false;

    if (hasNonDeterministicFunctions(select.clone()))
        return false;

    ASTPtr having_pred = select.having()->clone();

    /// Reference: remove HAVING, keep GROUP BY and everything else.
    auto ref_ast = select.clone();
    auto & ref_select = ref_ast->as<ASTSelectQuery &>();
    ref_select.setExpression(ASTSelectQuery::Expression::HAVING, {});
    stripOrderAndLimit(ref_select);
    String ref_sql = formatAST(ref_ast);
    if (ref_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    /// Build 3 partitioned queries — partition on HAVING.
    auto clone1_ast = select.clone();
    stripOrderAndLimit(clone1_ast->as<ASTSelectQuery &>());

    auto clone2_ast = select.clone();
    clone2_ast->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::HAVING, makeASTFunction("not", having_pred->clone()));
    stripOrderAndLimit(clone2_ast->as<ASTSelectQuery &>());

    auto clone3_ast = select.clone();
    clone3_ast->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::HAVING, makeASTFunction("isNull", having_pred->clone()));
    stripOrderAndLimit(clone3_ast->as<ASTSelectQuery &>());

    auto list = make_intrusive<ASTExpressionList>();
    list->children.push_back(clone1_ast);
    list->children.push_back(clone2_ast);
    list->children.push_back(clone3_ast);

    auto union_query = make_intrusive<ASTSelectWithUnionQuery>();
    union_query->union_mode = SelectUnionMode::UNION_ALL;
    union_query->is_normalized = true;
    union_query->list_of_selects = list;
    union_query->children.push_back(list);

    ASTPtr union_ast = union_query;
    String union_sql = formatAST(union_ast);
    if (union_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleChecks);
    LOG_TRACE(logger, "TLP HAVING oracle: reference: {}", ref_sql);
    LOG_TRACE(logger, "TLP HAVING oracle: partitioned: {}", union_sql);

    /// Compare as sets — HAVING partitions produce independent group sets.
    auto ref_rows_opt = executeAndCollectSortedUniqueRows(ref_sql, context);
    auto part_rows_opt = executeAndCollectSortedUniqueRows(union_sql, context);
    if (!ref_rows_opt || !part_rows_opt)
        return false; /// Output exceeded MAX_ORACLE_OUTPUT_SIZE — skip.
    auto & ref_rows = *ref_rows_opt;
    auto & part_rows = *part_rows_opt;

    if (ref_rows != part_rows)
    {
        ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleMismatches);
        throw Exception(ErrorCodes::AST_FUZZER_ORACLE_MISMATCH,
            "TLP HAVING oracle mismatch!\n"
            "Reference query ({} unique rows): {}\n"
            "Partitioned query ({} unique rows): {}",
            ref_rows.size(), ref_sql,
            part_rows.size(), union_sql);
    }

    LOG_TRACE(logger, "TLP HAVING oracle passed ({} unique rows)", ref_rows.size());
    return true;
}


bool QueryOracleChecker::checkDQP(const ASTSelectQuery & select, const ContextMutablePtr & context)
{
    /// DQP (Differential Query Plans): run the same query with different optimizer settings.
    /// If results differ, an optimization is producing wrong results.
    /// Only run with ~10% probability to avoid excessive overhead.
    if (thread_local_rng() % 10 != 0)
        return false;

    if (!isSafeForOracle(select))
        return false;

    if (hasNonDeterministicFunctions(select.clone()))
        return false;

    auto query_ast = select.clone();
    stripOrderAndLimit(query_ast->as<ASTSelectQuery &>());
    String query_sql = formatAST(query_ast);
    if (query_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    /// Execute with default settings.
    auto default_rows_opt = executeAndCollectSortedRows(query_sql, context);
    if (!default_rows_opt)
        return false; /// Output exceeded MAX_ORACLE_OUTPUT_SIZE — skip.
    auto & default_rows = *default_rows_opt;

    /// Skip empty results — DQP is most valuable for non-empty results.
    if (default_rows.empty())
        return false;

    /// Settings pairs to toggle. Each pair flips an optimizer setting.
    static const std::vector<std::pair<String, Field>> settings_variants[] = {
        {{"optimize_read_in_order", Field(UInt64(0))}},
        {{"optimize_aggregation_in_order", Field(UInt64(0))}},
        {{"optimize_trivial_count_query", Field(false)}},
        {{"optimize_move_to_prewhere", Field(false)}},
        {{"query_plan_remove_redundant_sorting", Field(false)}},
        {{"optimize_rewrite_sum_if_to_count_if", Field(false)}},
        /// `enable_optimize_predicate_expression` is unconditionally `false` in
        /// `makeOracleContext`, so toggling it here would be a no-op.
        {{"optimize_if_chain_to_multiif", Field(false)}},
        {{"optimize_if_transform_strings_to_enum", Field(false)}},
        {{"optimize_functions_to_subcolumns", Field(false)}},
        {{"optimize_normalize_count_variants", Field(false)}},
        {{"optimize_injective_functions_inside_uniq", Field(false)}},
        {{"optimize_substitute_columns", Field(false)}},
        {{"query_plan_enable_optimizations", Field(false)}},
    };

    /// Pick one random settings variant.
    size_t variant_idx = thread_local_rng() % std::size(settings_variants);
    const auto & settings = settings_variants[variant_idx];

    ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleChecks);

    String setting_name = settings[0].first;
    LOG_TRACE(logger, "DQP oracle: query: {}, toggling: {}", query_sql, setting_name);

    try
    {
        auto variant_rows_opt = executeWithSettings(query_sql, context, settings);
        if (!variant_rows_opt)
            return false; /// Output exceeded MAX_ORACLE_OUTPUT_SIZE — skip this oracle run.
        auto & variant_rows = *variant_rows_opt;

        if (default_rows != variant_rows)
        {
            ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleMismatches);
            throw Exception(ErrorCodes::AST_FUZZER_ORACLE_MISMATCH,
                "DQP oracle mismatch! Setting: {}\n"
                "Default ({} rows): {}\n"
                "With {}=off ({} rows): {}",
                setting_name,
                default_rows.size(), query_sql,
                setting_name, variant_rows.size(), query_sql);
        }
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::AST_FUZZER_ORACLE_MISMATCH)
            throw;
        /// The variant query might fail with a different error — that's OK.
        LOG_TRACE(logger, "DQP oracle: variant query failed (expected): {}", e.message());
        return false;
    }

    LOG_TRACE(logger, "DQP oracle passed ({} rows, setting: {})", default_rows.size(), setting_name);
    return true;
}


bool QueryOracleChecker::checkTLPAggregate(const ASTSelectQuery & select, const ContextMutablePtr & context)
{
    if (!select.where())
        return false;

    if (!isSafeForOracle(select))
        return false;

    if (!hasAggregates(select))
        return false;

    /// Skip queries with HAVING — the TLP transformation cannot push HAVING into
    /// the partitioned inner queries because HAVING filters on aggregate results,
    /// which are only correct after merging all partitions.
    if (select.having())
        return false;

    if (hasNonDeterministicFunctions(select.clone()))
        return false;

    /// Collect aggregate functions from the SELECT list.
    SafeAggregateScan agg_data;
    scanAggregatesSafe(select.select(), agg_data);
    if (agg_data.aggregates.empty())
        return false;

    /// Aggregates that are non-associative on floating-point inputs (`sum`,
    /// `avg`, variance/stddev/covariance/correlation, etc.) can legitimately
    /// produce different rounded values between direct evaluation and the
    /// metamorphic `aggState`/`aggMerge` partition-then-combine path, because
    /// `Float32`/`Float64` addition is not associative. Result comparison is
    /// exact row equality, so allowing them yields false oracle mismatches.
    /// We can't see argument types at the AST level, so blanket-reject the
    /// names that are known to be float-sensitive.
    static const std::unordered_set<String> non_associative_aggregates = {
        "sum", "sumKahan", "sumWithOverflow",
        "avg", "avgWeighted",
        "stddevPop", "stddevSamp", "stddevPopStable", "stddevSampStable",
        "varPop", "varSamp", "varPopStable", "varSampStable",
        "covarPop", "covarSamp", "covarPopStable", "covarSampStable",
        "corr", "corrStable",
        "skewPop", "skewSamp",
        "kurtPop", "kurtSamp",
    };

    /// Skip aggregates that already have combinators (e.g. sumIf, countIf,
    /// avgArray, etc.) — appending State to these produces double-combinator
    /// names that may not resolve correctly.
    for (const auto & aggregate_ast : agg_data.aggregates)
    {
        const auto * agg_func = aggregate_ast->as<ASTFunction>();
        if (!agg_func)
            return false;
        const auto & name = agg_func->name;
        if (non_associative_aggregates.contains(name))
            return false;
        /// Check for common combinator suffixes.
        if (name.ends_with("If") || name.ends_with("Array") || name.ends_with("Map")
            || name.ends_with("State") || name.ends_with("Merge")
            || name.ends_with("ForEach") || name.ends_with("Distinct")
            || name.ends_with("OrDefault") || name.ends_with("OrNull")
            || name.ends_with("Resample") || name.ends_with("ArgMin")
            || name.ends_with("ArgMax")
            || name.ends_with("RespectNulls") || name.ends_with("IgnoreNulls"))
            return false;
    }

    /// Every SELECT-list expression must be EITHER an exact aggregate from the
    /// collected list (we'll rewrite it as `aggMerge(_s_N)`) OR a bare GROUP BY
    /// expression (we'll pass it through unchanged). If an aggregate is nested
    /// inside a non-aggregate expression (e.g. `plus(count(), 1)`), the
    /// per-expression rewrite at line ~1069 won't find a top-level match and
    /// the outer query ends up running the aggregate over the UNION ALL of
    /// already-grouped rows, producing wrong results. Reject such queries.
    {
        std::unordered_set<UInt64> aggregate_hashes;
        for (const auto & agg : agg_data.aggregates)
            aggregate_hashes.insert(agg->getTreeHash(/*ignore_aliases=*/true).low64);

        std::unordered_set<UInt64> group_by_hashes;
        if (select.groupBy())
            for (const auto & g : select.groupBy()->children)
                group_by_hashes.insert(g->getTreeHash(/*ignore_aliases=*/true).low64);

        for (const auto & select_expr : select.select()->children)
        {
            UInt64 h = select_expr->getTreeHash(/*ignore_aliases=*/true).low64;
            if (aggregate_hashes.contains(h) || group_by_hashes.contains(h))
                continue;
            return false;
        }
    }

    ASTPtr predicate = select.where()->clone();

    /// Build the reference query: remove WHERE, keep everything else.
    auto ref_ast = select.clone();
    auto & ref_select = ref_ast->as<ASTSelectQuery &>();
    ref_select.setExpression(ASTSelectQuery::Expression::WHERE, {});
    stripOrderAndLimit(ref_select);
    String ref_sql = formatAST(ref_ast);
    if (ref_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    /// Build the inner SELECT for partitioned subqueries:
    /// Replace each agg(args) with aggState(args) AS _s_N.
    /// Keep GROUP BY columns in the SELECT list so the outer query can group by them.
    auto inner_ast = select.clone();
    auto & inner_select = inner_ast->as<ASTSelectQuery &>();
    stripOrderAndLimit(inner_select);
    inner_select.setExpression(ASTSelectQuery::Expression::HAVING, {});

    bool has_group_by = inner_select.groupBy() != nullptr;

    /// First pass: assign aliases to each aggregate function.
    /// Build a map from aggregate AST pointer to (state_alias, merge_func).
    std::unordered_map<const IAST *, String> agg_to_alias;
    size_t state_idx = 0;
    for (const auto & aggregate_ast : agg_data.aggregates)
    {
        const auto * agg_func = aggregate_ast->as<ASTFunction>();
        if (!agg_func)
            return false;
        agg_to_alias[agg_func] = fmt::format("_s_{}", state_idx);
        ++state_idx;
    }

    /// Build inner SELECT list: GROUP BY columns first (needed for outer GROUP BY),
    /// then aggState(args) AS _s_N for each aggregate.
    auto new_inner_select_list = make_intrusive<ASTExpressionList>();
    if (has_group_by)
    {
        for (const auto & group_expr : inner_select.groupBy()->children)
            new_inner_select_list->children.push_back(group_expr->clone());
    }
    for (const auto & aggregate_ast : agg_data.aggregates)
    {
        const auto * agg_func = aggregate_ast->as<ASTFunction>();
        String alias = agg_to_alias[agg_func];

        auto state_func_ast = agg_func->clone();
        auto & state_func = state_func_ast->as<ASTFunction &>();
        state_func.name = agg_func->name + "State";
        state_func.setAlias(alias);
        new_inner_select_list->children.push_back(std::move(state_func_ast));
    }

    /// Build the outer SELECT list preserving original column order.
    /// Walk the original SELECT list: for each expression, check if it's an
    /// aggregate (replace with aggMerge) or a non-aggregate (pass through).
    auto outer_select_list = make_intrusive<ASTExpressionList>();
    for (const auto & select_expr : select.select()->children)
    {
        /// Check if this expression is one of the collected aggregates.
        bool is_aggregate = false;
        for (const auto & aggregate_ast : agg_data.aggregates)
        {
            if (select_expr.get() == aggregate_ast.get()
                || select_expr->getTreeHash(/*ignore_aliases=*/true) == aggregate_ast->getTreeHash(/*ignore_aliases=*/true))
            {
                const auto * agg_func = aggregate_ast->as<ASTFunction>();
                String alias = agg_to_alias[agg_func];
                auto merge_func = makeASTFunction(agg_func->name + "Merge", make_intrusive<ASTIdentifier>(alias));
                outer_select_list->children.push_back(std::move(merge_func));
                is_aggregate = true;
                break;
            }
        }
        if (!is_aggregate)
            outer_select_list->children.push_back(select_expr->clone());
    }

    inner_select.setExpression(ASTSelectQuery::Expression::SELECT, std::move(new_inner_select_list));

    /// Build three partitioned inner queries.
    auto inner1 = inner_ast->clone();
    /// inner1 keeps the original WHERE

    auto inner2 = inner_ast->clone();
    inner2->as<ASTSelectQuery &>().setExpression(
        ASTSelectQuery::Expression::WHERE, makeASTFunction("not", predicate->clone()));

    auto inner3 = inner_ast->clone();
    inner3->as<ASTSelectQuery &>().setExpression(
        ASTSelectQuery::Expression::WHERE, makeASTFunction("isNull", predicate->clone()));

    /// Build UNION ALL.
    auto union_list = make_intrusive<ASTExpressionList>();
    union_list->children.push_back(inner1);
    union_list->children.push_back(inner2);
    union_list->children.push_back(inner3);

    auto union_query = make_intrusive<ASTSelectWithUnionQuery>();
    union_query->union_mode = SelectUnionMode::UNION_ALL;
    union_query->is_normalized = true;
    union_query->list_of_selects = union_list;
    union_query->children.push_back(union_list);

    /// Build the outer query: SELECT aggMerge(_s_N), ... FROM (UNION ALL) [GROUP BY g]
    String union_sql = formatAST(ASTPtr(union_query));

    /// Build outer query as string — easier than AST construction for a subquery FROM.
    String outer_select_str;
    {
        WriteBufferFromOwnString buf;
        outer_select_list->format(buf, IAST::FormatSettings(/*one_line=*/true));
        outer_select_str = buf.str();
    }

    String group_by_str;
    if (has_group_by)
    {
        WriteBufferFromOwnString buf;
        inner_select.groupBy()->format(buf, IAST::FormatSettings(/*one_line=*/true));
        group_by_str = fmt::format(" GROUP BY {}", buf.str());
    }

    String metamorphic_sql = fmt::format(
        "SELECT {} FROM ({}){}",
        outer_select_str, union_sql, group_by_str);

    if (metamorphic_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleChecks);

    LOG_TRACE(logger, "TLP Aggregate oracle: reference query: {}", ref_sql);
    LOG_TRACE(logger, "TLP Aggregate oracle: metamorphic query: {}", metamorphic_sql);

    /// Compare full sorted row content. The State/Merge path should produce
    /// identical results for deterministic aggregates (which we already filter for).
    auto ref_rows_opt = executeAndCollectSortedRows(ref_sql, context);
    auto meta_rows_opt = executeAndCollectSortedRows(metamorphic_sql, context);
    if (!ref_rows_opt || !meta_rows_opt)
        return false; /// Output exceeded MAX_ORACLE_OUTPUT_SIZE — skip.
    auto & ref_rows = *ref_rows_opt;
    auto & meta_rows = *meta_rows_opt;

    if (ref_rows != meta_rows)
    {
        ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleMismatches);

        throw Exception(ErrorCodes::AST_FUZZER_ORACLE_MISMATCH,
            "TLP Aggregate oracle mismatch!\n"
            "Reference query ({} rows): {}\n"
            "Metamorphic query ({} rows): {}",
            ref_rows.size(), ref_sql,
            meta_rows.size(), metamorphic_sql);
    }

    LOG_TRACE(logger, "TLP Aggregate oracle passed ({} rows, {} aggregates)", ref_rows.size(), state_idx);
    return true;
}


bool QueryOracleChecker::checkIdentityWhere(const ASTSelectQuery & select, const ContextMutablePtr & context)
{
    /// Metamorphic identity oracle.
    /// Verifies that equivalent WHERE predicates produce identical results.
    ///
    /// This works for any SELECT with WHERE — even queries with LIMIT, DISTINCT,
    /// GROUP BY, HAVING, or aggregates. We don't change query structure, only
    /// rewrite the WHERE predicate in a provably-equivalent way.
    ///
    /// Requires ORDER BY for deterministic comparison when query has LIMIT, since
    /// LIMIT is order-dependent. Otherwise sorts results for set comparison.

    if (!select.where())
        return false;
    if (!select.tables())
        return false;

    /// PREWHERE + WHERE interactions produce false positives due to type coercions
    /// that behave differently under fuzzer-relaxed settings.
    if (select.prewhere())
        return false;

    /// WITH CUBE/ROLLUP/GROUPING SETS combined with WHERE predicate rewriting
    /// can produce false positives due to interactions with correlated subqueries
    /// and the multi-way grouping.
    if (select.group_by_with_rollup || select.group_by_with_cube
        || select.group_by_with_totals || select.group_by_with_grouping_sets)
        return false;

    /// ARRAY JOIN / PASTE JOIN are safe here because we don't change structure.
    /// But window functions and non-deterministic results aren't reproducible.
    if (hasNonDeterministicFunctions(select.clone()))
        return false;

    /// `hasNonDeterministicFunctions` is name-based and does not filter general
    /// window functions like `row_number()`, `rank()`, `dense_rank()` over a
    /// window without an explicit ORDER BY. Those produce implementation-defined
    /// row numbers; rewriting the WHERE predicate is permitted to reorder rows
    /// seen by the window function, which legitimately changes the assignment.
    /// Use the same gate as `checkSubqueryWrap` (issue #105743).
    if (select.select() && hasWindowFunctionWithoutOrderBy(select.select()))
        return false;

    /// LIMIT is unsafe even with ORDER BY: if the sort key is not unique the
    /// engine can legitimately pick different rows among ties for the reference
    /// vs the rewritten predicate, producing a spurious "mismatch". Forbid LIMIT
    /// entirely for Identity WHERE.
    if (select.limitLength())
        return false;
    /// `LIMIT BY` is order-sensitive in the same way: for non-unique ordering
    /// equivalent predicates can legitimately pick different rows among ties.
    if (select.limitBy())
        return false;

    ASTPtr predicate = select.where()->clone();

    /// Build reference query: original.
    auto ref_ast = select.clone();
    /// Strip any inline `SETTINGS` clause the fuzzed query may carry — otherwise
    /// it overrides the guard rails set by `makeOracleContext` (notably
    /// `max_result_rows`, `max_result_bytes`, `result_overflow_mode`), so the
    /// oracle could compare truncated outputs and surface false mismatches.
    ref_ast->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::SETTINGS, {});
    String ref_sql = formatAST(ref_ast);
    if (ref_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    /// Variant 1: WHERE NOT(NOT(p)) — tests NOT handling.
    auto v1_ast = select.clone();
    auto & v1 = v1_ast->as<ASTSelectQuery &>();
    v1.setExpression(ASTSelectQuery::Expression::SETTINGS, {});
    v1.setExpression(ASTSelectQuery::Expression::WHERE,
        makeASTFunction("not", makeASTFunction("not", predicate->clone())));
    String v1_sql = formatAST(v1_ast);
    if (v1_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    /// Variant 2: WHERE (p) AND (1) — tests constant-AND folding.
    auto v2_ast = select.clone();
    auto & v2 = v2_ast->as<ASTSelectQuery &>();
    v2.setExpression(ASTSelectQuery::Expression::SETTINGS, {});
    v2.setExpression(ASTSelectQuery::Expression::WHERE,
        makeASTFunction("and", predicate->clone(), make_intrusive<ASTLiteral>(Field(UInt8(1)))));
    String v2_sql = formatAST(v2_ast);
    if (v2_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    /// Variant 3: WHERE (p) OR (0) — tests constant-OR folding.
    auto v3_ast = select.clone();
    auto & v3 = v3_ast->as<ASTSelectQuery &>();
    v3.setExpression(ASTSelectQuery::Expression::SETTINGS, {});
    v3.setExpression(ASTSelectQuery::Expression::WHERE,
        makeASTFunction("or", predicate->clone(), make_intrusive<ASTLiteral>(Field(UInt8(0)))));
    String v3_sql = formatAST(v3_ast);
    if (v3_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleChecks);

    LOG_TRACE(logger, "Identity WHERE oracle: reference: {}", ref_sql);
    LOG_TRACE(logger, "Identity WHERE oracle: variant NOT(NOT): {}", v1_sql);
    LOG_TRACE(logger, "Identity WHERE oracle: variant AND 1: {}", v2_sql);
    LOG_TRACE(logger, "Identity WHERE oracle: variant OR 0: {}", v3_sql);

    auto ref_rows_opt = executeAndCollectSortedRows(ref_sql, context);
    auto v1_rows_opt = executeAndCollectSortedRows(v1_sql, context);
    auto v2_rows_opt = executeAndCollectSortedRows(v2_sql, context);
    auto v3_rows_opt = executeAndCollectSortedRows(v3_sql, context);
    if (!ref_rows_opt || !v1_rows_opt || !v2_rows_opt || !v3_rows_opt)
        return false; /// Output exceeded MAX_ORACLE_OUTPUT_SIZE — skip.
    auto & ref_rows = *ref_rows_opt;
    auto & v1_rows = *v1_rows_opt;
    auto & v2_rows = *v2_rows_opt;
    auto & v3_rows = *v3_rows_opt;

    auto check_variant = [&](const String & name, const String & sql, const std::vector<String> & rows)
    {
        if (ref_rows != rows)
        {
            ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleMismatches);
            throw Exception(ErrorCodes::AST_FUZZER_ORACLE_MISMATCH,
                "Identity WHERE ({}) oracle mismatch!\n"
                "Reference query ({} rows): {}\n"
                "Variant query ({} rows): {}",
                name, ref_rows.size(), ref_sql, rows.size(), sql);
        }
    };

    check_variant("NOT(NOT p)", v1_sql, v1_rows);
    check_variant("p AND 1", v2_sql, v2_rows);
    check_variant("p OR 0", v3_sql, v3_rows);

    LOG_TRACE(logger, "Identity WHERE oracle passed ({} rows, 3 variants)", ref_rows.size());
    return true;
}


bool QueryOracleChecker::checkSubqueryWrap(const ASTSelectQuery & select, const ContextMutablePtr & context)
{
    /// Subquery pushdown oracle.
    /// Verifies that `SELECT ... FROM t WHERE p` equals
    /// `SELECT ... FROM (<original>) ORDER BY ... LIMIT ...` when the outer has no WHERE.
    ///
    /// Simpler formulation: wrap the entire query as a subquery with SELECT * outside.
    /// Result should be identical (stripped to set semantics since ORDER may be lost).

    if (!select.tables())
        return false;
    if (hasNonDeterministicFunctions(select.clone()))
        return false;

    /// PREWHERE produces false positives with suspicious type coercions.
    if (select.prewhere())
        return false;

    /// Skip if query has LIMIT without ORDER BY (non-deterministic row selection).
    if (select.limitLength() && !select.orderBy())
        return false;

    /// Skip WITH TOTALS / ROLLUP / CUBE / GROUPING SETS — the wrapping changes
    /// which columns are visible in the outer SELECT and the modifier semantics.
    if (select.group_by_with_rollup || select.group_by_with_cube
        || select.group_by_with_totals || select.group_by_with_grouping_sets)
        return false;

    /// `row_number()`/`rank()`/etc. over a window without an explicit ORDER BY
    /// produce implementation-defined row numbers — wrapping the query in
    /// `SELECT * FROM (...)` is permitted to reorder rows seen by the window
    /// function, which legitimately changes the assignment. We can't tell that
    /// apart from a real mismatch without scope resolution. Conservatively
    /// skip any query that contains such a window function (issue #105743).
    if (select.select() && hasWindowFunctionWithoutOrderBy(select.select()))
        return false;

    auto ref_ast = select.clone();
    stripOrderAndLimit(ref_ast->as<ASTSelectQuery &>());
    String ref_sql = formatAST(ref_ast);
    if (ref_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    String wrapped_sql = fmt::format("SELECT * FROM ({})", ref_sql);
    if (wrapped_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleChecks);

    LOG_TRACE(logger, "Subquery wrap oracle: reference: {}", ref_sql);
    LOG_TRACE(logger, "Subquery wrap oracle: wrapped: {}", wrapped_sql);

    auto ref_rows_opt = executeAndCollectSortedRows(ref_sql, context);
    auto wrapped_rows_opt = executeAndCollectSortedRows(wrapped_sql, context);
    if (!ref_rows_opt || !wrapped_rows_opt)
        return false; /// Output exceeded MAX_ORACLE_OUTPUT_SIZE — skip.
    auto & ref_rows = *ref_rows_opt;
    auto & wrapped_rows = *wrapped_rows_opt;

    if (ref_rows != wrapped_rows)
    {
        ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleMismatches);
        throw Exception(ErrorCodes::AST_FUZZER_ORACLE_MISMATCH,
            "Subquery wrap oracle mismatch!\n"
            "Reference query ({} rows): {}\n"
            "Wrapped query ({} rows): {}",
            ref_rows.size(), ref_sql,
            wrapped_rows.size(), wrapped_sql);
    }

    LOG_TRACE(logger, "Subquery wrap oracle passed ({} rows)", ref_rows.size());
    return true;
}


void QueryOracleChecker::tryPopulateTable(const ASTSelectQuery & select, const ContextMutablePtr & context)
{
    /// With 80% probability, try to insert random data into all tables referenced by the query.
    /// This ensures the oracle checks non-empty results even when the fuzzer creates empty tables.
    if (thread_local_rng() % 5 == 0)
        return;

    ASTPtr tables = select.tables();
    if (!tables || tables->children.empty())
        return;

    /// Iterate over all table expressions (main table + joined tables).
    for (const auto & table_child : tables->children)
    {
        const auto * tables_element = table_child->as<ASTTablesInSelectQueryElement>();
        if (!tables_element || !tables_element->table_expression)
            continue;

        const auto * table_expr = tables_element->table_expression->as<ASTTableExpression>();
        if (!table_expr || !table_expr->database_and_table_name)
            continue;

        auto * table_id = table_expr->database_and_table_name->as<ASTTableIdentifier>();
        if (!table_id)
            continue;

        String database = table_id->getDatabaseName();
        String table = table_id->shortName();
        if (table.empty())
            continue;

        /// Skip system tables.
        if (database == "system" || database == "INFORMATION_SCHEMA" || database == "information_schema")
            continue;

        String qualified = database.empty() ? backQuoteIfNeed(table) : (backQuoteIfNeed(database) + "." + backQuoteIfNeed(table));

        /// Build an INSERT ... SELECT * FROM generateRandom(...) LIMIT 100 query.
        String db_for_query = database.empty() ? "currentDatabase()" : ("'" + database + "'");
        String insert_query = fmt::format(
            "INSERT INTO {} SELECT * FROM generateRandom("
            "(SELECT arrayStringConcat(groupArray(concat(name, ' ', type)), ', ') "
            "FROM system.columns WHERE database = {} AND table = '{}'), 1, 10) LIMIT 100",
            qualified, db_for_query, table);

        try
        {
            auto oracle_context = makeOracleContext(context);
            oracle_context->setDefaultFormat("Null");

            ReadBufferFromString istr(insert_query);
            WriteBufferFromOwnString ostr;
            executeQuery(istr, ostr, oracle_context, {}, QueryFlags{.internal = true});

            LOG_TRACE(logger, "Populated table {} with random data for oracle check", qualified);
        }
        catch (...)
        {
            LOG_TRACE(logger, "Failed to populate table {} (skipping): {}", qualified, getCurrentExceptionMessage(false));
        }
    }
}


bool QueryOracleChecker::check(const ASTPtr & query_ast, const ContextMutablePtr & context)
{
    const ASTSelectQuery * select = extractSimpleSelect(query_ast);
    if (!select)
    {
        LOG_TRACE(logger, "Oracle skip: not a simple SELECT");
        return false;
    }

    /// `system.*` and `INFORMATION_SCHEMA.*` are non-deterministic snapshots
    /// of live server state; reading them twice (reference + rewrite) almost
    /// always shows drift in `processes`, `merges`, `metric_log`, etc.
    /// Reject the whole query at the gate to avoid spurious mismatches in
    /// every oracle.
    if (referencesNonDeterministicDatabase(*select))
    {
        LOG_TRACE(logger, "Oracle skip: query reads from system database");
        return false;
    }

    /// FROM clause is a subquery containing a UNION — see
    /// `fromContainsUnionSubquery` for why this can't be checked reliably.
    if (fromContainsUnionSubquery(*select))
    {
        LOG_TRACE(logger, "Oracle skip: FROM is a subquery containing UNION");
        return false;
    }

    /// Log which features the query has, to understand oracle coverage.
    LOG_TRACE(logger, "Oracle candidate: WHERE={} GROUP_BY={} HAVING={} DISTINCT={} agg={} PREWHERE={} LIMIT={} tables={}",
        select->where() != nullptr,
        select->groupBy() != nullptr,
        select->having() != nullptr,
        select->distinct,
        hasAggregates(*select),
        select->prewhere() != nullptr,
        select->limitLength() != nullptr,
        select->tables() != nullptr);

    /// NOTE: tryPopulateTable is disabled — it inserts random data that can cause
    /// false positive mismatches when reference and partitioned queries observe
    /// different row counts due to concurrent inserts within the same check.
    /// The fuzzer corpus should provide tables with sufficient data instead.
    /// tryPopulateTable(*select, context);

    bool any_check_performed = false;

    /// TLP WHERE oracle
    try
    {
        if (checkTLPWhere(*select, context))
            any_check_performed = true;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::AST_FUZZER_ORACLE_MISMATCH)
            throw;
        LOG_TRACE(logger, "TLP WHERE oracle execution error (skipping): {}", e.message());
    }
    catch (...)
    {
        LOG_TRACE(logger, "TLP WHERE oracle execution error (skipping): {}", getCurrentExceptionMessage(false));
    }

    /// NoREC oracle
    try
    {
        if (checkNoREC(*select, context))
            any_check_performed = true;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::AST_FUZZER_ORACLE_MISMATCH)
            throw;
        LOG_TRACE(logger, "NoREC oracle execution error (skipping): {}", e.message());
    }
    catch (...)
    {
        LOG_TRACE(logger, "NoREC oracle execution error (skipping): {}", getCurrentExceptionMessage(false));
    }

    /// TLP Aggregate oracle (uses State/Merge combinators for any aggregate)
    try
    {
        if (checkTLPAggregate(*select, context))
            any_check_performed = true;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::AST_FUZZER_ORACLE_MISMATCH)
            throw;
        LOG_TRACE(logger, "TLP Aggregate oracle execution error (skipping): {}", e.message());
    }
    catch (...)
    {
        LOG_TRACE(logger, "TLP Aggregate oracle execution error (skipping): {}", getCurrentExceptionMessage(false));
    }

    /// TLP DISTINCT oracle (uses UNION DISTINCT instead of UNION ALL)
    try
    {
        if (checkTLPDistinct(*select, context))
            any_check_performed = true;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::AST_FUZZER_ORACLE_MISMATCH)
            throw;
        LOG_TRACE(logger, "TLP DISTINCT oracle execution error (skipping): {}", e.message());
    }
    catch (...)
    {
        LOG_TRACE(logger, "TLP DISTINCT oracle execution error (skipping): {}", getCurrentExceptionMessage(false));
    }

    /// TLP GROUP BY oracle (set comparison for non-aggregate GROUP BY)
    try
    {
        if (checkTLPGroupBy(*select, context))
            any_check_performed = true;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::AST_FUZZER_ORACLE_MISMATCH)
            throw;
        LOG_TRACE(logger, "TLP GROUP BY oracle execution error (skipping): {}", e.message());
    }
    catch (...)
    {
        LOG_TRACE(logger, "TLP GROUP BY oracle execution error (skipping): {}", getCurrentExceptionMessage(false));
    }

    /// TLP HAVING oracle (partitions on HAVING instead of WHERE)
    try
    {
        if (checkTLPHaving(*select, context))
            any_check_performed = true;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::AST_FUZZER_ORACLE_MISMATCH)
            throw;
        LOG_TRACE(logger, "TLP HAVING oracle execution error (skipping): {}", e.message());
    }
    catch (...)
    {
        LOG_TRACE(logger, "TLP HAVING oracle execution error (skipping): {}", getCurrentExceptionMessage(false));
    }

    /// DQP oracle (differential query plans — same query, different optimizer settings)
    try
    {
        if (checkDQP(*select, context))
            any_check_performed = true;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::AST_FUZZER_ORACLE_MISMATCH)
            throw;
        LOG_TRACE(logger, "DQP oracle execution error (skipping): {}", e.message());
    }
    catch (...)
    {
        LOG_TRACE(logger, "DQP oracle execution error (skipping): {}", getCurrentExceptionMessage(false));
    }

    /// Identity WHERE oracle (rewrites WHERE into equivalent forms — NOT(NOT p), p AND 1, p OR 0)
    try
    {
        if (checkIdentityWhere(*select, context))
            any_check_performed = true;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::AST_FUZZER_ORACLE_MISMATCH)
            throw;
        LOG_TRACE(logger, "Identity WHERE oracle execution error (skipping): {}", e.message());
    }
    catch (...)
    {
        LOG_TRACE(logger, "Identity WHERE oracle execution error (skipping): {}", getCurrentExceptionMessage(false));
    }

    /// Subquery wrap oracle (wraps original as subquery and verifies identical result)
    try
    {
        if (checkSubqueryWrap(*select, context))
            any_check_performed = true;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::AST_FUZZER_ORACLE_MISMATCH)
            throw;
        LOG_TRACE(logger, "Subquery wrap oracle execution error (skipping): {}", e.message());
    }
    catch (...)
    {
        LOG_TRACE(logger, "Subquery wrap oracle execution error (skipping): {}", getCurrentExceptionMessage(false));
    }

    return any_check_performed;
}

}
