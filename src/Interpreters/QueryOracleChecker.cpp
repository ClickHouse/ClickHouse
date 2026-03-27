#include <Interpreters/QueryOracleChecker.h>

#include <Common/ProfileEvents.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/GetAggregatesVisitor.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
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
extern const int LOGICAL_ERROR;
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
};

/// Maximum formatted query length for oracle sub-queries.
constexpr size_t MAX_ORACLE_QUERY_LENGTH = 10000;

/// Maximum total output size from an oracle sub-query (bytes).
constexpr size_t MAX_ORACLE_OUTPUT_SIZE = 10 * 1024 * 1024;

/// Walk an AST tree and check if any ASTFunction has a name in the given set.
bool hasNonDeterministicFunctionsImpl(const ASTPtr & ast)
{
    if (!ast)
        return false;

    if (const auto * func = ast->as<ASTFunction>())
    {
        if (non_deterministic_functions.contains(func->name))
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
    std::vector<String> rows;
    if (output.empty())
        return rows;

    size_t start = 0;
    while (start < output.size())
    {
        size_t end = output.find('\n', start);
        if (end == String::npos)
        {
            rows.push_back(output.substr(start));
            break;
        }
        if (end > start || end + 1 < output.size()) /// skip trailing empty line
            rows.push_back(output.substr(start, end - start));
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
    /// ARRAY JOIN and PASTE JOIN are NOT safe.
    if (hasArrayJoin(select) || hasPasteJoin(select))
        return false;
    if (select.distinct)
        return false;
    if (select.limitLength())
        return false;
    if (select.limitBy())
        return false;
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
        GetAggregatesVisitor::Data data;
        GetAggregatesVisitor(data).visit(select.select());
        if (!data.window_functions.empty())
            return false;
    }

    return true;
}


bool QueryOracleChecker::hasAggregates(const ASTSelectQuery & select)
{
    if (!select.select())
        return false;
    GetAggregatesVisitor::Data data;
    GetAggregatesVisitor(data).visit(select.select());
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
    oracle_context->setCurrentQueryId("");
    return oracle_context;
}


std::vector<String> QueryOracleChecker::executeAndCollectSortedRows(const String & query, const ContextMutablePtr & context)
{
    auto oracle_context = makeOracleContext(context);
    oracle_context->setDefaultFormat("TabSeparated");

    /// Use the ReadBuffer/WriteBuffer executeQuery API — this is crash-safe because
    /// ClickHouse handles all column serialization within the pipeline internally,
    /// writing formatted text directly to the output buffer.
    ReadBufferFromString istr(query);
    WriteBufferFromOwnString ostr;

    executeQuery(istr, ostr, oracle_context, {}, QueryFlags{.internal = true});

    String output = ostr.str();
    if (output.size() > MAX_ORACLE_OUTPUT_SIZE)
        return {}; /// Too much output, skip

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


std::vector<String> QueryOracleChecker::executeAndCollectSortedUniqueRows(const String & query, const ContextMutablePtr & context)
{
    auto rows = executeAndCollectSortedRows(query, context);
    rows.erase(std::unique(rows.begin(), rows.end()), rows.end());
    return rows;
}


std::vector<String> QueryOracleChecker::executeWithSettings(
    const String & query, const ContextMutablePtr & context,
    const std::vector<std::pair<String, Field>> & settings)
{
    auto oracle_context = makeOracleContext(context);
    oracle_context->setDefaultFormat("TabSeparated");
    for (const auto & [name, value] : settings)
        oracle_context->setSetting(name, value);

    ReadBufferFromString istr(query);
    WriteBufferFromOwnString ostr;
    executeQuery(istr, ostr, oracle_context, {}, QueryFlags{.internal = true});

    String output = ostr.str();
    if (output.size() > MAX_ORACLE_OUTPUT_SIZE)
        return {};

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
    auto ref_rows = executeAndCollectSortedRows(ref_sql, context);
    auto part_rows = executeAndCollectSortedRows(union_sql, context);

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
        size_t ri = 0, pi = 0;
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

        throw Exception(ErrorCodes::LOGICAL_ERROR, "{}", message);
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

        throw Exception(ErrorCodes::LOGICAL_ERROR,
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
    if (hasArrayJoin(select) || hasPasteJoin(select))
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

    auto ref_rows = executeAndCollectSortedRows(ref_sql, context);
    auto part_rows = executeAndCollectSortedRows(union_sql, context);

    if (ref_rows != part_rows)
    {
        ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleMismatches);
        throw Exception(ErrorCodes::LOGICAL_ERROR,
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
    auto ref_rows = executeAndCollectSortedUniqueRows(ref_sql, context);
    auto part_rows = executeAndCollectSortedUniqueRows(union_sql, context);

    if (ref_rows != part_rows)
    {
        ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleMismatches);
        throw Exception(ErrorCodes::LOGICAL_ERROR,
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
    auto ref_rows = executeAndCollectSortedUniqueRows(ref_sql, context);
    auto part_rows = executeAndCollectSortedUniqueRows(union_sql, context);

    if (ref_rows != part_rows)
    {
        ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleMismatches);
        throw Exception(ErrorCodes::LOGICAL_ERROR,
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
    auto default_rows = executeAndCollectSortedRows(query_sql, context);

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
    };

    /// Pick one random settings variant.
    size_t variant_idx = thread_local_rng() % std::size(settings_variants);
    const auto & settings = settings_variants[variant_idx];

    ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleChecks);

    String setting_name = settings[0].first;
    LOG_TRACE(logger, "DQP oracle: query: {}, toggling: {}", query_sql, setting_name);

    try
    {
        auto variant_rows = executeWithSettings(query_sql, context, settings);

        if (default_rows != variant_rows)
        {
            ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleMismatches);
            throw Exception(ErrorCodes::LOGICAL_ERROR,
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
        if (e.code() == ErrorCodes::LOGICAL_ERROR)
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

    if (hasNonDeterministicFunctions(select.clone()))
        return false;

    /// Collect aggregate functions from the SELECT list.
    GetAggregatesVisitor::Data agg_data;
    GetAggregatesVisitor(agg_data).visit(select.select());
    if (agg_data.aggregates.empty())
        return false;

    ASTPtr predicate = select.where()->clone();

    /// Build the reference query: remove WHERE, keep everything else.
    /// Add.
    auto ref_ast = select.clone();
    auto & ref_select = ref_ast->as<ASTSelectQuery &>();
    ref_select.setExpression(ASTSelectQuery::Expression::WHERE, {});
    stripOrderAndLimit(ref_select);
    String ref_sql = formatAST(ref_ast) + "";
    if (ref_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    /// Build the inner SELECT for partitioned subqueries:
    /// Replace each agg(args) with aggState(args) AS _s_N.
    /// Keep GROUP BY columns in the SELECT list so the outer query can group by them.
    auto inner_ast = select.clone();
    auto & inner_select = inner_ast->as<ASTSelectQuery &>();
    stripOrderAndLimit(inner_select);

    /// Build new SELECT list: [group_by_cols...,] aggState(args) AS _s_0, aggState(args) AS _s_1, ...
    auto new_inner_select_list = make_intrusive<ASTExpressionList>();

    /// Include GROUP BY columns in the inner SELECT (needed for outer GROUP BY).
    bool has_group_by = inner_select.groupBy() != nullptr;
    if (has_group_by)
    {
        for (const auto & group_expr : inner_select.groupBy()->children)
            new_inner_select_list->children.push_back(group_expr->clone());
    }

    /// Transform aggregates: agg(args) -> aggState(args) AS _s_N
    /// Also build the outer SELECT list: aggMerge(_s_N)
    auto outer_select_list = make_intrusive<ASTExpressionList>();
    if (has_group_by)
    {
        for (const auto & group_expr : inner_select.groupBy()->children)
            outer_select_list->children.push_back(group_expr->clone());
    }

    size_t state_idx = 0;
    for (const auto & aggregate_ast : agg_data.aggregates)
    {
        const auto * agg_func = aggregate_ast->as<ASTFunction>();
        if (!agg_func)
            return false;

        String alias = fmt::format("_s_{}", state_idx);

        /// Inner: aggState(args) AS _s_N
        auto state_func_ast = agg_func->clone();
        auto & state_func = state_func_ast->as<ASTFunction &>();
        state_func.name = agg_func->name + "State";
        state_func.setAlias(alias);
        new_inner_select_list->children.push_back(std::move(state_func_ast));

        /// Outer: aggMerge(_s_N)
        auto merge_func = makeASTFunction(agg_func->name + "Merge", make_intrusive<ASTIdentifier>(alias));
        outer_select_list->children.push_back(std::move(merge_func));

        ++state_idx;
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

    /// Compare row counts only (not full content) because aggregate values may differ
    /// in low-order bits due to floating-point accumulation order differences between
    /// direct computation and the State/Merge path.
    String ref_count_sql = fmt::format("SELECT count() FROM ({})", ref_sql);
    String meta_count_sql = fmt::format("SELECT count() FROM ({})", metamorphic_sql);

    Field ref_count = executeScalar(ref_count_sql, context);
    Field meta_count = executeScalar(meta_count_sql, context);

    if (ref_count != meta_count)
    {
        ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleMismatches);

        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "TLP Aggregate oracle mismatch!\n"
            "Reference query ({} rows): {}\n"
            "Metamorphic query ({} rows): {}",
            ref_count, ref_sql,
            meta_count, metamorphic_sql);
    }

    LOG_TRACE(logger, "TLP Aggregate oracle passed ({} rows, {} aggregates)", ref_count, state_idx);
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

        auto table_id = table_expr->database_and_table_name->as<ASTTableIdentifier>();
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
        return false;

    /// Try to populate the table with random data so the oracle checks non-empty results.
    tryPopulateTable(*select, context);

    bool any_check_performed = false;

    /// TLP WHERE oracle
    try
    {
        if (checkTLPWhere(*select, context))
            any_check_performed = true;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::LOGICAL_ERROR)
            throw; /// Oracle mismatch — propagate to crash the server
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
        if (e.code() == ErrorCodes::LOGICAL_ERROR)
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
        if (e.code() == ErrorCodes::LOGICAL_ERROR)
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
        if (e.code() == ErrorCodes::LOGICAL_ERROR)
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
        if (e.code() == ErrorCodes::LOGICAL_ERROR)
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
        if (e.code() == ErrorCodes::LOGICAL_ERROR)
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
        if (e.code() == ErrorCodes::LOGICAL_ERROR)
            throw;
        LOG_TRACE(logger, "DQP oracle execution error (skipping): {}", e.message());
    }
    catch (...)
    {
        LOG_TRACE(logger, "DQP oracle execution error (skipping): {}", getCurrentExceptionMessage(false));
    }

    return any_check_performed;
}

}
