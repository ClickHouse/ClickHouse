#include <Interpreters/QueryOracleChecker.h>

#include <Common/ProfileEvents.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/GetAggregatesVisitor.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTWithAlias.h>
#include <Common/FieldVisitorToString.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/IStorage.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Core/Joins.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Common/QueryFuzzer.h>
#include <Common/quoteString.h>
#include <Common/thread_local_rng.h>
#include <Poco/String.h>

#include <algorithm>
#include <unordered_set>


namespace ProfileEvents
{
extern const Event ASTFuzzerOracleChecks;
extern const Event ASTFuzzerOracleTLPAggregateChecks;
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

/// Backstop list of function names that invalidate oracle checks. Most regular
/// functions are picked up dynamically via `FunctionFactory::tryGet`'s
/// `isDeterministic` (no list maintenance needed there). This set covers what
/// `FunctionFactory` does not reach: table functions (registered in
/// `TableFunctionFactory`), and aggregate functions whose metamorphic
/// `State`/`Merge` rewrite legitimately produces a different value than direct
/// evaluation (so they aren't "non-deterministic functions" in the
/// `system.functions` sense, but they are unsafe for oracle equality).
const std::unordered_set<String> non_deterministic_functions = {
    "rand", "rand32", "rand64", "randConstant", "randUniform", "randNormal",
    "randBernoulli", "randExponential", "randChiSquared", "randStudentT",
    "randFisherF", "randLogNormal", "randPoisson",
    "generateUUIDv4", "generateUUIDv7", "generateSnowflakeID",
    "now", "now64", "today", "yesterday",
    "rowNumberInBlock", "blockNumber", "blockSize",
    "runningDifference", "runningDifferenceStartingWithFirstValue",
    /// `neighbor` and `runningAccumulate` read across rows in physical block
    /// order (offset neighbours / a running state), so any rewrite that reorders,
    /// repartitions or reblocks the input (TLP partitions, NoREC, DQP setting
    /// toggles, multi-thread reads) changes their per-row output legitimately.
    "neighbor", "runningAccumulate",
    "currentDatabase", "queryID", "serverUUID",
    "getSetting", "fuzzBits", "throwIf",
    /// `indexHint` filters at granule granularity: the rows that survive it
    /// depend on index layout, not on the predicate value, so any rewrite
    /// that moves or negates it (TLP partitions, NoREC `countIf`) changes
    /// the result legitimately.
    "indexHint",
    /// `viewExplain` (the table function behind `SELECT ... FROM (EXPLAIN ...)`)
    /// returns the query plan as rows. The plan text legitimately changes when
    /// DQP toggles an optimizer setting, so comparing such results is comparing
    /// plans, not data.
    "viewExplain",
    "file", "url", "s3", "hdfs", "input",
    "numbers", "numbers_mt", "zeros", "zeros_mt", "generateRandom",
    "generate_series", "generateSeries",
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
    /// than direct computation due to approximate merging algorithms. Block both
    /// the singular and plural-result variants (the fuzzer dictionary contains
    /// `quantiles*` as well, and they share the same approximate merge logic).
    "median", "quantile", "quantiles",
    "quantileTDigest", "quantileTDigestWeighted",
    "quantilesTDigest", "quantilesTDigestWeighted",
    "quantileGK", "quantilesGK",
    "quantileBFloat16", "quantileBFloat16Weighted",
    "quantilesBFloat16", "quantilesBFloat16Weighted",
    "quantileDD", "quantilesDD",
    "quantileTiming", "quantileTimingWeighted",
    "quantilesTiming", "quantilesTimingWeighted",
    "quantileDeterministic", "quantilesDeterministic",
    "quantileExact", "quantileExactWeighted",
    "quantilesExact", "quantilesExactWeighted",
    "quantileExactLow", "quantileExactHigh",
    "quantilesExactLow", "quantilesExactHigh",
    "quantileExactExclusive", "quantileExactInclusive",
    "quantilesExactExclusive", "quantilesExactInclusive",
    "quantileInterpolatedWeighted", "quantilesInterpolatedWeighted",
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
    /// `argMin`/`argMax`/`groupConcat` are order-dependent on ties: the
    /// `State`/`Merge`, DQP and subquery-rewrite paths can legitimately
    /// pick a different "arg" value or concatenation order than direct
    /// evaluation, so exact row equality is wrong here.
    "argMin", "argMax", "groupConcat",
    /// Approximate/formatting-dependent aggregates.
    "entropy", "exponentialMovingAverage", "exponentialTimeDecayedAvg",
    "simpleLinearRegression", "sparkBar", "histogram",
    "retentionState",
    /// `largestTriangleThreeBuckets` (LTTB) downsamples to a fixed number of
    /// points; the selection depends on input row order, and with the `Distinct`
    /// combinator the deduplicated set feeds LTTB in a parallel-merge-dependent
    /// order, so the result varies run-to-run even at a fixed setting. Any
    /// plan-changing rewrite (DQP setting toggle, State/Merge, subquery wrap)
    /// then legitimately differs from direct evaluation.
    "largestTriangleThreeBuckets",
    /// Depends on physical data layout, not values.
    "estimateCompressionRatio",
    /// Statistical hypothesis-test / correlation aggregates: they return
    /// floating-point statistics or p-values computed with rank/tie handling
    /// and non-associative summation, so the State/Merge, DQP and
    /// subquery-rewrite paths legitimately differ from direct evaluation.
    "mannWhitneyUTest", "studentTTest", "welchTTest", "meanZTest",
    "kolmogorovSmirnovTest", "rankCorr", "theilsU", "cramersV",
    "cramersVBiasCorrected", "contingency", "categoricalInformationValue",
};

/// Maximum formatted query length for oracle sub-queries.
constexpr size_t MAX_ORACLE_QUERY_LENGTH = 10000;

/// Maximum total output size from an oracle sub-query (bytes).
constexpr size_t MAX_ORACLE_OUTPUT_SIZE = 10 * 1024 * 1024;

/// Maximum row count for an oracle sub-query result. Caps memory before the
/// post-execution size check in `executeAndCollect*` triggers.
constexpr size_t MAX_ORACLE_RESULT_ROWS = 10'000'000;

/// Case-insensitive view of `non_deterministic_functions`. ClickHouse resolves
/// aggregate (and scalar) function names case-insensitively and the parser
/// preserves whatever spelling the fuzzer produced, so a query using `SUM`,
/// `argmax`, or `ANY` must still match the unsafe set. Compare lowercased.
const std::unordered_set<String> non_deterministic_functions_lower = []
{
    std::unordered_set<String> result;
    result.reserve(non_deterministic_functions.size());
    for (const auto & name : non_deterministic_functions)
        result.insert(Poco::toLower(name));
    return result;
}();

/// Aggregate-function combinator suffixes — shared with the fuzzer, see
/// `aggregate_combinator_suffixes` in `QueryFuzzer.h`.
const Strings & combinator_suffixes = aggregate_combinator_suffixes;

/// Strip the LONGEST matching combinator suffix from `name`, returning false
/// when nothing matched. Longest-match (rather than first-in-list) is what
/// keeps overlapping combinators correct regardless of list order: e.g.
/// `*SimpleState` must lose `SimpleState`, not `State` (which would strand a
/// `Simple`), and `*RespectNulls` must lose `RespectNulls`, not `Null`.
bool stripLongestCombinatorSuffix(String & name)
{
    const String * best = nullptr;
    for (const auto & suffix : combinator_suffixes)
        if (name.size() > suffix.size() && name.ends_with(suffix)
            && (!best || suffix.size() > best->size()))
            best = &suffix;
    if (!best)
        return false;
    name.resize(name.size() - best->size());
    return true;
}

/// Strip aggregate-function combinator suffixes from the right of `name`
/// repeatedly, so e.g. `first_valueOrNullDistinct` becomes `first_value`.
/// Cheap and safe: we never strip into an empty string and stop as soon as no
/// suffix matches.
String stripAggregateCombinators(String name)
{
    while (stripLongestCombinatorSuffix(name)) {}
    return name;
}

/// True if `name`, after removing zero or more combinator suffixes, names an
/// entry of `non_deterministic_functions` (matched case-insensitively).
/// Membership must be tested at EVERY stripping stage, not only at the
/// fixpoint: real aggregate names can themselves end in a combinator-looking
/// word, e.g. `groupUniqArrayOrNull` strips to `groupUniqArray` (a set
/// member), but one more iteration eats the literal `Array` and produces
/// `groupUniq`, which the set does not contain.
bool isOracleUnsafeFunctionName(String name)
{
    while (true)
    {
        if (non_deterministic_functions_lower.contains(Poco::toLower(name)))
            return true;
        if (!stripLongestCombinatorSuffix(name))
            return false;
    }
}

/// Walk an AST tree and check whether any `ASTFunction` references something
/// non-deterministic. The primary source of truth is `FunctionFactory` —
/// every regular function exposes `isDeterministic`, so newly-added
/// non-deterministic functions are caught automatically without needing to
/// touch the local list. The static `non_deterministic_functions` set above
/// is the backstop for two cases `FunctionFactory` cannot reach:
///   1. Table functions (`file`, `url`, `s3`, `numbers`, ...) — registered in
///      `TableFunctionFactory`; would slip through as scalar calls otherwise.
///   2. Aggregate functions whose `State`/`Merge` rewrite legitimately
///      diverges from direct evaluation (`any`, `topK`, `quantile*`, `sum` on
///      floats, ...) — semantically deterministic but unsafe for our exact
///      equality oracle.
/// We strip aggregate combinator suffixes before lookup so e.g.
/// `first_valueOrNull` resolves to `first_value` — the fuzzer routinely
/// appends `*OrNull`/`*Distinct`/`*State` chains and neither
/// `FunctionFactory::tryGet` nor the backstop set knows the chained name.
bool hasNonDeterministicFunctionsImpl(const ASTPtr & ast, const ContextPtr & context)
{
    if (!ast)
        return false;

    if (const auto * table_expr = ast->as<ASTTableExpression>())
    {
        /// SAMPLE picks a row subset; which rows end up in the subset is not
        /// stable across the plan rewrites the oracles perform, so any sampled
        /// table makes result comparison meaningless.
        if (table_expr->sample_size)
            return true;

        /// A table function as a source (`numbers`, `generateRandom`, `s3`,
        /// `url`, `remote`, `cluster`, `mysql`, `postgresql`, `mongodb`,
        /// `iceberg*`, `fuzzJSON`, ...) is outside what the oracle can
        /// validate: it may read external or mutable data, produce random
        /// rows, or return a different snapshot on each of the oracle's
        /// repeated reads. The static backstop set above only names a handful
        /// of the table functions the fuzzer can emit (and they are not scalar
        /// functions, so `FunctionFactory::tryGet` below never rejects them),
        /// so reject ANY table-function source generically.
        if (table_expr->table_function)
            return true;
    }

    if (const auto * func = ast->as<ASTFunction>())
    {
        const String stripped = stripAggregateCombinators(func->name);
        if (isOracleUnsafeFunctionName(func->name))
            return true;

        /// Experimental `timeSeries*ToGrid` aggregates bucket points onto a
        /// parameterized grid; the result depends on grid parameters and
        /// point ordering, so the metamorphic State/Merge and DQP rewrites
        /// legitimately diverge. Whole family gated by prefix (it is growing).
        /// Aggregate names resolve case-insensitively, so compare lowercased —
        /// `TIMESERIES...` must not slip past the gate.
        const String name_lower = Poco::toLower(func->name);
        const String stripped_lower = Poco::toLower(stripped);
        if (name_lower.starts_with("timeseries") || stripped_lower.starts_with("timeseries"))
            return true;

        /// Comparator-based array sorts are not stable on ties: with a
        /// non-injective lambda key (e.g. `arrayReverseSort(x -> 0, arr)`) the
        /// relative order of tied elements is implementation-defined and can
        /// differ between plans, so the produced array *content* differs. The
        /// single-argument forms sort by value and stay deterministic.
        /// Compared lowercased for the same reason as above; over-matching an
        /// unresolvable spelling merely skips one more query, which is safe.
        static const std::unordered_set<String> lambda_sort_functions = {
            "arraysort", "arrayreversesort", "arraypartialsort", "arraypartialreversesort"};
        if (lambda_sort_functions.contains(name_lower)
            && func->arguments && func->arguments->children.size() >= 2)
            return true;

        for (const auto & name : {std::cref(func->name), std::cref(stripped)})
        {
            if (const auto resolver = FunctionFactory::instance().tryGet(name.get(), context))
                if (!resolver->isDeterministic())
                    return true;
            if (UserDefinedSQLFunctionFactory::instance().tryGet(name.get()))
                /// SQL UDF determinism is not introspectable — treat as non-deterministic.
                return true;
            if (const auto udf_exec = UserDefinedExecutableFunctionFactory::tryGet(name.get(), context))
                if (!udf_exec->isDeterministic())
                    return true;
        }
    }

    for (const auto & child : ast->children)
    {
        if (hasNonDeterministicFunctionsImpl(child, context))
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
        /// `unnest` is registered as a case-insensitive alias of `arrayJoin`,
        /// and the parser preserves the caller's spelling, so match both names
        /// lowercased. Over-matching a spelling that would not resolve merely
        /// skips one more query, which is the safe direction for this gate.
        const String name_lower = Poco::toLower(func->name);
        if (name_lower == "arrayjoin" || name_lower == "unnest")
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
/// Scan every slot of an ASTSelectQuery that can host a window function:
/// SELECT list, QUALIFY clause, and named WINDOW definitions. The bare
/// `hasWindowFunctionWithoutOrderBy(select.select())` we used previously
/// missed `QUALIFY row_number() OVER () = 1` and `WINDOW w AS (...)` cases.
bool hasWindowFunctionWithoutOrderBy(const ASTPtr & ast);
bool hasWindowFunctionWithoutOrderByAnywhere(const ASTSelectQuery & select)
{
    if (select.select() && hasWindowFunctionWithoutOrderBy(select.select()))
        return true;
    if (select.qualify() && hasWindowFunctionWithoutOrderBy(select.qualify()))
        return true;
    if (select.window() && hasWindowFunctionWithoutOrderBy(select.window()))
        return true;
    return false;
}

/// True if the subtree contains at least one column identifier.
bool containsIdentifier(const ASTPtr & ast)
{
    if (!ast)
        return false;
    if (ast->as<ASTIdentifier>())
        return true;
    for (const auto & child : ast->children)
        if (containsIdentifier(child))
            return true;
    return false;
}

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
            /// An ORDER BY whose every key is a constant (the fuzzer loves
            /// `ORDER BY toLowCardinality('...')`) orders nothing: all rows
            /// tie, so the order is as implementation-defined as having no
            /// ORDER BY at all.
            bool any_identifier = false;
            for (const auto & key : def->order_by->children)
                any_identifier = any_identifier || containsIdentifier(key);
            if (!any_identifier)
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

/// Collects every alias defined anywhere in the subtree (e.g. `1025 AS x`
/// nested inside an array literal in the SELECT list). Also collects the
/// column names redefined by a `* REPLACE (expr AS name)` transformer: like a
/// regular alias, such a name is visible in WHERE (ClickHouse makes SELECT
/// aliases visible there), so NoREC's `countIf` rewrite — which drops the
/// SELECT list and thus the REPLACE — would bind the WHERE reference to the
/// original column instead, a false mismatch.
void collectAliases(const ASTPtr & ast, std::unordered_set<String> & out)
{
    if (!ast)
        return;
    const String & alias = ast->tryGetAlias();
    if (!alias.empty())
        out.insert(alias);
    if (const auto * replacement = ast->as<ASTColumnsReplaceTransformer::Replacement>())
        out.insert(replacement->name);
    for (const auto & child : ast->children)
        collectAliases(child, out);
}

/// True if the subtree references any of the given names as an identifier.
bool referencesAnyAlias(const ASTPtr & ast, const std::unordered_set<String> & aliases)
{
    if (!ast)
        return false;
    if (const auto * ident = ast->as<ASTIdentifier>())
        if (aliases.contains(ident->name()) || aliases.contains(ident->shortName()))
            return true;
    for (const auto & child : ast->children)
        if (referencesAnyAlias(child, aliases))
            return true;
    return false;
}

/// Settings that cap how much a query reads/produces, plus their overflow
/// modes. With `*_overflow_mode = 'break'` they silently truncate results,
/// and the truncation is asymmetric between oracle rewrites — e.g. a trivial
/// `count()` reads no rows at all while the TLP partitions read and get cut
/// at `max_rows_to_read`. A query carrying any of these inline (top level or
/// in a subquery) cannot be compared; session-level leakage from a seed's
/// `SET` is neutralized in `makeOracleContext`.
const std::unordered_set<String> truncating_settings = {
    "max_rows_to_read", "max_bytes_to_read", "read_overflow_mode",
    "max_rows_to_read_leaf", "max_bytes_to_read_leaf", "read_overflow_mode_leaf",
    "max_rows_to_group_by", "group_by_overflow_mode",
    "max_rows_to_sort", "max_bytes_to_sort", "sort_overflow_mode",
    "max_result_rows", "max_result_bytes", "result_overflow_mode",
    "max_rows_in_distinct", "max_bytes_in_distinct", "distinct_overflow_mode",
    "max_rows_to_transfer", "max_bytes_to_transfer", "transfer_overflow_mode",
    "max_rows_in_join", "max_bytes_in_join", "join_overflow_mode",
    "max_rows_in_set", "max_bytes_in_set", "set_overflow_mode",
    "max_execution_time", "max_execution_time_leaf", "timeout_overflow_mode", "timeout_overflow_mode_leaf",
    "max_estimated_execution_time",
    /// The `limit` / `offset` SETTINGS (distinct from LIMIT/OFFSET clauses)
    /// apply a final LIMIT/OFFSET to every query result. That truncation is
    /// not distributive over the oracle rewrites — the reference and the
    /// partitioned UNION are limited/offset over differently-ordered results,
    /// so they keep different row subsets (observed: `SET offset = 10` made a
    /// single batch produce 174 TLP mismatches).
    "limit", "offset",
    /// Not truncation per se, but the same "results depend on which granules
    /// the predicate prunes" effect: with exact mode off, skip indexes under
    /// FINAL may keep stale row versions whose newer version lives in a
    /// pruned granule — documented behaviour, not comparable across rewrites.
    "use_skip_indexes_if_final", "use_skip_indexes_if_final_exact_mode",
    /// Changes aggregate semantics asymmetrically across oracle rewrites:
    /// `count()` over zero input rows becomes NULL while NoREC's `countIf`
    /// aggregates every row and returns 0. Pinned off in `makeOracleContext`
    /// for session leakage, but an inline `SETTINGS` clause survives in the
    /// clones and overrides the pin, so gate it here too.
    "aggregate_functions_null_for_empty",
    /// Same asymmetry from the other direction: with this on, an aggregation
    /// with no GROUP BY over an *empty* input returns ZERO rows instead of one
    /// default row. NoREC's `count()` over an empty (always-false WHERE)
    /// subquery then yields no row, while the `countIf` form over the
    /// non-empty table yields one `0` row — a false mismatch. (This was the
    /// cause of the long-unreproducible NoREC count()=NULL family, surfaced
    /// once mismatch reproducers started carrying their active settings.)
    "empty_result_for_aggregation_by_empty_set",
    /// Appends an extremes block (blank separator + min/max rows) to every
    /// result; the oracle's row collection would count those as data rows.
    "extremes",
};

bool hasTruncatingInlineSettings(const ASTPtr & ast)
{
    if (!ast)
        return false;
    if (const auto * set_query = ast->as<ASTSetQuery>())
        for (const auto & change : set_query->changes)
            if (truncating_settings.contains(change.name))
                return true;
    for (const auto & child : ast->children)
        if (hasTruncatingInlineSettings(child))
            return true;
    return false;
}

/// Settings whose uniform removal from both sides of an oracle comparison
/// cannot change the produced rows — they only bound resources or control
/// logging. `stripOrderAndLimit` deletes the top-level inline `SETTINGS`
/// clause from every clone it prepares, so a query is only comparable when
/// everything that clause carries is in this allowlist: a semantically
/// significant setting (`join_use_nulls`, `apply_mutations_on_fly`,
/// `optimize_*`, `final`, ...) would make the oracle validate a different
/// query than the one that actually succeeded. Anything not explicitly listed
/// here (and not already rejected by `truncating_settings`) causes the whole
/// query to be skipped — fail closed, since new settings appear all the time.
const std::unordered_set<String> strippable_resource_settings = {
    "max_memory_usage", "max_memory_usage_for_user", "max_untracked_memory",
    "max_bytes_before_external_group_by", "max_bytes_before_external_sort",
    "max_bytes_ratio_before_external_group_by", "max_bytes_ratio_before_external_sort",
    "max_threads", "max_insert_threads", "max_final_threads", "max_block_size",
    "max_query_size", "max_ast_depth", "max_ast_elements", "max_expanded_ast_elements",
    "max_parser_depth", "max_parser_backtracks", "max_execution_speed",
    "log_comment", "log_queries", "log_query_threads", "log_processors_profiles",
    "send_logs_level", "priority", "os_thread_priority",
};

/// True if any inline `SETTINGS` clause (top level or nested) carries a
/// setting outside `strippable_resource_settings`. Checked recursively for
/// symmetry with `hasTruncatingInlineSettings`: nested clauses are never
/// stripped, but proving a nested semantic setting harmless would require
/// per-oracle reasoning, so skip conservatively.
bool hasNonStrippableInlineSettings(const ASTPtr & ast)
{
    if (!ast)
        return false;
    if (const auto * set_query = ast->as<ASTSetQuery>())
        for (const auto & change : set_query->changes)
            if (!strippable_resource_settings.contains(change.name))
                return true;
    for (const auto & child : ast->children)
        if (hasNonStrippableInlineSettings(child))
            return true;
    return false;
}

/// Nested SETTINGS clauses are preserved in oracle clones. In particular, a
/// nested thread-count setting overrides the single-thread pin in
/// `makeOracleContext`, reopening the nested-pipeline lifetime race which that
/// pin is intended to avoid. The top-level clause is stripped uniformly, so it
/// remains safe to allow there.
bool hasNestedThreadSettings(const ASTPtr & ast, const ASTPtr & top_level_settings)
{
    if (!ast)
        return false;
    if (ast != top_level_settings)
    {
        if (const auto * set_query = ast->as<ASTSetQuery>())
            for (const auto & change : set_query->changes)
                if (change.name == "max_threads" || change.name == "max_insert_threads" || change.name == "max_final_threads")
                    return true;
    }
    for (const auto & child : ast->children)
        if (hasNestedThreadSettings(child, top_level_settings))
            return true;
    return false;
}

/// True if `clause` defines an alias that is referenced anywhere else in the
/// SELECT. ClickHouse aliases are visible query-wide, so an oracle rewrite
/// that removes or replaces such a clause (TLP's reference query drops WHERE
/// or HAVING entirely) silently rebinds those references — e.g. the fuzzer
/// produces `SELECT (SELECT x) FROM t WHERE f(... AS x)`: with the WHERE
/// present `x` is the alias, without it `x` is the table column.
bool clauseDefinesAliasUsedElsewhere(const ASTSelectQuery & select, const ASTPtr & clause)
{
    if (!clause)
        return false;
    std::unordered_set<String> aliases;
    collectAliases(clause, aliases);
    if (aliases.empty())
        return false;
    for (const auto & child : select.children)
    {
        if (child == clause)
            continue;
        if (referencesAnyAlias(child, aliases))
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

/// True if the subtree contains a bare `*` or qualified `t.*`. Asterisk
/// expansion is context-sensitive: when an oracle wraps the query in a
/// subquery or re-projects it (TLP partitions, NoREC, subquery wrap), a `*`
/// inside GROUP BY / a projection re-expands over a different column set, so
/// the rewrite is not equivalent to the original — a false mismatch. The
/// fuzzer readily produces these (e.g. `GROUP BY toString(g, *, NULL)`).
bool containsAsterisk(const ASTPtr & ast)
{
    if (!ast)
        return false;
    if (ast->as<ASTAsterisk>() || ast->as<ASTQualifiedAsterisk>())
        return true;
    for (const auto & child : ast->children)
        if (containsAsterisk(child))
            return true;
    return false;
}

/// True if any ORDER BY element anywhere in the query uses `WITH FILL`.
/// `WITH FILL` synthesises extra rows to fill gaps in the ordered sequence,
/// so the result *set* (not just its order) depends on the ORDER BY. The
/// oracles strip ORDER BY and compare row sets, which removes the FILL on one
/// side but not necessarily the other (e.g. inside a window definition or a
/// subquery) — a false mismatch. Checked recursively.
bool hasWithFillAnywhere(const ASTPtr & ast)
{
    if (!ast)
        return false;
    if (const auto * order_elem = ast->as<ASTOrderByElement>())
        if (order_elem->with_fill)
            return true;
    for (const auto & child : ast->children)
        if (hasWithFillAnywhere(child))
            return true;
    return false;
}

/// True if any table expression in the query uses `FINAL`. FINAL's dedup is
/// global over the table's row versions and is NOT distributive over a WHERE
/// predicate: the TLP rewrites split rows into `p` / `NOT p` / `p IS NULL`
/// branches with `UNION ALL`, and FINAL applied within each branch can keep a
/// different "winning" version per branch, so a key whose versions straddle
/// the partitions survives in more than one branch — the partitioned result
/// then has more rows than the reference. Not a bug, just non-distributive;
/// skip the whole query. Checked recursively (FINAL may be in a subquery).
bool usesFinalAnywhere(const ASTPtr & ast)
{
    if (!ast)
        return false;
    if (const auto * table_expr = ast->as<ASTTableExpression>())
        if (table_expr->final)
            return true;
    for (const auto & child : ast->children)
        if (usesFinalAnywhere(child))
            return true;
    return false;
}

/// ASOF JOIN tie-breaking among equal asof-column values is
/// implementation-defined, so which right-side row a left row pairs with can
/// change between plans — observed as a `Subquery wrap` false mismatch.
/// Checked recursively: an ASOF join in any subquery taints the whole query.
bool hasAsofJoinAnywhere(const ASTPtr & ast)
{
    if (!ast)
        return false;
    if (const auto * join = ast->as<ASTTableJoin>())
        if (join->strictness == JoinStrictness::Asof)
            return true;
    for (const auto & child : ast->children)
        if (hasAsofJoinAnywhere(child))
            return true;
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
bool subtreeContainsSetOperation(const ASTPtr & ast)
{
    if (!ast)
        return false;
    if (const auto * union_query = ast->as<ASTSelectWithUnionQuery>())
        if (union_query->list_of_selects && union_query->list_of_selects->children.size() > 1)
            return true;
    if (ast->as<ASTSelectIntersectExceptQuery>())
        return true;
    for (const auto & child : ast->children)
        if (subtreeContainsSetOperation(child))
            return true;
    return false;
}

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
        /// Search the whole subquery subtree, not just the immediate child:
        /// the union can hide one or more wrapper levels down, e.g.
        /// `FROM (SELECT * FROM (SELECT 1 AS x UNION ALL SELECT '1' AS x))`
        /// has the same `Variant(...)` / pushdown ambiguity as a direct union.
        /// INTERSECT / EXCEPT parse into `ASTSelectIntersectExceptQuery` (not a
        /// multi-child union list), so they are matched as their own node kind.
        if (subtreeContainsSetOperation(table_expr->subquery))
            return true;
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
/// Recursively true if any table identifier anywhere in the query (including
/// CTEs and subqueries, which `referencesNonDeterministicDatabase` does not
/// reach) lives in `system` / `INFORMATION_SCHEMA`. Those are live snapshots
/// (`system.events`, `system.metrics`, ...) that drift between the reference
/// and rewrite executions — a false mismatch (observed: a CTE
/// `WITH x AS (SELECT ... FROM system.events ...)`).
/// An unqualified table name (`FROM processes`) resolves against the session's
/// current database, so `current_database` must be supplied by the caller —
/// after `USE system`, bare `FROM processes` reads `system.processes` and must
/// be rejected just like the qualified spelling.
bool referencesSystemDatabaseAnywhere(const ASTPtr & ast, const String & current_database)
{
    if (!ast)
        return false;
    if (const auto * table_id = ast->as<ASTTableIdentifier>())
    {
        String db = table_id->getDatabaseName();
        if (db.empty())
            db = current_database;
        if (db == "system" || db == "INFORMATION_SCHEMA" || db == "information_schema")
            return true;
    }
    for (const auto & child : ast->children)
        if (referencesSystemDatabaseAnywhere(child, current_database))
            return true;
    return false;
}

/// `current_database` resolves unqualified table names, same as in
/// `referencesSystemDatabaseAnywhere` above. It defaults to empty at the
/// `isSafeForOracle` call site, which has no context — the recursive
/// context-aware check in `QueryOracleChecker::check` covers that case.
bool referencesNonDeterministicDatabase(const ASTSelectQuery & select, const String & current_database = {})
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
        if (database.empty())
            database = current_database;
        if (database == "system" || database == "INFORMATION_SCHEMA" || database == "information_schema")
            return true;
    }
    return false;
}

/// True if any table referenced anywhere in the query has the `Distributed`
/// engine — not only in the top-level FROM, but also inside subqueries and
/// CTEs. A `Distributed` table hidden under a subquery (e.g.
/// `SELECT * FROM (SELECT * FROM dist_tbl) WHERE p`) still routes to remote
/// shards, so the reference and rewritten queries can route or combine shard
/// results differently and report a false mismatch. We therefore walk every
/// `ASTTableIdentifier` in the tree. The engine is not visible at the AST
/// level, so each is resolved through the catalog. A catalog lookup that
/// throws is treated conservatively as "distributed" (fail closed): we cannot
/// prove the table is safe, so we skip the query rather than risk a false
/// `AST_FUZZER_ORACLE_MISMATCH`.
bool referencesDistributedTableAnywhere(const ASTPtr & ast, const ContextPtr & context)
{
    if (!ast)
        return false;
    if (const auto * table_id = ast->as<ASTTableIdentifier>())
    {
        String database = table_id->getDatabaseName();
        if (database.empty())
            database = context->getCurrentDatabase();
        try
        {
            auto storage = DatabaseCatalog::instance().tryGetTable({database, table_id->shortName()}, context);
            if (storage && storage->getName() == "Distributed")
                return true;
        }
        catch (...)
        {
            /// Ok: fail closed. If the table cannot be resolved we cannot prove it
            /// is not `Distributed`, so treat it as distributed (unsafe) and skip
            /// the query rather than risk a false oracle mismatch. Deliberately not
            /// logged: this runs per-AST-node on a hot path.
            return true;
        }
    }
    for (const auto & child : ast->children)
        if (referencesDistributedTableAnywhere(child, context))
            return true;
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
    if (hasArrayJoinFunction(select.clone()))
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
    /// Bare `OFFSET` (without `LIMIT`) is order-sensitive: `stripOrderAndLimit`
    /// deletes it for the reference/rewrite runs, so if a rewrite changes rows
    /// only inside the skipped prefix the stripped bags differ and we raise a
    /// false mismatch even though the observable post-`OFFSET` result matches.
    /// Reject at the gate instead of silently deleting it.
    if (select.limitOffset())
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
    /// `GROUP BY ALL` sets `select.group_by_all` but leaves `select.groupBy()`
    /// empty, so the per-oracle grouping guards (which test `select.groupBy()`)
    /// do not recognise it and a grouping query slips into the non-grouping
    /// TLP / NoREC / DISTINCT paths. There the reference side drops `WHERE` and
    /// keeps one row per group, while the partitioned side runs three
    /// `GROUP BY ALL` branches and `UNION ALL`s them, duplicating groups that
    /// span more than one partition — a false `AST_FUZZER_ORACLE_MISMATCH`.
    /// Skip the whole query rather than special-case every oracle.
    if (select.group_by_all)
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


bool QueryOracleChecker::hasNonDeterministicFunctions(const ASTPtr & ast, const ContextPtr & context)
{
    return hasNonDeterministicFunctionsImpl(ast, context);
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


std::pair<ASTPtr, ASTPtr> QueryOracleChecker::buildTLPReferenceAndPartitions(
    const ASTSelectQuery & select,
    ASTSelectQuery::Expression clause,
    const ASTPtr & predicate,
    SelectUnionMode union_mode,
    const std::function<void(const ASTPtr &)> & transform)
{
    /// Strip (and transform) once on a shared base, then derive the reference
    /// and every partition from it, so all four queries are adjusted identically.
    auto base_ast = select.clone();
    stripOrderAndLimit(base_ast->as<ASTSelectQuery &>());
    if (transform)
        transform(base_ast);

    auto ref_ast = base_ast->clone();
    ref_ast->as<ASTSelectQuery &>().setExpression(clause, {});

    auto make_partition = [&](ASTPtr partition_predicate)
    {
        auto clone_ast = base_ast->clone();
        if (partition_predicate)
            clone_ast->as<ASTSelectQuery &>().setExpression(clause, std::move(partition_predicate));
        return clone_ast;
    };

    auto list = make_intrusive<ASTExpressionList>();
    list->children.push_back(make_partition(nullptr)); /// The original predicate, kept as-is.
    list->children.push_back(make_partition(makeASTFunction("not", predicate->clone())));
    list->children.push_back(make_partition(makeASTFunction("isNull", predicate->clone())));

    auto union_query = make_intrusive<ASTSelectWithUnionQuery>();
    union_query->union_mode = union_mode;
    union_query->is_normalized = true;
    union_query->list_of_selects = list;
    union_query->children.push_back(list);

    return {std::move(ref_ast), ASTPtr(std::move(union_query))};
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
    /// A seed query's `SET aggregate_functions_null_for_empty = 1` would leak
    /// into oracle sub-queries and break NoREC: `count()` over zero input rows
    /// becomes NULL while `countIf` still aggregates every row and returns 0.
    /// Pin it off so both sides of every comparison use standard semantics.
    oracle_context->setSetting("aggregate_functions_null_for_empty", Field(false));
    /// See `truncating_settings`: with this on, `count()` over an empty input
    /// returns zero rows while the NoREC `countIf` form returns one `0` row.
    oracle_context->setSetting("empty_result_for_aggregation_by_empty_set", Field(false));
    /// Constraint-based optimization trusts `CONSTRAINT ... ASSUME ...`
    /// without checking. When a table's data violates its ASSUME constraint
    /// (the fuzz corpus has such tables, e.g. `constraint_test_*`), the
    /// optimizer may simplify a predicate using the (false) assumption in one
    /// rewrite form but evaluate the real data in another, so the reference
    /// and rewrite legitimately disagree — optimizer-dependent by design, not
    /// a bug. Evaluate real data consistently by pinning the constraint
    /// optimizer (and the CNF conversion that feeds it) off.
    oracle_context->setSetting("optimize_using_constraints", Field(false));
    oracle_context->setSetting("convert_query_to_cnf", Field(false));
    /// Neutralize session-leaked read/result caps (a seed's `SET
    /// max_rows_to_read = N, read_overflow_mode = 'break'` would truncate
    /// oracle sub-queries asymmetrically — see `truncating_settings`).
    /// `max_result_rows`/`max_result_bytes`/`result_overflow_mode` and
    /// `max_execution_time` are pinned above/below to the oracle's own caps,
    /// which throw rather than truncate.
    for (const auto * cap : {"max_rows_to_read", "max_bytes_to_read",
                             "max_rows_to_read_leaf", "max_bytes_to_read_leaf",
                             "max_rows_to_group_by", "max_rows_to_sort", "max_bytes_to_sort",
                             "max_rows_in_distinct", "max_bytes_in_distinct",
                             "max_rows_to_transfer", "max_bytes_to_transfer",
                             "max_rows_in_join", "max_bytes_in_join",
                             "max_rows_in_set", "max_bytes_in_set",
                             "max_estimated_execution_time",
                             /// `limit`/`offset` settings: a final LIMIT/OFFSET
                             /// on every result, non-distributive over rewrites.
                             "limit", "offset"})
        oracle_context->setSetting(cap, Field(UInt64(0)));
    for (const auto * mode : {"read_overflow_mode", "read_overflow_mode_leaf",
                              "group_by_overflow_mode", "sort_overflow_mode",
                              "distinct_overflow_mode", "transfer_overflow_mode",
                              "join_overflow_mode", "set_overflow_mode",
                              "timeout_overflow_mode"})
        oracle_context->setSetting(mode, String("throw"));
    /// Session-leaked `SET use_skip_indexes_if_final = 1,
    /// use_skip_indexes_if_final_exact_mode = 0` would make oracle
    /// sub-queries keep stale FINAL row versions (see `truncating_settings`).
    oracle_context->setSetting("use_skip_indexes_if_final_exact_mode", Field(true));
    /// A session-leaked `SET extremes = 1` would append extremes blocks to
    /// every oracle sub-query result (counted as data rows).
    oracle_context->setSetting("extremes", Field(false));
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
    oracle_context->setSetting("max_final_threads", Field(UInt64(1)));
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

    if (hasNonDeterministicFunctions(select.clone(), context))
        return false;

    if (clauseDefinesAliasUsedElsewhere(select, select.where()))
        return false;

    ASTPtr predicate = select.where()->clone();

    /// Reference: the original query without WHERE (and without ORDER BY/LIMIT);
    /// JOINs, GROUP BY, and other clauses are preserved. Partitioned: the
    /// UNION ALL of `WHERE p` / `WHERE NOT p` / `WHERE isNull(p)`.
    auto [ref_ast, union_ast] = buildTLPReferenceAndPartitions(
        select, ASTSelectQuery::Expression::WHERE, predicate, SelectUnionMode::UNION_ALL);

    String ref_sql = formatAST(ref_ast);
    if (ref_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;
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

    if (hasNonDeterministicFunctions(select.clone(), context))
        return false;

    if (clauseDefinesAliasUsedElsewhere(select, select.where()))
        return false;

    /// The `countIf` rewrite drops the SELECT list. If the predicate
    /// references an alias defined there (the fuzzer produces e.g.
    /// `SELECT indexOf([1025 AS x, ...], ...) ... WHERE x = 1`), the
    /// reference silently rebinds to the table column in the rewritten
    /// query — different semantics, false mismatch.
    {
        std::unordered_set<String> select_aliases;
        collectAliases(select.select(), select_aliases);
        if (!select_aliases.empty() && referencesAnyAlias(select.where(), select_aliases))
            return false;
    }

    /// The rewrite also drops the WITH clause; an alias defined there can be
    /// referenced by the predicate even invisibly (through `*` expansion), so
    /// any WITH disqualifies the rewrite.
    if (select.with())
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
    if (hasArrayJoinFunction(select.clone()))
        return false;
    if (select.limitLength() || select.limitBy() || select.limitOffset() || select.prewhere() || select.qualify())
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

    if (hasNonDeterministicFunctions(select.clone(), context))
        return false;

    if (clauseDefinesAliasUsedElsewhere(select, select.where()))
        return false;

    ASTPtr predicate = select.where()->clone();

    /// Reference: remove WHERE, keep DISTINCT. Partitioned: UNION DISTINCT
    /// (not UNION ALL) of the three partitions, to deduplicate across them.
    auto [ref_ast, union_ast] = buildTLPReferenceAndPartitions(
        select, ASTSelectQuery::Expression::WHERE, predicate, SelectUnionMode::UNION_DISTINCT);

    String ref_sql = formatAST(ref_ast);
    if (ref_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;
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

    if (hasNonDeterministicFunctions(select.clone(), context))
        return false;

    if (clauseDefinesAliasUsedElsewhere(select, select.where()))
        return false;

    /// A non-injective projection (e.g. `SELECT g % 2 ... GROUP BY g`) can
    /// render two distinct groups to the same output row; the dedupe on the
    /// partitioned side would then hide a dropped group, masking a real
    /// wrong-result regression. Earlier this oracle simply rejected any query
    /// whose SELECT list didn't equal the GROUP BY list by tree hash, but that
    /// starves it almost completely (fuzzed SELECT lists nearly never match).
    /// Instead, append the GROUP BY expressions themselves to the SELECT list
    /// of every clone (reference and all three partitions identically): the
    /// projection becomes injective per group, so dropped or duplicated groups
    /// stay visible, while the original SELECT expressions — the server already
    /// validated they are functions of the group keys — remain checked too.
    const auto append_group_keys = [](const ASTPtr & query_ast)
    {
        auto & sel = query_ast->as<ASTSelectQuery &>();
        for (const auto & g : sel.groupBy()->children)
            sel.select()->children.push_back(g->clone());
    };

    ASTPtr predicate = select.where()->clone();

    /// Reference: remove WHERE, keep GROUP BY. The `append_group_keys`
    /// transform runs once on the shared base, so the reference and all
    /// three partitions get the group keys appended identically.
    auto [ref_ast, union_ast] = buildTLPReferenceAndPartitions(
        select, ASTSelectQuery::Expression::WHERE, predicate, SelectUnionMode::UNION_ALL, append_group_keys);

    String ref_sql = formatAST(ref_ast);
    if (ref_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;
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

    if (hasNonDeterministicFunctions(select.clone(), context))
        return false;

    if (clauseDefinesAliasUsedElsewhere(select, select.having()))
        return false;

    ASTPtr having_pred = select.having()->clone();

    /// Reference: remove HAVING, keep GROUP BY and everything else.
    /// Partitioned: partition on HAVING instead of WHERE.
    auto [ref_ast, union_ast] = buildTLPReferenceAndPartitions(
        select, ASTSelectQuery::Expression::HAVING, having_pred, SelectUnionMode::UNION_ALL);

    String ref_sql = formatAST(ref_ast);
    if (ref_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;
    String union_sql = formatAST(union_ast);
    if (union_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleChecks);
    LOG_TRACE(logger, "TLP HAVING oracle: reference: {}", ref_sql);
    LOG_TRACE(logger, "TLP HAVING oracle: partitioned: {}", union_sql);

    /// Compare as a bag — TLP HAVING partitions complete groups by the
    /// HAVING predicate, so the three branches are disjoint at the group
    /// level and `UNION ALL` preserves bag multiplicity. Deduplicating
    /// would let a wrong result pass when different groups produce
    /// identical output rows (e.g. `SELECT count() FROM t GROUP BY g
    /// HAVING count() > 0` returning `{10, 10}` — if the partitioned
    /// rewrite drops one group, both sides become `{10}` after dedupe).
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
            "TLP HAVING oracle mismatch!\n"
            "Reference query ({} rows): {}\n"
            "Partitioned query ({} rows): {}",
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
    /// Probability-gated to limit overhead; DQP is the only oracle that catches
    /// optimizer bugs directly (e.g. the `query_plan_remove_redundant_distinct`
    /// over CUBE/ROLLUP wrong result), so it gets a generous share.
    if (thread_local_rng() % 4 != 0)
        return false;

    if (!isSafeForOracle(select))
        return false;

    if (hasNonDeterministicFunctions(select.clone(), context))
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

    if (hasNonDeterministicFunctions(select.clone(), context))
        return false;

    if (clauseDefinesAliasUsedElsewhere(select, select.where()))
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
    /// Lowercased: aggregate names resolve case-insensitively (AVG == avg), but the
    /// parser preserves the original spelling, so we must lowercase before the lookup
    /// (same as `non_deterministic_functions_lower` above). Otherwise `AVG`/`SUM`
    /// bypass this denylist while `AVGState`/`SUMState` still resolve, producing
    /// false State/Merge oracle mismatches on floating-point inputs.
    static const std::unordered_set<String> non_associative_aggregates = {
        "sum", "sumkahan", "sumwithoverflow",
        "avg", "avgweighted",
        "stddevpop", "stddevsamp", "stddevpopstable", "stddevsampstable",
        "varpop", "varsamp", "varpopstable", "varsampstable",
        "covarpop", "covarsamp", "covarpopstable", "covarsampstable",
        "corr", "corrstable",
        "skewpop", "skewsamp",
        "kurtpop", "kurtsamp",
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
        const String base_name = stripAggregateCombinators(name);
        if (non_associative_aggregates.contains(Poco::toLower(base_name)))
            return false;
        /// Existing combinators need special argument and State/Merge handling.
        /// Use the shared complete suffix list, rather than duplicating a
        /// partial denylist here.
        if (base_name != name)
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
    /// Alias each inner GROUP BY key as `_g_N`. A computed key such as
    /// `toUInt8(v % 2)` only resolves inside the subquery; the outer query runs
    /// over `FROM (<union>)` where `v` no longer exists, so re-emitting the raw
    /// key expression in the outer SELECT / GROUP BY throws `UNKNOWN_IDENTIFIER`
    /// and silently drops grouped-aggregate cases with computed keys. Project
    /// the key under a stable alias and reference that alias from the outside.
    std::unordered_map<UInt64, String> group_key_alias; /// tree-hash -> `_g_N`
    if (has_group_by)
    {
        size_t g_idx = 0;
        for (const auto & group_expr : inner_select.groupBy()->children)
        {
            /// A GROUP BY key that is an asterisk / `COLUMNS(...)` matcher derives
            /// directly from `IAST` and cannot take an alias (`IAST::setAlias`
            /// throws `LOGICAL_ERROR`), and it could not be referenced by a stable
            /// alias from the outer query anyway. Skip the oracle for such queries.
            /// `dynamic_cast`, not `as<...>`: `ASTWithAlias` is a base class and
            /// `IAST::as` is an exact-typeid cast, so `as<ASTWithAlias>()` is false
            /// for every concrete node — it silently disabled this oracle for ALL
            /// grouped queries (caught by 04658_ast_fuzzer_oracle_tlp_aggregate_counter).
            if (!dynamic_cast<const ASTWithAlias *>(group_expr.get()))
                return false;
            String g_alias = fmt::format("_g_{}", g_idx++);
            group_key_alias[group_expr->getTreeHash(/*ignore_aliases=*/true).low64] = g_alias;
            auto g_clone = group_expr->clone();
            g_clone->setAlias(g_alias);
            new_inner_select_list->children.push_back(std::move(g_clone));
        }
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
        {
            /// Non-aggregate select items are GROUP BY keys (guaranteed by the
            /// gate above). Reference the inner `_g_N` alias, not the raw key
            /// expression, which does not resolve outside the subquery.
            auto it = group_key_alias.find(select_expr->getTreeHash(/*ignore_aliases=*/true).low64);
            if (it != group_key_alias.end())
                outer_select_list->children.push_back(make_intrusive<ASTIdentifier>(it->second));
            else
                outer_select_list->children.push_back(select_expr->clone());
        }
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
        /// Group by the inner `_g_N` aliases (see the aliasing above), not the raw
        /// key expressions — those reference columns that exist only inside the
        /// subquery.
        String keys;
        for (size_t i = 0; i < inner_select.groupBy()->children.size(); ++i)
            keys += (i ? ", " : "") + fmt::format("_g_{}", i);
        group_by_str = fmt::format(" GROUP BY {}", keys);
    }

    String metamorphic_sql = fmt::format(
        "SELECT {} FROM ({}){}",
        outer_select_str, union_sql, group_by_str);

    if (metamorphic_sql.size() > MAX_ORACLE_QUERY_LENGTH)
        return false;

    ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleChecks);
    /// Dedicated counter so a test can prove this specific oracle path ran —
    /// with `ast_fuzzer_runs > 0` the mutated query may lose the aggregate
    /// shape, so a plain "query succeeded" assertion proves nothing.
    ProfileEvents::increment(ProfileEvents::ASTFuzzerOracleTLPAggregateChecks);

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
    if (hasNonDeterministicFunctions(select.clone(), context))
        return false;

    /// `hasNonDeterministicFunctions` is name-based and does not filter general
    /// window functions like `row_number()`, `rank()`, `dense_rank()` over a
    /// window without an explicit ORDER BY. Those produce implementation-defined
    /// row numbers; rewriting the WHERE predicate is permitted to reorder rows
    /// seen by the window function, which legitimately changes the assignment.
    /// Use the same gate as `checkSubqueryWrap` (issue #105743).
    if (hasWindowFunctionWithoutOrderByAnywhere(select))
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
    /// `OFFSET` (without `LIMIT`) skips a prefix of the result. For non-unique
    /// `ORDER BY` keys, the rewritten WHERE may legitimately reorder tied rows,
    /// so the same `OFFSET` skips a different prefix and the comparison fails
    /// with a spurious mismatch. Same order-tie family as `LIMIT` / `LIMIT BY`.
    if (select.limitOffset())
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
    if (hasNonDeterministicFunctions(select.clone(), context))
        return false;

    /// PREWHERE produces false positives with suspicious type coercions.
    if (select.prewhere())
        return false;

    /// Skip any query with LIMIT / LIMIT BY / OFFSET. The reference clone is
    /// passed through `stripOrderAndLimit` below, so admitting these shapes
    /// would compare the *unbounded* query instead of the top-N / per-key /
    /// offset query that actually succeeded — hiding wrong-result bugs in
    /// LIMIT handling and, worse, comparing different semantics than the seed.
    /// Even LIMIT with ORDER BY is unsafe: on non-unique sort keys the engine
    /// may legitimately pick different rows among ties on each side.
    if (select.limitLength() || select.limitBy() || select.limitOffset())
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
    if (hasWindowFunctionWithoutOrderByAnywhere(select))
        return false;

    /// `stripOrderAndLimit` removes ORDER BY. Reject row-expanding functions
    /// anywhere in the query before it can remove an `ORDER BY arrayJoin(...)`
    /// expression and make the oracle validate a different query shape.
    if (hasArrayJoin(select) || hasPasteJoin(select) || hasArrayJoinFunction(select.clone()))
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
        /// Both sides read the very same relation (`T` vs `SELECT * FROM (T)`) and
        /// the comparison is over sorted row-sets, so ordering is already
        /// normalized out. A difference can therefore only come from a
        /// non-deterministic read of the base query itself — e.g. a
        /// merge-collapsing engine read without `FINAL`
        /// (`SummingMergeTree`/`AggregatingMergeTree`/`ReplacingMergeTree`/
        /// `CollapsingMergeTree`), an `AggregateFunction` state column whose
        /// serialized bytes are not canonical, or a non-deterministic function.
        /// Re-execute the reference once more: if it is not stable across two
        /// consecutive runs the query is non-deterministic, so this is an oracle
        /// false positive rather than a subquery-wrapping bug — skip it.
        auto ref_rows_again_opt = executeAndCollectSortedRows(ref_sql, context);
        if (!ref_rows_again_opt || *ref_rows_again_opt != ref_rows)
            return false;

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


namespace
{
/// Format the context's non-default settings as `name=value, ...`. Attached
/// to every mismatch exception so a mismatch found mid-fuzz-sequence (where
/// earlier `SET`/`SETTINGS` mutations have drifted the session) is
/// reproducible standalone — without this, many real-looking mismatches
/// could not be reproduced because the active settings were unknown.
String formatChangedSettings(const ContextPtr & context)
{
    WriteBufferFromOwnString buf;
    bool first = true;
    for (const auto & change : context->getSettingsRef().changes())
    {
        if (!first)
            buf << ", ";
        first = false;
        buf << change.name << "=" << applyVisitor(FieldVisitorToString(), change.value);
    }
    return buf.str();
}
}

bool QueryOracleChecker::check(const ASTPtr & query_ast, const ContextMutablePtr & context)
{
    /// The oracle runs after the fuzzed query finished, so the context's
    /// process-list entry is already gone. `getProcessListElement` treats a
    /// set-but-expired pointer as a logical error, and some functions read it
    /// at create-time (e.g. `h3PolygonToCellsWithContainment`), which the
    /// gates reach through `FunctionFactory::tryGet` and the oracle queries
    /// through query analysis. Detach the dead pointer up front; oracle
    /// sub-queries are internal and are not registered in the process list.
    context->setProcessListElement({});

    const ASTSelectQuery * select = extractSimpleSelect(query_ast);
    if (!select)
    {
        LOG_TRACE(logger, "Oracle skip: not a simple SELECT");
        return false;
    }

    if (hasTruncatingInlineSettings(query_ast))
    {
        LOG_TRACE(logger, "Oracle skip: query carries truncating settings (read/result caps)");
        return false;
    }

    /// The oracles delete the top-level inline `SETTINGS` clause from their
    /// clones (see `stripOrderAndLimit`), which is only sound when every
    /// setting it carries is result-invariant. A semantic setting like
    /// `join_use_nulls` or `apply_mutations_on_fly` would make the oracle
    /// validate a different query than the one that actually succeeded.
    if (hasNonStrippableInlineSettings(query_ast))
    {
        LOG_TRACE(logger, "Oracle skip: query carries inline settings that cannot be stripped safely");
        return false;
    }

    if (hasNestedThreadSettings(query_ast, select->settings()))
    {
        LOG_TRACE(logger, "Oracle skip: query carries nested thread-count settings");
        return false;
    }

    /// An explicit `FORMAT ...` / `INTO OUTFILE` survives on the fuzzed AST
    /// (the fuzzer preserves `ASTQueryWithOutput::format_ast`), and
    /// `executeAndCollectSortedRows` relies on the default `TabSeparated` to
    /// parse rows. `setDefaultFormat` does NOT override an explicit format, so
    /// `FORMAT Null` would make every side an empty vector (vacuous pass) and
    /// `FORMAT Pretty`/`JSONEachRow` would compare serializer output instead of
    /// row values. Skip such queries — the oracle only reasons about rows.
    if (const auto * with_output = query_ast->as<ASTQueryWithOutput>())
    {
        if (with_output->format_ast || with_output->out_file)
        {
            LOG_TRACE(logger, "Oracle skip: explicit FORMAT / INTO OUTFILE (oracle compares TabSeparated rows)");
            return false;
        }
    }

    if (hasAsofJoinAnywhere(query_ast))
    {
        LOG_TRACE(logger, "Oracle skip: ASOF JOIN (tie-breaking is plan-dependent)");
        return false;
    }

    if (hasWithFillAnywhere(query_ast))
    {
        LOG_TRACE(logger, "Oracle skip: ORDER BY ... WITH FILL (synthesises order-dependent rows)");
        return false;
    }

    if (usesFinalAnywhere(query_ast))
    {
        LOG_TRACE(logger, "Oracle skip: FINAL (dedup is not distributive over WHERE partitions)");
        return false;
    }

    /// `Distributed` tables route to remote shards (here, test clusters whose
    /// replicas all point at localhost), so a row is read once per shard and
    /// the reference vs rewrite can route/dedup differently — the oracle can't
    /// validate the result. Resolve each referenced table's engine (including
    /// tables hidden inside subqueries / CTEs) and skip.
    if (referencesDistributedTableAnywhere(query_ast, context))
    {
        LOG_TRACE(logger, "Oracle skip: query reads from a Distributed table");
        return false;
    }

    /// A `*` inside GROUP BY / HAVING re-expands over a different column set
    /// when an oracle wraps or re-projects the query, breaking key
    /// equivalence. (A bare top-level `SELECT *` or `count(*)` is fine — it
    /// expands over the same table on both sides — so only the grouping
    /// clauses are gated, not the SELECT list.)
    if ((select->groupBy() && containsAsterisk(select->groupBy()))
        || (select->having() && containsAsterisk(select->having())))
    {
        LOG_TRACE(logger, "Oracle skip: asterisk in GROUP BY / HAVING (context-sensitive expansion)");
        return false;
    }

    /// `GROUP BY ALL` is a grouping query whose grouping keys are only known
    /// after analysis (`select->groupBy()` is empty at the AST level). The
    /// per-oracle grouping guards test `select->groupBy()`, so without this
    /// top-level reject a `GROUP BY ALL` query would reach the non-grouping
    /// oracles (some of which mirror `isSafeForOracle` inline rather than
    /// calling it) and produce a false mismatch. See `isSafeForOracle`.
    if (select->group_by_all)
    {
        LOG_TRACE(logger, "Oracle skip: GROUP BY ALL (grouping keys not known at AST level)");
        return false;
    }

    /// `system.*` and `INFORMATION_SCHEMA.*` are non-deterministic snapshots
    /// of live server state; reading them twice (reference + rewrite) almost
    /// always shows drift in `processes`, `merges`, `metric_log`, etc.
    /// Reject the whole query at the gate to avoid spurious mismatches in
    /// every oracle.
    if (referencesNonDeterministicDatabase(*select, context->getCurrentDatabase())
        || referencesSystemDatabaseAnywhere(query_ast, context->getCurrentDatabase()))
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

    bool any_check_performed = false;

    /// Run one oracle under a uniform guard: its own mismatch
    /// (`AST_FUZZER_ORACLE_MISMATCH`) propagates and is annotated with the
    /// reproduction settings by the outer handler below; any other execution
    /// error means the rewrite was not comparable on this query (e.g. a
    /// function the rewrite cannot analyse), so it is swallowed and the
    /// remaining oracles still run. `name` reproduces the per-oracle log wording.
    auto run_oracle = [&](std::string_view name, auto && check_fn)
    {
        try
        {
            if (check_fn())
                any_check_performed = true;
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::AST_FUZZER_ORACLE_MISMATCH)
                throw;
            LOG_TRACE(logger, "{} oracle execution error (skipping): {}", name, e.message());
        }
        catch (...)
        {
            LOG_TRACE(logger, "{} oracle execution error (skipping): {}", name, getCurrentExceptionMessage(false));
        }
    };

    try
    {
        run_oracle("TLP WHERE", [&] { return checkTLPWhere(*select, context); });
        run_oracle("NoREC", [&] { return checkNoREC(*select, context); });
        /// TLP Aggregate oracle (uses State/Merge combinators for any aggregate).
        run_oracle("TLP Aggregate", [&] { return checkTLPAggregate(*select, context); });
        /// TLP DISTINCT oracle (uses UNION DISTINCT instead of UNION ALL).
        run_oracle("TLP DISTINCT", [&] { return checkTLPDistinct(*select, context); });
        /// TLP GROUP BY oracle (set comparison for non-aggregate GROUP BY).
        run_oracle("TLP GROUP BY", [&] { return checkTLPGroupBy(*select, context); });
        /// TLP HAVING oracle (partitions on HAVING instead of WHERE).
        run_oracle("TLP HAVING", [&] { return checkTLPHaving(*select, context); });
        /// DQP oracle (differential query plans — same query, different optimizer settings).
        run_oracle("DQP", [&] { return checkDQP(*select, context); });
        /// Identity WHERE oracle (rewrites WHERE into equivalent forms — NOT(NOT p), p AND 1, p OR 0).
        run_oracle("Identity WHERE", [&] { return checkIdentityWhere(*select, context); });
        /// Subquery wrap oracle (wraps original as subquery and verifies identical result).
        run_oracle("Subquery wrap", [&] { return checkSubqueryWrap(*select, context); });
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::AST_FUZZER_ORACLE_MISMATCH)
        {
            const String changed = formatChangedSettings(context);
            if (!changed.empty())
                e.addMessage("Active non-default settings (for reproduction): {}", changed);
        }
        throw;
    }

    return any_check_performed;
}

}
