#pragma once

#include <Core/Field.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>

#include <optional>
#include <vector>


namespace DB
{

/// Applies a suite of correctness oracle checks to a successfully-executed fuzzed SELECT
/// query. Throws `AST_FUZZER_ORACLE_MISMATCH` on a real mismatch — the bug-finding signal.
///
/// The current set of oracles is implemented in the corresponding `check*` methods below
/// (TLP WHERE/DISTINCT/GROUP BY/HAVING/Aggregate, NoREC, DQP, Identity WHERE, Subquery wrap).
/// See `check` for the dispatch logic and per-oracle preconditions.
class QueryOracleChecker
{
public:
    /// Run oracle checks on a successfully-executed fuzzed query AST.
    /// Returns true if at least one oracle check was performed.
    /// Throws `AST_FUZZER_ORACLE_MISMATCH` on oracle mismatch.
    bool check(const ASTPtr & query_ast, const ContextMutablePtr & context);

private:
    bool checkTLPWhere(const ASTSelectQuery & select, const ContextMutablePtr & context);
    bool checkTLPDistinct(const ASTSelectQuery & select, const ContextMutablePtr & context);
    bool checkTLPGroupBy(const ASTSelectQuery & select, const ContextMutablePtr & context);
    bool checkTLPHaving(const ASTSelectQuery & select, const ContextMutablePtr & context);
    bool checkNoREC(const ASTSelectQuery & select, const ContextMutablePtr & context);
    bool checkTLPAggregate(const ASTSelectQuery & select, const ContextMutablePtr & context);
    bool checkDQP(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Metamorphic identity oracle: verifies that `WHERE p`, `WHERE NOT(NOT p)`,
    /// `WHERE (p) AND (1)`, and `WHERE (p) OR (0)` all return the same rows.
    /// Unlike TLP, this works on ANY SELECT that has WHERE — including queries
    /// with LIMIT, DISTINCT, GROUP BY, HAVING, or aggregates, because it doesn't
    /// change query structure, only rewrites the WHERE predicate in equivalent ways.
    bool checkIdentityWhere(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Subquery pushdown oracle: verifies that
    /// `SELECT ... FROM t WHERE p`
    /// equals
    /// `SELECT ... FROM (SELECT * FROM t) WHERE p`.
    /// Tests predicate pushdown through subqueries.
    bool checkSubqueryWrap(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Try to insert random data into the table referenced by the SELECT query.
    void tryPopulateTable(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Check if the SELECT list contains aggregate functions.
    static bool hasAggregates(const ASTSelectQuery & select);

    /// Execute a query and return sorted deduplicated rows (set semantics).
    /// `std::nullopt` means the output exceeded `MAX_ORACLE_OUTPUT_SIZE` and the
    /// caller should skip the oracle rather than treat the result as empty.
    static std::optional<std::vector<String>> executeAndCollectSortedUniqueRows(const String & query, const ContextMutablePtr & context);

    /// Execute a query with specific settings overrides. Returns `std::nullopt` on overflow.
    static std::optional<std::vector<String>> executeWithSettings(const String & query, const ContextMutablePtr & context, const std::vector<std::pair<String, Field>> & settings);

    /// Execute a query using the ReadBuffer/WriteBuffer executeQuery API and return
    /// the output as a sorted vector of rows (one string per row, tab-separated columns).
    /// This is crash-safe because ClickHouse handles all serialization internally.
    /// Returns `std::nullopt` if the formatted output exceeds `MAX_ORACLE_OUTPUT_SIZE`,
    /// so callers don't mistake "skipped due to overflow" for "real empty result".
    static std::optional<std::vector<String>> executeAndCollectSortedRows(const String & query, const ContextMutablePtr & context);

    /// Execute a scalar query (returns a single value) and return the Field.
    static Field executeScalar(const String & query, const ContextMutablePtr & context);

    /// Build a fresh context for oracle sub-queries.
    static ContextMutablePtr makeOracleContext(const ContextMutablePtr & base_context);

    /// Extract the single ASTSelectQuery from an AST if it is a simple
    /// non-UNION SELECT. Returns nullptr otherwise.
    static const ASTSelectQuery * extractSimpleSelect(const ASTPtr & ast);

    /// Check if a SELECT query is structurally safe for oracle testing.
    static bool isSafeForOracle(const ASTSelectQuery & select);

    /// Check if the AST contains non-deterministic functions. Uses
    /// `FunctionFactory::isDeterministic` as the primary source of truth and
    /// falls back to a small list of table-function and oracle-unsafe-aggregate
    /// names for the cases the factory does not cover.
    static bool hasNonDeterministicFunctions(const ASTPtr & ast, const ContextPtr & context);

    /// Strip ORDER BY, LIMIT, LIMIT BY, SETTINGS, INTERPOLATE from a cloned ASTSelectQuery.
    static void stripOrderAndLimit(ASTSelectQuery & select);

    /// Format an AST to a one-line SQL string.
    static String formatAST(const ASTPtr & ast);

    LoggerPtr logger = getLogger("QueryOracleChecker");
};

}
