#pragma once

#include <Core/Field.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>

#include <vector>


namespace DB
{

/// Applies correctness oracle checks (TLP WHERE and NoREC) to a successfully-executed
/// fuzzed SELECT query. Throws LOGICAL_ERROR on oracle mismatch — the bug-finding signal.
///
/// TLP WHERE: verifies that removing the WHERE predicate and instead partitioning via
/// UNION ALL of (WHERE p, WHERE NOT p, WHERE p IS NULL) produces the same result set.
///
/// NoREC: verifies that SELECT count() ... WHERE cond equals SELECT countIf(cond) FROM ...
class QueryOracleChecker
{
public:
    /// Run oracle checks on a successfully-executed fuzzed query AST.
    /// Returns true if at least one oracle check was performed.
    /// Throws LOGICAL_ERROR on oracle mismatch.
    bool check(const ASTPtr & query_ast, const ContextMutablePtr & context);

private:
    bool checkTLPWhere(const ASTSelectQuery & select, const ContextMutablePtr & context);
    bool checkNoREC(const ASTSelectQuery & select, const ContextMutablePtr & context);
    bool checkTLPAggregate(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Try to insert random data into the table referenced by the SELECT query.
    void tryPopulateTable(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Check if the SELECT list contains aggregate functions.
    static bool hasAggregates(const ASTSelectQuery & select);

    /// Execute a query using the ReadBuffer/WriteBuffer executeQuery API and return
    /// the output as a sorted vector of rows (one string per row, tab-separated columns).
    /// This is crash-safe because ClickHouse handles all serialization internally.
    static std::vector<String> executeAndCollectSortedRows(const String & query, const ContextMutablePtr & context);

    /// Execute a scalar query (returns a single value) and return the Field.
    static Field executeScalar(const String & query, const ContextMutablePtr & context);

    /// Build a fresh context for oracle sub-queries.
    static ContextMutablePtr makeOracleContext(const ContextMutablePtr & base_context);

    /// Extract the single ASTSelectQuery from an AST if it is a simple
    /// non-UNION SELECT. Returns nullptr otherwise.
    static const ASTSelectQuery * extractSimpleSelect(const ASTPtr & ast);

    /// Check if a SELECT query is structurally safe for oracle testing.
    static bool isSafeForOracle(const ASTSelectQuery & select);

    /// Check if the AST contains non-deterministic functions.
    static bool hasNonDeterministicFunctions(const ASTPtr & ast);

    /// Strip ORDER BY, LIMIT, LIMIT BY, SETTINGS, INTERPOLATE from a cloned ASTSelectQuery.
    static void stripOrderAndLimit(ASTSelectQuery & select);

    /// Format an AST to a one-line SQL string.
    static String formatAST(const ASTPtr & ast);

    LoggerPtr logger = getLogger("QueryOracleChecker");
};

}
