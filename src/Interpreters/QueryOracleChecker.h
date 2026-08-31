#pragma once

#include <Core/Field.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/SelectUnionMode.h>
#include <Interpreters/QueryOracles/OracleGate.h>
#include <Interpreters/Context_fwd.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>

#include <functional>
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

    /// The individual oracles. Public because `OracleRegistry` dispatches over member
    /// pointers to them (phase-0 adapters); prefer `check` for direct use. Each returns
    /// true iff it performed a comparison and throws `AST_FUZZER_ORACLE_MISMATCH` on a
    /// real mismatch.
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

    /// GROUP BY key permutation oracle: grouping keys form a set, so `GROUP BY a, b` must
    /// return the identical result multiset as `GROUP BY b, a`. Catches multi-key grouping /
    /// aggregation bugs that depend on key order.
    bool checkGroupByKeyPermutation(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// DISTINCT-via-GROUP-BY oracle: `SELECT DISTINCT <exprs> ...` must return the identical
    /// result set as `SELECT <exprs> ... GROUP BY <exprs>`. The two go through different
    /// execution paths (DistinctStep vs aggregation), so a divergence is a real bug.
    bool checkDistinctViaGroupBy(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// PREWHERE-equivalence oracle: on a single MergeTree-family table, `SELECT ... WHERE p` must
    /// return the identical result multiset as `SELECT ... PREWHERE p` (PREWHERE is a transparent
    /// read-time optimization of WHERE). A divergence is a real PREWHERE bug.
    bool checkPrewhereEquivalence(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Skip-index-equivalence oracle: skip indexes only prune granules, they must never change the
    /// result. Running the same query with `use_skip_indexes=0` vs `=1` must return the identical
    /// multiset; a difference is a real skip-index granule-pruning bug.
    bool checkSkipIndexEquivalence(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Setting-flip sweep: run byte-identical SQL with a result-invariant optimizer/cache setting
    /// toggled off vs on. Such settings (query condition cache, lazy materialization, plan-level
    /// PREWHERE move, ...) must never change the result multiset, so a divergence is a real bug.
    bool checkSettingFlipSweep(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Codec round-trip oracle (self-seeded, ignores the fuzzed query): codecs are lossless, so
    /// identical data stored under CODEC(NONE) vs compression codecs must read back identical.
    /// Creates its own fixture tables via OracleFixture; rate-limited.
    bool checkCodecRoundtrip(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Engine-equivalence oracle (self-seeded): identical schema + data stored in a MergeTree vs a
    /// row-based engine (Memory/TinyLog/Log/StripeLog) must read back the identical multiset. A
    /// difference is a real engine read/serialization bug.
    bool checkEngineEquivalence(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Partition-equivalence oracle (self-seeded): PARTITION BY is transparent to results. The same
    /// query over identical data in a partitioned vs non-partitioned MergeTree must return the
    /// identical multiset; a difference is a real partition-pruning / cross-partition-merge bug.
    bool checkPartitionEquivalence(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// LowCardinality-equivalence oracle (self-seeded): LowCardinality(T) stores the same logical
    /// values as T. Identical data as LowCardinality(String) vs plain String must read back and
    /// group identically; a difference is a real LowCardinality bug.
    bool checkLowCardinalityEquivalence(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// SAMPLE-equivalence oracle (self-seeded): SAMPLE 1.0 reads the whole table, so on a table
    /// with a SAMPLE BY key `SELECT ... SAMPLE 1.0` must equal `SELECT ...`; a difference is a
    /// real sampling bug.
    bool checkSampleEquivalence(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Projection-equivalence oracle (self-seeded): an aggregating projection must not change
    /// results. The same integer-aggregate query with optimize_use_projections=0 vs =1 must be
    /// identical; a difference is a real projection bug. Integer aggregates only (float sums are
    /// non-associative across the projection vs base-table paths).
    bool checkProjectionEquivalence(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Aggregate-If identity oracle (self-seeded): `aggIf(x, cond)` must equal the same aggregate
    /// computed by arithmetic/if masking (e.g. sumIf(v,c)=sum(v*c), maxIf(v,c)=max(if(c,v,NULL))).
    /// Compares the -If combinator against a DIFFERENT computation path; a divergence is a real bug.
    bool checkAggregateIfIdentity(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// NULL-identity oracle (self-seeded): sound NULL-handling equivalences (ifNull/coalesce/
    /// nullIf/isNull) must hold row-for-row over Nullable data; a violation is a real bug.
    bool checkNullIdentity(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// CAST round-trip oracle (self-seeded): integer and Date values survive a String round-trip
    /// exactly (CAST(CAST(x AS String) AS T) == x); a violation is a real CAST/parse bug.
    bool checkCastRoundtrip(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Aggregate-state-column oracle (self-seeded): a -State written into an AggregatingMergeTree
    /// column, persisted across parts, and read back with -Merge must equal the direct aggregate
    /// over the raw data; a difference is a real aggregate state-I/O bug.
    bool checkAggregateStateColumn(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Tuple-summing oracle (self-seeded): a SummingMergeTree with a Tuple value column collapses
    /// rows per key by summing each element; a FINAL read must equal an element-wise sum over the
    /// same rows flattened into a plain MergeTree. A difference is a real SummingSortedAlgorithm bug.
    bool checkTupleSumming(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Schema round-trip oracle (self-seeded): a table's DDL must be an idempotent fixed point —
    /// recreating a table from its own SHOW CREATE and re-serializing must yield the identical DDL
    /// (modulo the table name). A difference is a real metadata serialization bug.
    bool checkSchemaRoundtrip(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// DELETE-mutation oracle (self-seeded): after DELETE FROM t WHERE p (non-null p), the
    /// surviving rows must equal a never-mutated snapshot filtered by NOT p. A difference is a
    /// real lightweight-delete / mutation bug.
    bool checkDeleteMutation(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// UPDATE-mutation oracle (self-seeded): after ALTER UPDATE x = e WHERE p, the table must equal
    /// a snapshot with e applied to x on rows matching p and x unchanged elsewhere. A difference is
    /// a real mutation bug.
    bool checkUpdateMutation(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// MATERIALIZE-INDEX invariance oracle (self-seeded): adding and materializing a skip index
    /// must not change the data. The table after ADD INDEX + MATERIALIZE INDEX must equal a
    /// never-mutated snapshot; a difference is a real index-materialization bug.
    bool checkMaterializeIndexInvariance(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// De-Morgan / comparison-symmetry oracle (self-seeded): three-valued-logic identities that
    /// must hold row-for-row over Nullable data (De Morgan, comparison operator symmetry).
    bool checkPredicateDeMorgan(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// ARRAY JOIN identity oracle (self-seeded): INNER ARRAY JOIN emits one row per array element,
    /// so count() over an array-joined table == sum(length(arr)) and sum(element) == sum(arraySum(arr)).
    /// A difference is a real ARRAY JOIN bug.
    bool checkArrayJoinIdentity(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Grouping-modifier equivalence oracle (self-seeded): CUBE(a,b) and ROLLUP(a,b) are defined as
    /// GROUPING SETS expansions, so their result multisets must be identical. A difference is a real
    /// grouping-modifier bug.
    bool checkGroupingSetsEquivalence(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Row-policy equivalence oracle (self-seeded): a single permissive row policy USING p must make
    /// SELECT * FROM t return exactly the rows that WHERE p returns without the policy. Self-checks
    /// that the policy actually applies in the oracle context and skips otherwise (keeps it sound).
    bool checkRowPolicyEquivalence(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// FINAL-merge oracle (self-seeded): reading a ReplacingMergeTree(ver) table with FINAL must equal
    /// the hand-written per-key max-version dedup argMax(v, ver) GROUP BY key.
    bool checkFinalMergeReplacing(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// WITH FILL oracle (self-seeded): with all real values on the fill grid and inside [FROM, TO),
    /// ORDER BY x WITH FILL FROM f TO t STEP s must produce exactly the grid {f, f+s, ...} positionally.
    bool checkWithFillGrid(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Pipe-operator equivalence oracle (self-seeded): a classic SELECT and its pipe rendering
    /// (FROM t |> WHERE p |> SELECT ...) must return the same multiset (pipe is pure syntax).
    bool checkPipeEquivalence(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Dictionary oracle (self-seeded): dictGet/dictHas against a hashed dictionary must equal the
    /// equivalent LEFT JOIN / IN lookup against the dictionary's own source table.
    bool checkDictGetVsJoin(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Materialized/ALIAS column oracle (self-seeded): reading a MATERIALIZED or ALIAS column must
    /// equal recomputing its defining expression.
    bool checkMaterializedColumn(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// ALTER MODIFY COLUMN oracle (self-seeded): a value-preserving column widening must equal an
    /// element-wise CAST of the pre-ALTER snapshot.
    bool checkAlterModifyWiden(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Lightweight-update oracle (self-seeded): a lightweight UPDATE (patch part) must leave a table
    /// in the same state as the equivalent heavy ALTER UPDATE, apply_patch_parts must toggle the
    /// patch coherently, and OPTIMIZE FINAL materialization must be a no-op on the applied result.
    bool checkLightweightUpdate(const ASTSelectQuery & select, const ContextMutablePtr & context);

    /// Window-frame equivalence oracle (self-seeded): the implicit default frame equals the explicit
    /// RANGE UNBOUNDED PRECEDING..CURRENT ROW, and a whole-partition frame is identical across the
    /// ROWS/RANGE/GROUPS modes.
    bool checkWindowEquivalence(const ASTSelectQuery & select, const ContextMutablePtr & context);

private:
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

    /// Extract the single ASTSelectQuery from an AST if it is a simple
    /// non-UNION SELECT. Returns nullptr otherwise.
    static const ASTSelectQuery * extractSimpleSelect(const ASTPtr & ast);

    /// Check if a SELECT query is structurally safe for oracle testing.
    static bool isSafeForOracle(const ASTSelectQuery & select, GateRelax relax = GateRelax::None);

    /// Check if the AST contains non-deterministic functions. Uses
    /// `FunctionFactory::isDeterministic` as the primary source of truth and
    /// falls back to a small list of table-function and oracle-unsafe-aggregate
    /// names for the cases the factory does not cover.
    static bool hasNonDeterministicFunctions(const ASTPtr & ast, const ContextPtr & context);

    /// Strip ORDER BY, LIMIT, LIMIT BY, SETTINGS, INTERPOLATE from a cloned ASTSelectQuery.
    static void stripOrderAndLimit(ASTSelectQuery & select);

    /// Build the two sides of a TLP comparison from `select`: the reference
    /// (the query without `clause`) and the partitioned UNION of three clones
    /// whose `clause` is the original `predicate`, `NOT predicate`, and
    /// `isNull(predicate)`. `stripOrderAndLimit` is applied once to a shared
    /// base clone, and `transform` (when set) post-processes that base so the
    /// reference and every partition are adjusted identically.
    static std::pair<ASTPtr, ASTPtr> buildTLPReferenceAndPartitions(
        const ASTSelectQuery & select,
        ASTSelectQuery::Expression clause,
        const ASTPtr & predicate,
        SelectUnionMode union_mode,
        const std::function<void(const ASTPtr &)> & transform = {});

    /// Format an AST to a one-line SQL string.
    static String formatAST(const ASTPtr & ast);

    LoggerPtr logger = getLogger("QueryOracleChecker");
};

}
