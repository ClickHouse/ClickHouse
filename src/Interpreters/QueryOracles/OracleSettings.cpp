#include <Interpreters/QueryOracles/OracleSettings.h>

#include <vector>

namespace DB
{

std::span<const PinnedSetting> oraclePinnedSettings()
{
    /// Built once. Order is irrelevant (all applied unconditionally). The `why` strings are
    /// the curation record; keep them when adding a row.
    static const std::vector<PinnedSetting> pins = []
    {
        std::vector<PinnedSetting> p;

        /// No recursive fuzzing / oracle re-entry from oracle sub-queries and fixture DDL.
        p.push_back({"ast_fuzzer_runs", Field(Float64(0)), "no recursive fuzzing in nested oracle queries"});
        p.push_back({"ast_fuzzer_oracle", Field(false), "no recursive oracle checks in nested oracle queries"});

        /// The oracle's own wall-clock cap (throws, never truncates).
        p.push_back({"max_execution_time", Field(UInt64(10)), "oracle sub-query time cap"});

        /// Prevent the optimizer from pushing TLP predicates across subquery/JOIN boundaries.
        p.push_back({"enable_optimize_predicate_expression", Field(false), "TLP predicate must not cross subquery/JOIN boundaries"});

        /// A seed's `SET aggregate_functions_null_for_empty = 1` would leak into oracle
        /// sub-queries and break NoREC: `count()` over zero input rows becomes NULL while
        /// `countIf` still aggregates every row and returns 0.
        p.push_back({"aggregate_functions_null_for_empty", Field(false), "NoREC: count() vs countIf() must agree on empty input"});
        /// With this on, `count()` over an empty input returns zero rows while the NoREC
        /// `countIf` form returns one `0` row.
        p.push_back({"empty_result_for_aggregation_by_empty_set", Field(false), "NoREC: empty-input aggregation must return one row"});

        /// Constraint-based optimization trusts `CONSTRAINT ... ASSUME ...` without checking;
        /// corpus tables violate their ASSUME, so a predicate simplified via the false
        /// assumption in one rewrite but evaluated on real data in another legitimately
        /// disagrees. Pin the constraint optimizer (and the CNF conversion feeding it) off.
        p.push_back({"optimize_using_constraints", Field(false), "do not trust unchecked ASSUME constraints"});
        p.push_back({"convert_query_to_cnf", Field(false), "feeds optimize_using_constraints"});

        /// Neutralize session-leaked read/result caps that would truncate oracle sub-queries
        /// asymmetrically. The oracle's own caps (max_result_rows/bytes below) throw instead.
        for (const auto * cap : {"max_rows_to_read", "max_bytes_to_read",
                                 "max_rows_to_read_leaf", "max_bytes_to_read_leaf",
                                 "max_rows_to_group_by", "max_rows_to_sort", "max_bytes_to_sort",
                                 "max_rows_in_distinct", "max_bytes_in_distinct",
                                 "max_rows_to_transfer", "max_bytes_to_transfer",
                                 "max_rows_in_join", "max_bytes_in_join",
                                 "max_rows_in_set", "max_bytes_in_set",
                                 "max_estimated_execution_time",
                                 /// a final LIMIT/OFFSET on every result is non-distributive over rewrites.
                                 "limit", "offset"})
            p.push_back({cap, Field(UInt64(0)), "neutralize leaked read/result cap (would truncate one side)"});
        for (const auto * mode : {"read_overflow_mode", "read_overflow_mode_leaf",
                                  "group_by_overflow_mode", "sort_overflow_mode",
                                  "distinct_overflow_mode", "transfer_overflow_mode",
                                  "join_overflow_mode", "set_overflow_mode",
                                  "timeout_overflow_mode"})
            p.push_back({mode, Field(String("throw")), "leaked overflow mode must throw, not silently break"});

        /// Session-leaked `SET use_skip_indexes_if_final = 1, ..._exact_mode = 0` would keep
        /// stale FINAL row versions in oracle sub-queries.
        p.push_back({"use_skip_indexes_if_final_exact_mode", Field(true), "exact FINAL under skip indexes"});
        /// A leaked `SET extremes = 1` would append extremes blocks (counted as data rows).
        p.push_back({"extremes", Field(false), "no extremes blocks in oracle results"});

        /// Cap result size so oracle sub-queries cannot allocate unbounded memory; throw (not
        /// break) so the caller's `output.size() > MAX_ORACLE_OUTPUT_SIZE` post-check is not
        /// defeated by a silently truncated result sitting exactly at the cap.
        p.push_back({"max_result_rows", Field(UInt64(MAX_ORACLE_RESULT_ROWS)), "oracle result row cap (throws)"});
        p.push_back({"max_result_bytes", Field(UInt64(MAX_ORACLE_OUTPUT_SIZE)), "oracle result byte cap (throws)"});
        p.push_back({"result_overflow_mode", Field(String("throw")), "result cap must throw, not truncate"});

        /// Run oracle sub-queries single-threaded so the nested pipeline cannot have
        /// background-pool workers running while the caller tears the pipeline down. TSan
        /// caught a heap-use-after-free that fired exactly in that window; constraining the
        /// nested execution to the caller thread closes the race. Do NOT raise these without
        /// the dedicated TSan-soak validation milestone.
        p.push_back({"max_threads", Field(UInt64(1)), "single-thread pin closes a pipeline-teardown UAF (TSan)"});
        p.push_back({"max_insert_threads", Field(UInt64(1)), "single-thread pin (see max_threads)"});
        p.push_back({"max_final_threads", Field(UInt64(1)), "single-thread pin (see max_threads)"});
        p.push_back({"output_format_parallel_formatting", Field(false), "single-thread pin (see max_threads)"});

        /// DDL enablement for OracleFixture: a seed's `SET readonly=1` denies fixture DDL from
        /// the setting path (ContextAccess enforces readonly from the setting, not from the
        /// internal query flag), and `SET implicit_transaction=1` wraps oracle statements in a
        /// transaction under which DDL/unsupported kinds throw NOT_IMPLEMENTED. Either would
        /// silently disable every fixture oracle fleet-wide, so both are pinned off.
        p.push_back({"readonly", Field(UInt64(0)), "fixture DDL must not be denied by a leaked SET readonly"});
        p.push_back({"implicit_transaction", Field(false), "oracle statements must not run inside an implicit transaction (DDL throws)"});

        return p;
    }();

    return {pins.data(), pins.size()};
}

bool isPinnedByOracleContext(std::string_view name)
{
    for (const auto & pin : oraclePinnedSettings())
        if (pin.name == name)
            return true;
    return false;
}

}
