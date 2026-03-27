#pragma once

#include <IO/WriteBufferFromOwnString.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <string>
#include <vector>

namespace DB
{

/// Estimates the marginal benefit of hypothetical skip indexes over the baseline
/// (after PK + partition + existing index pruning).
/// Used by EXPLAIN WHATIF.
class WhatIfIndexEstimator
{
public:
    /// Per-hypothetical-index result.
    struct IndexResult
    {
        String index_name;
        String index_type;

        /// Top-level status: is this index relevant to the query?
        enum Status { Applicable, NotApplicable };
        Status status = NotApplicable;
        String not_applicable_reason;

        /// Estimation results (meaningful only when status == Applicable).
        UInt64 estimated_marks = 0;
        UInt64 estimated_parts = 0;
        double skip_ratio = 0.0;

        /// Empirical status.
        enum EmpiricalStatus { Ok, Timeout, Unsupported, Disabled };
        EmpiricalStatus empirical_status = Disabled;
        String estimate_source; /// "empirical", "statistical", "unavailable"

        /// Sampling stats.
        UInt64 sampled_parts = 0;
        UInt64 total_parts = 0;
        UInt64 sampled_marks = 0;
        UInt64 total_marks = 0;
        UInt64 elapsed_ms = 0;
        UInt64 budget_ms = 0;

        /// Confidence: HIGH, MEDIUM, LOW.
        String confidence;

        /// Cost scores.
        UInt64 storage_estimate_bytes = 0;
        String cpu_check_cost_score;       /// LOW, MEDIUM, HIGH
        String maintenance_cost_score;     /// LOW, MEDIUM, HIGH

        /// Warnings.
        std::vector<String> warnings;
    };

    /// Full EXPLAIN WHATIF result.
    struct Result
    {
        /// Baseline (after PK + partition + existing indexes).
        UInt64 baseline_parts = 0;
        UInt64 baseline_marks = 0;
        UInt64 baseline_est_bytes = 0;
        String database;
        String table;

        /// Per-index results.
        std::vector<IndexResult> index_results;

        /// Format the result as text.
        void format(WriteBuffer & out) const;
    };

    /// Run the what-if analysis. Builds query plan, extracts baseline,
    /// evaluates each hypothetical index.
    static Result run(const ASTPtr & select_query, ContextPtr context, const ASTPtr & explain_settings);
};

}
