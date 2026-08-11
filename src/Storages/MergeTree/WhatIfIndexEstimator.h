#pragma once

#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <string>
#include <vector>

namespace DB
{

/// Estimates the benefit of hypothetical skip indexes over the baseline
/// (after PK + partition + existing index pruning). Used by EXPLAIN WHATIF
class WhatIfIndexEstimator
{
public:
    struct IndexResult
    {
        String index_name;
        String index_type;

        enum Status { Applicable, NotApplicable };
        Status status = NotApplicable;
        String not_applicable_reason;

        /// Meaningful only when status == Applicable
        UInt64 estimated_marks = 0;
        double skip_ratio = 0.0;

        enum EmpiricalStatus { Ok, Unsupported, Disabled };
        EmpiricalStatus empirical_status = Disabled;
        /// Why the empirical estimate could not run, set only when empirical_status == Unsupported
        String empirical_unsupported_reason;

        enum EstimateSource { Empirical, Statistical, ApplicabilityOnly };
        EstimateSource estimate_source = ApplicabilityOnly;

        /// Both pairs come from the same analysis, so a merge finishing mid-estimate cannot make
        /// a sampled count exceed its total. The totals are what the query could have read after
        /// partition pruning, which is also the denominator sampling will divide
        UInt64 sampled_parts = 0;
        UInt64 total_parts = 0;
        UInt64 sampled_marks = 0;
        UInt64 total_marks = 0;
        UInt64 elapsed_us = 0;
    };

    struct Result
    {
        /// Baseline after PK + partition + existing indexes
        UInt64 baseline_parts = 0;
        UInt64 baseline_marks = 0;
        UInt64 baseline_est_bytes = 0;
        String database;
        String table;

        std::vector<IndexResult> index_results;

        void format(WriteBuffer & out) const;
    };

    static Result run(const ASTPtr & select_query, ContextPtr context, const ASTPtr & explain_settings);
};

}
