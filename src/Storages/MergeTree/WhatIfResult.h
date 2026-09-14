#pragma once

#include <base/types.h>

#include <vector>

namespace DB
{

class WriteBuffer;

/// One hypothetical object evaluated against the baseline read
struct WhatIfCandidateResult
{
    String name;
    /// Index type: `minmax`, `set`, ...
    String type;

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

    UInt64 sampled_parts = 0;
    UInt64 total_parts = 0;
    UInt64 sampled_marks = 0;
    UInt64 total_marks = 0;
    UInt64 elapsed_us = 0;
};

/// The whole `EXPLAIN WHATIF` answer: the baseline read plus one row per candidate.
/// Rendered by `WhatIfResultFormatter.cpp`
struct WhatIfResult
{
    /// Baseline after PK + partition + existing indexes
    UInt64 baseline_parts = 0;
    UInt64 baseline_marks = 0;
    UInt64 baseline_est_bytes = 0;
    String database;
    String table;

    std::vector<WhatIfCandidateResult> candidates;

    void format(WriteBuffer & out) const;
};

}
