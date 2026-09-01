#pragma once

#include <base/types.h>

namespace DB
{

/// Cost of the step-digest machinery for one optimizer run (the performance gate on memo-wide
/// deduplication). Owned by `OptimizerContext`; not synchronized - the optimizer runs
/// single-threaded.
struct StepDigestCounters
{
    /// Completed digest passes, full and logical together, whether they produced a fingerprint or bytes.
    UInt64 digests_written = 0;
    /// Total bytes produced by those passes.
    UInt64 digest_bytes_written = 0;
    /// Digest passes run only to confirm a fingerprint match byte-exactly.
    UInt64 digest_confirmations = 0;
};

/// RAII scope that makes `counters` the target for the four digest entry points to increment on
/// this thread - `computeStepFullFingerprint` / `stepFullDigestsEqual` and their logical
/// counterparts `computeStepLogicalFingerprint` / `stepLogicalDigestsEqual`, which share one
/// implementation and one set of counters. The digest code has no context parameter,
/// so it reaches its counters through a thread_local active pointer instead; increments are a
/// no-op when no scope is active. The optimizer is single-threaded, so a plain (non-atomic)
/// pointer is enough. Installed by `CascadesOptimizer::optimize` around the pass and by gtests
/// around assertions; nested scopes restore the outer one on destruction.
class CurrentStepDigestCounters
{
public:
    explicit CurrentStepDigestCounters(StepDigestCounters & counters);
    ~CurrentStepDigestCounters();

    CurrentStepDigestCounters(const CurrentStepDigestCounters &) = delete;
    CurrentStepDigestCounters & operator=(const CurrentStepDigestCounters &) = delete;

    static StepDigestCounters * get() { return current; }

private:
    static thread_local StepDigestCounters * current;
    StepDigestCounters * previous;
};

}
