#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

/// Flush coverage report to file, depending on coverage system
/// proposed by compiler (llvm for clang and gcov for gcc).
///
/// Noop if build without coverage (WITH_COVERAGE_DEPTH=0).
/// Thread safe (use exclusive lock).
/// Idempotent, may be called multiple times.
void dumpCoverageReportIfPossible();

/// Initialize the coverage mapping from the server's own ELF sections.
/// Reads /proc/self/exe to parse __llvm_covmap and __llvm_covfun sections.
/// Builds a counter→region index for fast per-test scanning.
/// Call once at server startup.
/// Noop if build without coverage (WITH_COVERAGE_DEPTH=0).
void loadCoverageMapping();

/// Atomically flush current coverage for the previous test → reset counters → arm new test name.
/// Call before each test. Empty name flushes without starting a new test.
/// Noop if build without coverage (WITH_COVERAGE_DEPTH=0).
void setCoverageTest(std::string_view test_name);

/// Reset the accumulated coverage.
/// For compatibility: equivalent to setCoverageTest("").
/// Noop if build without coverage (WITH_COVERAGE_DEPTH=0).
void resetCoverage();

#if WITH_COVERAGE_DEPTH

/// Each entry is (name_hash, func_hash, counter_id, min_depth).
///
/// name_hash, func_hash  — identify the function (match __llvm_profile_data fields).
/// counter_id            — index of the non-zero counter within the function's counter
///                         array; 0 = entry, 1…N = branch/statement counters.  Gives
///                         statement-level region granularity.
/// min_depth             — minimum call-stack depth at which the function was entered
///                         since the last counter reset, as tracked by the
///                         -finstrument-functions shadow stack (see coverage.cpp).
///                         255 means "depth not tracked" (binary built without the flag).
///                         Depth 1 = called directly by the test driver; higher = indirect.
using CovCounter = std::tuple<uint64_t, uint64_t, uint32_t, uint8_t>;

/// Return (name_hash, func_hash, counter_id) triples for every counter > 0
/// since the last counter reset.  One entry per non-zero counter per function,
/// so a function with 8 non-zero counters contributes 8 entries.
std::vector<CovCounter> getCurrentCoveredNameRefs();

/// One indirect-call observation: caller identified by (name_hash, func_hash),
/// callee by its load-relative text offset (stable across ASLR restarts for the
/// same binary) and call count.  The callee offset is resolved to a name_hash
/// by CoverageCollection using dladdr() at flush time.
struct IndirectCallEntry
{
    uint64_t caller_name_hash;
    uint64_t caller_func_hash;
    uint64_t callee_offset;   /// callee text address − binary load base
    uint64_t call_count;
};

/// Return all indirect-call observations accumulated since the last counter reset.
/// Only populated when -fprofile-instr-generate value profiling is active
/// (NumValueSites[0] > 0 in __llvm_profile_data records).
std::vector<IndirectCallEntry> getCurrentIndirectCalls();

/// Callback invoked by setCoverageTest when flushing coverage for the previous test.
/// Arguments: (test_name, covered_counters, indirect_calls).
using CoverageFlushCallback = std::function<void(
    std::string_view,
    const std::vector<CovCounter> &,
    const std::vector<IndirectCallEntry> &)>;

/// Register a callback that is called by setCoverageTest before resetting counters.
/// Only one callback can be registered at a time; a second call overwrites the first.
/// Thread-safe.
void registerCoverageFlushCallback(CoverageFlushCallback cb);

#endif
