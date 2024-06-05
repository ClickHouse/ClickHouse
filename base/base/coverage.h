#pragma once

#include <span>
#include <cstdint>

/// Flush coverage report to file, depending on coverage system
/// proposed by compiler (llvm for clang and gcov for gcc).
///
/// Noop if build without coverage (WITH_COVERAGE=0).
/// Thread safe (use exclusive lock).
/// Idempotent, may be called multiple times.
void dumpCoverageReportIfPossible();

/// This is effective if SANITIZE_COVERAGE is enabled at build time.
/// Get accumulated unique program addresses of the instrumented parts of the code,
/// seen so far after program startup or after previous reset.
/// The returned span will be represented as a sparse map, containing mostly zeros, which you should filter away.
std::span<const uintptr_t> getCurrentCoverage();

/// Similar but not being reset.
std::span<const uintptr_t> getCumulativeCoverage();

/// Get all instrumented addresses that could be in the coverage.
std::span<const uintptr_t> getAllInstrumentedAddresses();

/// Reset the accumulated coverage.
/// This is useful to compare coverage of different tests, including differential coverage.
void resetCoverage();
