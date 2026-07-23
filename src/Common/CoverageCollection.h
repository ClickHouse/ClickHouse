#pragma once

#if defined(__ELF__) && !defined(OS_FREEBSD) && WITH_COVERAGE_DEPTH

#include <Interpreters/Context_fwd.h>
#include <base/coverage.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace DB
{

/// Look up each (name_hash, func_hash, counter_id) triple in the pre-loaded LLVM coverage
/// map and insert one row into system.coverage_log with the resolved (file, line_start,
/// line_end) arrays.  The counter_id gives statement-level granularity: each non-zero
/// counter in a function corresponds to a specific basic-block region, so the resolved
/// regions are narrower than the whole-function regions from the entry-counter-only approach.
void collectAndInsertCoverage(
    std::string_view test_name,
    const std::vector<CovCounter> & name_refs,
    const std::vector<IndirectCallEntry> & indirect_calls,
    ContextPtr context);

/// Returns the number of entries in the lazily-loaded coverage map.
size_t getCoverageMapSize();

/// Returns how many of the given counters have a match in the coverage map.
size_t countCoverageMatches(const std::vector<CovCounter> & name_refs);

/// Returns the first key in the coverage map (for diagnostics), 0 if empty.
uint64_t getFirstCoverageMapKey();

/// Returns (non_empty_file_count, zero_line_count, first_file_len<<32|first_line).
std::tuple<size_t, size_t, uint64_t> diagCoverageRegions(const std::vector<CovCounter> & name_refs);

/// Returns {files, line_starts, line_ends} for all currently covered regions.
struct CurrentCoverageRegions
{
    std::vector<std::string> files;
    std::vector<uint32_t> line_starts;
    std::vector<uint32_t> line_ends;
};
CurrentCoverageRegions getCurrentCoverageRegions();

}

#endif
