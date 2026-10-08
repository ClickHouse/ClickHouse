#pragma once

#if defined(__ELF__) && WITH_COVERAGE_DEPTH

#include <cstdint>
#include <string>
#include <vector>

namespace DB
{

struct CoverageRegion
{
    uint64_t name_hash;    /// Matches __llvm_profile_data::NameRef
    uint64_t func_hash;    /// Matches __llvm_profile_data::FuncHash
    uint32_t counter_id;   /// Index into the function's counter array; 0 = entry counter
    bool is_branch;        /// True for BranchRegion (one side of an if/switch condition)
    bool is_true_branch;   /// When is_branch: true = taken when condition is true
    std::string file;
    uint32_t line_start;
    uint32_t line_end;
};

/// Parse `__llvm_covfun` and `__llvm_covmap` ELF sections from the binary at binary_path.
/// Returns one CoverageRegion per (function, counter_id) pair for all code regions
/// that use a direct counter reference (tag==1 in LLVM counter encoding).
/// Expression-based counters (add/sub of two counters) are skipped.
/// Multiple regions may share the same counter_id; for each (name_hash, func_hash, counter_id)
/// key the narrowest region (smallest line range) is kept.
std::vector<CoverageRegion> readLLVMCoverageMapping(const char * binary_path);

}

#endif
