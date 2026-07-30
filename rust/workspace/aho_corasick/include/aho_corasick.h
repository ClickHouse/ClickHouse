#pragma once

#include <cstdint>

extern "C" {

/// Opaque handle to a compiled daachorse automaton. Patterns and haystacks must already be
/// case-folded by the caller (see MultiSearchAhoCorasickCache.h). Patterns are de-duplicated on
/// create; empty patterns are the caller's responsibility (handled on the C++ side).
struct AhoCorasickHandle;

/// Builds an automaton. Returns nullptr on failure or panic. An empty pattern set yields a valid
/// handle that matches nothing.
AhoCorasickHandle * aho_corasick_create(
    const uint8_t * const * patterns,
    const uint64_t * pattern_sizes,
    uint64_t num_patterns);

/// Searches a ColumnString batch: `haystack_offsets` are cumulative end positions in
/// `haystack_data` (strings are not null-terminated). Writes one byte per row into `results`
/// (0 = no match, 1 = match). Zeros `results` before searching. Returns 0 on success, non-zero on
/// panic (results remain zeroed).
int32_t aho_corasick_search_batch(
    const AhoCorasickHandle * handle,
    const uint8_t * haystack_data,
    const uint64_t * haystack_offsets,
    uint64_t num_rows,
    uint8_t * results);

/// Heap bytes used by the automaton, for cache sizing. Returns 0 if handle is nullptr.
uint64_t aho_corasick_heap_bytes(const AhoCorasickHandle * handle);

/// Frees a handle from aho_corasick_create. Accepts nullptr.
void aho_corasick_free(AhoCorasickHandle * handle);

} // extern "C"
