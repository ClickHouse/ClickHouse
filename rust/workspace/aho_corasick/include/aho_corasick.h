#pragma once

#include <cstdint>

extern "C" {

/// Opaque handle to the Aho-Corasick automaton
struct AhoCorasickHandle;

/// Creates an Aho-Corasick automaton from the given patterns.
///
/// Uses daachorse (double-array trie Aho-Corasick) for fast multi-pattern search.
///
/// @param patterns Pointer to array of pattern data pointers
/// @param pattern_sizes Pointer to array of pattern sizes (uint64_t)
/// @param num_patterns Number of patterns
/// @param case_insensitive If true, use ASCII case-insensitive matching
/// @returns Pointer to the automaton handle, or nullptr on error
AhoCorasickHandle * aho_corasick_create(
    const uint8_t * const * patterns,
    const uint64_t * pattern_sizes,
    uint64_t num_patterns,
    bool case_insensitive);

/// Searches for any pattern match in a batch of haystacks.
///
/// This function is optimized for ClickHouse's ColumnString format where:
/// - haystack_data is a contiguous buffer of all strings (with null terminators)
/// - haystack_offsets is an array of cumulative end positions
///
/// @param handle The automaton handle from aho_corasick_create
/// @param haystack_data Pointer to contiguous haystack data
/// @param haystack_offsets Pointer to array of cumulative offsets (ClickHouse format)
/// @param num_rows Number of haystacks to search
/// @param results Output array of uint8_t (0 = no match, 1 = match found)
void aho_corasick_search_batch(
    const AhoCorasickHandle * handle,
    const uint8_t * haystack_data,
    const uint64_t * haystack_offsets,
    uint64_t num_rows,
    uint8_t * results);

/// Frees the Aho-Corasick automaton handle.
///
/// @param handle The handle to free (may be nullptr)
void aho_corasick_free(AhoCorasickHandle * handle);

} // extern "C"
