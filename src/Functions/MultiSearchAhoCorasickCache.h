#pragma once

#include "config.h"

#if USE_AHO_CORASICK

#include <memory>
#include <Core/Field.h>
#include <Common/PODArray.h>

#include <aho_corasick.h>

namespace DB
{

/// How needles and haystacks are case-folded before they reach the automaton.
enum class MultiSearchCaseMode : uint8_t
{
    Sensitive = 0,       /// No folding.
    InsensitiveAscii,    /// Lowercase only ASCII A-Z (other bytes unchanged).
    InsensitiveUtf8,     /// One-code-point Unicode lowercase mapping, matching the legacy searcher.
};

/// Appends the case-folded form of [data, data + size) to `output`, returning false if the input
/// contained invalid UTF-8 (only meaningful for InsensitiveUtf8). Folds one code point at a time via
/// `utf8CaseFold` to match `UTF8CaseInsensitiveStringSearcher`. Each byte that does not start a valid
/// sequence is replaced by a single sentinel byte, so a match never covers it and can start at the next byte.
bool appendFoldedForMultiSearch(MultiSearchCaseMode case_mode, const char * data, size_t size, PaddedPODArray<UInt8> & output);

/// One compiled Aho-Corasick automaton together with its memory footprint.
/// Owns the Rust handle and frees it on destruction.
struct AhoCorasickAutomaton
{
    AhoCorasickAutomaton(AhoCorasickHandle * handle_, size_t memory_bytes_)
        : handle(handle_), memory_bytes(memory_bytes_)
    {
    }

    ~AhoCorasickAutomaton();

    AhoCorasickAutomaton(const AhoCorasickAutomaton &) = delete;
    AhoCorasickAutomaton & operator=(const AhoCorasickAutomaton &) = delete;
    AhoCorasickAutomaton(AhoCorasickAutomaton &&) = delete;
    AhoCorasickAutomaton & operator=(AhoCorasickAutomaton &&) = delete;

    AhoCorasickHandle * handle = nullptr;
    size_t memory_bytes = 0;
};

/// Sets the maximum bytes retained by the process-wide direct-mapped cache and clears it.
/// Zero disables caching.
void setMultiSearchAutomatonCacheMaxSize(size_t max_bytes);

/// Builds (or fetches from the process-wide cache) the automaton for `needles` under `case_mode`.
/// The needles are case-folded before building. The returned shared pointer keeps the automaton alive
/// for the duration of the search even if it is evicted concurrently. Throws if the automaton cannot
/// be built. `needles` must not contain empty strings (empty needles are handled by the caller).
///
/// The cache is direct-mapped: a collision replaces the resident entry. Concurrent requests for the
/// same entry share one construction, following Hyperscan's GlobalCacheTable in Regexps.h.
std::shared_ptr<const AhoCorasickAutomaton> getOrBuildAhoCorasickAutomaton(MultiSearchCaseMode case_mode, const Array & needles);

}

#endif
