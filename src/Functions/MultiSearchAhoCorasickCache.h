#pragma once

#include "config.h"

#if USE_AHO_CORASICK

#include <memory>
#include <Core/Field.h>
#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>

#include <aho_corasick.h>

namespace DB
{

/// One compiled Aho-Corasick automaton together with its memory footprint (the cache weight).
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

struct AhoCorasickAutomatonWeightFunction
{
    size_t operator()(const AhoCorasickAutomaton & automaton) const { return automaton.memory_bytes; }
};

/// Server-global, memory-bounded cache of compiled automata, shared across threads and across all
/// multiSearch* variants. The case-folding mode is part of the key so variants do not alias.
class MultiSearchAhoCorasickCache
    : public CacheBase<UInt128, AhoCorasickAutomaton, UInt128Hash, AhoCorasickAutomatonWeightFunction>
{
public:
    using Base = CacheBase<UInt128, AhoCorasickAutomaton, UInt128Hash, AhoCorasickAutomatonWeightFunction>;
    using Base::Base;
};

class MultiSearchAhoCorasickCacheFactory
{
public:
    static MultiSearchAhoCorasickCacheFactory & instance();

    void init(size_t cache_size_in_bytes, size_t cache_size_in_elements);
    MultiSearchAhoCorasickCache * tryGetCache();

private:
    std::unique_ptr<MultiSearchAhoCorasickCache> cache;
};

/// Builds (or fetches from the cache) the automaton for `needles` under `case_mode` (one of
/// AhoCorasickCaseMode). The returned shared pointer keeps the automaton alive for the duration of
/// the search even if it is evicted concurrently. Throws if the automaton cannot be built.
/// `needles` must not contain empty strings (empty needles are handled by the caller).
std::shared_ptr<const AhoCorasickAutomaton> getOrBuildAhoCorasickAutomaton(uint8_t case_mode, const Array & needles);

}

#endif
