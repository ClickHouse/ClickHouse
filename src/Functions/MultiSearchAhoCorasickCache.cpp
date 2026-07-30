#include <Functions/MultiSearchAhoCorasickCache.h>

#if USE_AHO_CORASICK

#include <algorithm>
#include <array>
#include <mutex>
#include <vector>

#include <Core/Defines.h>
#include <Common/Exception.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/UTF8Helpers.h>
#include <Poco/Unicode.h>

namespace CurrentMetrics
{
    extern const Metric MultiSearchAutomatonCacheBytes;
}

namespace ProfileEvents
{
    extern const Event AhoCorasickCacheHit;
    extern const Event AhoCorasickCacheMiss;
    extern const Event AhoCorasickCacheCollision;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

bool appendFoldedForMultiSearch(MultiSearchCaseMode case_mode, const char * data, size_t size, PaddedPODArray<UInt8> & output)
{
    const auto * pos = reinterpret_cast<const UInt8 *>(data);
    const auto * const end = pos + size;

    switch (case_mode)
    {
        case MultiSearchCaseMode::Sensitive:
            output.insert(pos, end);
            return true;

        case MultiSearchCaseMode::InsensitiveAscii:
            for (; pos != end; ++pos)
                output.push_back((*pos >= 'A' && *pos <= 'Z') ? static_cast<UInt8>(*pos + ('a' - 'A')) : *pos);
            return true;

        case MultiSearchCaseMode::InsensitiveUtf8:
            break;
    }

    /// A marker that cannot occur in valid folded UTF-8, so it never matches a (valid) needle and
    /// never merges with neighbouring bytes into a spurious match across a malformed sequence.
    static constexpr UInt8 INVALID_SEQUENCE_MARKER = 0xFF;

    bool valid = true;
    while (pos != end)
    {
        auto code_point = UTF8::convertUTF8ToCodePoint(reinterpret_cast<const char *>(pos), end - pos);
        if (!code_point)
        {
            valid = false;
            output.push_back(INVALID_SEQUENCE_MARKER);
            pos += std::min<size_t>(UTF8::seqLength(*pos), end - pos);
            continue;
        }

        UInt8 folded[4];
        const size_t folded_size = UTF8::convertCodePointToUTF8(
            Poco::Unicode::toLower(static_cast<int>(*code_point)), reinterpret_cast<char *>(folded), sizeof(folded));
        output.insert(folded, folded + folded_size);
        pos += UTF8::seqLength(*pos);
    }
    return valid;
}

AhoCorasickAutomaton::~AhoCorasickAutomaton()
{
    if (handle)
        aho_corasick_free(handle);
}

namespace
{

UInt128 computeKey(MultiSearchCaseMode case_mode, const Array & needles)
{
    SipHash hash;
    hash.update(static_cast<uint8_t>(case_mode));
    for (const auto & needle : needles)
    {
        const String & s = needle.safeGet<String>();
        hash.update(s.size());
        hash.update(s.data(), s.size());
    }
    return hash.get128();
}

std::shared_ptr<AhoCorasickAutomaton> buildAutomaton(MultiSearchCaseMode case_mode, const Array & needles)
{
    /// Fold needles into one contiguous buffer. Invalid-UTF8 needles cannot match anything in a
    /// UTF-8 search (the legacy searcher omits them too), so drop them here.
    PaddedPODArray<UInt8> folded_data;
    std::vector<uint64_t> folded_ends;
    folded_ends.reserve(needles.size());
    for (const auto & needle : needles)
    {
        const String & s = needle.safeGet<String>();
        const size_t start = folded_data.size();
        if (appendFoldedForMultiSearch(case_mode, s.data(), s.size(), folded_data))
            folded_ends.push_back(folded_data.size());
        else
            folded_data.resize(start);
    }

    std::vector<const uint8_t *> pattern_ptrs;
    std::vector<uint64_t> pattern_sizes;
    pattern_ptrs.reserve(folded_ends.size());
    pattern_sizes.reserve(folded_ends.size());
    uint64_t prev_end = 0;
    for (uint64_t folded_end : folded_ends)
    {
        pattern_ptrs.push_back(reinterpret_cast<const uint8_t *>(folded_data.data()) + prev_end);
        pattern_sizes.push_back(folded_end - prev_end);
        prev_end = folded_end;
    }

    std::unique_ptr<AhoCorasickHandle, decltype(&aho_corasick_free)> handle(
        aho_corasick_create(
            pattern_ptrs.data(),
            pattern_sizes.data(),
            static_cast<uint64_t>(pattern_ptrs.size())),
        &aho_corasick_free);

    if (!handle)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Failed to build Aho-Corasick automaton for {} patterns (too many states)",
            needles.size());

    /// Rust allocations reach the intercepted C `malloc`, which accounts memory but never throws.
    /// The limit is therefore enforced here, after the build. The overshoot is bounded: the
    /// automaton is proportional to the folded needle bytes, and those are already charged to the
    /// query by the column and buffer allocations that produced them.
    CurrentMemoryTracker::check();
    const size_t memory_bytes = aho_corasick_heap_bytes(handle.get());
    auto automaton = std::make_shared<AhoCorasickAutomaton>(handle.get(), memory_bytes);
    handle.release();
    return automaton;
}

class DeferredAutomaton
{
public:
    DeferredAutomaton(MultiSearchCaseMode case_mode_, const Array & needles_)
        : case_mode(case_mode_)
        , needles(needles_)
    {
    }

    std::shared_ptr<const AhoCorasickAutomaton> get()
    {
        std::lock_guard lock(mutex);
        if (!automaton)
        {
            automaton = buildAutomaton(case_mode, needles);
            Array{}.swap(needles);
        }
        return automaton;
    }

private:
    const MultiSearchCaseMode case_mode;
    Array needles;
    std::shared_ptr<const AhoCorasickAutomaton> automaton;
    std::mutex mutex;
};

struct CacheBucket
{
    UInt128 key;
    std::shared_ptr<DeferredAutomaton> deferred;
    size_t memory_bytes = 0;
};

struct Cache
{
    std::mutex mutex;
    std::array<CacheBucket, DEFAULT_MULTI_SEARCH_AUTOMATON_CACHE_SLOTS> buckets{};
    size_t max_bytes = DEFAULT_MULTI_SEARCH_AUTOMATON_CACHE_MAX_SIZE;
    size_t retained_bytes = 0;
};

Cache & cache()
{
    static Cache instance;
    return instance;
}

} // namespace

void setMultiSearchAutomatonCacheMaxSize(size_t max_bytes)
{
    auto & global_cache = cache();
    std::lock_guard lock(global_cache.mutex);
    if (global_cache.max_bytes == max_bytes)
        return;

    global_cache.max_bytes = max_bytes;
    global_cache.retained_bytes = 0;
    global_cache.buckets = {};
    CurrentMetrics::set(CurrentMetrics::MultiSearchAutomatonCacheBytes, 0);
}

std::shared_ptr<const AhoCorasickAutomaton> getOrBuildAhoCorasickAutomaton(MultiSearchCaseMode case_mode, const Array & needles)
{
    const UInt128 key = computeKey(case_mode, needles);
    auto & global_cache = cache();
    const size_t bucket_index = static_cast<size_t>(key % global_cache.buckets.size());
    std::shared_ptr<DeferredAutomaton> deferred;
    bool hit = false;
    bool collision = false;

    {
        std::lock_guard lock(global_cache.mutex);
        const auto & bucket = global_cache.buckets[bucket_index];
        if (global_cache.max_bytes != 0 && bucket.deferred && bucket.key == key)
        {
            deferred = bucket.deferred;
            hit = true;
        }
    }

    if (!hit)
    {
        /// Copy the needles outside the cache lock. The array holds every pattern, so a copy under
        /// the lock would block all other lookups in the process.
        auto fresh = std::make_shared<DeferredAutomaton>(case_mode, needles);

        std::lock_guard lock(global_cache.mutex);
        auto & bucket = global_cache.buckets[bucket_index];
        if (global_cache.max_bytes != 0 && bucket.deferred && bucket.key == key)
        {
            /// Another thread inserted the same key during the copy.
            deferred = bucket.deferred;
            hit = true;
        }
        else
        {
            deferred = fresh;
            if (global_cache.max_bytes != 0)
            {
                if (bucket.deferred)
                {
                    collision = true;
                    global_cache.retained_bytes -= bucket.memory_bytes;
                    CurrentMetrics::sub(CurrentMetrics::MultiSearchAutomatonCacheBytes, bucket.memory_bytes);
                }
                bucket = {key, deferred, 0};
            }
        }
    }

    ProfileEvents::increment(hit ? ProfileEvents::AhoCorasickCacheHit : ProfileEvents::AhoCorasickCacheMiss);
    if (collision)
        ProfileEvents::increment(ProfileEvents::AhoCorasickCacheCollision);

    std::shared_ptr<const AhoCorasickAutomaton> automaton;
    try
    {
        automaton = deferred->get();
    }
    catch (...)
    {
        std::lock_guard lock(global_cache.mutex);
        auto & bucket = global_cache.buckets[bucket_index];
        if (bucket.deferred == deferred)
            bucket = {};
        throw;
    }

    {
        std::lock_guard lock(global_cache.mutex);
        auto & bucket = global_cache.buckets[bucket_index];
        if (bucket.deferred == deferred && bucket.memory_bytes == 0)
        {
            if (automaton->memory_bytes <= global_cache.max_bytes - global_cache.retained_bytes)
            {
                bucket.memory_bytes = automaton->memory_bytes;
                global_cache.retained_bytes += automaton->memory_bytes;
                CurrentMetrics::add(CurrentMetrics::MultiSearchAutomatonCacheBytes, automaton->memory_bytes);
            }
            else
                bucket = {};
        }
    }

    return automaton;
}

}

#endif
