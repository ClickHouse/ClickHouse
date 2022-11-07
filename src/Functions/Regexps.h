#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <Functions/likePatternToRegexp.h>
#include <Common/Exception.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/ProfileEvents.h>
#include <Common/config.h>
#include <base/defines.h>
#include <base/StringRef.h>
#include <boost/container_hash/hash.hpp>

#include "config_functions.h"

#if USE_VECTORSCAN
#    include <hs.h>
#endif

namespace ProfileEvents
{
extern const Event RegexpCreated;
}


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace Regexps
{
using Regexp = OptimizedRegularExpressionSingleThreaded;
using RegexpPtr = std::shared_ptr<Regexp>;

template <bool like, bool no_capture, bool case_insensitive>
inline Regexp createRegexp(const String & pattern)
{
    int flags = OptimizedRegularExpression::RE_DOT_NL;
    if constexpr (no_capture)
        flags |= OptimizedRegularExpression::RE_NO_CAPTURE;
    if constexpr (case_insensitive)
        flags |= OptimizedRegularExpression::RE_CASELESS;

    if constexpr (like)
        return {likePatternToRegexp(pattern), flags};
    else
        return {pattern, flags};
}

/// Caches compiled re2 objects for given string patterns. Intended to support the common situation of a small set of patterns which are
/// evaluated over and over within the same query. In these situations, usage of the cache will save unnecessary pattern re-compilation.
/// However, we must be careful that caching does not add too much static overhead to overall pattern evaluation. Therefore, the cache is
/// intentionally very lightweight: a) no thread-safety/mutexes, b) small & fixed capacity, c) no collision list, d) but also no open
/// addressing, instead collisions simply replace the existing element.
class LocalCacheTable
{
public:
    using RegexpPtr = std::shared_ptr<Regexp>;

    template <bool like, bool no_capture, bool case_insensitive>
    RegexpPtr getOrSet(const String & pattern)
    {
        Bucket & bucket = known_regexps[hasher(pattern) % CACHE_SIZE];

        if (bucket.regexp == nullptr) [[unlikely]]
            /// insert new entry
            bucket = {pattern, std::make_shared<Regexp>(createRegexp<like, no_capture, case_insensitive>(pattern))};
        else
            if (pattern != bucket.pattern)
                /// replace existing entry
                bucket = {pattern, std::make_shared<Regexp>(createRegexp<like, no_capture, case_insensitive>(pattern))};

        return bucket.regexp;
    }

private:
    constexpr static size_t CACHE_SIZE = 100; /// collision probability

    std::hash<String> hasher;
    struct Bucket
    {
        String pattern;   /// key
        RegexpPtr regexp; /// value
    };
    using CacheTable = std::array<Bucket, CACHE_SIZE>;
    CacheTable known_regexps;
};

}

#if USE_VECTORSCAN

namespace MultiRegexps
{
template <typename Deleter, Deleter deleter>
struct HyperscanDeleter
{
    template <typename T>
    void operator()(T * ptr) const
    {
        deleter(ptr);
    }
};

/// Helper unique pointers to correctly delete the allocated space when hyperscan cannot compile something and we throw an exception.
using CompilerError = std::unique_ptr<hs_compile_error_t, HyperscanDeleter<decltype(&hs_free_compile_error), &hs_free_compile_error>>;
using ScratchPtr = std::unique_ptr<hs_scratch_t, HyperscanDeleter<decltype(&hs_free_scratch), &hs_free_scratch>>;
using DataBasePtr = std::unique_ptr<hs_database_t, HyperscanDeleter<decltype(&hs_free_database), &hs_free_database>>;

/// Database is thread safe across multiple threads and Scratch is not but we can copy it whenever we use it in the searcher.
class Regexps
{
public:
    Regexps(hs_database_t * db_, hs_scratch_t * scratch_) : db{db_}, scratch{scratch_} { }

    hs_database_t * getDB() const { return db.get(); }
    hs_scratch_t * getScratch() const { return scratch.get(); }

private:
    DataBasePtr db;
    ScratchPtr scratch;
};

class DeferredConstructedRegexps
{
public:
    explicit DeferredConstructedRegexps(std::function<Regexps()> constructor_)
        : constructor(std::move(constructor_))
    {}

    Regexps * get()
    {
        std::lock_guard lock(mutex);
        if (regexps)
            return &*regexps;
        regexps = constructor();
        return &*regexps;
    }

private:
    std::function<Regexps()> constructor TSA_GUARDED_BY(mutex);
    std::optional<Regexps> regexps TSA_GUARDED_BY(mutex);
    std::mutex mutex;
};

using DeferredConstructedRegexpsPtr = std::shared_ptr<DeferredConstructedRegexps>;

template <bool save_indices, bool WithEditDistance>
inline Regexps constructRegexps(const std::vector<String> & str_patterns, [[maybe_unused]] std::optional<UInt32> edit_distance)
{
    /// Common pointers
    std::vector<const char *> patterns;
    std::vector<unsigned int> flags;

    /// Pointer for external edit distance compilation
    std::vector<hs_expr_ext> ext_exprs;
    std::vector<const hs_expr_ext *> ext_exprs_ptrs;

    patterns.reserve(str_patterns.size());
    flags.reserve(str_patterns.size());

    if constexpr (WithEditDistance)
    {
        ext_exprs.reserve(str_patterns.size());
        ext_exprs_ptrs.reserve(str_patterns.size());
    }

    for (std::string_view ref : str_patterns)
    {
        patterns.push_back(ref.data());
        /* Flags below are the pattern matching flags.
         * HS_FLAG_DOTALL is a compile flag where matching a . will not exclude newlines. This is a good
         * performance practice according to Hyperscan API. https://intel.github.io/hyperscan/dev-reference/performance.html#dot-all-mode
         * HS_FLAG_ALLOWEMPTY is a compile flag where empty strings are allowed to match.
         * HS_FLAG_UTF8 is a flag where UTF8 literals are matched.
         * HS_FLAG_SINGLEMATCH is a compile flag where each pattern match will be returned only once. it is a good performance practice
         * as it is said in the Hyperscan documentation. https://intel.github.io/hyperscan/dev-reference/performance.html#single-match-flag
         */
        flags.push_back(HS_FLAG_DOTALL | HS_FLAG_SINGLEMATCH | HS_FLAG_ALLOWEMPTY | HS_FLAG_UTF8);
        if constexpr (WithEditDistance)
        {
            /// Hyperscan currently does not support UTF8 matching with edit distance.
            flags.back() &= ~HS_FLAG_UTF8;
            ext_exprs.emplace_back();
            /// HS_EXT_FLAG_EDIT_DISTANCE is a compile flag responsible for Levenstein distance.
            ext_exprs.back().flags = HS_EXT_FLAG_EDIT_DISTANCE;
            ext_exprs.back().edit_distance = edit_distance.value();
            ext_exprs_ptrs.push_back(&ext_exprs.back());
        }
    }
    hs_database_t * db = nullptr;
    hs_compile_error_t * compile_error;

    std::unique_ptr<unsigned int[]> ids;

    /// We mark the patterns to provide the callback results.
    if constexpr (save_indices)
    {
        ids.reset(new unsigned int[patterns.size()]);
        for (size_t i = 0; i < patterns.size(); ++i)
            ids[i] = i + 1;
    }

    hs_error_t err;
    if constexpr (!WithEditDistance)
        err = hs_compile_multi(
            patterns.data(),
            flags.data(),
            ids.get(),
            patterns.size(),
            HS_MODE_BLOCK,
            nullptr,
            &db,
            &compile_error);
    else
        err = hs_compile_ext_multi(
            patterns.data(),
            flags.data(),
            ids.get(),
            ext_exprs_ptrs.data(),
            patterns.size(),
            HS_MODE_BLOCK,
            nullptr,
            &db,
            &compile_error);

    if (err != HS_SUCCESS)
    {
        /// CompilerError is a unique_ptr, so correct memory free after the exception is thrown.
        CompilerError error(compile_error);

        if (error->expression < 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, String(error->message));
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Pattern '{}' failed with error '{}'", str_patterns[error->expression], String(error->message));
    }

    ProfileEvents::increment(ProfileEvents::RegexpCreated);

    /// We allocate the scratch space only once, then copy it across multiple threads with hs_clone_scratch
    /// function which is faster than allocating scratch space each time in each thread.
    hs_scratch_t * scratch = nullptr;
    err = hs_alloc_scratch(db, &scratch);

    /// If not HS_SUCCESS, it is guaranteed that the memory would not be allocated for scratch.
    if (err != HS_SUCCESS)
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not allocate scratch space for hyperscan");

    return {db, scratch};
}

/// Maps string pattern vectors + edit distance to compiled vectorscan regexps. Uses the same eviction mechanism as the LocalCacheTable for
/// re2 patterns. Because vectorscan regexes are overall more heavy-weight (more expensive compilation, regexes can grow up to multiple
/// MBs, usage of scratch space), 1. GlobalCacheTable is a global singleton and, as a result, needs locking 2. the pattern compilation is
/// done outside GlobalCacheTable's lock, at the cost of another level of locking.
struct GlobalCacheTable
{
    constexpr static size_t CACHE_SIZE = 500; /// collision probability

    struct Bucket
    {
        std::vector<String> patterns;          /// key
        std::optional<UInt32> edit_distance;   /// key
        /// The compiled patterns and their state (vectorscan 'database' + scratch space) are wrapped in a shared_ptr. Refcounting guarantees
        /// that eviction of a pattern does not affect parallel threads still using the pattern.
        DeferredConstructedRegexpsPtr regexps; /// value
    };

    std::array<Bucket, CACHE_SIZE> known_regexps TSA_GUARDED_BY(mutex);
    std::mutex mutex;

    static size_t getBucketIndexFor(const std::vector<String> patterns, std::optional<UInt32> edit_distance)
    {
        size_t hash = 0;
        for (const auto & pattern : patterns)
            boost::hash_combine(hash, pattern);
        boost::hash_combine(hash, edit_distance);
        return hash % CACHE_SIZE;
    }
};

/// If WithEditDistance is False, edit_distance must be nullopt. Also, we use templates here because each instantiation of function template
/// has its own copy of local static variables which must not be the same for different hyperscan compilations.
template <bool save_indices, bool WithEditDistance>
inline DeferredConstructedRegexpsPtr getOrSet(const std::vector<std::string_view> & patterns, std::optional<UInt32> edit_distance)
{
    static GlobalCacheTable pool; /// Different variables for different pattern parameters, thread-safe in C++11

    std::vector<String> str_patterns;
    str_patterns.reserve(patterns.size());
    for (const auto & pattern : patterns)
        str_patterns.emplace_back(String(pattern));

    size_t bucket_idx = GlobalCacheTable::getBucketIndexFor(str_patterns, edit_distance);

    /// Lock cache to find compiled regexp for given pattern vector + edit distance.
    std::lock_guard lock(pool.mutex);

    GlobalCacheTable::Bucket & bucket = pool.known_regexps[bucket_idx];

    /// Pattern compilation is expensive and we don't want to block other threads reading from / inserting into the cache while we hold the
    /// cache lock during pattern compilation. Therefore, when a cache entry is created or replaced, only set the regexp constructor method
    /// and compile outside the cache lock.
    /// Note that the string patterns and the edit distance is passed into the constructor lambda by value, i.e. copied - it is not an
    /// option to reference the corresponding string patterns / edit distance key in the cache table bucket because the cache entry may
    /// already be evicted at the time the compilation starts.

    if (bucket.regexps == nullptr) [[unlikely]]
    {
        /// insert new entry
        auto deferred_constructed_regexps = std::make_shared<DeferredConstructedRegexps>(
                [str_patterns, edit_distance]()
                {
                    return constructRegexps<save_indices, WithEditDistance>(str_patterns, edit_distance);
                });
        bucket = {std::move(str_patterns), edit_distance, deferred_constructed_regexps};
    }
    else
        if (bucket.patterns != str_patterns || bucket.edit_distance != edit_distance)
        {
            /// replace existing entry
            auto deferred_constructed_regexps = std::make_shared<DeferredConstructedRegexps>(
                    [str_patterns, edit_distance]()
                    {
                        return constructRegexps<save_indices, WithEditDistance>(str_patterns, edit_distance);
                    });
            bucket = {std::move(str_patterns), edit_distance, deferred_constructed_regexps};
        }

    return bucket.regexps;
}

}

#endif // USE_VECTORSCAN

}
