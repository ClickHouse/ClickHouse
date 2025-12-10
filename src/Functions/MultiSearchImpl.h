#pragma once

#include <vector>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <base/scope_guard.h>
#include "config.h"

#if USE_AHO_CORASICK
#    include <aho_corasick.h>
#    include <array>
#    include <memory>
#    include <boost/container_hash/hash.hpp>
#    include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event AhoCorasickCacheHit;
    extern const Event AhoCorasickCacheMiss;
}
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

template <typename Name, typename Impl>
struct MultiSearchImpl
{
    using ResultType = UInt8;
    /// Variable for understanding, if we used offsets for the output, most
    /// likely to determine whether the function returns ColumnVector of ColumnArray.
    static constexpr bool is_column_array = false;
    static constexpr auto name = Name::name;

    static auto getReturnType() { return std::make_shared<DataTypeNumber<ResultType>>(); }

    static void vectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const Array & needles_arr,
        PaddedPODArray<UInt8> & res,
        PaddedPODArray<UInt64> & /*offsets*/,
        bool /*allow_hyperscan*/,
        size_t /*max_hyperscan_regexp_length*/,
        size_t /*max_hyperscan_regexp_total_length*/,
        bool /*reject_expensive_hyperscan_regexps*/,
        bool force_daachorse,
        size_t input_rows_count)
    {
        // For performance of Volnitsky search, it is crucial to save only one byte for pattern number.
        // When more than 255 patterns are provided, or when force_daachorse is set,
        // use Aho-Corasick (daachorse) which handles thousands of patterns efficiently.
        if (force_daachorse || needles_arr.size() > std::numeric_limits<UInt8>::max())
        {
#if USE_AHO_CORASICK
            vectorConstantAhoCorasick(haystack_data, haystack_offsets, needles_arr, res, input_rows_count);
            return;
#else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Function {} with more than 255 patterns requires Aho-Corasick support which is not available in this build. "
                "Either recompile with Aho-Corasick support enabled or reduce patterns to 255 or fewer.",
                name);
#endif
        }

        std::vector<std::string_view> needles;
        needles.reserve(needles_arr.size());
        for (const auto & needle : needles_arr)
            needles.emplace_back(needle.safeGet<String>());

        auto searcher = Impl::createMultiSearcherInBigHaystack(needles);

        res.resize(input_rows_count);

        size_t iteration = 0;
        while (searcher.hasMoreToSearch())
        {
            size_t prev_haystack_offset = 0;
            for (size_t j = 0; j < input_rows_count; ++j)
            {
                const auto * haystack = &haystack_data[prev_haystack_offset];
                const auto * haystack_end = haystack + haystack_offsets[j] - prev_haystack_offset;
                if (iteration == 0 || !res[j])
                    res[j] = searcher.searchOne(haystack, haystack_end);
                prev_haystack_offset = haystack_offsets[j];
            }
            ++iteration;
        }
        if (iteration == 0)
            std::fill(res.begin(), res.end(), 0);
    }

#if USE_AHO_CORASICK
private:
    /// RAII wrapper for AhoCorasickHandle
    struct AhoCorasickHandleWrapper
    {
        AhoCorasickHandle * handle = nullptr;

        AhoCorasickHandleWrapper() = default;
        explicit AhoCorasickHandleWrapper(AhoCorasickHandle * h) : handle(h) {}

        ~AhoCorasickHandleWrapper()
        {
            if (handle)
                aho_corasick_free(handle);
        }

        AhoCorasickHandleWrapper(const AhoCorasickHandleWrapper &) = delete;
        AhoCorasickHandleWrapper & operator=(const AhoCorasickHandleWrapper &) = delete;
        AhoCorasickHandleWrapper(AhoCorasickHandleWrapper &&) = delete;
        AhoCorasickHandleWrapper & operator=(AhoCorasickHandleWrapper &&) = delete;
    };

    /// Thread-local hash table cache for compiled Aho-Corasick handles.
    /// Follows the same pattern as LocalCacheTable in Regexps.h for re2 caching.
    /// Fixed-size array with hash-based indexing; collisions replace existing entries.
    struct AhoCorasickCache
    {
        static constexpr size_t CACHE_SIZE = 1'000;

        struct Bucket
        {
            std::vector<String> patterns;
            bool case_insensitive = false;
            std::shared_ptr<AhoCorasickHandleWrapper> handle;
        };

        std::array<Bucket, CACHE_SIZE> buckets;

        static size_t computeBucketIndex(const Array & needles_arr, bool case_insensitive)
        {
            size_t hash = case_insensitive ? 1 : 0;
            for (const auto & needle : needles_arr)
                boost::hash_combine(hash, needle.safeGet<String>());
            return hash % CACHE_SIZE;
        }

        static bool patternsMatch(const std::vector<String> & cached, const Array & needles_arr)
        {
            if (cached.size() != needles_arr.size())
                return false;
            for (size_t i = 0; i < cached.size(); ++i)
                if (cached[i] != needles_arr[i].safeGet<String>())
                    return false;
            return true;
        }

        AhoCorasickHandle * getOrCreate(const Array & needles_arr, bool case_insensitive)
        {
            Bucket & bucket = buckets[computeBucketIndex(needles_arr, case_insensitive)];

            /// Check if cached entry matches
            if (bucket.handle
                && bucket.case_insensitive == case_insensitive
                && patternsMatch(bucket.patterns, needles_arr))
            {
                ProfileEvents::increment(ProfileEvents::AhoCorasickCacheHit);
                return bucket.handle->handle;
            }

            ProfileEvents::increment(ProfileEvents::AhoCorasickCacheMiss);

            /// Cache miss or collision - build new handle
            std::vector<const uint8_t *> pattern_ptrs;
            std::vector<uint64_t> pattern_sizes;
            std::vector<String> pattern_storage;

            pattern_ptrs.reserve(needles_arr.size());
            pattern_sizes.reserve(needles_arr.size());
            pattern_storage.reserve(needles_arr.size());

            for (const auto & needle : needles_arr)
            {
                pattern_storage.push_back(needle.safeGet<String>());
                const auto & str = pattern_storage.back();
                pattern_ptrs.push_back(reinterpret_cast<const uint8_t *>(str.data()));
                pattern_sizes.push_back(str.size());
            }

            AhoCorasickHandle * handle = aho_corasick_create(
                pattern_ptrs.data(),
                pattern_sizes.data(),
                static_cast<uint64_t>(pattern_ptrs.size()),
                case_insensitive);

            if (!handle)
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Failed to create Aho-Corasick automaton for function {} with {} patterns. "
                    "Possible causes: invalid patterns, out of memory, or incompatible pattern data.",
                    name, needles_arr.size());
            }

            /// Store in bucket (replaces any existing entry)
            bucket.patterns = std::move(pattern_storage);
            bucket.case_insensitive = case_insensitive;
            bucket.handle = std::make_shared<AhoCorasickHandleWrapper>(handle);

            return handle;
        }
    };

    static AhoCorasickCache & getCache()
    {
        static thread_local AhoCorasickCache cache;
        return cache;
    }

public:
    /// Aho-Corasick based search for large pattern sets (>255 patterns).
    /// Uses thread-local cache to reuse compiled handle across blocks.
    static void vectorConstantAhoCorasick(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const Array & needles_arr,
        PaddedPODArray<UInt8> & res,
        size_t input_rows_count)
    {
        res.resize(input_rows_count);

        if (needles_arr.empty())
        {
            std::fill(res.begin(), res.end(), 0);
            return;
        }

        constexpr bool case_insensitive = !Impl::case_sensitive;
        AhoCorasickHandle * handle = getCache().getOrCreate(needles_arr, case_insensitive);

        aho_corasick_search_batch(
            handle,
            reinterpret_cast<const uint8_t *>(haystack_data.data()),
            reinterpret_cast<const uint64_t *>(haystack_offsets.data()),
            static_cast<uint64_t>(input_rows_count),
            reinterpret_cast<uint8_t *>(res.data()));
    }
#endif

    static void vectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const IColumn & needles_data,
        const ColumnArray::Offsets & needles_offsets,
        PaddedPODArray<ResultType> & res,
        PaddedPODArray<UInt64> & /*offsets*/,
        bool /*allow_hyperscan*/,
        size_t /*max_hyperscan_regexp_length*/,
        size_t /*max_hyperscan_regexp_total_length*/,
        bool /*reject_expensive_hyperscan_regexps*/,
        bool /*force_daachorse*/,
        size_t input_rows_count)
    {
        res.resize(input_rows_count);

        size_t prev_haystack_offset = 0;
        size_t prev_needles_offset = 0;

        const ColumnString & needles_data_string = checkAndGetColumn<ColumnString>(needles_data);

        std::vector<std::string_view> needles;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            needles.reserve(needles_offsets[i] - prev_needles_offset);

            for (size_t j = prev_needles_offset; j < needles_offsets[i]; ++j)
                needles.emplace_back(needles_data_string.getDataAt(j));

            const auto * const haystack = &haystack_data[prev_haystack_offset];
            const size_t haystack_length = haystack_offsets[i] - prev_haystack_offset;

            size_t iteration = 0;
            for (const auto & needle : needles)
            {
                auto searcher = Impl::createSearcherInSmallHaystack(needle.data(), needle.size());
                if (iteration == 0 || !res[i])
                {
                    const auto * match = searcher.search(haystack, haystack_length);
                    res[i] = (match != haystack + haystack_length);
                }
                ++iteration;
            }
            if (iteration == 0)
                res[i] = 0;

            prev_haystack_offset = haystack_offsets[i];
            prev_needles_offset = needles_offsets[i];
            needles.clear();
        }
    }
};

}
