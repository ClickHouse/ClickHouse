#pragma once

#include <Storages/MergeTree/GinIndexStore.h>
#include <vector>

namespace DB
{

static constexpr UInt64 DEFAULT_NGRAM_SIZE = 3;
static constexpr auto DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.001; /// 0.1%

static inline constexpr auto TEXT_INDEX_NAME = "text";

enum class GinSearchMode : uint8_t
{
    Any,
    All
};

class GinQueryString
{
public:
    GinQueryString() = default;
    GinQueryString(std::string_view query_string_, const std::vector<String> & tokens_);

    /// Getter
    const String & getQueryString() const { return query_string; }
    const std::vector<String> & getTokens() const { return tokens; }

    /// Set the query string of the filter
    void setQueryString(std::string_view query_string_) { query_string = query_string_; }

    /// Add token which are tokens generated from the query string
    bool addToken(std::string_view token)
    {
        if (token.length() > FST::MAX_TOKEN_LENGTH)
            return false;

        tokens.push_back(String(token));
        return true;
    }

private:
    /// Query string of the filter
    String query_string;
    /// Tokens from query string
    std::vector<String> tokens;
};

struct GinSegmentWithRowIdRange
{
    /// Segment ID of the row ID range
    UInt32 segment_id;

    /// First and last row ID in the range (both are inclusive)
    UInt32 range_rowid_start;
    UInt32 range_rowid_end;
};

using GinSegmentsWithRowIdRange = std::vector<GinSegmentWithRowIdRange>;

/// GinFilter provides two types of functionality:
/// 1) it builds a text index, and
/// 2) it filters the unmatched rows according to its query string.
class GinFilter
{
public:
    struct Parameters
    {
        Parameters(
            String tokenizer_,
            UInt64 segment_digestion_threshold_bytes_,
            double bloom_filter_false_positive_rate_,
            std::optional<UInt64> ngram_size_,
            std::optional<std::vector<String>> separators_);

        String tokenizer;
        /// Digestion threshold to split a segment. By default, it is 0 (zero) which means unlimited.
        UInt64 segment_digestion_threshold_bytes;
        /// Bloom filter false positive rate, by default it's 0.1%.
        double bloom_filter_false_positive_rate;
        /// For ngram tokenizer
        std::optional<UInt64> ngram_size;
        /// For split tokenizer
        std::optional<std::vector<String>> separators;

        bool operator<=>(const Parameters & other) const = default;
    };

    GinFilter() = default;

    /// Add token and its row ID to the postings list builder for building the text index for the given store.
    void add(const String & token, UInt32 row_id, GinIndexStorePtr & store) const;

    /// Accumulate (segment_id, rowid_start, rowid_end) for building the text index.
    void addRowIdRangeToGinFilter(UInt32 segment_id, UInt32 rowid_start, UInt32 rowid_end);

    /// Check if the filter (built from query string) contains any rows in given filter by using given postings list cache.
    bool contains(const GinQueryString & query_string, GinPostingsListsCacheForStore & postings_lists_cache_for_store, GinSearchMode mode = GinSearchMode::All) const;

    const GinSegmentsWithRowIdRange & getSegmentsWithRowIdRange() const { return segments_with_rowid_range; }
    GinSegmentsWithRowIdRange & getSegmentsWithRowIdRange() { return segments_with_rowid_range; }

    size_t memoryUsageBytes() const;

private:
    /// Row ID ranges which are (segment_id, rowid_start, rowid_end)
    GinSegmentsWithRowIdRange segments_with_rowid_range;
};

}
