#pragma once

#include <Storages/MergeTree/GinIndexStore.h>
#include <vector>

namespace DB
{

static constexpr UInt64 UNLIMITED_ROWS_PER_POSTINGS_LIST = 0;
static constexpr UInt64 DEFAULT_NGRAM_SIZE = 3;
static constexpr auto DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.001; /// 0.1%

static inline constexpr auto TEXT_INDEX_NAME = "text";

enum class GinSearchMode : uint8_t
{
    Any,
    All
};

struct GinSegmentWithRowIdRange
{
    /// Segment ID of the row ID range
    UInt32 segment_id;

    /// First row ID in the range
    UInt32 range_start;

    /// Last row ID in the range (inclusive)
    UInt32 range_end;
};

using GinSegmentWithRowIdRangeVector = std::vector<GinSegmentWithRowIdRange>;

class GinQueryString
{
public:
    GinQueryString() = default;
    GinQueryString(std::string_view query_string_, const std::vector<String> & search_terms_);

    /// Getter
    const String & getQueryString() const { return query_string; }
    const std::vector<String> & getTerms() const { return terms; }

    /// Set the query string of the filter
    void setQueryString(std::string_view query_string_)
    {
        query_string = query_string_;
    }

    /// Add term which are tokens generated from the query string
    bool addTerm(std::string_view term)
    {
        if (term.length() > FST::MAX_TERM_LENGTH) [[unlikely]]
            return false;

        terms.push_back(String(term));
        return true;
    }

private:
    /// Query string of the filter
    String query_string;
    /// Tokenized terms from query string
    std::vector<String> terms;
};

/// GinFilter provides underlying functionalities for building text index and also
/// it does filtering the unmatched rows according to its query string.
/// It also builds and uses skipping index which stores (segment_id, rowid_start, rowid_end) triples.
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

        bool operator<=>(const Parameters& other) const = default;
    };

    GinFilter() = default;

    /// Add term (located at 'data' with length 'len') and its row ID to the postings list builder
    /// for building text index for the given store.
    void add(const String & term, UInt32 rowID, GinIndexStorePtr & store) const;

    /// Accumulate (segment_id, rowid_start, rowid_end) for building skipping index
    void addRowRangeToGinFilter(UInt32 segment_id, UInt32 rowid_start, UInt32 rowid_end);

    /// Clear the content
    void clear();

    /// Check if the filter (built from query string) contains any rows in given filter by using
    /// given postings list cache
    bool contains(const GinQueryString & gin_query_string, PostingsCacheForStore & cache_store, GinSearchMode mode = GinSearchMode::All) const;

    const GinSegmentWithRowIdRangeVector & getFilter() const { return rowid_ranges; }
    GinSegmentWithRowIdRangeVector & getFilter() { return rowid_ranges; }

    size_t memoryUsageBytes() const;

private:
    /// Row ID ranges which are (segment_id, rowid_start, rowid_end)
    GinSegmentWithRowIdRangeVector rowid_ranges;
};

}
