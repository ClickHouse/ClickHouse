#pragma once

#include <Storages/MergeTree/GinIndexStore.h>
#include <vector>
#include <memory>

namespace DB
{

static inline constexpr auto INVERTED_INDEX_NAME = "inverted";

struct GinFilterParameters
{
    GinFilterParameters(size_t ngrams_, Float64 density_);

    size_t ngrams;
    Float64 density;
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

/// GinFilter provides underlying functionalities for building inverted index and also
/// it does filtering the unmatched rows according to its query string.
/// It also builds and uses skipping index which stores (segmentID, RowIDStart, RowIDEnd) triples.
class GinFilter
{
public:

    explicit GinFilter(const GinFilterParameters & params_);

    /// Add term (located at 'data' with length 'len') and its row ID to the postings list builder
    /// for building inverted index for the given store.
    void add(const char * data, size_t len, UInt32 rowID, GinIndexStorePtr & store, UInt64 limit) const;

    /// Accumulate (segmentID, RowIDStart, RowIDEnd) for building skipping index
    void addRowRangeToGinFilter(UInt32 segmentID, UInt32 rowIDStart, UInt32 rowIDEnd);

    /// Clear the content
    void clear();

    /// Check if the filter (built from query string) contains any rows in given filter by using
    /// given postings list cache
    bool contains(const GinFilter & filter, PostingsCacheForStore & cache_store) const;

    /// Set the query string of the filter
    void setQueryString(const char * data, size_t len)
    {
        query_string = String(data, len);
    }

    /// Add term which are tokens generated from the query string
    void addTerm(const char * data, size_t len)
    {
        if (len > FST::MAX_TERM_LENGTH)
            return;
        terms.push_back(String(data, len));
    }

    /// Getter
    const String & getQueryString() const { return query_string; }
    const std::vector<String> & getTerms() const { return terms; }
    const GinSegmentWithRowIdRangeVector & getFilter() const { return rowid_ranges; }
    GinSegmentWithRowIdRangeVector & getFilter() { return rowid_ranges; }

private:
    /// Filter parameters
    const GinFilterParameters & params;

    /// Query string of the filter
    String query_string;

    /// Tokenized terms from query string
    std::vector<String> terms;

    /// Row ID ranges which are (segmentID, RowIDStart, RowIDEnd)
    GinSegmentWithRowIdRangeVector rowid_ranges;

    /// Check if the given postings list cache has matched rows by using the filter
    bool match(const GinPostingsCache & postings_cache) const;
};

using GinFilters = std::vector<GinFilter>;

}
