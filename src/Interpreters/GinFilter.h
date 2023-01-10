#pragma once

#include <vector>
#include <memory>
#include <Storages/MergeTree/GinIndexStore.h>
namespace DB
{
struct GinFilterParameters
{
    explicit GinFilterParameters(size_t ngrams_, Float64 density_);

    size_t ngrams;
    Float64 density;
};

struct GinSegmentWithRowIDRange
{
    /// Segment ID of the row ID range
    UInt32 segment_id;

    /// First row ID in the range
    UInt32 range_start;

    /// Last row ID in the range (inclusive)
    UInt32 range_end;
};

/// GinFilter provides underlying functionalities for building inverted index and also
/// it does filtering the unmatched rows according to its query string.
/// It also builds and uses skipping index which stores (segmentID, RowIDStart, RowIDEnd) triples.
class GinFilter
{
public:
    using GinSegmentWithRowIDRanges = std::vector<GinSegmentWithRowIDRange>;

    explicit GinFilter(const GinFilterParameters& params_);

    /// Add term(which length is 'len' and located at 'data') and its row ID to
    /// the postings list builder for building inverted index for the given store.
    void add(const char* data, size_t len, UInt32 rowID, GinIndexStorePtr& store, UInt64 limit);

    /// Accumulate (segmentID, RowIDStart, RowIDEnd) for building skipping index
    void addRowRangeToGinFilter(UInt32 segmentID, UInt32 rowIDStart, UInt32 rowIDEnd);

    /// Clear the content
    void clear();

    /// Check if the filter(built from query string) contains any rows in given filter 'af' by using
    /// given postings list cache
    bool contains(const GinFilter& af, PostingsCacheForStore &store) const;

    /// Const getter for the row ID ranges
    const GinSegmentWithRowIDRanges& getFilter() const { return rowid_ranges; }

    /// Mutable getter for the row ID ranges
    GinSegmentWithRowIDRanges& getFilter() { return rowid_ranges; }

    /// Set the query string of the filter
    void setQueryString(const char* data, size_t len)
    {
        query_string = String(data, len);
    }

    /// Const getter of the query string
    const String &getQueryString() const { return query_string; }

    /// Add term which are tokens generated from the query string
    void addTerm(const char* data, size_t len)
    {
        if (len > FST::MAX_TERM_LENGTH)
            return;
        terms.push_back(String(data, len));
    }

    /// Const getter of terms(generated from the query string)
    const std::vector<String>& getTerms() const { return terms;}

    /// Check if the given postings list cache has matched rows by using the filter
    bool match(const PostingsCache& postings_cache) const;

    /// Get filter name ("inverted")
    static String getName();

    /// Constant of filter name
    static constexpr auto FilterName = "inverted";
private:
    /// Filter parameters
    const GinFilterParameters& params;

    /// Query string of the filter
    String query_string;

    /// Tokenized terms from query string
    std::vector<String> terms;

    /// Row ID ranges which are (segmentID, RowIDStart, RowIDEnd)
    GinSegmentWithRowIDRanges rowid_ranges;

    /// Helper method for checking if postings list cache is empty
    static bool hasEmptyPostingsList(const PostingsCache& postings_cache);

    /// Helper method to check if the postings list cache has intersection with given row ID range
    static bool matchInRange(const PostingsCache& postings_cache, UInt32 segment_id, UInt32 range_start, UInt32 range_end);
};

using GinFilterPtr = std::shared_ptr<GinFilter>;

}
