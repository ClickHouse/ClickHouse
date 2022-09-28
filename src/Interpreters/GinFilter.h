#pragma once

#include <vector>
#include <memory>
#include <Storages/MergeTree/GinIndexStore.h>
namespace DB
{
struct GinFilterParameters
{
    explicit GinFilterParameters(size_t ngrams_);

    size_t ngrams;
};

struct RowIDRange
{
    /// Segment ID of the row ID range
    UInt32 segment_id;

    /// First row ID in the range
    UInt32 range_start;

    /// Last row ID in the range
    UInt32 range_end;
};

class GinFilter
{
public:
    using RowIDRangeContainer = std::vector<RowIDRange>;

    explicit GinFilter(const GinFilterParameters& params);

    void add(const char* data, size_t len, UInt32 rowID, GinIndexStorePtr& store);

    void addRowRangeToGinFilter(UInt32 segmentID, UInt32 rowIDStart, UInt32 rowIDEnd);

    void clear();

    size_t size() const { return rowid_range_container.size(); }

    bool contains(const GinFilter& af, PostingsCacheForStore &store) const;

    const RowIDRangeContainer& getFilter() const { return rowid_range_container; }

    RowIDRangeContainer& getFilter() { return rowid_range_container; }

    void setQueryString(const char* data, size_t len)
    {
        query_string = String(data, len);
    }

    const String &getQueryString() const { return query_string; }

    void addTerm(const char* data, size_t len)
    {
        if (len > FST::MAX_TERM_LENGTH)
            return;
        terms.push_back(String(data, len));
    }

    const std::vector<String>& getTerms() const { return terms;}

    bool match(const PostingsCachePtr& postings_cache) const;

    static String getName();

    static constexpr auto FilterName = "inverted";
private:
    [[maybe_unused]] size_t ngrams;

    String query_string;

    std::vector<String> terms;

    RowIDRangeContainer rowid_range_container;

    static bool hasEmptyPostingsList(const PostingsCachePtr& postings_cache);

    static bool matchInRange(const PostingsCachePtr& postings_cache, UInt32 segment_id, UInt32 range_start, UInt32 range_end);
};

using GinFilterPtr = std::shared_ptr<GinFilter>;

}
