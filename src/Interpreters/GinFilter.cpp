// NOLINTBEGIN(clang-analyzer-optin.core.EnumCastOutOfRange)

#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Disks/DiskLocal.h>
#include <Interpreters/GinFilter.h>
#include <Storages/MergeTree/GinIndexStore.h>
#include <Storages/MergeTree/MergeTreeIndexBloomFilterText.h>
#include <Storages/MergeTree/MergeTreeIndexGin.h>
#include <city.h>

namespace DB
{

GinFilter::Parameters::Parameters(
    String tokenizer_,
    UInt64 segment_digestion_threshold_bytes_,
    double bloom_filter_false_positive_rate_,
    std::optional<UInt64> ngram_size_,
    std::optional<std::vector<String>> separators_)
    : tokenizer(std::move(tokenizer_))
    , segment_digestion_threshold_bytes(segment_digestion_threshold_bytes_)
    , bloom_filter_false_positive_rate(bloom_filter_false_positive_rate_)
    , ngram_size(ngram_size_)
    , separators(separators_)
{
}

GinQueryString::GinQueryString(std::string_view query_string_, const std::vector<String> & search_terms_)
    : query_string(query_string_)
    , terms(search_terms_)
{
}

void GinFilter::add(const String & term, UInt32 rowID, GinIndexStorePtr & store) const
{
    if (term.length() > FST::MAX_TERM_LENGTH)
        return;

    auto it = store->getPostingsListBuilder().find(term);

    if (it != store->getPostingsListBuilder().end())
    {
        if (!it->second->contains(rowID))
            it->second->add(rowID);
    }
    else
    {
        auto builder = std::make_shared<GinIndexPostingsBuilder>();
        builder->add(rowID);

        store->setPostingsBuilder(term, builder);
    }
}

/// This method assumes segmentIDs are in increasing order, which is true since rows are
/// digested sequentially and segments are created sequentially too.
void GinFilter::addRowRangeToGinFilter(UInt32 segment_id, UInt32 rowid_start, UInt32 rowid_end)
{
    /// check segment ids are monotonic increasing
    assert(rowid_ranges.empty() || rowid_ranges.back().segment_id <= segment_id);

    if (!rowid_ranges.empty())
    {
        /// Try to merge the rowID range with the last one in the container
        GinSegmentWithRowIdRange & last_rowid_range = rowid_ranges.back();

        if (last_rowid_range.segment_id == segment_id &&
            last_rowid_range.range_end + 1 == rowid_start)
        {
            last_rowid_range.range_end = rowid_end;
            return;
        }
    }
    rowid_ranges.push_back({segment_id, rowid_start, rowid_end});
}

void GinFilter::clear()
{
    rowid_ranges.clear();
}

namespace
{

/// Helper method for checking if postings list cache is empty
bool hasEmptyPostingsList(const GinPostingsCache & postings_cache)
{
    if (postings_cache.empty())
        return true;

    for (const auto & term_postings : postings_cache)
    {
        const GinSegmentedPostingsListContainer & container = term_postings.second;
        if (container.empty())
            return true;
    }
    return false;
}

/// Helper method to check if all terms in postings list cache has intersection with given row ID range
bool matchAllInRange(const GinPostingsCache & postings_cache, UInt32 segment_id, UInt32 range_start, UInt32 range_end)
{
    /// Check for each term
    GinIndexPostingsList range_bitset;
    range_bitset.addRange(range_start, range_end + 1);

    for (const auto & term_postings : postings_cache)
    {
        /// Check if it is in the same segment by searching for segment_id
        const GinSegmentedPostingsListContainer & container = term_postings.second;
        auto container_it = container.find(segment_id);
        if (container_it == container.cend())
            return false;
        auto min_in_container = container_it->second->minimum();
        auto max_in_container = container_it->second->maximum();

        if (range_start > max_in_container || min_in_container > range_end)
            return false;

        range_bitset &= *container_it->second;

        if (range_bitset.isEmpty())
            return false;
    }
    return true;
}

/// Helper method to check if any term in postings list cache has intersection with given row ID range
bool matchAnyInRange(const GinPostingsCache & postings_cache, UInt32 segment_id, UInt32 range_start, UInt32 range_end)
{
    /// Check for each term
    GinIndexPostingsList postings_bitset;
    for (const auto & term_postings : postings_cache)
    {
        /// Check if it is in the same segment by searching for segment_id
        const GinSegmentedPostingsListContainer & container = term_postings.second;
        if (auto container_it = container.find(segment_id); container_it != container.cend())
            postings_bitset |= *container_it->second;
    }

    GinIndexPostingsList range_bitset;
    range_bitset.addRange(range_start, range_end + 1);
    return range_bitset.intersect(postings_bitset);
}


template <GinSearchMode search_mode>
bool matchInRange(const GinSegmentWithRowIdRangeVector & rowid_ranges, const GinPostingsCache & postings_cache)
{
    if (hasEmptyPostingsList(postings_cache))
        switch (search_mode)
        {
            case GinSearchMode::Any: {
                if (postings_cache.size() == 1)
                    /// Definitely no match when there is a single term in ANY search mode and the term does not exists in FST.
                    return false;
                break;
            }
            case GinSearchMode::All:
                return false;
        }

    /// Check for each row ID ranges
    for (const auto & rowid_range : rowid_ranges)
    {
        switch (search_mode)
        {
            case GinSearchMode::Any: {
                if (matchAnyInRange(postings_cache, rowid_range.segment_id, rowid_range.range_start, rowid_range.range_end))
                    return true;
                break;
            }
            case GinSearchMode::All: {
                if (matchAllInRange(postings_cache, rowid_range.segment_id, rowid_range.range_start, rowid_range.range_end))
                    return true;
                break;
            }
        }
    }
    return false;
}

}

bool GinFilter::contains(const GinQueryString & gin_query_string, PostingsCacheForStore & cache_store, GinSearchMode search_mode) const
{
    if (gin_query_string.getTerms().empty())
        return true;

    GinPostingsCachePtr postings_cache = cache_store.getPostings(gin_query_string.getQueryString());
    if (postings_cache == nullptr)
    {
        GinIndexStoreDeserializer reader(cache_store.store);
        postings_cache = reader.createPostingsCacheFromTerms(gin_query_string.getTerms());
        cache_store.cache[gin_query_string.getQueryString()] = postings_cache;
    }

    switch (search_mode)
    {
        case GinSearchMode::Any:
            return matchInRange<GinSearchMode::Any>(rowid_ranges, *postings_cache);
        case GinSearchMode::All:
            return matchInRange<GinSearchMode::All>(rowid_ranges, *postings_cache);
    }
}


size_t GinFilter::memoryUsageBytes() const
{
    return rowid_ranges.capacity() * sizeof(rowid_ranges[0]);
}

}

// NOLINTEND(clang-analyzer-optin.core.EnumCastOutOfRange)
