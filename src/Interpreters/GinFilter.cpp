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

GinQueryString::GinQueryString(std::string_view query_string_, const std::vector<String> & tokens_)
    : query_string(query_string_)
    , tokens(tokens_)
{
}

void GinFilter::add(const String & token, UInt32 row_id, GinIndexStorePtr & store) const
{
    if (token.length() > FST::MAX_TOKEN_LENGTH)
        return;

    auto it = store->getTokenPostingsLists().find(token);

    if (it != store->getTokenPostingsLists().end())
    {
        if (!it->second->contains(row_id))
            it->second->add(row_id);
    }
    else
    {
        auto postings_list_builder = std::make_shared<GinPostingsListBuilder>();
        postings_list_builder->add(row_id);

        store->setPostingsListBuilder(token, postings_list_builder);
    }
}

/// This method assumes segmentIDs are in increasing order, which is true since rows are
/// digested sequentially and segments are created sequentially too.
void GinFilter::addRowIdRangeToGinFilter(UInt32 segment_id, UInt32 rowid_start, UInt32 rowid_end)
{
    /// Check that segment ids are monotonic increasing
    chassert(segments_with_rowid_range.empty() || segments_with_rowid_range.back().segment_id <= segment_id);

    if (!segments_with_rowid_range.empty())
    {
        /// Try to merge the row_id range with the last one in the container
        GinSegmentWithRowIdRange & last_rowid_range = segments_with_rowid_range.back();

        if (last_rowid_range.segment_id == segment_id &&
            last_rowid_range.range_rowid_end + 1 == rowid_start)
        {
            last_rowid_range.range_rowid_end = rowid_end;
            return;
        }
    }
    segments_with_rowid_range.push_back({segment_id, rowid_start, rowid_end});
}

namespace
{

/// Helper method for checking if postings list cache is empty
bool hasEmptyPostingsList(const GinPostingsListsCache & postings_lists_cache)
{
    if (postings_lists_cache.empty())
        return true;

    for (const auto & cache_entry : postings_lists_cache)
    {
        const GinSegmentPostingsLists & segment_postings_lists = cache_entry.second;
        if (segment_postings_lists.empty())
            return true;
    }
    return false;
}

/// Helper method to check if all tokens in postings list cache has intersection with given row ID range
bool matchAllInRange(const GinPostingsListsCache & postings_lists_cache, UInt32 segment_id, UInt32 range_rowid_start, UInt32 range_rowid_end)
{
    /// Check for each tokens
    GinPostingsList range_bitset;
    range_bitset.addRange(range_rowid_start, range_rowid_end + 1);

    for (const auto & cache_entry : postings_lists_cache)
    {
        /// Check if it is in the same segment by searching for segment_id
        const GinSegmentPostingsLists & segment_postings_lists = cache_entry.second;
        auto container_it = segment_postings_lists.find(segment_id);
        if (container_it == segment_postings_lists.end())
            return false;

        UInt32 min_in_container = container_it->second->minimum();
        UInt32 max_in_container = container_it->second->maximum();

        if (range_rowid_start > max_in_container || min_in_container > range_rowid_end)
            return false;

        range_bitset &= *container_it->second;

        if (range_bitset.isEmpty())
            return false;
    }
    return true;
}

/// Helper method to check if any token in postings list cache has intersection with given row ID range
bool matchAnyInRange(const GinPostingsListsCache & postings_lists_cache, UInt32 segment_id, UInt32 range_rowid_start, UInt32 range_rowid_end)
{
    /// Check for each token
    GinPostingsList postings_bitset;
    for (const auto & cache_entry : postings_lists_cache)
    {
        /// Check if it is in the same segment by searching for segment_id
        const GinSegmentPostingsLists & segment_postings_lists = cache_entry.second;
        if (auto container_it = segment_postings_lists.find(segment_id); container_it != segment_postings_lists.end())
            postings_bitset |= *container_it->second;
    }

    GinPostingsList range_bitset;
    range_bitset.addRange(range_rowid_start, range_rowid_end + 1);
    return range_bitset.intersect(postings_bitset);
}


template <GinSearchMode search_mode>
bool matchInRange(const GinSegmentsWithRowIdRange & segments_with_rowid_range, const GinPostingsListsCache & postings_lists_cache)
{
    if (hasEmptyPostingsList(postings_lists_cache))
    {
        switch (search_mode)
        {
            case GinSearchMode::Any: {
                if (postings_lists_cache.size() == 1)
                    /// Definitely no match when there is a single token in ANY search mode and the token does not exists in FST.
                    return false;
                break;
            }
            case GinSearchMode::All:
                return false;
        }
    }

    /// Check for each row ID ranges
    for (const auto & segment_with_rowid_range : segments_with_rowid_range)
    {
        switch (search_mode)
        {
            case GinSearchMode::Any: {
                if (matchAnyInRange(postings_lists_cache, segment_with_rowid_range.segment_id, segment_with_rowid_range.range_rowid_start, segment_with_rowid_range.range_rowid_end))
                    return true;
                break;
            }
            case GinSearchMode::All: {
                if (matchAllInRange(postings_lists_cache, segment_with_rowid_range.segment_id, segment_with_rowid_range.range_rowid_start, segment_with_rowid_range.range_rowid_end))
                    return true;
                break;
            }
        }
    }
    return false;
}

}

bool GinFilter::contains(const GinQueryString & query_string, GinPostingsListsCacheForStore & postings_lists_cache_for_store, GinSearchMode search_mode) const
{
    if (query_string.getTokens().empty())
        return true;

    GinPostingsListsCachePtr postings_lists_cache = postings_lists_cache_for_store.getPostingsLists(query_string.getQueryString());
    if (postings_lists_cache == nullptr)
    {
        GinIndexStoreDeserializer deserializer(postings_lists_cache_for_store.store);
        postings_lists_cache = deserializer.createPostingsListsCacheFromTokens(query_string.getTokens());
        postings_lists_cache_for_store.cache[query_string.getQueryString()] = postings_lists_cache;
    }

    switch (search_mode)
    {
        case GinSearchMode::Any:
            return matchInRange<GinSearchMode::Any>(segments_with_rowid_range, *postings_lists_cache);
        case GinSearchMode::All:
            return matchInRange<GinSearchMode::All>(segments_with_rowid_range, *postings_lists_cache);
    }
}


size_t GinFilter::memoryUsageBytes() const
{
    return segments_with_rowid_range.capacity() * sizeof(segments_with_rowid_range[0]);
}

}

// NOLINTEND(clang-analyzer-optin.core.EnumCastOutOfRange)
