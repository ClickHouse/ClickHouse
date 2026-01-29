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
#include <Storages/MergeTree/MergeTreeIndexFullText.h>
#include <string>
#include <algorithm>
#include <city.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

GinFilterParameters::GinFilterParameters(size_t ngrams_, UInt64 max_rows_per_postings_list_)
    : ngrams(ngrams_)
    , max_rows_per_postings_list(max_rows_per_postings_list_)
{
    if (max_rows_per_postings_list == UNLIMITED_ROWS_PER_POSTINGS_LIST)
        max_rows_per_postings_list = std::numeric_limits<UInt64>::max();

    if (ngrams > 8)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The size of full-text index filter cannot be greater than 8");
}

GinFilter::GinFilter(const GinFilterParameters & params_)
    : params(params_)
{
}

void GinFilter::add(const char * data, size_t len, UInt32 rowID, GinIndexStorePtr & store) const
{
    if (len > FST::MAX_TERM_LENGTH)
        return;

    String term(data, len);
    auto it = store->getPostingsListBuilder().find(term);

    if (it != store->getPostingsListBuilder().end())
    {
        if (!it->second->contains(rowID))
            it->second->add(rowID);
    }
    else
    {
        auto builder = std::make_shared<GinIndexPostingsBuilder>(params.max_rows_per_postings_list);
        builder->add(rowID);

        store->setPostingsBuilder(term, builder);
    }
}

/// This method assumes segmentIDs are in increasing order, which is true since rows are
/// digested sequentially and segments are created sequentially too.
void GinFilter::addRowRangeToGinFilter(UInt32 segmentID, UInt32 rowIDStart, UInt32 rowIDEnd)
{
    /// check segment ids are monotonic increasing
    assert(rowid_ranges.empty() || rowid_ranges.back().segment_id <= segmentID);

    if (!rowid_ranges.empty())
    {
        /// Try to merge the rowID range with the last one in the container
        GinSegmentWithRowIdRange & last_rowid_range = rowid_ranges.back();

        if (last_rowid_range.segment_id == segmentID &&
            last_rowid_range.range_end+1 == rowIDStart)
        {
            last_rowid_range.range_end = rowIDEnd;
            return;
        }
    }
    rowid_ranges.push_back({segmentID, rowIDStart, rowIDEnd});
}

void GinFilter::clear()
{
    query_string.clear();
    terms.clear();
    rowid_ranges.clear();
}

bool GinFilter::contains(const GinFilter & filter, PostingsCacheForStore & cache_store) const
{
    if (filter.getTerms().empty())
        return true;

    GinPostingsCachePtr postings_cache = cache_store.getPostings(filter.getQueryString());
    if (postings_cache == nullptr)
    {
        GinIndexStoreDeserializer reader(cache_store.store);
        postings_cache = reader.createPostingsCacheFromTerms(filter.getTerms());
        cache_store.cache[filter.getQueryString()] = postings_cache;
    }

    return match(*postings_cache);
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

/// Helper method to check if the postings list cache has intersection with given row ID range
bool matchInRange(const GinPostingsCache & postings_cache, UInt32 segment_id, UInt32 range_start, UInt32 range_end)
{
    /// Check for each term
    GinIndexPostingsList intersection_result;
    bool intersection_result_init = false;

    for (const auto & term_postings : postings_cache)
    {
        /// Check if it is in the same segment by searching for segment_id
        const GinSegmentedPostingsListContainer & container = term_postings.second;
        auto container_it = container.find(segment_id);
        if (container_it == container.cend())
            return false;
        auto min_in_container = container_it->second->minimum();
        auto max_in_container = container_it->second->maximum();

        //check if the postings list has always match flag
        if (container_it->second->cardinality() == 1 && UINT32_MAX == min_in_container)
            continue; //always match

        if (range_start > max_in_container ||  min_in_container > range_end)
            return false;

        /// Delay initialization as late as possible
        if (!intersection_result_init)
        {
            intersection_result_init = true;
            intersection_result.addRange(range_start, range_end+1);
        }
        intersection_result &= *container_it->second;
        if (intersection_result.cardinality() == 0)
            return false;
    }
    return true;
}

}

bool GinFilter::match(const GinPostingsCache & postings_cache) const
{
    if (hasEmptyPostingsList(postings_cache))
        return false;

    /// Check for each row ID ranges
    for (const auto & rowid_range: rowid_ranges)
        if (matchInRange(postings_cache, rowid_range.segment_id, rowid_range.range_start, rowid_range.range_end))
            return true;
    return false;
}

}

// NOLINTEND(clang-analyzer-optin.core.EnumCastOutOfRange)
