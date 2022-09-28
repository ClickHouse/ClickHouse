#include <string>
#include <algorithm>
#include <city.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Storages/MergeTree/MergeTreeIndexGin.h>
#include <Storages/MergeTree/MergeTreeIndexFullText.h>
#include <Disks/DiskLocal.h>
#include <Storages/MergeTree/GinIndexStore.h>
#include <Interpreters/GinFilter.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
GinFilterParameters::GinFilterParameters(size_t ngrams_)
    : ngrams(ngrams_)
{
    if (ngrams > 8)
        throw Exception("The size of gin filter cannot be greater than 8", ErrorCodes::BAD_ARGUMENTS);
}

GinFilter::GinFilter(const GinFilterParameters & params)
    : ngrams(params.ngrams)
{
}

void GinFilter::add(const char* data, size_t len, UInt32 rowID, GinIndexStorePtr& store)
{
    if (len > FST::MAX_TERM_LENGTH)
        return;

    std::string token(data, len);
    auto it = store->getPostings().find(token);

    if (it != store->getPostings().end())
    {
        if (!it->second->contains(rowID))
            it->second->add(rowID);
    }
    else
    {
        GinIndexPostingsBuilderPtr builder = std::make_shared<GinIndexPostingsBuilder>();
        builder->add(rowID);

        store->getPostings()[token] = builder;
    }
}

/// This method assumes segmentIDs are in increasing order, which is true since rows are
/// digested sequentially and segments are created sequentially too.
void GinFilter::addRowRangeToGinFilter(UInt32 segmentID, UInt32 rowIDStart, UInt32 rowIDEnd)
{
    if (!rowid_range_container.empty())
    {
        /// Try to merge the rowID range with the last one in the container
        if (rowid_range_container.back().segment_id == segmentID &&
            rowid_range_container.back().range_end+1 == rowIDStart)
        {
            rowid_range_container.back().range_end = rowIDEnd;
            return;
        }
    }
    rowid_range_container.push_back({segmentID, rowIDStart, rowIDEnd});
}

void GinFilter::clear()
{
    terms.clear();
    rowid_range_container.clear();
}

bool GinFilter::hasEmptyPostingsList(const PostingsCachePtr& postings_cache)
{
    if (postings_cache->empty())
        return true;

    for (const auto& term_postings : *postings_cache)
    {
        const SegmentedPostingsListContainer& container = term_postings.second;
        if (container.empty())
            return true;
    }
    return false;
}

bool GinFilter::matchInRange(const PostingsCachePtr& postings_cache, UInt32 segment_id, UInt32 range_start, UInt32 range_end)
{
    /// Check for each terms
    GinIndexPostingsList intersection_result;
    bool intersection_result_init = false;

    for (const auto& term_postings : *postings_cache)
    {
        const SegmentedPostingsListContainer& container = term_postings.second;
        auto container_it{ container.find(segment_id) };
        if (container_it == container.cend())
        {
            return false;
        }
        auto min_in_container = container_it->second->minimum();
        auto max_in_container = container_it->second->maximum();
        if (range_start > max_in_container ||  min_in_container > range_end)
        {
            return false;
        }

        /// Delay initialization as late as possible
        if (!intersection_result_init)
        {
            intersection_result_init = true;
            intersection_result.addRange(range_start, range_end+1);
        }
        intersection_result &= *container_it->second;
        if (intersection_result.cardinality() == 0)
        {
            return false;
        }
    }
    return true;
}

bool GinFilter::match(const PostingsCachePtr& postings_cache) const
{
    if (hasEmptyPostingsList(postings_cache))
    {
        return false;
    }

    /// Check for each row ID ranges
    for (const auto &rowid_range: rowid_range_container)
    {
        if (matchInRange(postings_cache, rowid_range.segment_id, rowid_range.range_start, rowid_range.range_end))
        {
            return true;
        }
    }
    return false;
}

bool GinFilter::contains(const GinFilter & filter, PostingsCacheForStore &cache_store) const
{
    if (filter.getTerms().empty())
        return true;

    PostingsCachePtr postings_cache = cache_store.getPostings(filter.getQueryString());
    if (postings_cache == nullptr)
    {
        GinIndexStoreDeserializer reader(cache_store.store);
        postings_cache = reader.loadPostingsIntoCache(filter.getTerms());
        cache_store.cache[filter.getQueryString()] = postings_cache;
    }

    return match(postings_cache);
}

String GinFilter::getName()
{
    return FilterName;
}

}
