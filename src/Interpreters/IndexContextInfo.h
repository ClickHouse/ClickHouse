#pragma once

#include <Storages/MergeTree/RangesInDataPart.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <unordered_map>
#include <memory>
#include <optional>
#include <vector>
#include <unistd.h>

namespace DB
{

struct PostingsCacheForStore;

// TODO: JAM Move this to it's own file
class IndexContextInfo
{
public:
    using IndexPostingsCacheForStoreMap = std::unordered_map<String, std::shared_ptr<const PostingsCacheForStore>>;

    struct PartInfo {
		DataPartPtr data_part;
        MarkRanges ranges;
        IndexPostingsCacheForStoreMap postings_cache_for_store_part;
    };

    const std::shared_ptr<UsefulSkipIndexes> skip_indexes;
    const std::vector<std::optional<PartInfo>> part_info_vector;


    // /// TODO: JAM This can be used to set a size limit and stop caching.
    // bool emplaceRangesInDataPart(size_t part_idx, IndexPostingsCacheForStoreMap entry)
    // {
    //     std::lock_guard lk(postings_cache_for_store_part_mutex);
    //     auto [_, emplaced] = postings_cache_for_store_part_cache.emplace(part_idx, entry);
    //     return emplaced;
    // }


    // /// TODO: JAM This can be used to set a size limit and stop caching.
    // bool emplacePostingsCacheForStore(size_t part_idx, IndexPostingsCacheForStoreMap entry)
    // {
    //     std::lock_guard lk(postings_cache_for_store_part_mutex);
    //     auto [_, emplaced] = postings_cache_for_store_part_cache.emplace(part_idx, entry);
    //     return emplaced;
    // }

    // std::pair<
    //     std::shared_ptr<const PostingsCacheForStore>,
    //     std::shared_lock<std::shared_mutex>> getPostingsCacheForStore(size_t part_idx, const String &index_name)
    // {
    //     try {
    //         auto lock = std::shared_lock(postings_cache_for_store_part_mutex);
    //         std::shared_ptr<const PostingsCacheForStore> postings_cache = postings_cache_for_store_part_cache.at(part_idx).at(index_name);
    //         return std::make_pair(postings_cache, std::move(lock));
    //     }
    //     catch(const std::out_of_range&)
    //     {
    //         return std::make_pair(nullptr, std::shared_lock<std::shared_mutex>{});
    //     }
    // }
};


using IndexContextInfoPtr = std::shared_ptr<IndexContextInfo>;

}
