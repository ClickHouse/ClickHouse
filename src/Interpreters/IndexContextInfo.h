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

struct GinPostingsListsCacheForStore;

class IndexContextInfo
{
public:
    using IndexPostingsCacheForStoreMap = std::unordered_map<String, std::shared_ptr<const GinPostingsListsCacheForStore>>;

    struct PartInfo
    {
        DataPartPtr data_part;
        MarkRanges ranges;
        IndexPostingsCacheForStoreMap postings_cache_for_store_part;
    };

    const std::shared_ptr<UsefulSkipIndexes> skip_indexes;
    const std::vector<std::optional<PartInfo>> part_info_vector;
};


using IndexContextInfoPtr = std::shared_ptr<IndexContextInfo>;

}
