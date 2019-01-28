#pragma once

#include <Storages/MergeTree/MergeSelector.h>


namespace DB
{

/// need comment
class TTLMergeSelector : public IMergeSelector
{
public:
    /// Parameter max_total_size_to_merge is ignored.
    PartsInPartition select(
        const Partitions & partitions,
        const size_t max_total_size_to_merge) override;
};

}
