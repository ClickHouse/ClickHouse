#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MemoryMergeSelector.h>
#include <cmath>


namespace DB
{

namespace
{

/** Estimates best set of parts to merge within passed alternatives.
  * within specified length limits, return the longest part segments with minimal total size.
  */
struct Estimator
{
    using Iterator = MemoryMergeSelector::PartsRange::const_iterator;

    void consider(Iterator begin, Iterator end, size_t sum_size)
    {
        size_t current_len = end - begin;
        if (current_len > part_count)
        {
            part_count = current_len;
            best_begin = begin;
            best_end = end;
        }
        else if(current_len == part_count)
        {
            double current_score = sum_size;
            if (!min_score || current_score < min_score)
            {
                min_score = current_score;
                best_begin = begin;
                best_end = end;
            }
        }
    }

    MemoryMergeSelector::PartsRange getBest() const
    {
        return MemoryMergeSelector::PartsRange(best_begin, best_end);
    }

    double min_score = 0;
    size_t part_count = 0;
    Iterator best_begin {};
    Iterator best_end {};
};

void selectWithinPartition(
    const MemoryMergeSelector::PartsRange & parts,
    const size_t max_total_size_to_merge,
    Estimator & estimator,
    const MemoryMergeSelector::Settings & settings)
{
    size_t memory_part_count = 0;
    auto iter = parts.rbegin();
    while(iter != parts.rend())
    {
        const MergeTreeData::DataPartPtr & part = *(static_cast<const MergeTreeData::DataPartPtr *>((*iter).data));

        if (part->getType() != MergeTreeDataPartType::IN_MEMORY)
            break;
        else /// prevent SimpleMergeSelector merging InMemory parts.
        {
            auto & part_info = const_cast<MemoryMergeSelector::Part &>(*iter);
            part_info.shall_participate_in_merges = false;

            memory_part_count++;
        }
        iter++;
    }

    if (memory_part_count < settings.min_parts_to_merge)
        return;

    int idx = parts.size() - 1;
    size_t selected_count = 0;
    size_t sum_size = 0;
    while (true)
    {
        sum_size  += parts[idx].size;
        selected_count++;
        if (selected_count > settings.max_parts_to_merge || sum_size > max_total_size_to_merge || selected_count == memory_part_count)
            break;

        idx--;
    }
    estimator.consider(parts.begin() + idx, parts.end(), sum_size);
}
}

void MemoryMergeSelector::dumpPartsInfo(const MemoryMergeSelector::PartsRange & parts, String prefix)
{
    std::ostringstream debug_buf;
    for (const IMergeSelector::Part & part : parts)
    {
        const MergeTreeData::DataPartPtr treeDataPart = *(static_cast<const MergeTreeData::DataPartPtr *>(part.data));
        const auto info = treeDataPart->info;
        debug_buf << info.getPartName() << ", " << info.min_block << "," << info.max_block << ", " << info.level <<
                ", " << info.mutation << ", should merge:" << part.shall_participate_in_merges << std::endl;
    }
    LOG_DEBUG(log, "\n----- {} \n {}---------\n", prefix, debug_buf.str());
}

MemoryMergeSelector::PartsRange MemoryMergeSelector::select(
    const PartsRanges & parts_ranges,
    const size_t max_total_size_to_merge)
{
    Estimator estimator;

    int idx = parts_ranges.size() -1 ;
    bool printDebug = false;
    while (idx >=0)
    {
        auto & parts_range = parts_ranges[idx];
        const MergeTreeData::DataPartPtr & part = *(static_cast<const MergeTreeData::DataPartPtr *>(parts_range.back().data));
        /// InMemory parts lie in the right side of the list.
        if(part->getType() != MergeTreeDataPartType::IN_MEMORY)
            break;

        if (part->storage.getStorageID().table_name.ends_with("t_num"))
            printDebug = true;

        if(printDebug)
            dumpPartsInfo(parts_range, "candidate segments: ");

        selectWithinPartition(parts_range, max_total_size_to_merge, estimator, settings);


        --idx;
    }

    if(printDebug)
        dumpPartsInfo(estimator.getBest(), "selected part range: ");

    return estimator.getBest();
}

}
