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
        else if(current_len == part_count){
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
    size_t parts_size = parts.size();
    if (parts_size < settings.min_parts_to_merge)
        return;

    size_t range_end = parts_size;
    int range_begin = range_end - 1;

    size_t sum_size = 0;
    while (range_begin >= 0)
    {
        const MergeTreeData::DataPartPtr part = *(static_cast<const MergeTreeData::DataPartPtr *>(parts[range_begin].data));

        if (part->getType() != MergeTreeDataPartType::IN_MEMORY)
            break;

        sum_size  += parts[range_begin].size;
        if (range_end - range_begin <= settings.max_parts_to_merge && sum_size <= max_total_size_to_merge)
            --range_begin;
        else
            break;
    }
    estimator.consider(parts.begin() + range_begin, parts.begin() + range_end, sum_size);


}
}

void MemoryMergeSelector::dumpPartsInfo(const MemoryMergeSelector::PartsRange & parts, String prefix)
{
    std::ostringstream debug_buf;
    for (const IMergeSelector::Part & part : parts)
    {
        const MergeTreeData::DataPartPtr treeDataPart = *(static_cast<const MergeTreeData::DataPartPtr *>(part.data));
        const auto info = treeDataPart->info;
        debug_buf << info.partition_id << ", " << info.min_block << "," << info.max_block << ", " << info.level << ", " << info.mutation
                  << std::endl;
    }
    LOG_DEBUG(log, "------------{} part info: \n {}", prefix, debug_buf.str());
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
        const auto & parts_range = parts_ranges[idx];
        const MergeTreeData::DataPartPtr & part = *(static_cast<const MergeTreeData::DataPartPtr *>(parts_range.back().data));
        if(part->getType() != MergeTreeDataPartType::IN_MEMORY) break;

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
