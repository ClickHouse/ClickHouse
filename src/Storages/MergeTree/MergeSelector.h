#pragma once

#include <cstddef>
#include <ctime>
#include <vector>
#include <functional>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/** Interface of algorithm to select data parts to merge
 *   (merge is also known as "compaction").
  * Following properties depend on it:
  *
  * 1. Number of data parts at some moment in time.
  *    If parts are merged frequently, then data will be represented by lower number of parts, in average,
  *     but with cost of higher write amplification.
  *
  * 2. Write amplification ratio: how much times, on average, source data was written
  *     (during initial writes and followed merges).
  *
  * Number of parallel merges are controlled outside of scope of this interface.
  */
class IMergeSelector
{
public:
    /// Information about data part relevant to merge selecting strategy.
    struct Part
    {
        /// Size of data part in bytes.
        size_t size = 0;

        /// How old this data part in seconds.
        time_t age = 0;

        /// Depth of tree of merges by which this part was created. New parts has zero level.
        unsigned level = 0;

        /// Opaque pointer to avoid dependencies (it is not possible to do forward declaration of typedef).
        const void * data = nullptr;

        /// Information about different TTLs for part. Can be used by
        /// TTLSelector to assign merges with TTL.
        const MergeTreeDataPartTTLInfos * ttl_infos = nullptr;

        /// Part compression codec definition.
        ASTPtr compression_codec_desc;

        bool shall_participate_in_merges = true;
    };

    /// Parts are belong to partitions. Only parts within same partition could be merged.
    using PartsRange = std::vector<Part>;

    /// Parts are in some specific order. Parts could be merged only in contiguous ranges.
    using PartsRanges = std::vector<PartsRange>;

    /** Function could be called at any frequency and it must decide, should you do any merge at all.
      * If better not to do any merge, it returns empty result.
      */
    virtual PartsRange select(
        const PartsRanges & parts_ranges,
        size_t max_total_size_to_merge) = 0;

    virtual ~IMergeSelector() = default;
};

}
