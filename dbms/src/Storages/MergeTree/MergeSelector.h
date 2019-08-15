#pragma once

#include <cstddef>
#include <ctime>
#include <vector>
#include <functional>


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
        size_t size;

        /// How old this data part in seconds.
        time_t age;

        /// Depth of tree of merges by which this part was created. New parts has zero level.
        unsigned level;

        /// Opaque pointer to avoid dependencies (it is not possible to do forward declaration of typedef).
        const void * data;

        /// Minimal time, when we need to delete some data from this part
        time_t min_ttl;
    };

    /// Parts are belong to partitions. Only parts within same partition could be merged.
    using PartsInPartition = std::vector<Part>;

    /// Parts are in some specific order. Parts could be merged only in contiguous ranges.
    using Partitions = std::vector<PartsInPartition>;

    /** Function could be called at any frequency and it must decide, should you do any merge at all.
      * If better not to do any merge, it returns empty result.
      */
    virtual PartsInPartition select(
        const Partitions & partitions,
        const size_t max_total_size_to_merge) = 0;

    virtual ~IMergeSelector() {}
};

}
