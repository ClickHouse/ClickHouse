#include <Interpreters/AdaptiveAggregation/Combining/AdaptiveTailsGate.h>

#include <base/defines.h>

namespace DB
{

void AdaptiveTailsGate::watchClosedBatch(const UInt64 * hashes, size_t records)
{
    chassert(!open);

    /// One record in `partitionsPerBucket` goes to a watched partition, and the first record of each watched
    /// partition has none to compare with, so this many records give about `watch_window` comparisons.
    const size_t watched_records = (watch_window + watched_hashes.size()) * layout.partitionsPerBucket();
    const size_t begin = records - std::min(records, watched_records);
    /// The records before `begin` go unseen, so the comparisons start anew. A batch that is watched from its
    /// first record continues them: that record follows the last record of the batch before it.
    if (begin != 0)
        watched_seen.reset();

    size_t watched = 0;
    size_t repeats = 0;
    for (size_t i = begin; i < records; ++i)
    {
        const UInt64 hash = hashes[i];
        const size_t partition = layout.partitionOf(hash);
        if (layout.subOf(partition) != 0)
            continue;
        const size_t bucket = layout.bucketOf(partition);
        if (watched_seen[bucket])
        {
            ++watched;
            repeats += watched_hashes[bucket] == hash;
        }
        watched_seen.set(bucket);
        watched_hashes[bucket] = hash;
    }

    lookups += watched;
    hits += repeats;
    if (lookups >= watch_window)
        endWindow();
}

}
