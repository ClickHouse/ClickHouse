#pragma once

#include <base/types.h>

#include <string_view>

namespace Kafka
{

/// MurmurHash2, bit-compatible with the Apache Kafka Java client's
/// Utils.murmur2 (and librdkafka's rd_murmur2): the hash behind the default
/// partitioner, so keyed records land on the same partitions as other clients.
UInt32 murmur2(std::string_view data);

/// Partition for a keyed record: Java's Utils.toPositive(murmur2(key)) % count.
Int32 partitionForKey(std::string_view key, Int32 partition_count);

/// Sticky choice for unkeyed records (the Java >= 2.4 behavior): all records
/// go to one random partition until the owner re-sticks, which batches far
/// better than round-robin. Not thread-safe, like the producers that use it.
class StickyPartitioner
{
public:
    /// The current sticky partition, picking one on first use or when the
    /// partition count changed.
    Int32 partition(Int32 partition_count);
    /// Pick a new random partition on the next call (e.g. after a batch).
    void reset();

private:
    Int32 current = -1;
    Int32 known_partition_count = 0;
};

}
