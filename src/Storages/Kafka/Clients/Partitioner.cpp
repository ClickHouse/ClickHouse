#include <Storages/Kafka/Clients/Partitioner.h>

#include <Storages/Kafka/Clients/Error.h>

#include <Common/thread_local_rng.h>

#include <cstring>
#include <random>

namespace Kafka
{

UInt32 murmur2(std::string_view data)
{
    /// Transcribed from rd_murmur2 in contrib/librdkafka/src/rdmurmur2.c,
    /// itself MurmurHashNeutral2 by Austin Appleby. Bit-compatible with the
    /// Java client's Utils.murmur2, which uses this exact seed and constants.
    constexpr UInt32 m = 0x5bd1e995;
    constexpr UInt32 r = 24;

    UInt32 h = 0x9747b28c ^ static_cast<UInt32>(data.size());

    const auto * bytes = reinterpret_cast<const UInt8 *>(data.data());
    size_t len = data.size();

    while (len >= 4)
    {
        /// A 4-byte little-endian chunk. All platforms ClickHouse supports are
        /// little-endian, so a plain memcpy load matches the reference's
        /// htole32 path (and its byte-assembling unaligned path).
        UInt32 k;
        std::memcpy(&k, bytes, 4);

        k *= m;
        k ^= k >> r;
        k *= m;
        h *= m;
        h ^= k;

        bytes += 4;
        len -= 4;
    }

    /// The remaining sub-word.
    switch (len)
    {
        case 3:
            h ^= static_cast<UInt32>(bytes[2]) << 16;
            [[fallthrough]];
        case 2:
            h ^= static_cast<UInt32>(bytes[1]) << 8;
            [[fallthrough]];
        case 1:
            h ^= static_cast<UInt32>(bytes[0]);
            h *= m;
    }

    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;

    return h;
}

Int32 partitionForKey(std::string_view key, Int32 partition_count)
{
    if (partition_count <= 0)
        throw Exception(ErrorKind::Config, "Partition count must be positive, got {}", partition_count);

    /// Java's Utils.toPositive: clear the sign bit, then take the modulo.
    return static_cast<Int32>(murmur2(key) & 0x7fffffff) % partition_count;
}

Int32 StickyPartitioner::partition(Int32 partition_count)
{
    if (partition_count <= 0)
        throw Exception(ErrorKind::Config, "Partition count must be positive, got {}", partition_count);

    if (current < 0 || partition_count != known_partition_count)
    {
        current = std::uniform_int_distribution<Int32>(0, partition_count - 1)(thread_local_rng);
        known_partition_count = partition_count;
    }

    return current;
}

void StickyPartitioner::reset()
{
    /// Keep known_partition_count: a reset followed by a call with the same
    /// count must still pick a fresh partition.
    current = -1;
}

}
