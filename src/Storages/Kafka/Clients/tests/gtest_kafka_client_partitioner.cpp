#include <Storages/Kafka/Clients/Partitioner.h>

#include <Storages/Kafka/Clients/Error.h>

#include <gtest/gtest.h>

#include <base/types.h>

#include <string_view>

using Kafka::murmur2;
using Kafka::partitionForKey;
using Kafka::StickyPartitioner;

TEST(KafkaClientPartitioner, Murmur2GoldenVectors)
{
    /// Golden vectors from unittest_murmur2 in
    /// contrib/librdkafka/src/rdmurmur2.c:125-155, which match the Apache
    /// Kafka Java client's Utils.murmur2. The contrib table stores the *raw*
    /// murmur2 value cast to int32_t - not the Java toPositive
    /// (& 0x7fffffff) value: the assert at rdmurmur2.c:161 compares
    /// `(int32_t)h`, and several entries (0xd067cf64, ...) have the sign bit
    /// set. So we compare against the full 32-bit hash. The contrib NULL entry
    /// equals the "" entry (both hash zero-length input), so the empty-string
    /// vector below covers it.
    EXPECT_EQ(murmur2("kafka"), 0xd067cf64u);
    EXPECT_EQ(murmur2("giberish123456789"), 0x8f552b0cu);
    /// "1234" at offsets 0..3: contrib uses these to exercise unaligned reads.
    EXPECT_EQ(murmur2("1234"), 0x9fc97b14u);
    EXPECT_EQ(murmur2("234"), 0xe7c009cau);
    EXPECT_EQ(murmur2("34"), 0x873930dau);
    EXPECT_EQ(murmur2("4"), 0x5a4b5ca1u);
    /// "PreAmbleWillBeRemoved,ThePrePartThatIs" at offsets 0..3, same idea.
    EXPECT_EQ(murmur2("PreAmbleWillBeRemoved,ThePrePartThatIs"), 0x78424f1cu);
    EXPECT_EQ(murmur2("reAmbleWillBeRemoved,ThePrePartThatIs"), 0x4a62b377u);
    EXPECT_EQ(murmur2("eAmbleWillBeRemoved,ThePrePartThatIs"), 0xe0e4e09eu);
    EXPECT_EQ(murmur2("AmbleWillBeRemoved,ThePrePartThatIs"), 0x62b8b43fu);
    EXPECT_EQ(murmur2(""), 0x106e08d9u);
}

TEST(KafkaClientPartitioner, PartitionForKeyIsStable)
{
    const Int32 first = partitionForKey("kafka", 12);
    for (int i = 0; i < 100; ++i)
        EXPECT_EQ(partitionForKey("kafka", 12), first);

    /// Known vector: murmur2("kafka") == 0xd067cf64 (rdmurmur2.c:143), so the
    /// partition at count 12 is toPositive(0xd067cf64) % 12.
    EXPECT_EQ(first, static_cast<Int32>(0xd067cf64u & 0x7fffffff) % 12);
    EXPECT_EQ(first, 4);
}

TEST(KafkaClientPartitioner, PartitionForKeyRange)
{
    const std::string_view keys[]
        = {"kafka",
           "giberish123456789",
           "",
           "4",
           std::string_view("a\0b\0c", 5),
           "PreAmbleWillBeRemoved,ThePrePartThatIs"};
    const Int32 counts[] = {1, 2, 3, 7, 12, 100};

    for (const auto & key : keys)
    {
        for (Int32 count : counts)
        {
            const Int32 partition = partitionForKey(key, count);
            EXPECT_GE(partition, 0);
            EXPECT_LT(partition, count);
        }
    }
}

TEST(KafkaClientPartitioner, StickyStaysUntilReset)
{
    StickyPartitioner sticky;

    const Int32 chosen = sticky.partition(16);
    EXPECT_GE(chosen, 0);
    EXPECT_LT(chosen, 16);
    for (int i = 0; i < 100; ++i)
        EXPECT_EQ(sticky.partition(16), chosen);

    /// After reset() a fresh partition is picked; it may legitimately be the
    /// same one, so assert only the range and the renewed stickiness.
    sticky.reset();
    const Int32 re_chosen = sticky.partition(16);
    EXPECT_GE(re_chosen, 0);
    EXPECT_LT(re_chosen, 16);
    for (int i = 0; i < 100; ++i)
        EXPECT_EQ(sticky.partition(16), re_chosen);

    /// A partition count change re-picks into the new range.
    const Int32 shrunk = sticky.partition(4);
    EXPECT_GE(shrunk, 0);
    EXPECT_LT(shrunk, 4);
    for (int i = 0; i < 100; ++i)
        EXPECT_EQ(sticky.partition(4), shrunk);
}

TEST(KafkaClientPartitioner, InvalidCountThrows)
{
    try
    {
        partitionForKey("key", 0);
        FAIL() << "Expected Kafka::Exception for partition count 0";
    }
    catch (const Kafka::Exception & e)
    {
        EXPECT_EQ(e.getKind(), Kafka::ErrorKind::Config);
    }

    try
    {
        StickyPartitioner sticky;
        sticky.partition(-1);
        FAIL() << "Expected Kafka::Exception for partition count -1";
    }
    catch (const Kafka::Exception & e)
    {
        EXPECT_EQ(e.getKind(), Kafka::ErrorKind::Config);
    }
}
