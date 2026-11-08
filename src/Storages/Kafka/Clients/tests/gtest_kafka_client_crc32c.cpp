#include <Storages/Kafka/Clients/Protocol/Crc32c.h>

#include <gtest/gtest.h>

#include <base/types.h>

using Kafka::Protocol::crc32c;

TEST(KafkaClientCrc32c, Crc32cKnownVectors)
{
    // "123456789" is the canonical CRC32C check value.
    EXPECT_EQ(crc32c(""), 0x00000000);
    EXPECT_EQ(crc32c("123456789"), 0xE3069283);
}

TEST(KafkaClientCrc32c, Crc32cIsOrderDependent)
{
    EXPECT_NE(crc32c("abc"), crc32c("cba"));
}

TEST(KafkaClientCrc32c, Crc32cHandlesEmbeddedZeroBytes)
{
    const String with_zeros("a\0b\0c", 5);
    const String without_zeros("abc", 3);
    EXPECT_NE(crc32c(with_zeros), crc32c(without_zeros));
}
