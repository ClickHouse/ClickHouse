#include <Storages/Kafka/Clients/Protocol/Records.h>

#include <gtest/gtest.h>

#include <base/types.h>

using namespace Kafka::Protocol;
using Kafka::Bytes;
using Kafka::ErrorKind;
using Kafka::Exception;
using Kafka::RecordHeader;

TEST(KafkaClientRecords, RecordBatchRoundTripsWithASingleRecord)
{
    BatchRecord record;
    record.key = Bytes{"k"};
    record.value = Bytes{"v"};
    const RecordBatch batch = RecordBatch::make(0, {record});

    Writer w;
    batch.encode(w);
    Reader r(w.view());
    const RecordBatch got = RecordBatch::decode(r);
    EXPECT_EQ(got, batch);
    EXPECT_TRUE(r.empty());
}

TEST(KafkaClientRecords, RecordBatchRoundTripsWithHeadersNullsAndEmptyValues)
{
    BatchRecord first;
    first.offset_delta = 0;
    first.timestamp_delta = 0;
    first.key = std::nullopt;
    first.value = Bytes{"first"};
    first.headers = {{"h1", Bytes{"v1"}}};

    BatchRecord second;
    second.offset_delta = 1;
    second.timestamp_delta = 100;
    second.key = Bytes{"k2"};
    second.value = std::nullopt;
    second.headers = {{"h2", std::nullopt}, {"h3", Bytes{}}};

    const RecordBatch batch = RecordBatch::make(42, {first, second});
    EXPECT_EQ(batch.last_offset_delta, 1);
    EXPECT_EQ(batch.max_timestamp, 100);
    EXPECT_EQ(batch.nextOffset(), 44);

    Writer w;
    batch.encode(w);
    Reader r(w.view());
    const RecordBatch got = RecordBatch::decode(r);
    EXPECT_EQ(got, batch);
}

TEST(KafkaClientRecords, CrcMismatchIsRejected)
{
    BatchRecord record;
    record.key = Bytes{"k"};
    record.value = Bytes{"v"};
    const RecordBatch batch = RecordBatch::make(0, {record});

    Writer w;
    batch.encode(w);
    String bytes = w.take();
    /// Flip a bit inside the CRC-covered region (after the 21-byte prefix:
    /// baseOffset 8 + batchLength 4 + partitionLeaderEpoch 4 + magic 1 + crc 4).
    bytes[21] ^= 0x01;
    Reader r(bytes);
    try
    {
        RecordBatch::decode(r);
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Protocol);
        EXPECT_TRUE(String(e.message()).contains("CRC mismatch"));
    }
}

TEST(KafkaClientRecords, NonV2MagicIsRejectedAsNotSupported)
{
    const RecordBatch batch = RecordBatch::make(0, {BatchRecord{}});
    Writer w;
    batch.encode(w);
    String bytes = w.take();
    /// Magic sits after baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4).
    bytes[16] = 1;
    Reader r(bytes);
    try
    {
        RecordBatch::decode(r);
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::NotSupported);
    }
}

TEST(KafkaClientRecords, CompressedBatchesAreRejectedWithTheCodecName)
{
    RecordBatch batch = RecordBatch::make(0, {BatchRecord{}});
    batch.attributes = 4;  // zstd — encode writes the bits verbatim, records stay raw
    Writer w;
    batch.encode(w);
    Reader r(w.view());
    try
    {
        RecordBatch::decode(r);
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::NotSupported);
        EXPECT_TRUE(String(e.message()).contains("zstd"));
    }
}

TEST(KafkaClientRecords, ControlBatchFlagIsExposed)
{
    RecordBatch batch = RecordBatch::make(7, {BatchRecord{}});
    batch.attributes = 0x20;
    Writer w;
    batch.encode(w);
    Reader r(w.view());
    const RecordBatch got = RecordBatch::decode(r);
    EXPECT_TRUE(got.isControl());
    EXPECT_EQ(got.nextOffset(), 8);
}

TEST(KafkaClientRecords, MultiBatchBlobRoundTrips)
{
    const RecordBatch b1 = RecordBatch::make(0, {BatchRecord{}});
    BatchRecord r2;
    r2.value = Bytes{"x"};
    const RecordBatch b2 = RecordBatch::make(10, {r2});

    const String blob = encodeBatches({b1, b2});
    const DecodedBatches got = decodeBatches(blob);
    EXPECT_FALSE(got.truncated);
    ASSERT_EQ(got.batches.size(), 2);
    EXPECT_EQ(got.batches[0], b1);
    EXPECT_EQ(got.batches[1], b2);
}

TEST(KafkaClientRecords, TruncatedTrailingBatchIsToleratedAndFlaggedCutInsideTheSecondBatchBody)
{
    BatchRecord rec;
    rec.value = Bytes{"payload-payload-payload"};
    const RecordBatch b1 = RecordBatch::make(0, {rec});
    const RecordBatch b2 = RecordBatch::make(1, {rec});
    String blob = encodeBatches({b1, b2});

    blob.resize(blob.size() - 5);
    const DecodedBatches got = decodeBatches(blob);
    EXPECT_TRUE(got.truncated);
    ASSERT_EQ(got.batches.size(), 1);
    EXPECT_EQ(got.batches[0], b1);
}

TEST(KafkaClientRecords, TruncatedTrailingBatchIsToleratedAndFlaggedCutInsideTheSecondBatchLengthPrefix)
{
    BatchRecord rec;
    rec.value = Bytes{"payload-payload-payload"};
    const RecordBatch b1 = RecordBatch::make(0, {rec});
    const RecordBatch b2 = RecordBatch::make(1, {rec});
    String blob = encodeBatches({b1, b2});

    const size_t first_size = encodeBatches({b1}).size();
    blob.resize(first_size + 6);
    const DecodedBatches got = decodeBatches(blob);
    EXPECT_TRUE(got.truncated);
    ASSERT_EQ(got.batches.size(), 1);
}

TEST(KafkaClientRecords, TruncatedTrailingBatchIsToleratedAndFlaggedEmptyBlobDecodesToNothing)
{
    BatchRecord rec;
    rec.value = Bytes{"payload-payload-payload"};
    const RecordBatch b1 = RecordBatch::make(0, {rec});
    const RecordBatch b2 = RecordBatch::make(1, {rec});
    String blob = encodeBatches({b1, b2});

    const DecodedBatches got = decodeBatches({});
    EXPECT_FALSE(got.truncated);
    EXPECT_TRUE(got.batches.empty());
}
