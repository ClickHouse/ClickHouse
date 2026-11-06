#include <Storages/Kafka/Clients/Protocol/Compression.h>
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

TEST(KafkaClientRecords, CompressedBatchRoundTripsPerCodec)
{
    BatchRecord first;
    first.key = Bytes{"k"};
    first.value = Bytes{"the quick brown fox jumps over the lazy dog, repeatedly and compressibly: aaaaaaaaaaaaaaaaaaaaaaaa"};
    BatchRecord second;
    second.offset_delta = 1;
    second.key = std::nullopt;
    second.value = Bytes{"another value"};
    const RecordBatch batch = RecordBatch::make(7, {first, second});

    std::vector<CompressionType> codecs{CompressionType::Gzip, CompressionType::Lz4, CompressionType::Zstd};
    if (compressionSupported(CompressionType::Snappy))
        codecs.push_back(CompressionType::Snappy);

    for (const auto codec : codecs)
    {
        Writer w;
        batch.encode(w, codec);
        Reader r(w.view());
        const RecordBatch got = RecordBatch::decode(r);
        EXPECT_TRUE(r.empty()) << compressionTypeName(codec);
        EXPECT_EQ(got.compressionType(), codec);
        EXPECT_EQ(got.records, batch.records) << compressionTypeName(codec);
        EXPECT_EQ(got.base_offset, batch.base_offset);
        EXPECT_EQ(got.last_offset_delta, batch.last_offset_delta);
    }
}

namespace
{

/// A data or control batch for the read_committed assembly tests.
RecordBatch makeTransactionalBatch(Int64 base_offset, Int64 producer_id, bool transactional, size_t record_count)
{
    std::vector<BatchRecord> records;
    for (size_t i = 0; i < record_count; ++i)
    {
        BatchRecord record;
        record.offset_delta = static_cast<Int32>(i);
        record.value = Bytes{fmt::format("value-{}", base_offset + static_cast<Int64>(i))};
        records.push_back(std::move(record));
    }
    RecordBatch batch = RecordBatch::make(base_offset, std::move(records));
    batch.producer_id = producer_id;
    if (transactional)
        batch.attributes |= 0x10;
    return batch;
}

/// A control batch with one transaction marker: type 0 = ABORT, 1 = COMMIT.
RecordBatch makeControlBatch(Int64 base_offset, Int64 producer_id, Int16 marker_type)
{
    BatchRecord marker;
    /// Control record key: version Int16 (0), type Int16, big-endian.
    Bytes key(4, '\0');
    key[3] = static_cast<char>(marker_type);
    marker.key = std::move(key);
    RecordBatch batch = RecordBatch::make(base_offset, {marker});
    batch.producer_id = producer_id;
    batch.attributes |= 0x10 | 0x20;
    return batch;
}

std::vector<Int64> offsetsOf(const AssembledRecords & assembled)
{
    std::vector<Int64> out;
    for (const auto & record : assembled.records)
        out.push_back(record.offset);
    return out;
}

}

TEST(KafkaClientRecords, AssembleFiltersAbortedTransactionsUnderReadCommitted)
{
    /// Producer 7: an aborted transaction (offsets 0-1 + ABORT marker at 2),
    /// then a committed one (offsets 3-4 + COMMIT marker at 5).
    const String blob = encodeBatches(
        {makeTransactionalBatch(0, 7, true, 2),
         makeControlBatch(2, 7, 0),
         makeTransactionalBatch(3, 7, true, 2),
         makeControlBatch(5, 7, 1)});

    const auto read_committed = assembleFetchedRecords(blob, 0, true, {{.producer_id = 7, .first_offset = 0}});
    EXPECT_EQ(offsetsOf(read_committed), (std::vector<Int64>{3, 4}));
    EXPECT_EQ(read_committed.next_offset, 6);
    EXPECT_FALSE(read_committed.truncated);

    /// The same blob under read_uncommitted returns everything but the markers.
    const auto read_uncommitted = assembleFetchedRecords(blob, 0, false, {});
    EXPECT_EQ(offsetsOf(read_uncommitted), (std::vector<Int64>{0, 1, 3, 4}));
    EXPECT_EQ(read_uncommitted.next_offset, 6);
}

TEST(KafkaClientRecords, AssembleKeepsNonTransactionalRecordsOfAnAbortedProducer)
{
    /// The aborted range applies only to batches with the transactional bit.
    const String blob = encodeBatches({makeTransactionalBatch(0, 7, false, 2), makeControlBatch(2, 7, 0)});
    const auto assembled = assembleFetchedRecords(blob, 0, true, {{.producer_id = 7, .first_offset = 0}});
    EXPECT_EQ(offsetsOf(assembled), (std::vector<Int64>{0, 1}));
    EXPECT_EQ(assembled.next_offset, 3);
}

TEST(KafkaClientRecords, AssembleDropsTrailingAbortedRecordsWithoutAMarker)
{
    /// The ABORT marker may be past the fetched range: the records still vanish.
    const String blob = encodeBatches({makeTransactionalBatch(10, 7, true, 3)});
    const auto assembled = assembleFetchedRecords(blob, 10, true, {{.producer_id = 7, .first_offset = 10}});
    EXPECT_TRUE(assembled.records.empty());
    EXPECT_EQ(assembled.next_offset, 13);
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
