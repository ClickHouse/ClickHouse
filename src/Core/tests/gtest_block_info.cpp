#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Core/BlockInfo.h>
#include <Core/ProtocolDefines.h>
#include <IO/ReadBufferFromString.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int INCORRECT_DATA;
}

namespace
{

/// A `BlockInfo` with only the `out_of_order_buckets` field, declaring `declared_count` bucket ids
/// and sending `sent_count` of them.
std::string outOfOrderBucketsWire(size_t declared_count, size_t sent_count)
{
    WriteBufferFromOwnString out;
    writeVarUInt(3, out);
    writeVarUInt(declared_count, out);
    for (size_t i = 0; i < sent_count; ++i)
        writeBinary(static_cast<Int32>(i), out);
    writeVarUInt(0, out);
    return out.str();
}

}

TEST(BlockInfo, OutOfOrderBucketsCountIsBounded)
{
    /// The count comes from the peer, and the vector would be resized to it before any bucket id
    /// arrives: a gigabyte of `Int32` is four gigabytes.
    const std::string wire = outOfOrderBucketsWire(1ULL << 30, 0);
    ReadBufferFromString in(wire);
    BlockInfo info;
    try
    {
        info.read(in, DBMS_MIN_REVISION_WITH_OUT_OF_ORDER_BUCKETS_IN_AGGREGATION);
        ADD_FAILURE() << "an out of order buckets count of a gigabyte was accepted";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::TOO_LARGE_ARRAY_SIZE) << e.displayText();
    }
}

TEST(BlockInfo, AllBucketIdsStillRoundTrip)
{
    /// A list cannot name more than the 256 buckets of two-level aggregation, and such a list is read.
    BlockInfo sent;
    for (Int32 i = 0; i < 256; ++i)
        sent.out_of_order_buckets.push_back(i);

    WriteBufferFromOwnString out;
    sent.write(out, DBMS_MIN_REVISION_WITH_OUT_OF_ORDER_BUCKETS_IN_AGGREGATION);
    const std::string wire = out.str();

    ReadBufferFromString in(wire);
    BlockInfo received;
    received.read(in, DBMS_MIN_REVISION_WITH_OUT_OF_ORDER_BUCKETS_IN_AGGREGATION);
    EXPECT_EQ(received.out_of_order_buckets, sent.out_of_order_buckets);
    EXPECT_TRUE(in.eof());
}

TEST(BlockInfo, BucketNumbersAreInRange)
{
    auto round_trip = [](Int32 bucket_num, Int32 out_of_order_bucket)
    {
        BlockInfo sent;
        sent.bucket_num = bucket_num;
        sent.out_of_order_buckets.push_back(out_of_order_bucket);

        WriteBufferFromOwnString out;
        sent.write(out, DBMS_MIN_REVISION_WITH_OUT_OF_ORDER_BUCKETS_IN_AGGREGATION);
        const std::string wire = out.str();

        ReadBufferFromString in(wire);
        BlockInfo received;
        received.read(in, DBMS_MIN_REVISION_WITH_OUT_OF_ORDER_BUCKETS_IN_AGGREGATION);
        EXPECT_EQ(received.bucket_num, bucket_num);
        EXPECT_EQ(received.out_of_order_buckets, sent.out_of_order_buckets);
    };

    /// -1 means that the block does not belong to a bucket of two-level aggregation, which has 256 buckets.
    round_trip(-1, 0);
    round_trip(0, 255);
    round_trip(255, 17);

    for (auto [bucket_num, out_of_order_bucket] : {std::pair{256, 0}, {-2, 0}, {0, -1}, {0, 256}})
    {
        try
        {
            round_trip(bucket_num, out_of_order_bucket);
            ADD_FAILURE() << "bucket numbers " << bucket_num << " and " << out_of_order_bucket << " were accepted";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA) << e.displayText();
        }
    }
}
