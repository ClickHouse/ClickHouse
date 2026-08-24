#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/PuffinDeletionVectorReader.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromString.h>

#include <vector>

using namespace DB;

TEST(PuffinNonSeekableBufferLimit, StopsBeforeExceedingAbsoluteLimit)
{
    constexpr size_t max_buffered_size = 100;
    std::vector<UInt8> out = {'P', 'F', 'A', '1'};

    /// Stream after the already-buffered header would push the total over the limit.
    const String rest(max_buffered_size, 'x');
    ReadBufferFromString buf(rest);

    EXPECT_THROW(appendReadBufferWithAbsoluteSizeLimit(buf, out, max_buffered_size), Exception);
    EXPECT_LE(out.size(), max_buffered_size);
}

TEST(PuffinNonSeekableBufferLimit, AcceptsInputWithinLimit)
{
    constexpr size_t max_buffered_size = 100;
    std::vector<UInt8> out = {'P', 'F', 'A', '1'};

    const String rest(50, 'y');
    ReadBufferFromString buf(rest);

    ASSERT_NO_THROW(appendReadBufferWithAbsoluteSizeLimit(buf, out, max_buffered_size));
    EXPECT_EQ(out.size(), 54u);
    EXPECT_EQ(out[0], 'P');
    EXPECT_EQ(out[4], 'y');
}

TEST(PuffinNonSeekableBufferLimit, RejectsAlreadyOversizedPrefix)
{
    std::vector<UInt8> out(101, 'z');
    ReadBufferFromString buf(String{});

    EXPECT_THROW(appendReadBufferWithAbsoluteSizeLimit(buf, out, /*max_buffered_size=*/100), Exception);
}

TEST(PuffinNonSeekableBufferLimit, ProductionCeilingCoversMaxDvPlusFooter)
{
    EXPECT_EQ(
        PUFFIN_NON_SEEKABLE_MAX_BUFFERED_SIZE,
        PUFFIN_MAGIC_SIZE + PUFFIN_DV_MAX_BLOB_SIZE + PUFFIN_MAGIC_SIZE + PUFFIN_FOOTER_MAX_PAYLOAD_SIZE
            + PUFFIN_FOOTER_TRAILER_SIZE);
}
