#include <gtest/gtest.h>

#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/VFS/AppendLog.h>
#include <Poco/Checksum.h>

#include <filesystem>

using namespace DB;
namespace fs = std::filesystem;

const fs::path log_dir = "tmp/";
const std::vector<char> msg{'T', 'e', 's', 't'};
const std::vector<char> msg1{'T', 'e', 's', 't', '1'};
const std::vector<char> msg2{'T', 'e', 's', 't', '2'};
const std::vector<char> msg3{'T', 'e', 's', 't', '3'};


class WALTest : public ::testing::Test
{
protected:
    void SetUp() override { fs::remove_all(log_dir); }
    //void TearDown() override { SetUp(); }
};


TEST_F(WALTest, Main)
{
    DB::WAL::AppendLog alog(log_dir);

    auto idx = alog.append("test message");
    EXPECT_EQ(idx, 0);

    idx = alog.append(msg);
    EXPECT_EQ(idx, 1);
    EXPECT_EQ(alog.size(), 2);
}

TEST_F(WALTest, TestEntryChecksum)
{
    WAL::AppendLog  alog(log_dir);

    UInt64 expected_index = 0;
    Poco::Checksum crc(Poco::Checksum::Type::TYPE_CRC32);
    crc.update(reinterpret_cast<const char *>(&expected_index), sizeof(expected_index));
    crc.update(msg.data(), static_cast<unsigned int>(msg.size()));

    alog.append(msg);
    auto entries = alog.readFront(1);
    EXPECT_EQ(entries[0].checksum, crc.checksum());
}

TEST_F(WALTest, TestLastIndex)
{
    {
        WAL::AppendLog  alog(log_dir);

        alog.append(msg);
        alog.append(msg);
        alog.append(msg);

        ASSERT_EQ(alog.getNextIndex(), 3);
    }

    {
        WAL::AppendLog  alog(log_dir);
        alog.append(msg);

        ASSERT_EQ(alog.getNextIndex(), 4);
    }
}

TEST_F(WALTest, TestReadFront)
{
    WAL::AppendLog  alog(log_dir);

    alog.append(msg);
    WAL::Entries entries = alog.readFront(1);

    EXPECT_EQ(entries.size(), 1);
    EXPECT_EQ(entries[0].index, 0);
    EXPECT_EQ(entries[0].data, msg);
}

TEST_F(WALTest, TestReadFrontMultiple)
{
    WAL::AppendLog  alog(log_dir);

    alog.append(msg1);
    alog.append(msg2);

    {
        WAL::Entries entries = alog.readFront(1);
        EXPECT_EQ(entries[0].data, msg1);
        EXPECT_EQ(entries[0].index, 0);
    }

    {
        WAL::Entries entries = alog.readFront(2);
        EXPECT_EQ(entries[0].data, msg1);
        EXPECT_EQ(entries[0].index, 0);
        EXPECT_EQ(entries[1].data, msg2);
        EXPECT_EQ(entries[1].index, 1);
    }

    {
        WAL::Entries entries = alog.readFront(3);
        EXPECT_EQ(entries.size(), 2);
    }
}

TEST_F(WALTest, TestDrop)
{
    {
        WAL::AppendLog  alog(log_dir);

        alog.append(msg1);
        alog.append(msg2);
        size_t dropped = alog.dropUpTo(1);

        EXPECT_EQ(dropped, 1);
        EXPECT_EQ(alog.size(), 1);
        EXPECT_EQ(alog.getNextIndex(), 2);
    }

    {
        WAL::AppendLog  alog(log_dir);

        EXPECT_EQ(alog.size(), 1);
        EXPECT_EQ(alog.getNextIndex(), 2);
    }
}

TEST_F(WALTest, TestReadAfterDrop)
{
    WAL::AppendLog  alog(log_dir);
    alog.append(msg1);
    alog.append(msg2);

    size_t dropped = alog.dropUpTo(1);
    EXPECT_EQ(dropped, 1);
    EXPECT_EQ(alog.size(), 1);

    WAL::Entries entries = alog.readFront(1);
    EXPECT_EQ(entries.size(), 1);
    EXPECT_EQ(entries[0].index, 1);

    alog.dropUpTo(2);
    entries = alog.readFront(1);
    EXPECT_EQ(entries.size(), 0);
    EXPECT_EQ(alog.size(), 0);
}

TEST_F(WALTest, TestSegmentRotation)
{
    WAL::AppendLog  alog(log_dir, WAL::Settings{.max_segment_size = 100});
    std::vector<char> buf_1(50, 0xAB);
    std::vector<char> buf_2(50, 0xCD);
    alog.append(buf_1);
    alog.append(buf_2);

    EXPECT_EQ(alog.segmentsCount(), 2);

    WAL::Entries entries = alog.readFront(2);
    EXPECT_EQ(entries.size(), 2);
    EXPECT_EQ(entries[0].data, buf_1);
    EXPECT_EQ(entries[0].index, 0);
    EXPECT_EQ(entries[1].data, buf_2);
    EXPECT_EQ(entries[1].index, 1);
}

TEST_F(WALTest, TestCorruptedException)
{
    fs::path segment_path = log_dir / "log_00000000000000000000";
    WAL::AppendLog  alog(log_dir, WAL::Settings{.max_segment_size = 100});
    alog.append(msg);

    // Break size byte in the segment file
    DB::WriteBufferFromFile buf(segment_path);
    buf.write(0x01);
    buf.sync();

    EXPECT_NO_THROW(alog.size());
    EXPECT_THROW(alog.readFront(1), DB::Exception);

    // Now all public methods fails
    EXPECT_THROW(alog.readFront(1), DB::Exception);
    EXPECT_THROW(alog.append(msg), DB::Exception);
    EXPECT_THROW(alog.size(), DB::Exception);
    EXPECT_THROW(alog.dropUpTo(1), DB::Exception);
    EXPECT_THROW(alog.getNextIndex(), DB::Exception);
    EXPECT_THROW(alog.getStartIndex(), DB::Exception);
    EXPECT_THROW(alog.segmentsCount(), DB::Exception);
}
