#include <gtest/gtest.h>

#include <Disks/ObjectStorages/VFS/AppendLog.h>
#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <Poco/Checksum.h>

#include <filesystem>

namespace fs = std::filesystem;

constexpr char log_dir[] = "tmp";
const std::vector<char> msg {'T', 'e', 's', 't'};
const std::vector<char> msg1 {'T', 'e', 's', 't', '1'};
const std::vector<char> msg2 {'T', 'e', 's', 't', '2'};
const std::vector<char> msg3 {'T', 'e', 's', 't', '3'};


class AppendLogTest : public ::testing::Test
{
protected:
    void SetUp() override { fs::remove_all(log_dir); }
    void TearDown() override { SetUp(); }
};


TEST_F(AppendLogTest, Main)
{
    DB::AppendLog alog(log_dir);

    auto idx = alog.append("test message");
    EXPECT_EQ(idx, 0);

    idx = alog.append(msg);
    EXPECT_EQ(idx, 1);
    EXPECT_EQ(alog.size(), 2);
}

TEST_F(AppendLogTest, TestEntryChecksum)
{
    DB::AppendLog alog(log_dir);

    UInt64 expected_index = 0;
    Poco::Checksum crc(Poco::Checksum::Type::TYPE_CRC32);
    crc.update(reinterpret_cast<const char*>(&expected_index), sizeof(expected_index));
    crc.update(msg.data(), static_cast<unsigned int>(msg.size()));

    alog.append(msg);
    auto entries = alog.readFront(1);
    EXPECT_EQ(entries[0].checksum, crc.checksum());
}

TEST_F(AppendLogTest, TestLastIndex)
{   
    {
        DB::AppendLog alog(log_dir);

        alog.append(msg);
        alog.append(msg);
        alog.append(msg);

        ASSERT_EQ(alog.getNextIndex(), 3);
    }    

    {
        DB::AppendLog alog(log_dir);
        alog.append(msg);

        ASSERT_EQ(alog.getNextIndex(), 4);
    }   
}

TEST_F(AppendLogTest, TestReadFront)
{
    DB::AppendLog alog(log_dir);

    alog.append(msg);
    std::vector<DB::LogEntry> entry = alog.readFront(1);

    EXPECT_EQ(entry.size(), 1);
    EXPECT_EQ(entry[0].index, 0);
    EXPECT_EQ(entry[0].data, msg);
}

TEST_F(AppendLogTest, TestReadFrontMultiple)
{
    DB::AppendLog alog(log_dir);

    alog.append(msg1);
    alog.append(msg2);

    {
        std::vector<DB::LogEntry> entries = alog.readFront(1);
        EXPECT_EQ(entries[0].data, msg1);
        EXPECT_EQ(entries[0].index, 0);
    }

    {
        std::vector<DB::LogEntry> entries = alog.readFront(2);
        EXPECT_EQ(entries[0].data, msg1);
        EXPECT_EQ(entries[0].index, 0);
        EXPECT_EQ(entries[1].data, msg2);
        EXPECT_EQ(entries[1].index, 1);
    }

    {
        std::vector<DB::LogEntry> entries = alog.readFront(3);
        EXPECT_EQ(entries.size(), 2);
    }
}

TEST_F(AppendLogTest, TestDrop)
{
    {
        DB::AppendLog alog(log_dir);

        alog.append(msg1);
        alog.append(msg2);
        size_t dropped = alog.dropUpTo(1);

        EXPECT_EQ(dropped, 1);
        EXPECT_EQ(alog.size(), 1);
        EXPECT_EQ(alog.getNextIndex(), 2);
    }

    {
        DB::AppendLog alog(log_dir);

        EXPECT_EQ(alog.size(), 1);
        EXPECT_EQ(alog.getNextIndex(), 2);
    }
}

TEST_F(AppendLogTest, TestReadAfterDrop)
{
    DB::AppendLog alog(log_dir);
    alog.append(msg1);
    alog.append(msg2);

    size_t dropped = alog.dropUpTo(1);
    EXPECT_EQ(dropped, 1);
    EXPECT_EQ(alog.size(), 1);

    std::vector<DB::LogEntry> entries = alog.readFront(1);
    EXPECT_EQ(entries.size(), 1);
    EXPECT_EQ(entries[0].index, 1);    

    alog.dropUpTo(2);
    entries = alog.readFront(1);
    EXPECT_EQ(entries.size(), 0);
    EXPECT_EQ(alog.size(), 0);
}
