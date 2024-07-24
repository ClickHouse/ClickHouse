#include <gtest/gtest.h>

#include <Disks/DiskLocal.h>
#include <Disks/ObjectStorages/VFS/JSONSerializer.h>
#include <Disks/ObjectStorages/VFS/VFSLog.h>

#include <Poco/Checksum.h>

#include <filesystem>

using namespace DB;
namespace fs = std::filesystem;

const fs::path log_dir = "tmp/";
const String remote_path = "/cloud_storage/remote/path";
const String local_path = "/table/path/to/part/file";


class VFSLogTest : public ::testing::Test
{
protected:
    void SetUp() override { fs::remove_all(log_dir); }
};

TEST_F(VFSLogTest, Serialization)
{
    {
        VFSEvent event_in{remote_path, local_path, {}, Poco::Timestamp(), VFSAction::LINK};

        auto buf = JSONSerializer::serialize(event_in);
        VFSEvent event_out = JSONSerializer::deserialize(buf);

        EXPECT_EQ(event_in, event_out);
    }

    {
        VFSEvent event_in{remote_path, local_path, WALInfo{"src_replica", UUIDHelpers::generateV4(), 1}, Poco::Timestamp(), VFSAction::REQUEST};

        auto buf = JSONSerializer::serialize(event_in);
        VFSEvent event_out = JSONSerializer::deserialize(buf);

        EXPECT_EQ(event_in, event_out);
    }
}

TEST_F(VFSLogTest, Main)
{
    DB::VFSLog vfs_log(log_dir);
    VFSEvent event{remote_path, local_path, {}, Poco::Timestamp(), VFSAction::LINK};

    vfs_log.write(event);
    EXPECT_EQ(vfs_log.size(), 1);

    VFSLogItems items = vfs_log.read(1);
    EXPECT_EQ(items.size(), 1);
    EXPECT_EQ(items[0].wal.index, 0);

    vfs_log.dropUpTo(1);
    EXPECT_EQ(vfs_log.size(), 0);
}
