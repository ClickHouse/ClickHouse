#include <gtest/gtest.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Disks/tests/gtest_disk.h>
#include <atomic>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <unordered_set>
#include <unistd.h>

namespace fs = std::filesystem;

namespace
{

std::mutex created_paths_mutex;
std::unordered_set<std::string> created_paths;

std::string normalizePath(std::string path)
{
    while (path.size() > 1 && path.back() == '/')
        path.pop_back();
    return fs::weakly_canonical(path).string();
}

}

DB::DiskPtr createDisk(const std::string & name)
{
    static std::atomic<size_t> counter{0};

    auto path = fs::temp_directory_path()
        / ("clickhouse_gtest_disk_" + std::to_string(getpid()) + "_" + std::to_string(counter++) + "_" + name);

    /// The name is unique, so an existing directory belongs to something else. Fail instead of adopting
    /// it, because `destroyDisk` removes the directory recursively.
    if (!fs::create_directory(path))
        throw std::runtime_error("Directory for the test disk already exists: " + path.string());

    auto disk_path = path.string() + "/";
    {
        std::lock_guard lock(created_paths_mutex);
        created_paths.insert(normalizePath(disk_path));
    }

    return std::make_shared<DB::DiskLocal>("local_disk", disk_path);
}

void destroyDisk(DB::DiskPtr & disk)
{
    if (!disk)
        return;

    const auto path = disk->getPath();
    disk.reset();

    /// Remove only a directory this helper created.
    {
        std::lock_guard lock(created_paths_mutex);
        if (!created_paths.erase(normalizePath(path)))
            throw std::runtime_error("Refusing to remove a directory not created by createDisk: " + path);
    }

    fs::remove_all(path);
}

class DiskTest : public testing::Test
{
public:
    void SetUp() override { disk = createDisk(); }
    void TearDown() override { destroyDisk(disk); }

    DB::DiskPtr disk;
};


TEST_F(DiskTest, createDirectories)
{
    disk->createDirectories("test_dir1/");
    EXPECT_TRUE(disk->existsDirectory("test_dir1/"));

    disk->createDirectories("test_dir2/nested_dir/");
    EXPECT_TRUE(disk->existsDirectory("test_dir2/nested_dir/"));
}


TEST_F(DiskTest, writeFile)
{
    {
        std::unique_ptr<DB::WriteBuffer> out = disk->writeFile("test_file");
        writeString("test data", *out);
        out->finalize();
    }

    String data;
    {
        std::unique_ptr<DB::ReadBuffer> in = disk->readFile("test_file", DB::getReadSettings());
        readString(data, *in);
    }

    EXPECT_EQ("test data", data);
    EXPECT_EQ(data.size(), disk->getFileSize("test_file"));
}


TEST_F(DiskTest, readFile)
{
    {
        std::unique_ptr<DB::WriteBuffer> out = disk->writeFile("test_file");
        writeString("test data", *out);
        out->finalize();
    }

    auto read_settings = DB::getReadSettings();

    // Test SEEK_SET
    {
        String buf(4, '0');
        std::unique_ptr<DB::SeekableReadBuffer> in = disk->readFile("test_file", read_settings);

        in->seek(5, SEEK_SET);

        in->readStrict(buf.data(), 4);
        EXPECT_EQ("data", buf);
    }

    // Test SEEK_CUR
    {
        std::unique_ptr<DB::SeekableReadBuffer> in = disk->readFile("test_file", read_settings);
        String buf(4, '0');

        in->readStrict(buf.data(), 4);
        EXPECT_EQ("test", buf);

        // Skip whitespace
        in->seek(1, SEEK_CUR);

        in->readStrict(buf.data(), 4);
        EXPECT_EQ("data", buf);
    }
}


TEST_F(DiskTest, iterateDirectory)
{
    disk->createDirectories("test_dir/nested_dir/");

    {
        auto iter = disk->iterateDirectory("");
        EXPECT_TRUE(iter->isValid());
        EXPECT_EQ("test_dir/", iter->path());
        iter->next();
        EXPECT_FALSE(iter->isValid());
    }

    {
        auto iter = disk->iterateDirectory("test_dir/");
        EXPECT_TRUE(iter->isValid());
        EXPECT_EQ("test_dir/nested_dir/", iter->path());
        iter->next();
        EXPECT_FALSE(iter->isValid());
    }
}
