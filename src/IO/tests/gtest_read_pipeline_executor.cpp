#include <gtest/gtest.h>

#include <IO/ReadPipeline.h>
#include <IO/PipelineReadBuffer.h>
#include <IO/ReadSettings.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <unistd.h>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using namespace DB;

namespace
{

unsigned char patternByte(size_t i)
{
    return static_cast<unsigned char>(i % 256);
}

class ReadPipelineExecutorTest : public ::testing::Test
{
protected:
    fs::path root;
    ObjectStoragePtr storage;

    void SetUp() override
    {
        root = fs::temp_directory_path() / ("ch_gtest_read_pipeline_executor_" + std::to_string(::getpid()));
        std::error_code ec;
        fs::remove_all(root, ec);
        fs::create_directories(root);
        storage = std::make_shared<LocalObjectStorage>(
            LocalObjectStorageSettings(/*disk_name_=*/"test_local", /*key_prefix_=*/root.string(), /*read_only_=*/false));
    }

    void TearDown() override
    {
        std::error_code ec;
        fs::remove_all(root, ec);
    }

    /// `LocalObjectStorage::readObject` opens `remote_path` directly, so use the
    /// full file path as the object key.
    StoredObject writeObject(const std::string & name, size_t size)
    {
        auto path = (root / name).string();
        std::ofstream f(path, std::ios::binary);
        for (size_t i = 0; i < size; ++i)
            f.put(static_cast<char>(patternByte(i)));
        f.close();
        return StoredObject(path, path, size);
    }

    std::unique_ptr<ReadBufferFromFileBase> build(const StoredObject & obj)
    {
        ReadSettings settings;
        settings.reader_executor.enabled = true;
        ReadPipeline pipeline;
        pipeline.setSource(storage, StoredObjects{obj}, settings);
        return pipeline.build();
    }
};

TEST_F(ReadPipelineExecutorTest, KnownSizeObjectRoutesThroughExecutor)
{
    auto buf = build(writeObject("data.bin", 1024));

    /// The executor branch must be taken for a known-size object-storage read.
    ASSERT_NE(dynamic_cast<PipelineReadBuffer *>(buf.get()), nullptr);

    std::vector<char> data(1024);
    buf->readStrict(data.data(), data.size());
    for (size_t i = 0; i < data.size(); ++i)
        ASSERT_EQ(static_cast<unsigned char>(data[i]), patternByte(i)) << "at offset " << i;
}

TEST_F(ReadPipelineExecutorTest, UnknownOrEmptySizeObjectFallsBack)
{
    /// `bytes_size == 0` (unknown size or empty object) must not use the executor.
    auto buf = build(writeObject("empty.bin", 0));
    EXPECT_EQ(dynamic_cast<PipelineReadBuffer *>(buf.get()), nullptr);
}

}
