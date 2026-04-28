#include <gtest/gtest.h>

#include <Storages/MergeTree/ANNIndex/ANNGroupCoverage.h>
#include <Storages/MergeTree/ANNIndex/IANNGroupStorage.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromFileBase.h>

#include <map>
#include <string>

using namespace DB;

namespace
{

class InMemoryWriteBuffer final : public WriteBufferFromFileBase
{
public:
    InMemoryWriteBuffer(std::string * target_, std::string name_)
        : WriteBufferFromFileBase(4096, nullptr, 0)
        , target(target_)
        , name(std::move(name_))
    {
    }
    ~InMemoryWriteBuffer() override
    {
        try { finalize(); } catch (...) {}
    }
    std::string getFileName() const override { return name; }
    void sync() override {}

protected:
    void nextImpl() override
    {
        if (!offset())
            return;
        target->append(working_buffer.begin(), offset());
    }

private:
    std::string * target;
    std::string name;
};

class MemGroupStorage final : public IANNGroupStorage
{
public:
    std::string getFullPath() const override { return "/mem/group"; }
    std::string getRelativePath() const override { return "group"; }
    std::string getGroupDir() const override { return "group"; }
    bool exists() const override { return true; }
    bool existsFile(const std::string & name) const override { return files.contains(name); }
    size_t getFileSize(const std::string & name) const override { return files.at(name).size(); }
    Poco::Timestamp getFileLastModified(const std::string &) const override { return {}; }

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const std::string & name, const ReadSettings &, std::optional<size_t>) const override
    {
        return std::make_unique<ReadBufferFromOwnMemoryFile>(name, files.at(name));
    }

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const std::string & name, size_t, WriteMode mode, const WriteSettings &) override
    {
        if (mode == WriteMode::Rewrite)
            files[name].clear();
        return std::make_unique<InMemoryWriteBuffer>(&files[name], name);
    }

    void removeFileIfExists(const std::string & name) override { files.erase(name); }
    void renameFile(const std::string & from, const std::string & to) override
    {
        files[to] = std::move(files[from]);
        files.erase(from);
    }
    void renameDirectoryTo(const std::string &) override {}

    std::map<std::string, std::string> files;
};
}

TEST(ANNGroupCoverageTest, AddAndContainsSinglePartition)
{
    ANNGroupCoverage cov;
    cov.addPart(0xAABB, 10, 20);
    cov.addPart(0xAABB, 30, 40);

    EXPECT_FALSE(cov.empty());
    EXPECT_EQ(cov.partitionCount(), 1u);
    EXPECT_TRUE(cov.containsPart(0xAABB, 10, 20));
    EXPECT_TRUE(cov.containsPart(0xAABB, 15, 18));
    EXPECT_TRUE(cov.containsPart(0xAABB, 30, 40));
    EXPECT_FALSE(cov.containsPart(0xAABB, 18, 30));
    EXPECT_FALSE(cov.containsPart(0xAABB, 50, 60));
}

TEST(ANNGroupCoverageTest, IndependentPartitions)
{
    ANNGroupCoverage cov;
    cov.addPart(1, 0, 100);
    cov.addPart(2, 50, 150);

    EXPECT_EQ(cov.partitionCount(), 2u);
    EXPECT_TRUE(cov.containsPart(1, 10, 20));
    EXPECT_FALSE(cov.containsPart(1, 110, 120));
    EXPECT_TRUE(cov.containsPart(2, 100, 150));
    EXPECT_FALSE(cov.containsPart(3, 0, 0));
}

TEST(ANNGroupCoverageTest, WriteReadRoundTrip)
{
    MemGroupStorage storage;

    ANNGroupCoverage cov;
    cov.addPart(0x1111, 0, 10);
    cov.addPart(0x1111, 20, 30);
    cov.addPart(0x2222, 100, 200);
    cov.writeTo(storage);

    ANNGroupCoverage loaded;
    loaded.readFrom(storage);

    EXPECT_EQ(loaded.partitionCount(), cov.partitionCount());
    EXPECT_TRUE(loaded.containsPart(0x1111, 5, 5));
    EXPECT_TRUE(loaded.containsPart(0x1111, 20, 30));
    EXPECT_FALSE(loaded.containsPart(0x1111, 11, 19));
    EXPECT_TRUE(loaded.containsPart(0x2222, 100, 200));
}

TEST(ANNGroupCoverageTest, MagicCheckFails)
{
    MemGroupStorage storage;
    ANNGroupCoverage cov;
    cov.addPart(0xAA, 0, 5);
    cov.writeTo(storage);

    /// Corrupt the first byte of the magic.
    storage.files["coverage.bin"][0] = 'X';

    ANNGroupCoverage loaded;
    EXPECT_THROW(loaded.readFrom(storage), DB::Exception);
}

TEST(ANNGroupCoverageTest, ChecksumTamperingFails)
{
    MemGroupStorage storage;
    ANNGroupCoverage cov;
    cov.addPart(0xAA, 0, 5);
    cov.writeTo(storage);

    /// Flip a bit somewhere in the body (skip the 16-byte header).
    auto & blob = storage.files["coverage.bin"];
    ASSERT_GT(blob.size(), 20u);
    blob[20] ^= 0x01;

    ANNGroupCoverage loaded;
    EXPECT_THROW(loaded.readFrom(storage), DB::Exception);
}
