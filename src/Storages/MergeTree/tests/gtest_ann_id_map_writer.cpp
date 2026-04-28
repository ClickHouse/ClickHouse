#include <gtest/gtest.h>

#include <Storages/MergeTree/ANNIndex/PartRowId.h>
#include <Storages/MergeTree/ANNIndex/PartRowIdMap.h>
#include <Storages/MergeTree/ANNIndex/PartRowIdMapWriter.h>
#include <Storages/MergeTree/ANNIndex/IANNGroupStorage.h>

#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromFileBase.h>

#include <cstring>
#include <map>
#include <string>

using namespace DB;

static_assert(sizeof(PartRowId) == 24, "PartRowId on-disk layout must be 24 bytes");

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

TEST(PartRowIdMapWriterTest, EmptyWriterProducesHeaderAndFooter)
{
    using namespace PartRowIdMapFormat;

    MemGroupStorage storage;
    PartRowIdMapWriter writer;
    EXPECT_TRUE(writer.empty());
    EXPECT_EQ(writer.size(), 0u);

    writer.writeTo(storage, WriteSettings{});

    const auto & blob = storage.files.at("id_map.bin");
    ASSERT_EQ(blob.size(), HEADER_SIZE + FOOTER_SIZE);

    UInt32 magic = 0;
    std::memcpy(&magic, blob.data(), sizeof(magic));
    EXPECT_EQ(magic, MAGIC);

    UInt32 version = 0;
    std::memcpy(&version, blob.data() + 4, sizeof(version));
    EXPECT_EQ(version, VERSION);

    UInt32 record_size = 0;
    std::memcpy(&record_size, blob.data() + 8, sizeof(record_size));
    EXPECT_EQ(record_size, RECORD_SIZE);

    UInt32 hash_algo = 0;
    std::memcpy(&hash_algo, blob.data() + 12, sizeof(hash_algo));
    EXPECT_EQ(hash_algo, PARTITION_HASH_ALGO_SIPHASH64);

    UInt64 record_count = 1234;
    std::memcpy(&record_count, blob.data() + 16, sizeof(record_count));
    EXPECT_EQ(record_count, 0u);

    /// Footer: checksum = XXH64 of empty body; magic_end.
    UInt64 stored_checksum = 0;
    std::memcpy(&stored_checksum, blob.data() + HEADER_SIZE, sizeof(stored_checksum));
    const UInt64 expected_empty_checksum = PartRowIdMapFormat::hashBody(nullptr, 0);
    EXPECT_EQ(stored_checksum, expected_empty_checksum);

    UInt64 stored_magic_end = 0;
    std::memcpy(&stored_magic_end, blob.data() + HEADER_SIZE + 8, sizeof(stored_magic_end));
    EXPECT_EQ(stored_magic_end, MAGIC_END);
}

TEST(PartRowIdMapWriterTest, AppendAndWriteLayoutMatchesSpec)
{
    using namespace PartRowIdMapFormat;

    MemGroupStorage storage;
    PartRowIdMapWriter writer;
    writer.append(PartRowId{0x1111'2222'3333'4444ull, 10, 0});
    writer.append(PartRowId{0x1111'2222'3333'4444ull, 10, 1});
    writer.append(PartRowId{0x5555'6666'7777'8888ull, 42, 7});

    EXPECT_FALSE(writer.empty());
    EXPECT_EQ(writer.size(), 3u);

    writer.writeTo(storage, WriteSettings{});
    const auto & blob = storage.files.at("id_map.bin");

    const size_t expected_body = 3 * sizeof(PartRowId);
    ASSERT_EQ(blob.size(), HEADER_SIZE + expected_body + FOOTER_SIZE);

    /// Record count in header must reflect 3.
    UInt64 record_count = 0;
    std::memcpy(&record_count, blob.data() + 16, sizeof(record_count));
    EXPECT_EQ(record_count, 3u);

    /// Body bytes must equal the records verbatim.
    PartRowId read_back[3]{};
    std::memcpy(&read_back[0], blob.data() + HEADER_SIZE, expected_body);
    EXPECT_EQ(read_back[0], (PartRowId{0x1111'2222'3333'4444ull, 10, 0}));
    EXPECT_EQ(read_back[1], (PartRowId{0x1111'2222'3333'4444ull, 10, 1}));
    EXPECT_EQ(read_back[2], (PartRowId{0x5555'6666'7777'8888ull, 42, 7}));

    /// Checksum must be XXH64 over the body.
    UInt64 stored_checksum = 0;
    std::memcpy(&stored_checksum, blob.data() + HEADER_SIZE + expected_body, sizeof(stored_checksum));
    const UInt64 expected_checksum = PartRowIdMapFormat::hashBody(blob.data() + HEADER_SIZE, expected_body);
    EXPECT_EQ(stored_checksum, expected_checksum);

    /// Magic end marker.
    UInt64 stored_magic_end = 0;
    std::memcpy(&stored_magic_end, blob.data() + HEADER_SIZE + expected_body + 8,
                sizeof(stored_magic_end));
    EXPECT_EQ(stored_magic_end, MAGIC_END);
}
