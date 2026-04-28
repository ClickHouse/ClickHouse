#include <gtest/gtest.h>

#include <Storages/MergeTree/ANNIndex/PartRowId.h>
#include <Storages/MergeTree/ANNIndex/PartRowIdMap.h>
#include <Storages/MergeTree/ANNIndex/PartRowIdMapReader.h>
#include <Storages/MergeTree/ANNIndex/IANNGroupStorage.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromFileBase.h>

#include <cstring>
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

/// Build a well-formed `id_map.bin` blob for the given records.
std::string buildIdMapBlob(const std::vector<PartRowId> & records,
                           UInt32 magic = PartRowIdMapFormat::MAGIC,
                           UInt32 version = PartRowIdMapFormat::VERSION,
                           UInt32 record_size = PartRowIdMapFormat::RECORD_SIZE,
                           UInt32 hash_algo = PartRowIdMapFormat::PARTITION_HASH_ALGO_SIPHASH64)
{
    using namespace PartRowIdMapFormat;
    std::string blob;
    blob.resize(HEADER_SIZE);

    std::memcpy(blob.data() + 0,  &magic,        sizeof(magic));
    std::memcpy(blob.data() + 4,  &version,      sizeof(version));
    std::memcpy(blob.data() + 8,  &record_size,  sizeof(record_size));
    std::memcpy(blob.data() + 12, &hash_algo,    sizeof(hash_algo));
    const UInt64 record_count = records.size();
    std::memcpy(blob.data() + 16, &record_count, sizeof(record_count));
    const UInt64 reserved = 0;
    std::memcpy(blob.data() + 24, &reserved,     sizeof(reserved));

    const size_t body_bytes = records.size() * sizeof(PartRowId);
    const size_t body_offset = blob.size();
    blob.resize(body_offset + body_bytes);
    if (body_bytes > 0)
        std::memcpy(blob.data() + body_offset, records.data(), body_bytes);

    UInt64 checksum;
    if (body_bytes)
        checksum = PartRowIdMapFormat::hashBody(records.data(), body_bytes);
    else
        checksum = PartRowIdMapFormat::hashBody(nullptr, 0);

    const size_t footer_offset = blob.size();
    blob.resize(footer_offset + FOOTER_SIZE);
    std::memcpy(blob.data() + footer_offset,     &checksum,  sizeof(checksum));
    const UInt64 magic_end = MAGIC_END;
    std::memcpy(blob.data() + footer_offset + 8, &magic_end, sizeof(magic_end));

    return blob;
}

}

TEST(PartRowIdMapReaderTest, RoundTripLookupMatches)
{
    MemGroupStorage storage;
    std::vector<PartRowId> records = {
        {0xAAAA, 0, 0},
        {0xAAAA, 0, 1},
        {0xBBBB, 7, 0},
        {0xCCCC, 1000, 42},
    };
    storage.files["id_map.bin"] = buildIdMapBlob(records);

    PartRowIdMapReader reader;
    reader.loadFrom(storage, ReadSettings{});
    ASSERT_EQ(reader.size(), records.size());
    EXPECT_FALSE(reader.empty());
    for (UInt32 i = 0; i < records.size(); ++i)
        EXPECT_EQ(reader.lookup(i), records[i]);
}

TEST(PartRowIdMapReaderTest, EmptyFileIsValid)
{
    MemGroupStorage storage;
    storage.files["id_map.bin"] = buildIdMapBlob({});

    PartRowIdMapReader reader;
    reader.loadFrom(storage, ReadSettings{});
    EXPECT_TRUE(reader.empty());
    EXPECT_EQ(reader.size(), 0u);
}

TEST(PartRowIdMapReaderTest, BadMagicRejected)
{
    MemGroupStorage storage;
    storage.files["id_map.bin"] = buildIdMapBlob({{1, 2, 3}}, /*magic*/ 0xDEADBEEFu);

    PartRowIdMapReader reader;
    EXPECT_THROW(reader.loadFrom(storage, ReadSettings{}), DB::Exception);
}

TEST(PartRowIdMapReaderTest, BadVersionRejected)
{
    MemGroupStorage storage;
    storage.files["id_map.bin"] = buildIdMapBlob({{1, 2, 3}},
        PartRowIdMapFormat::MAGIC, /*version*/ 999u);

    PartRowIdMapReader reader;
    EXPECT_THROW(reader.loadFrom(storage, ReadSettings{}), DB::Exception);
}

TEST(PartRowIdMapReaderTest, BadRecordSizeRejected)
{
    MemGroupStorage storage;
    storage.files["id_map.bin"] = buildIdMapBlob({{1, 2, 3}},
        PartRowIdMapFormat::MAGIC, PartRowIdMapFormat::VERSION, /*record_size*/ 16u);

    PartRowIdMapReader reader;
    EXPECT_THROW(reader.loadFrom(storage, ReadSettings{}), DB::Exception);
}

TEST(PartRowIdMapReaderTest, ChecksumTamperingRejected)
{
    using namespace PartRowIdMapFormat;

    MemGroupStorage storage;
    std::vector<PartRowId> records = {{1, 2, 3}, {4, 5, 6}};
    storage.files["id_map.bin"] = buildIdMapBlob(records);

    /// Flip a single bit inside the body — checksum must catch it.
    storage.files["id_map.bin"][HEADER_SIZE + 1] ^= 0x08;

    PartRowIdMapReader reader;
    EXPECT_THROW(reader.loadFrom(storage, ReadSettings{}), DB::Exception);
}

TEST(PartRowIdMapReaderTest, BadMagicEndRejected)
{
    using namespace PartRowIdMapFormat;

    MemGroupStorage storage;
    std::vector<PartRowId> records = {{9, 9, 9}};
    storage.files["id_map.bin"] = buildIdMapBlob(records);

    /// Corrupt the magic_end without changing checksum.
    auto & blob = storage.files["id_map.bin"];
    ASSERT_GE(blob.size(), FOOTER_SIZE);
    blob[blob.size() - 1] ^= 0x01;

    PartRowIdMapReader reader;
    EXPECT_THROW(reader.loadFrom(storage, ReadSettings{}), DB::Exception);
}

TEST(PartRowIdMapReaderTest, TooSmallFileRejected)
{
    MemGroupStorage storage;
    storage.files["id_map.bin"] = std::string(8, '\0');

    PartRowIdMapReader reader;
    EXPECT_THROW(reader.loadFrom(storage, ReadSettings{}), DB::Exception);
}
