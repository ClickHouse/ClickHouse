#include <gtest/gtest.h>

#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <Core/Defines.h>
#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <IO/Archives/PackedArchiveWriter.h>
#include <IO/PackedFilesReader.h>
#include <IO/PackedFilesWriter.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBuffer.h>
#include <Common/Exception.h>
#include <Poco/TemporaryFile.h>

using namespace DB;

namespace
{

constexpr UInt8 VERSION = PackedFilesIO::VERSION_WITHOUT_UNCOMPRESSED_SIZE;

String readMember(const PackedFilesReader & reader, const DiskPtr & disk, const String & pack, const String & member)
{
    auto buf = reader.readFile(disk, pack, member, ReadSettings{}, std::nullopt);
    String result;
    readStringUntilEOF(result, *buf);
    return result;
}

/// Records `cancel` in a flag that outlives the buffer, since the writer owns and destroys it.
class CancelRecordingWriteBuffer : public WriteBuffer
{
public:
    explicit CancelRecordingWriteBuffer(bool & cancel_called_) : WriteBuffer(memory, sizeof(memory)), cancel_called(cancel_called_) {}

private:
    void nextImpl() override {}
    void cancelImpl() noexcept override { cancel_called = true; }

    char memory[16] = {};
    bool & cancel_called;
};

}

class PackedArchiveWriterTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        temp_dir = std::make_unique<Poco::TemporaryFile>();
        temp_dir->createDirectories();
        disk = std::make_shared<DiskLocal>("local_disk", temp_dir->path() + "/");
    }

    void TearDown() override
    {
        disk.reset();
        temp_dir.reset();
    }

    void writePack(const String & pack, const std::vector<std::pair<String, String>> & members)
    {
        VectorWithMemoryTracking<PackedArchiveWriter::Member> manifest;
        manifest.reserve(members.size());
        for (const auto & [name, body] : members)
            manifest.push_back({name, body.size()});

        auto out = disk->writeFile(pack, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
        PackedArchiveWriter writer(std::move(out), manifest, VERSION);
        for (const auto & [name, body] : members)
        {
            ReadBufferFromString in(body);
            writer.writeMember(name, in);
        }
        writer.finalize();
    }

    std::unique_ptr<Poco::TemporaryFile> temp_dir;
    DiskPtr disk;
};

/// N members of varied sizes, including empty, 1-byte, and one larger than a few MiB; each reads back
/// byte-identical through PackedFilesReader.
TEST_F(PackedArchiveWriterTest, RoundtripVariedSizes)
{
    String big;
    big.reserve(5 * 1024 * 1024);
    for (size_t i = 0; i < 5 * 1024 * 1024; ++i)
        big.push_back(static_cast<char>('a' + (i % 26)));

    std::vector<std::pair<String, String>> members = {
        {"0a/empty", ""},
        {"0b/single", "A"},
        {"1c/small", "hello packed world"},
        {"2d/big", big},
        {"3e/medium", std::string(4096, 'z')},
    };
    writePack("pack_0000", members);

    PackedFilesReader reader(disk, "pack_0000", ReadSettings{});
    for (const auto & [name, body] : members)
    {
        EXPECT_TRUE(reader.exists(name)) << name;
        EXPECT_EQ(reader.getFileSize(name), body.size()) << name;
        EXPECT_EQ(readMember(reader, disk, "pack_0000", name), body) << name;
    }
}

/// Header offsets match the body layout: the first member starts right after the serialized index
/// and offsets are contiguous even across an empty member in the middle.
TEST_F(PackedArchiveWriterTest, HeaderOffsetsContiguous)
{
    writePack("pack_offsets", {{"m0", std::string(10, 'x')}, {"m1", ""}, {"m2", std::string(33, 'x')}});

    const UInt64 first = PackedFilesWriter::getSerializedIndexSize({"m0", "m1", "m2"}, VERSION);
    const std::vector<std::tuple<String, UInt64, UInt64>> expected = {{"m0", first, 10}, {"m1", first + 10, 0}, {"m2", first + 10, 33}};
    PackedFilesReader reader(disk, "pack_offsets", ReadSettings{});
    std::vector<std::tuple<String, UInt64, UInt64>> actual;
    for (const auto & [name, file_offset] : reader.getIndex())
        actual.emplace_back(name, file_offset.offset, file_offset.size);
    EXPECT_EQ(actual, expected);
}

/// A manifest the writer rejects must cancel the destination, or an abandoned buffer could publish a pack.
/// Duplicate names are just a convenient way to make the constructor throw.
TEST_F(PackedArchiveWriterTest, DuplicateMemberCancelsOutput)
{
    bool cancel_called = false;
    VectorWithMemoryTracking<PackedArchiveWriter::Member> manifest = {{"m", 1}, {"m", 2}};
    EXPECT_THROW(PackedArchiveWriter(std::make_unique<CancelRecordingWriteBuffer>(cancel_called), manifest, VERSION), Exception);
    EXPECT_TRUE(cancel_called);
}
