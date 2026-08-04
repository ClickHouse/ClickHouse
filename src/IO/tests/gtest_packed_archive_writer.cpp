#include <gtest/gtest.h>

#include <string>
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

    /// Streams the given (member name, body) pairs into a pack object using PackedArchiveWriter.
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
/// byte-identical through PackedFilesReader (the reader is reused unchanged).
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

/// A ranged read returns only its own member's bytes: reading member "b" must not leak the adjacent
/// members' fill bytes.
TEST_F(PackedArchiveWriterTest, RangedReadRespectsMemberBoundary)
{
    std::vector<std::pair<String, String>> members = {
        {"a", std::string(100, 'A')},
        {"b", std::string(50, 'B')},
        {"c", std::string(75, 'C')},
    };
    writePack("pack_boundary", members);

    PackedFilesReader reader(disk, "pack_boundary", ReadSettings{});
    const String b = readMember(reader, disk, "pack_boundary", "b");
    ASSERT_EQ(b.size(), 50u);
    EXPECT_EQ(b, std::string(50, 'B'));
}

/// Header offsets match the body layout: the first member starts right after the serialized index
/// and offsets are contiguous even across an empty member in the middle.
TEST_F(PackedArchiveWriterTest, HeaderOffsetsContiguous)
{
    std::vector<std::pair<String, size_t>> expected_sizes = {{"m0", 10}, {"m1", 0}, {"m2", 33}};
    std::vector<std::pair<String, String>> members;
    Strings names;
    for (const auto & [name, size] : expected_sizes)
    {
        members.emplace_back(name, std::string(size, 'x'));
        names.push_back(name);
    }
    writePack("pack_offsets", members);

    PackedFilesReader reader(disk, "pack_offsets", ReadSettings{});
    const auto & index = reader.getIndex();

    UInt64 expected_offset = PackedFilesWriter::getSerializedIndexSize(names, VERSION);
    for (const auto & [name, size] : expected_sizes)
    {
        auto it = index.find(name);
        ASSERT_NE(it, index.end()) << name;
        EXPECT_EQ(it->second.offset, expected_offset) << name;
        EXPECT_EQ(it->second.size, size) << name;
        expected_offset += size;
    }
}

TEST_F(PackedArchiveWriterTest, SingleMember)
{
    writePack("pack_single", {{"only", "just one member"}});
    PackedFilesReader reader(disk, "pack_single", ReadSettings{});
    EXPECT_EQ(reader.getFileNames().size(), 1u);
    EXPECT_EQ(readMember(reader, disk, "pack_single", "only"), "just one member");
}

TEST_F(PackedArchiveWriterTest, ZeroMembers)
{
    writePack("pack_zero", {});
    PackedFilesReader reader(disk, "pack_zero", ReadSettings{});
    EXPECT_TRUE(reader.getFileNames().empty());
}
