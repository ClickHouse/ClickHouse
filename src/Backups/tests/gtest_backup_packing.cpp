#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include <Backups/BackupCoordinationFileInfos.h>
#include <Backups/BackupFileInfo.h>
#include <Backups/BackupPacker.h>
#include <Core/Defines.h>
#include <Disks/DiskLocal.h>
#include <IO/PackedFilesIO.h>
#include <IO/PackedFilesReader.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <Poco/TemporaryFile.h>

using namespace DB;

namespace
{

BackupFileInfo makeInfo(const String & name, UInt64 size, UInt128 checksum, UInt64 base_size = 0, UInt128 base_checksum = 0)
{
    BackupFileInfo info;
    info.file_name = name;
    info.size = size;
    info.checksum = checksum;
    info.base_size = base_size;
    info.base_checksum = base_checksum;
    return info;
}

BackupCoordinationFileInfos::Config packedConfig(UInt64 pack_size, UInt64 pack_min_size)
{
    return BackupCoordinationFileInfos::Config{
        /* plain_backup= */ false, BackupDataFileNameGeneratorType::FirstFileName, /* prefix_length= */ 3,
        /* pack_format= */ true, pack_size, pack_min_size};
}

BackupCoordinationFileInfos::Config plainDedupConfig()
{
    return BackupCoordinationFileInfos::Config{
        /* plain_backup= */ false, BackupDataFileNameGeneratorType::FirstFileName, /* prefix_length= */ 3};
}

/// Packed mode, checksum-derived names (the S3 default): data_file_name = hex(checksum).
BackupCoordinationFileInfos::Config packedChecksumConfig()
{
    return BackupCoordinationFileInfos::Config{
        /* plain_backup= */ false, BackupDataFileNameGeneratorType::Checksum, /* prefix_length= */ 0,
        /* pack_format= */ true, /* pack_size= */ 1000, /* pack_min_size= */ 1000};
}

/// Runs prepare() and returns the resolved file infos, keyed by file name.
std::map<String, BackupFileInfo> resolve(const BackupCoordinationFileInfos::Config & config, BackupFileInfos infos)
{
    BackupCoordinationFileInfos coordination(config);
    coordination.addFileInfos(std::move(infos), "");
    std::map<String, BackupFileInfo> result;
    for (const auto & info : coordination.getFileInfos(""))
        result.emplace(info.file_name, info);
    return result;
}

}

/// Packing off: every blob keeps its own object (pack_id == -1) -- the discriminator for the packed tests below.
TEST(BackupPackingAssignment, PackingOffLeavesOwnObjects)
{
    auto r = resolve(plainDedupConfig(), {makeInfo("a", 10, 1), makeInfo("b", 20, 2)});
    EXPECT_EQ(r["a"].pack_id, -1);
    EXPECT_EQ(r["b"].pack_id, -1);
}

/// Small blobs (< min_size) pack together; a blob >= min_size stays its own object.
TEST(BackupPackingAssignment, SmallFilesPackedLargeFilesOwnObject)
{
    auto r = resolve(packedConfig(/* pack_size= */ 1000, /* pack_min_size= */ 100),
        {makeInfo("small1", 10, 1), makeInfo("small2", 30, 2), makeInfo("large", 200, 3)});
    EXPECT_EQ(r["small1"].pack_id, 0);
    EXPECT_EQ(r["small2"].pack_id, 0);
    EXPECT_EQ(r["large"].pack_id, -1);
}

/// The running byte total is bounded by pack_size, so enough small blobs spill into further packs.
TEST(BackupPackingAssignment, BinPacksAcrossMultiplePacks)
{
    auto r = resolve(packedConfig(/* pack_size= */ 100, /* pack_min_size= */ 1000),
        {makeInfo("f0", 60, 1), makeInfo("f1", 60, 2), makeInfo("f2", 60, 3)});
    /// 60 + 60 > 100, so each blob lands in its own pack (assigned in sorted-name order).
    EXPECT_EQ(r["f0"].pack_id, 0);
    EXPECT_EQ(r["f1"].pack_id, 1);
    EXPECT_EQ(r["f2"].pack_id, 2);
}

/// Regression: same (size, checksum), different base = identical content -> must share one index AND name.
/// Under checksum naming, splitting by base collides two members on the same hex(checksum) name.
TEST(BackupPackingAssignment, ChecksumNameCollapsesSameChecksumDifferentBase)
{
    auto r = resolve(packedChecksumConfig(), {
        makeInfo("A", 100, 7, /* base_size= */ 0, /* base_checksum= */ 0),
        makeInfo("B", 100, 7, /* base_size= */ 50, /* base_checksum= */ 9),
    });
    EXPECT_EQ(r["A"].data_file_index, r["B"].data_file_index);
    EXPECT_EQ(r["A"].data_file_name, r["B"].data_file_name);
    EXPECT_FALSE(r["A"].data_file_name.empty());
}

/// Identical blobs collapse to one member; duplicates inherit the representative's pack_id.
TEST(BackupPackingAssignment, DuplicatesShareBlobAndPack)
{
    auto r = resolve(packedConfig(/* pack_size= */ 1000, /* pack_min_size= */ 1000),
        {makeInfo("dup1", 40, 5), makeInfo("dup2", 40, 5)});
    EXPECT_EQ(r["dup1"].data_file_index, r["dup2"].data_file_index);
    EXPECT_EQ(r["dup1"].pack_id, r["dup2"].pack_id);
    EXPECT_GE(r["dup1"].pack_id, 0);
}

/// Regression: pack a member from the READ REPRESENTATIVE (smallest-file_name of the (size, checksum) class,
/// which restore keys on), not the first entry -- a non-representative's base_size stores the wrong
/// [base_size, size) suffix and corrupts restore. Discriminator: entry-order-first fails both the
/// representative check and the byte roundtrip below.
TEST(BackupPacking, PackMemberUsesReadRepresentativeBaseSize)
{
    /// Add order puts non-representative "z_dup" (base_size 30) before representative "a_rep" (base_size 0).
    BackupFileInfos infos = {
        makeInfo("z_dup", 100, 7, /* base_size= */ 30, /* base_checksum= */ 9),
        makeInfo("a_rep", 100, 7, /* base_size= */ 0, /* base_checksum= */ 0),
    };
    BackupCoordinationFileInfos coordination(packedConfig(/* pack_size= */ 1000, /* pack_min_size= */ 1000));
    coordination.addFileInfos(std::move(infos), "");
    BackupFileInfos resolved = coordination.getFileInfos("");

    /// Representative = smallest file_name of the class => "a_rep" (base_size 0).
    const BackupFileInfo * representative = nullptr;
    for (const auto & info : resolved)
        if (info.file_name == "a_rep")
            representative = &info;
    ASSERT_NE(representative, nullptr);

    auto pack_to_members = BackupPacker::selectPackMembers(resolved);
    ASSERT_EQ(pack_to_members.size(), 1u);
    const auto & member_indices = pack_to_members.begin()->second;
    ASSERT_EQ(member_indices.size(), 1u);
    const BackupFileInfo & member = resolved[member_indices[0]];
    EXPECT_EQ(member.file_name, "a_rep");
    EXPECT_EQ(member.base_size, representative->base_size);
    EXPECT_EQ(member.base_size, 0u);

    /// E2E: pack the chosen member's suffix and read it back -- must equal the representative's whole file
    /// (base_size 0). "z_dup" would store the wrong 70-byte suffix.
    String content;
    for (size_t i = 0; i != 100; ++i)
        content.push_back(static_cast<char>('A' + (i % 26)));

    Poco::TemporaryFile temp_dir;
    temp_dir.createDirectories();
    auto disk = std::make_shared<DiskLocal>("local_disk", temp_dir.path() + "/");

    std::vector<BackupPacker::MemberSource> members;
    members.push_back(BackupPacker::MemberSource{
        member.data_file_name, member.size - member.base_size,
        [captured = content, base = member.base_size]() -> std::unique_ptr<ReadBuffer>
        {
            auto buf = std::make_unique<ReadBufferFromString>(captured);
            buf->ignore(base);
            return buf;
        }});
    BackupPacker::writePack(
        disk->writeFile("packs_0000", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {}),
        members, PackedFilesIO::VERSION_WITHOUT_UNCOMPRESSED_SIZE);

    PackedFilesReader reader(disk, "packs_0000", ReadSettings{});
    auto buf = reader.readFile(disk, "packs_0000", member.data_file_name, ReadSettings{}, std::nullopt);
    String got;
    readStringUntilEOF(got, *buf);
    EXPECT_EQ(got, content.substr(representative->base_size));
    EXPECT_EQ(got.size(), 100u);
}

/// Roundtrip: BackupPacker writes members (via create_read_buffer); PackedFilesReader reads them back byte-identical.
TEST(BackupPacker, WritePackRoundtrip)
{
    Poco::TemporaryFile temp_dir;
    temp_dir.createDirectories();
    auto disk = std::make_shared<DiskLocal>("local_disk", temp_dir.path() + "/");

    std::vector<std::pair<String, String>> bodies = {
        {"0a/col1.bin", "hello"},
        {"0b/col2.bin", std::string(4096, 'z')},
        {"0c/empty", ""},
    };

    std::vector<BackupPacker::MemberSource> members;
    for (const auto & [name, body] : bodies)
        members.push_back(BackupPacker::MemberSource{
            name, body.size(),
            [captured = body]() -> std::unique_ptr<ReadBuffer> { return std::make_unique<ReadBufferFromString>(captured); }});

    BackupPacker::writePack(
        disk->writeFile("packs_0000", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {}),
        members, PackedFilesIO::VERSION_WITHOUT_UNCOMPRESSED_SIZE);

    PackedFilesReader reader(disk, "packs_0000", ReadSettings{});
    for (const auto & [name, body] : bodies)
    {
        ASSERT_TRUE(reader.exists(name)) << name;
        EXPECT_EQ(reader.getFileSize(name), body.size()) << name;
        auto buf = reader.readFile(disk, "packs_0000", name, ReadSettings{}, std::nullopt);
        String got;
        readStringUntilEOF(got, *buf);
        EXPECT_EQ(got, body) << name;
    }
}
