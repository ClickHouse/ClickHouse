#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include <Backups/BackupCoordinationFileInfos.h>
#include <Backups/BackupFileInfo.h>
#include <Backups/BackupPacker.h>

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
        /* use_packed_format= */ true, pack_size, pack_min_size};
}

/// Packed mode, checksum-derived names (the S3 default): data_file_name = hex(checksum).
BackupCoordinationFileInfos::Config packedChecksumConfig()
{
    return BackupCoordinationFileInfos::Config{
        /* plain_backup= */ false, BackupDataFileNameGeneratorType::Checksum, /* prefix_length= */ 0,
        /* use_packed_format= */ true, /* pack_size= */ 1000, /* pack_min_size= */ 1000};
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

/// Small data files (< min_size) pack together; a data file >= min_size stays its own object.
TEST(BackupPackingAssignment, SmallFilesPackedLargeFilesOwnObject)
{
    auto r = resolve(packedConfig(/* pack_size= */ 1000, /* pack_min_size= */ 100),
        {makeInfo("small1", 10, 1), makeInfo("small2", 30, 2), makeInfo("large", 200, 3)});
    EXPECT_EQ(r["small1"].pack_id, 0);
    EXPECT_EQ(r["small2"].pack_id, 0);
    EXPECT_EQ(r["large"].pack_id, -1);
}

/// The running byte total is bounded by pack_size, so enough small data files spill into further packs.
TEST(BackupPackingAssignment, BinPacksAcrossMultiplePacks)
{
    auto r = resolve(packedConfig(/* pack_size= */ 100, /* pack_min_size= */ 1000),
        {makeInfo("f0", 60, 1), makeInfo("f1", 60, 2), makeInfo("f2", 60, 3)});
    /// 60 + 60 > 100, so each data file lands in its own pack (assigned in sorted-name order).
    EXPECT_EQ(r["f0"].pack_id, 0);
    EXPECT_EQ(r["f1"].pack_id, 1);
    EXPECT_EQ(r["f2"].pack_id, 2);
}

/// Same (size, checksum) with different base shares one index and one name; checksum naming would otherwise collide on hex(checksum).
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

/// The member must come from the smallest-file_name entry of the (size, checksum) class, which restore keys
/// on: another entry's base_size would store the wrong [base_size, size) suffix.
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

    auto member_indices_by_pack_id = BackupPacker::selectPackMembers(resolved);
    ASSERT_EQ(member_indices_by_pack_id.size(), 1u);
    const auto & member_indices = member_indices_by_pack_id[0];
    ASSERT_EQ(member_indices.size(), 1u);
    const BackupFileInfo & member = resolved[member_indices[0]];
    EXPECT_EQ(member.file_name, "a_rep");
    EXPECT_EQ(member.base_size, representative->base_size);
    EXPECT_EQ(member.base_size, 0u);
}

