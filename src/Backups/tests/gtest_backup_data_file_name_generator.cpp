#include <gtest/gtest.h>

#include <Backups/BackupFileInfo.h>
#include <Backups/BackupSettings.h>
#include <Backups/getBackupDataFileName.h>

#include <Poco/Util/MapConfiguration.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;
using namespace DB;

namespace
{
BackupFileInfo makeFileInfo(const std::string & name, UInt128 checksum)
{
    BackupFileInfo info;
    info.file_name = name;
    info.checksum = checksum;
    return info;
}
}

TEST(BackupDataFileNameGeneratorTest, FirstFileNameGenerator)
{
    auto prefix_length = 4;
    auto info = makeFileInfo("data.bin", UInt128(0x1234ABCDULL) << 96);
    auto path = getBackupDataFileName(info, BackupDataFileNameGeneratorType::FirstFileName, prefix_length);

    EXPECT_EQ(path, "data.bin");
}

TEST(BackupDataFileNameGeneratorTest, ChecksumGenerator)
{
    auto prefix_length = 4;
    auto info = makeFileInfo("data.bin", UInt128(0x1234ABCDULL) << 96);
    auto path = getBackupDataFileName(info, BackupDataFileNameGeneratorType::Checksum, prefix_length);

    EXPECT_TRUE(path.starts_with("1234/"));
}

TEST(BackupDataFileNameGeneratorTest, ChecksumGeneratorWithZeroPrefixLength)
{
    UInt128 checksum = static_cast<UInt128>(0x1234ABCDULL) << 96;
    auto info = makeFileInfo("test.bin", checksum);

    auto prefix_length = 0;
    std::string path = getBackupDataFileName(info, BackupDataFileNameGeneratorType::Checksum, prefix_length);
    EXPECT_EQ(path.find('/'), std::string::npos) << "Path should not contain slash";

    auto hex = getHexUIntLowercase(checksum);
    EXPECT_EQ(path, hex);
}

TEST(BackupDataFileNameGeneratorTest, ChecksumGeneratorWithMaxPrefixLength32)
{
    UInt128 checksum = static_cast<UInt128>(0x1234ABCDULL) << 96;
    auto info = makeFileInfo("test.bin", checksum);

    auto prefix_length = 32;
    std::string path = getBackupDataFileName(info, BackupDataFileNameGeneratorType::Checksum, prefix_length);

    auto hex = getHexUIntLowercase(checksum);

    auto prefix = hex.substr(0, prefix_length);
    EXPECT_EQ(prefix, path);

    auto suffix = hex.substr(prefix_length);
    EXPECT_TRUE(suffix.empty());

    EXPECT_TRUE(path.find('/') == std::string::npos) << "Should not contain slash";
}

TEST(BackupDataFileNameGeneratorTest, ThrowsWhenChecksumPrefixOutOfBound)
{
    UInt128 checksum = static_cast<UInt128>(0x1234ABCDULL) << 96;
    auto info = makeFileInfo("test.bin", checksum);

    auto prefix_length = 33;
    std::string path = getBackupDataFileName(info, BackupDataFileNameGeneratorType::Checksum, prefix_length);

    EXPECT_TRUE(path.find('/') == std::string::npos) << "Should not contain slash";
}

TEST(BackupDataFileNameGeneratorTest, ThrowsOnZeroChecksum)
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    GTEST_SKIP() << "this test trigger LOGICAL_ERROR, runs only if DEBUG_OR_SANITIZER_BUILD is not defined";
#else
    UInt128 checksum = 0;
    auto info = makeFileInfo("test.bin", checksum);

    auto prefix_length = 2;
    EXPECT_THROW(getBackupDataFileName(info, BackupDataFileNameGeneratorType::Checksum, prefix_length), Exception);
#endif
}
