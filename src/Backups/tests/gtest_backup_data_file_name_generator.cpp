#include <gtest/gtest.h>

#include <Backups/BackupFileInfo.h>
#include <Backups/BackupSettings.h>
#include <Backups/getBackupDataFileNameGenerator.h>

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

TEST(BackupDataFileNameGeneratorTest, ReturnsFirstFileNameGeneratorForPlainBackup)
{
    ConfigurationPtr config = Poco::AutoPtr<Poco::Util::MapConfiguration>(new Poco::Util::MapConfiguration());
    BackupSettings settings;
    settings.deduplicate_files = false; /// plain backup

    auto generator = getBackupDataFileNameGenerator(*config, settings);
    EXPECT_EQ(generator->getName(), "first_file_name");

    auto info = makeFileInfo("file1.bin", 123);
    EXPECT_EQ(generator->generate(info), "file1.bin");
}

TEST(BackupDataFileNameGeneratorTest, ReturnsChecksumGeneratorWhenChecksumSelected)
{
    ConfigurationPtr config = Poco::AutoPtr<Poco::Util::MapConfiguration>(new Poco::Util::MapConfiguration());
    BackupSettings settings;
    settings.deduplicate_files = true;
    settings.data_file_name_generator = "checksum";
    settings.data_file_name_prefix_length = 3;

    auto generator = getBackupDataFileNameGenerator(*config, settings);
    EXPECT_EQ(generator->getName(), "checksum");

    auto info = makeFileInfo("data.bin", UInt128(0x1234ABCDULL) << 96);
    auto path = generator->generate(info);

    EXPECT_TRUE(path.starts_with("123/"));
}

TEST(BackupDataFileNameGeneratorTest, ReadsGeneratorFromConfigIfNotInSettings)
{
    ConfigurationPtr config = Poco::AutoPtr<Poco::Util::MapConfiguration>(new Poco::Util::MapConfiguration());
    config->setString("backups.data_file_name_generator", "checksum");
    config->setUInt64("backups.data_file_name_prefix_length", 4);

    BackupSettings settings;
    settings.deduplicate_files = true;
    settings.data_file_name_generator = ""; /// not set explicitly

    auto generator = getBackupDataFileNameGenerator(*config, settings);
    EXPECT_EQ(generator->getName(), "checksum");

    auto info = makeFileInfo("test.bin", DB::UInt128(0xABCDEF12ULL) << 96);
    auto path = generator->generate(info);
    EXPECT_TRUE(path.starts_with("abcd/"));
}

TEST(BackupDataFileNameGeneratorTest, ChecksumGeneratorWithZeroPrefixLength)
{
    ConfigurationPtr config = Poco::AutoPtr<Poco::Util::MapConfiguration>(new Poco::Util::MapConfiguration());
    BackupSettings settings;
    settings.deduplicate_files = true;
    settings.data_file_name_generator = "checksum";
    settings.data_file_name_prefix_length = 0;

    auto generator = getBackupDataFileNameGenerator(*config, settings);
    EXPECT_EQ(generator->getName(), "checksum");

    UInt128 checksum = static_cast<UInt128>(0x1234ABCDULL) << 96;
    auto info = makeFileInfo("test.bin", checksum);

    std::string path = generator->generate(info);
    EXPECT_EQ(path.find('/'), std::string::npos) << "Path should not contain slash";

    auto hex = getHexUIntLowercase(checksum);
    EXPECT_EQ(path, hex);
}

TEST(BackupDataFileNameGeneratorTest, ChecksumGeneratorWithMaxPrefixLength32)
{
    ConfigurationPtr config = Poco::AutoPtr<Poco::Util::MapConfiguration>(new Poco::Util::MapConfiguration());
    BackupSettings settings;
    settings.deduplicate_files = true;
    settings.data_file_name_generator = "checksum";
    settings.data_file_name_prefix_length = 32;

    auto generator = getBackupDataFileNameGenerator(*config, settings);
    EXPECT_EQ(generator->getName(), "checksum");

    UInt128 checksum = static_cast<UInt128>(0x1234ABCDULL) << 96;
    auto info = makeFileInfo("test.bin", checksum);

    std::string path = generator->generate(info);

    auto hex = getHexUIntLowercase(checksum);

    auto prefix = hex.substr(0, 32);
    EXPECT_EQ(prefix, path);

    auto suffix = hex.substr(32);
    EXPECT_TRUE(suffix.empty());

    std::cout << path << std::endl;

    EXPECT_TRUE(path.find('/') == std::string::npos) << "Should not contain slash";
}

TEST(BackupDataFileNameGeneratorTest, ThrowsOnInvalidGeneratorName)
{
    ConfigurationPtr config = Poco::AutoPtr<Poco::Util::MapConfiguration>(new Poco::Util::MapConfiguration());
    BackupSettings settings;
    settings.deduplicate_files = true;
    settings.data_file_name_generator = "unknown";

    EXPECT_THROW({ auto generator = getBackupDataFileNameGenerator(*config, settings); }, Exception);
}

TEST(BackupDataFileNameGeneratorTest, ThrowsWhenPrefixUsedWithFirstFileName)
{
    ConfigurationPtr config = Poco::AutoPtr<Poco::Util::MapConfiguration>(new Poco::Util::MapConfiguration());
    BackupSettings settings;
    settings.deduplicate_files = true;
    settings.data_file_name_generator = "first_file_name";
    settings.data_file_name_prefix_length = 2;

    EXPECT_THROW({ auto generator = getBackupDataFileNameGenerator(*config, settings); }, Exception);
}

TEST(BackupDataFileNameGeneratorTest, ThrowsWhenChecksumPrefixOutOfBound)
{
    ConfigurationPtr config = Poco::AutoPtr<Poco::Util::MapConfiguration>(new Poco::Util::MapConfiguration());
    BackupSettings settings;
    settings.deduplicate_files = true;
    settings.data_file_name_generator = "checksum";
    settings.data_file_name_prefix_length = 33; /// invalid (>32)

    EXPECT_THROW({ auto generator = getBackupDataFileNameGenerator(*config, settings); }, Exception);
}

TEST(BackupDataFileNameGeneratorTest, ThrowsOnEmptyFileName)
{
    ConfigurationPtr config = Poco::AutoPtr<Poco::Util::MapConfiguration>(new Poco::Util::MapConfiguration());
    BackupSettings settings;
    settings.deduplicate_files = true;
    settings.data_file_name_generator = "checksum";
    settings.data_file_name_prefix_length = 2;

    auto generator = getBackupDataFileNameGenerator(*config, settings);
    EXPECT_EQ(generator->getName(), "checksum");

    BackupFileInfo info;
    info.file_name = "";
    info.checksum = 0x123;

    EXPECT_THROW(generator->generate(info), Exception);
}

TEST(BackupDataFileNameGeneratorTest, ThrowsOnZeroChecksum)
{
    ConfigurationPtr config = Poco::AutoPtr<Poco::Util::MapConfiguration>(new Poco::Util::MapConfiguration());
    BackupSettings settings;
    settings.deduplicate_files = true;
    settings.data_file_name_generator = "checksum";
    settings.data_file_name_prefix_length = 2;

    auto generator = getBackupDataFileNameGenerator(*config, settings);
    EXPECT_EQ(generator->getName(), "checksum");

    BackupFileInfo info;
    info.file_name = "data.bin";
    info.checksum = 0;

    EXPECT_THROW(generator->generate(info), Exception);
}
