#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Backups/BackupEntryFromAppendOnlyFile.h>
#include <Backups/BackupEntryFromImmutableFile.h>
#include <Backups/BackupEntryFromSmallFile.h>

#include <Disks/IDisk.h>
#include <Disks/DiskLocal.h>
#include <Disks/DiskEncrypted.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Poco/TemporaryFile.h>

using namespace DB;


class BackupEntriesTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        /// Make local disk.
        temp_dir = std::make_unique<Poco::TemporaryFile>();
        temp_dir->createDirectories();
        local_disk = std::make_shared<DiskLocal>("local_disk", temp_dir->path() + "/");

        /// Make encrypted disk.
        auto settings = std::make_unique<DiskEncryptedSettings>();
        settings->wrapped_disk = local_disk;
        settings->disk_path = "encrypted/";

        settings->current_algorithm = FileEncryption::Algorithm::AES_128_CTR;
        NoDumpString key = "1234567890123456";
        UInt128 fingerprint = FileEncryption::calculateKeyFingerprint(key);
        settings->all_keys[fingerprint] = key;
        settings->current_key = key;
        settings->current_key_fingerprint = fingerprint;

        encrypted_disk = std::make_shared<DiskEncrypted>("encrypted_disk", std::move(settings));
    }

    void TearDown() override
    {
        encrypted_disk.reset();
        local_disk.reset();
    }

    static void writeFile(DiskPtr disk, const String & filepath)
    {
        auto buf = disk->writeFile(filepath, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
        writeString(std::string_view{"Some text"}, *buf);
        buf->finalize();
    }

    static void writeEmptyFile(DiskPtr disk, const String & filepath)
    {
        auto buf = disk->writeFile(filepath, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
        buf->finalize();
    }

    static void appendFile(DiskPtr disk, const String & filepath)
    {
        auto buf = disk->writeFile(filepath, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
        writeString(std::string_view{"Appended"}, *buf);
        buf->finalize();
    }

    static String getChecksum(const BackupEntryPtr & backup_entry)
    {
        return getHexUIntUppercase(backup_entry->getChecksum({}));
    }

    static const constexpr std::string_view NO_CHECKSUM = "no checksum";

    static String getPartialChecksum(const BackupEntryPtr & backup_entry, UInt64 prefix_length)
    {
        auto partial_checksum = backup_entry->getPartialChecksum(prefix_length, {});
        if (!partial_checksum)
            return String{NO_CHECKSUM};
        return getHexUIntUppercase(*partial_checksum);
    }

    static String readAll(const BackupEntryPtr & backup_entry)
    {
        auto in = backup_entry->getReadBuffer({});
        String str;
        readStringUntilEOF(str, *in);
        return str;
    }

    std::unique_ptr<Poco::TemporaryFile> temp_dir;
    std::shared_ptr<DiskLocal> local_disk;
    std::shared_ptr<DiskEncrypted> encrypted_disk;
};


static const constexpr std::string_view ZERO_CHECKSUM = "00000000000000000000000000000000";

static const constexpr std::string_view SOME_TEXT_CHECKSUM = "28B5529750AC210952FFD366774363ED";
static const constexpr std::string_view S_CHECKSUM = "C27395C39AFB5557BFE47661CC9EB86C";
static const constexpr std::string_view SOME_TEX_CHECKSUM = "D00D9BE8D87919A165F14EDD31088A0E";
static const constexpr std::string_view SOME_TEXT_APPENDED_CHECKSUM = "5A1F10F638DC7A226231F3FD927D1726";

static const constexpr std::string_view PRECALCULATED_CHECKSUM = "1122334455667788AABBCCDDAABBCCDD";
static const constexpr UInt128 PRECALCULATED_CHECKSUM_UINT128 = (UInt128(0x1122334455667788) << 64) | 0xAABBCCDDAABBCCDD;
static const size_t PRECALCULATED_SIZE = 123;

TEST_F(BackupEntriesTest, BackupEntryFromImmutableFile)
{
    writeFile(local_disk, "a.txt");

    auto entry = std::make_shared<BackupEntryFromImmutableFile>(local_disk, "a.txt");
    EXPECT_EQ(entry->getSize(), 9);
    EXPECT_EQ(getChecksum(entry), SOME_TEXT_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 0), NO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 1), NO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 8), NO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 9), SOME_TEXT_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 1000), SOME_TEXT_CHECKSUM);
    EXPECT_EQ(readAll(entry), "Some text");

    writeEmptyFile(local_disk, "empty.txt");

    auto empty_entry = std::make_shared<BackupEntryFromImmutableFile>(local_disk, "empty.txt");
    EXPECT_EQ(empty_entry->getSize(), 0);
    EXPECT_EQ(getChecksum(empty_entry), ZERO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(empty_entry, 0), ZERO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(empty_entry, 1), ZERO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(empty_entry, 1000), ZERO_CHECKSUM);
    EXPECT_EQ(readAll(empty_entry), "");

    auto precalculated_entry = std::make_shared<BackupEntryFromImmutableFile>(local_disk, "a.txt", false, PRECALCULATED_SIZE, PRECALCULATED_CHECKSUM_UINT128);
    EXPECT_EQ(precalculated_entry->getSize(), PRECALCULATED_SIZE);

    EXPECT_EQ(getChecksum(precalculated_entry), PRECALCULATED_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(precalculated_entry, 0), NO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(precalculated_entry, 1), NO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(precalculated_entry, PRECALCULATED_SIZE - 1), NO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(precalculated_entry, PRECALCULATED_SIZE), PRECALCULATED_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(precalculated_entry, 1000), PRECALCULATED_CHECKSUM);
    EXPECT_EQ(readAll(precalculated_entry), "Some text");
}

TEST_F(BackupEntriesTest, BackupEntryFromAppendOnlyFile)
{
    writeFile(local_disk, "a.txt");

    auto entry = std::make_shared<BackupEntryFromAppendOnlyFile>(local_disk, "a.txt");
    EXPECT_EQ(entry->getSize(), 9);
    EXPECT_EQ(getChecksum(entry), SOME_TEXT_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 0), ZERO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 1), S_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 8), SOME_TEX_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 9), SOME_TEXT_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 1000), SOME_TEXT_CHECKSUM);
    EXPECT_EQ(readAll(entry), "Some text");

    appendFile(local_disk, "a.txt");

    EXPECT_EQ(entry->getSize(), 9);
    EXPECT_EQ(getChecksum(entry), SOME_TEXT_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 0), ZERO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 1), S_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 8), SOME_TEX_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 9), SOME_TEXT_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 1000), SOME_TEXT_CHECKSUM);
    EXPECT_EQ(readAll(entry), "Some text");

    auto appended_entry = std::make_shared<BackupEntryFromAppendOnlyFile>(local_disk, "a.txt");
    EXPECT_EQ(appended_entry->getSize(), 17);
    EXPECT_EQ(getChecksum(appended_entry), SOME_TEXT_APPENDED_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(appended_entry, 0), ZERO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(appended_entry, 1), S_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(appended_entry, 8), SOME_TEX_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(appended_entry, 9), SOME_TEXT_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(appended_entry, 22), SOME_TEXT_APPENDED_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(appended_entry, 1000), SOME_TEXT_APPENDED_CHECKSUM);
    EXPECT_EQ(readAll(appended_entry), "Some textAppended");

    writeEmptyFile(local_disk, "empty_appended.txt");

    auto empty_entry = std::make_shared<BackupEntryFromAppendOnlyFile>(local_disk, "empty_appended.txt");
    EXPECT_EQ(empty_entry->getSize(), 0);
    EXPECT_EQ(getChecksum(empty_entry), ZERO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(empty_entry, 0), ZERO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(empty_entry, 1), ZERO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(empty_entry, 1000), ZERO_CHECKSUM);
    EXPECT_EQ(readAll(empty_entry), "");

    appendFile(local_disk, "empty_appended.txt");
    EXPECT_EQ(empty_entry->getSize(), 0);
    EXPECT_EQ(getChecksum(empty_entry), ZERO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(empty_entry, 0), ZERO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(empty_entry, 1), ZERO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(empty_entry, 1000), ZERO_CHECKSUM);
    EXPECT_EQ(readAll(empty_entry), "");
}

TEST_F(BackupEntriesTest, PartialChecksumBeforeFullChecksum)
{
    writeFile(local_disk, "a.txt");

    auto entry = std::make_shared<BackupEntryFromAppendOnlyFile>(local_disk, "a.txt");
    EXPECT_EQ(entry->getSize(), 9);
    EXPECT_EQ(getPartialChecksum(entry, 0), ZERO_CHECKSUM);
    EXPECT_EQ(getChecksum(entry), SOME_TEXT_CHECKSUM);
    EXPECT_EQ(readAll(entry), "Some text");

    entry = std::make_shared<BackupEntryFromAppendOnlyFile>(local_disk, "a.txt");
    EXPECT_EQ(entry->getSize(), 9);
    EXPECT_EQ(getPartialChecksum(entry, 1), S_CHECKSUM);
    EXPECT_EQ(getChecksum(entry), SOME_TEXT_CHECKSUM);
    EXPECT_EQ(readAll(entry), "Some text");
}

TEST_F(BackupEntriesTest, BackupEntryFromSmallFile)
{
    auto read_settings = getContext().context->getReadSettings();
    writeFile(local_disk, "a.txt");
    auto entry = std::make_shared<BackupEntryFromSmallFile>(local_disk, "a.txt", read_settings);

    local_disk->removeFile("a.txt");

    EXPECT_EQ(entry->getSize(), 9);
    EXPECT_EQ(getChecksum(entry), SOME_TEXT_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 0), ZERO_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 1), S_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 8), SOME_TEX_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 9), SOME_TEXT_CHECKSUM);
    EXPECT_EQ(getPartialChecksum(entry, 1000), SOME_TEXT_CHECKSUM);
    EXPECT_EQ(readAll(entry), "Some text");
}

TEST_F(BackupEntriesTest, DecryptedEntriesFromEncryptedDisk)
{
    auto read_settings = getContext().context->getReadSettings();
    {
        writeFile(encrypted_disk, "a.txt");
        std::pair<BackupEntryPtr, bool /* partial_checksum_allowed */> test_cases[]
            = {{std::make_shared<BackupEntryFromImmutableFile>(encrypted_disk, "a.txt"), false},
               {std::make_shared<BackupEntryFromAppendOnlyFile>(encrypted_disk, "a.txt"), true},
               {std::make_shared<BackupEntryFromSmallFile>(encrypted_disk, "a.txt", read_settings), true}};
        for (const auto & [entry, partial_checksum_allowed] : test_cases)
        {
            EXPECT_EQ(entry->getSize(), 9);
            EXPECT_EQ(getChecksum(entry), SOME_TEXT_CHECKSUM);
            EXPECT_EQ(getPartialChecksum(entry, 0), partial_checksum_allowed ? ZERO_CHECKSUM : NO_CHECKSUM);
            EXPECT_EQ(getPartialChecksum(entry, 1), partial_checksum_allowed ? S_CHECKSUM : NO_CHECKSUM);
            EXPECT_EQ(getPartialChecksum(entry, 8), partial_checksum_allowed ? SOME_TEX_CHECKSUM : NO_CHECKSUM);
            EXPECT_EQ(getPartialChecksum(entry, 9), SOME_TEXT_CHECKSUM);
            EXPECT_EQ(getPartialChecksum(entry, 1000), SOME_TEXT_CHECKSUM);
            EXPECT_EQ(readAll(entry), "Some text");
        }
    }

    {
        writeEmptyFile(encrypted_disk, "empty.txt");
        BackupEntryPtr entries[]
            = {std::make_shared<BackupEntryFromImmutableFile>(encrypted_disk, "empty.txt"),
               std::make_shared<BackupEntryFromAppendOnlyFile>(encrypted_disk, "empty.txt"),
               std::make_shared<BackupEntryFromSmallFile>(encrypted_disk, "empty.txt", read_settings)};
        for (const auto & entry : entries)
        {
            EXPECT_EQ(entry->getSize(), 0);
            EXPECT_EQ(getChecksum(entry), ZERO_CHECKSUM);
            EXPECT_EQ(getPartialChecksum(entry, 0), ZERO_CHECKSUM);
            EXPECT_EQ(getPartialChecksum(entry, 1), ZERO_CHECKSUM);
            EXPECT_EQ(readAll(entry), "");
        }
    }

    {
        auto precalculated_entry = std::make_shared<BackupEntryFromImmutableFile>(encrypted_disk, "a.txt", false, PRECALCULATED_SIZE, PRECALCULATED_CHECKSUM_UINT128);
        EXPECT_EQ(precalculated_entry->getSize(), PRECALCULATED_SIZE);
        EXPECT_EQ(getChecksum(precalculated_entry), PRECALCULATED_CHECKSUM);
        EXPECT_EQ(getPartialChecksum(precalculated_entry, 0), NO_CHECKSUM);
        EXPECT_EQ(getPartialChecksum(precalculated_entry, 1), NO_CHECKSUM);
        EXPECT_EQ(getPartialChecksum(precalculated_entry, PRECALCULATED_SIZE), PRECALCULATED_CHECKSUM);
        EXPECT_EQ(getPartialChecksum(precalculated_entry, 1000), PRECALCULATED_CHECKSUM);
        EXPECT_EQ(readAll(precalculated_entry), "Some text");
    }
}

TEST_F(BackupEntriesTest, EncryptedEntriesFromEncryptedDisk)
{
    auto read_settings = getContext().context->getReadSettings();
    {
        writeFile(encrypted_disk, "a.txt");
        BackupEntryPtr entries[]
            = {std::make_shared<BackupEntryFromImmutableFile>(encrypted_disk, "a.txt", /* copy_encrypted= */ true),
               std::make_shared<BackupEntryFromAppendOnlyFile>(encrypted_disk, "a.txt", /* copy_encrypted= */ true),
               std::make_shared<BackupEntryFromSmallFile>(encrypted_disk, "a.txt", read_settings, /* copy_encrypted= */ true)};

        auto encrypted_checksum = getChecksum(entries[0]);
        EXPECT_NE(encrypted_checksum, NO_CHECKSUM);
        EXPECT_NE(encrypted_checksum, ZERO_CHECKSUM);
        EXPECT_NE(encrypted_checksum, SOME_TEXT_CHECKSUM);

        auto partial_checksum = getPartialChecksum(entries[1], 9);
        EXPECT_NE(partial_checksum, NO_CHECKSUM);
        EXPECT_NE(partial_checksum, ZERO_CHECKSUM);
        EXPECT_NE(partial_checksum, SOME_TEXT_CHECKSUM);
        EXPECT_NE(partial_checksum, encrypted_checksum);

        auto encrypted_data = readAll(entries[0]);
        EXPECT_EQ(encrypted_data.size(), 9 + FileEncryption::Header::kSize);

        for (const auto & entry : entries)
        {
            EXPECT_EQ(entry->getSize(), 9 + FileEncryption::Header::kSize);
            EXPECT_EQ(getChecksum(entry), encrypted_checksum);
            auto encrypted_checksum_9 = getPartialChecksum(entry, 9);
            EXPECT_TRUE(encrypted_checksum_9 == NO_CHECKSUM || encrypted_checksum_9 == partial_checksum);
            EXPECT_EQ(getPartialChecksum(entry, 9 + FileEncryption::Header::kSize), encrypted_checksum);
            EXPECT_EQ(getPartialChecksum(entry, 1000), encrypted_checksum);
            EXPECT_EQ(readAll(entry), encrypted_data);
        }
    }

    {
        writeEmptyFile(encrypted_disk, "empty.txt");
        BackupEntryPtr entries[]
            = {std::make_shared<BackupEntryFromImmutableFile>(encrypted_disk, "empty.txt", /* copy_encrypted= */ true),
               std::make_shared<BackupEntryFromAppendOnlyFile>(encrypted_disk, "empty.txt", /* copy_encrypted= */ true),
               std::make_shared<BackupEntryFromSmallFile>(encrypted_disk, "empty.txt", read_settings, /* copy_encrypted= */ true)};
        for (const auto & entry : entries)
        {
            EXPECT_EQ(entry->getSize(), 0);
            EXPECT_EQ(getChecksum(entry), ZERO_CHECKSUM);
            EXPECT_EQ(getPartialChecksum(entry, 0), ZERO_CHECKSUM);
            EXPECT_EQ(getPartialChecksum(entry, 1), ZERO_CHECKSUM);
            EXPECT_EQ(readAll(entry), "");
        }
    }

    {
        auto precalculated_entry = std::make_shared<BackupEntryFromImmutableFile>(encrypted_disk, "a.txt", /* copy_encrypted= */ true, PRECALCULATED_SIZE, PRECALCULATED_CHECKSUM_UINT128);
        EXPECT_EQ(precalculated_entry->getSize(), PRECALCULATED_SIZE + FileEncryption::Header::kSize);

        auto encrypted_checksum = getChecksum(precalculated_entry);
        EXPECT_NE(encrypted_checksum, NO_CHECKSUM);
        EXPECT_NE(encrypted_checksum, ZERO_CHECKSUM);
        EXPECT_NE(encrypted_checksum, SOME_TEXT_CHECKSUM);
        EXPECT_NE(encrypted_checksum, PRECALCULATED_CHECKSUM);

        EXPECT_EQ(getPartialChecksum(precalculated_entry, 0), NO_CHECKSUM);
        EXPECT_EQ(getPartialChecksum(precalculated_entry, 1), NO_CHECKSUM);
        EXPECT_EQ(getPartialChecksum(precalculated_entry, PRECALCULATED_SIZE), NO_CHECKSUM);
        EXPECT_EQ(getPartialChecksum(precalculated_entry, PRECALCULATED_SIZE + FileEncryption::Header::kSize), encrypted_checksum);
        EXPECT_EQ(getPartialChecksum(precalculated_entry, 1000), encrypted_checksum);

        auto encrypted_data = readAll(precalculated_entry);
        EXPECT_EQ(encrypted_data.size(), 9 + FileEncryption::Header::kSize);
    }
}
