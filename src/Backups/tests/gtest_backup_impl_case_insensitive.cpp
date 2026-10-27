#include <Backups/BackupImpl.h>

#include <Backups/BackupCoordinationLocal.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupFileInfo.h>
#include <Backups/BackupIO_File.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Core/UUID.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>

#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

#include <Poco/TemporaryFile.h>

#include <filesystem>

using namespace DB;

namespace fs = std::filesystem;

namespace DB::ErrorCodes
{
    extern const int BACKUP_ENTRY_ALREADY_EXISTS;
}

namespace
{

/// Resolves every name in lowercase, so names that differ only in case refer to the same file,
/// as on a case-insensitive filesystem.
class CaseFoldingBackupWriter : public BackupWriterFile
{
public:
    explicit CaseFoldingBackupWriter(const String & root_path_)
        : BackupWriterFile(root_path_, root_path_, ReadSettings{}, WriteSettings{})
    {
    }

    bool fileExists(const String & file_name) override { return BackupWriterFile::fileExists(toLowerCopyASCII(file_name)); }
    UInt64 getFileSize(const String & file_name) override { return BackupWriterFile::getFileSize(toLowerCopyASCII(file_name)); }

    std::unique_ptr<WriteBuffer> writeFile(const String & file_name) override
    {
        return BackupWriterFile::writeFile(toLowerCopyASCII(file_name));
    }

    std::unique_ptr<WriteBuffer> writeFileIfNotExists(const String & file_name) override
    {
        return BackupWriterFile::writeFileIfNotExists(toLowerCopyASCII(file_name));
    }

    void removeFile(const String & file_name) override { BackupWriterFile::removeFile(toLowerCopyASCII(file_name)); }

    void copyFile(const String & destination, const String & source, size_t size) override
    {
        BackupWriterFile::copyFile(toLowerCopyASCII(destination), toLowerCopyASCII(source), size);
    }
};

struct TestFile
{
    String name;
    String contents;
};

String readWholeFile(const fs::path & path)
{
    ReadBufferFromFile in(path.string());
    String res;
    readStringUntilEOF(res, in);
    return res;
}

bool hasCaseSensitivityProbe(const fs::path & dir)
{
    for (const auto & entry : fs::directory_iterator(dir))
        if (entry.path().filename().string().starts_with("clickhouse_case_sensitivity_check_"))
            return true;
    return false;
}

}

class BackupImplCaseInsensitive : public ::testing::Test
{
protected:
    void SetUp() override
    {
        temp_dir = std::make_unique<Poco::TemporaryFile>();
        temp_dir->createDirectories();
        root = temp_dir->path();
    }

    void TearDown() override
    {
        /// ~BackupImpl() asserts that the backup is either finalized or corrupted.
        if (backup && !finalized)
            backup->setIsCorrupted();
        backup.reset();
    }

    void createBackup(std::shared_ptr<IBackupWriter> writer)
    {
        BackupSettings backup_settings;
        backup_settings.data_file_name_prefix_length = 3;
        coordination = std::make_shared<BackupCoordinationLocal>(backup_settings, false, backup_concurrency_counters);

        BackupFactory::CreateParams params;
        params.open_mode = IBackup::OpenMode::WRITE;
        params.backup_info = BackupInfo::fromString("File('" + root.string() + "/')");
        params.context = getContext().context;
        params.backup_uuid = UUIDHelpers::generateV4();
        params.backup_coordination = coordination;
        params.fsync_backup_files = false;

        backup = std::make_unique<BackupImpl>(params, BackupImpl::ArchiveParams{}, std::move(writer));
    }

    /// Writes the files to the backup the way `BackupsWorker` does: collect the file infos, let the coordination assign
    /// the data file names, then write every entry.
    void writeFiles(const std::vector<TestFile> & files)
    {
        std::vector<BackupEntryPtr> entries;
        BackupFileInfos infos;
        for (const auto & file : files)
        {
            auto entry = std::make_shared<BackupEntryFromMemory>(file.contents);
            BackupFileInfo info;
            info.file_name = file.name;
            info.size = entry->getSize();
            info.checksum = entry->getChecksum(ReadSettings{});
            infos.push_back(std::move(info));
            entries.push_back(std::move(entry));
        }

        coordination->addFileInfos(std::move(infos));
        const auto & prepared_infos = coordination->getFileInfos();
        for (size_t i = 0; i != entries.size(); ++i)
            backup->writeFile(prepared_infos[i], entries[i]);
    }

    void finalize()
    {
        backup->finalizeWriting();
        finalized = true;
    }

    std::unique_ptr<Poco::TemporaryFile> temp_dir;
    fs::path root;
    BackupConcurrencyCounters backup_concurrency_counters;
    std::shared_ptr<BackupCoordinationLocal> coordination;
    std::unique_ptr<BackupImpl> backup;
    bool finalized = false;
};

TEST_F(BackupImplCaseInsensitive, FailsOnNamesDifferingOnlyInCase)
{
    createBackup(std::make_shared<CaseFoldingBackupWriter>(root.string()));

    try
    {
        writeFiles({{"metadata/d/T.sql", "CREATE TABLE d.T (x UInt8)"}, {"metadata/d/t.sql", "CREATE TABLE d.t (x UInt8)"}});
        FAIL() << "Expected the backup to fail, but both files were written and one of them replaced the other";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BACKUP_ENTRY_ALREADY_EXISTS) << e.displayText();
        EXPECT_NE(e.message().find("'metadata/d/T.sql'"), String::npos) << e.displayText();
        EXPECT_NE(e.message().find("'metadata/d/t.sql'"), String::npos) << e.displayText();
        EXPECT_NE(e.message().find("data_file_name_generator"), String::npos) << e.displayText();
    }

    EXPECT_FALSE(fs::exists(root / "metadata"));
    EXPECT_FALSE(hasCaseSensitivityProbe(root));
}

TEST_F(BackupImplCaseInsensitive, KeepsBothFilesOnCaseSensitiveDestination)
{
    {
        WriteBufferFromFile out((root / "a").string());
        out.finalize();
    }
    bool temp_dir_is_case_insensitive = fs::exists(root / "A");
    (void)fs::remove(root / "a");
    if (temp_dir_is_case_insensitive)
        GTEST_SKIP() << "The temporary directory " << root << " is case-insensitive";

    createBackup(std::make_shared<BackupWriterFile>(root.string(), root.string(), ReadSettings{}, WriteSettings{}));

    writeFiles({{"metadata/d/T.sql", "CREATE TABLE d.T (x UInt8)"}, {"metadata/d/t.sql", "CREATE TABLE d.t (x UInt8)"}});
    finalize();

    EXPECT_TRUE(fs::exists(root / ".backup"));
    EXPECT_EQ(readWholeFile(root / "metadata/d/T.sql"), "CREATE TABLE d.T (x UInt8)");
    EXPECT_EQ(readWholeFile(root / "metadata/d/t.sql"), "CREATE TABLE d.t (x UInt8)");
    EXPECT_FALSE(hasCaseSensitivityProbe(root));
}

TEST_F(BackupImplCaseInsensitive, AcceptsDeduplicatedFiles)
{
    createBackup(std::make_shared<CaseFoldingBackupWriter>(root.string()));

    writeFiles({{"metadata/d/T.sql", "SELECT 1"}, {"metadata/d/t.sql", "SELECT 1"}});
    finalize();

    EXPECT_TRUE(fs::exists(root / ".backup"));
    size_t num_data_files = 0;
    for (const auto & entry : fs::recursive_directory_iterator(root / "metadata"))
        if (entry.is_regular_file())
            ++num_data_files;
    EXPECT_EQ(num_data_files, 1U);
    EXPECT_FALSE(hasCaseSensitivityProbe(root));
}
