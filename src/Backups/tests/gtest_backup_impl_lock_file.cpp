#include <Backups/BackupImpl.h>

#include <Backups/BackupCoordinationLocal.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupFileInfo.h>
#include <Backups/BackupIO_Default.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

#include <unordered_map>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int INVALID_SHARD_ID;
}

namespace
{
struct ExceptionInfo
{
    String message;
    int error_code;
};

class FaultyBackupWriter final : public BackupWriterDefault
{
public:
    FaultyBackupWriter();
    ~FaultyBackupWriter() override = default;

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;

    std::unique_ptr<WriteBuffer> writeFile(const String & file_name) override;

    void copyFile(const String & destination, const String & source, size_t size) override;

    void removeFile(const String & file_name) override;

    size_t getNumberOfThrownExceptions(const String & injection_point);

    std::unordered_map<String, ExceptionInfo> injected_exceptions;
    std::unordered_map<String, size_t> thrown_exceptions;

    static const String FILE_EXISTS_INJECTION_POINT;
    static const String READ_FILE_INJECTION_POINT;

private:
    std::unique_ptr<ReadBuffer> readFile(const String & file_name, size_t expected_file_size) override;

    void checkInjectedException(const String & injection_point);

    std::unordered_map<String, String> files;
};

const String FaultyBackupWriter::FILE_EXISTS_INJECTION_POINT = "fileExists";
const String FaultyBackupWriter::READ_FILE_INJECTION_POINT = "readFile";

FaultyBackupWriter::FaultyBackupWriter()
    : BackupWriterDefault(ReadSettings{}, WriteSettings{}, getLogger("FaultyBackupWriter"))
{}


bool FaultyBackupWriter::fileExists(const String & file_name)
{
    checkInjectedException(FILE_EXISTS_INJECTION_POINT);

    return files.contains(file_name);
}

UInt64 FaultyBackupWriter::getFileSize(const String & file_name)
{
    return files.at(file_name).size();
}

std::unique_ptr<WriteBuffer> FaultyBackupWriter::writeFile(const String & file_name)
{
    return std::make_unique<WriteBufferFromString>(files[file_name]);
}

void FaultyBackupWriter::copyFile(const String & destination, const String & source, size_t)
{
    files[destination] = files.at(source);
}

void FaultyBackupWriter::removeFile(const String & file_name)
{
    files.erase(file_name);
}

size_t FaultyBackupWriter::getNumberOfThrownExceptions(const String & injection_point)
{
    const auto it = thrown_exceptions.find(injection_point);

    return it == thrown_exceptions.end() ? 0 : it->second;
}

std::unique_ptr<ReadBuffer> FaultyBackupWriter::readFile(const String & file_name, size_t)
{
    checkInjectedException(READ_FILE_INJECTION_POINT);

    return std::make_unique<ReadBufferFromString>(files.at(file_name));
}

void FaultyBackupWriter::checkInjectedException(const String & injection_point)
{
    const auto it = injected_exceptions.find(injection_point);
    if (it == injected_exceptions.end())
    {
        return;
    }

    thrown_exceptions[injection_point]++;

    throw Exception(it->second.error_code, "{}", it->second.message);
}

void writeBackupEntry(BackupImpl & backup, const String & backup_entry_data)
{
    BackupFileInfo info;

    info.file_name = "backup";
    info.data_file_name = "data";
    info.data_file_index = 0;
    info.checksum = UInt128(0x1234ABCDULL) << 96;

    auto entry = std::make_shared<BackupEntryFromMemory>(backup_entry_data.data(), backup_entry_data.size());

    backup.writeFile(info, entry);
}
}

class BackupImplLockFile : public testing::TestWithParam<String>
{
public:
    void SetUp() override;
    void TearDown() override;

    void createBackup();

protected:
    std::shared_ptr<FaultyBackupWriter> writer;
    BackupConcurrencyCounters backup_concurrency_counters;
    std::unique_ptr<BackupImpl> backup;
};

void BackupImplLockFile::SetUp()
{
    writer = std::make_shared<FaultyBackupWriter>();
}

void BackupImplLockFile::TearDown()
{
    if (!backup)
    {
        return;
    }

    // ~BackupImpl() asserts that backup is either corrupted or finalized.
    // Prevent assert from firing in case the test failed and backup was neither finalized nor corrupted.
    backup->setIsCorrupted();
}

void BackupImplLockFile::createBackup()
{
    BackupFactory::CreateParams params;
    params.open_mode = IBackup::OpenMode::WRITE;
    params.backup_info = BackupInfo::fromString("S3('http://example.com/backup/')");
    params.context = getContext().context;
    params.backup_uuid = UUIDHelpers::generateV4();
    BackupSettings backup_settings;
    // BackupCoordinationLocal requires checksum name generator
    backup_settings.data_file_name_generator = BackupDataFileNameGeneratorType::Checksum;
    backup_settings.data_file_name_prefix_length = 3;

    params.backup_coordination = std::make_shared<BackupCoordinationLocal>(backup_settings, false, backup_concurrency_counters);

    backup = std::make_unique<BackupImpl>(params, BackupImpl::ArchiveParams{}, writer);
}

TEST_P(BackupImplLockFile, CheckLockFileThrowsDuringWriteFile)
{
    createBackup();
    const auto backup_entry_data = String(128, 'A');
    // Random error code and message, which is unlikely to be thrown by the implementation, to make
    // sure that assert checks the intended exception.
    const int expected_error_code = ErrorCodes::INVALID_SHARD_ID;
    const String expected_error_message = "Failed to read lock file due to invalid shard"; // Random message
    writer->injected_exceptions.emplace(GetParam(), ExceptionInfo{.message = expected_error_message, .error_code = expected_error_code});
    EXPECT_THROW({
        try
        {
            writeBackupEntry(*backup, backup_entry_data);
            FAIL() << "Expected writing the backup to fail";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), expected_error_code) << e.displayText();
            EXPECT_EQ(e.message(), expected_error_message) << e.displayText();
            backup->setIsCorrupted();
            throw;
        }
    }, Exception);

    ASSERT_EQ(writer->getNumberOfThrownExceptions(GetParam()), 1);
}

TEST_P(BackupImplLockFile, CheckLockFileSilencesExceptionDuringRemoveAllFiles)
{
    createBackup();

    const int expected_error_code = ErrorCodes::INVALID_SHARD_ID; // Random error code
    const String expected_error_message = "Failed to read lock file due to invalid shard"; // Random message
    writer->injected_exceptions.emplace(GetParam(), ExceptionInfo{.message = expected_error_message, .error_code = expected_error_code});
    backup->setIsCorrupted();

    ASSERT_FALSE(backup->tryRemoveAllFiles());
    ASSERT_EQ(writer->getNumberOfThrownExceptions(GetParam()), 1);
}

INSTANTIATE_TEST_SUITE_P(CheckLockFileExceptions,
                         BackupImplLockFile,
                         testing::Values(FaultyBackupWriter::FILE_EXISTS_INJECTION_POINT, FaultyBackupWriter::READ_FILE_INJECTION_POINT),
                         [](const testing::TestParamInfo<BackupImplLockFile::ParamType> & param_info)
                         {
                            return param_info.param;
                         });
