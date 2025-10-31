#include <Backups/BackupFileInfo.h>
#include <Backups/IBackupDataFileNameGenerator.h>
#include <Backups/getBackupDataFileNameGenerator.h>

#include <Common/ErrorCodes.h>
#include <Common/Exception.h>

#include <filesystem>
#include <base/hex.h>

#include <cstddef>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{

extern const int ARGUMENT_OUT_OF_BOUND;
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;

}

}

namespace
{
constexpr auto FIRST_FILE_NAME_GENERATOR_NAME = "first_file_name";
constexpr auto CHECKSUM_GENERATOR_NAME = "checksum";

class BackupDataFileNameGeneratorFromChecksum : public DB::IBackupDataFileNameGenerator
{
public:
    explicit BackupDataFileNameGeneratorFromChecksum(size_t prefix_length_)
        : prefix_length(prefix_length_)
    {
        if (prefix_length > 32)
            throw DB::Exception(DB::ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Prefix length {} out of bound", prefix_length);
    }

    std::string getName() const override { return CHECKSUM_GENERATOR_NAME; }

    std::string generate(const DB::BackupFileInfo & file_info) override
    {
        if (file_info.file_name.empty())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Backup file name should not be empty");

        if (file_info.checksum == 0)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Backup checksum should not be zero {}", file_info.file_name);

        auto checksum_hex = getHexUIntLowercase(file_info.checksum);

        return fs::path(checksum_hex.substr(0, prefix_length)) / checksum_hex.substr(prefix_length);
    }

private:
    const size_t prefix_length;
};

class BackupDataFileNameGeneratorFromFirstFileName : public DB::IBackupDataFileNameGenerator
{
public:
    std::string getName() const override { return "first_file_name"; }

    std::string generate(const DB::BackupFileInfo & file_info) override { return file_info.file_name; }
};

}

namespace DB
{

BackupDataFileNameGeneratorPtr
getBackupDataFileNameGenerator(const Poco::Util::AbstractConfiguration & config, const BackupSettings & backup_settings)
{
    /// Plain backup (no deduplication).
    if (!backup_settings.deduplicate_files)
        return std::make_shared<BackupDataFileNameGeneratorFromFirstFileName>();

    std::string generator = backup_settings.data_file_name_generator.empty()
        ? config.getString("backups.data_file_name_generator", FIRST_FILE_NAME_GENERATOR_NAME)
        : backup_settings.data_file_name_generator;

    if (generator != FIRST_FILE_NAME_GENERATOR_NAME && generator != CHECKSUM_GENERATOR_NAME)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unknown backup data file name generator '{}'. Supported values are: '{}' or '{}'",
            generator,
            FIRST_FILE_NAME_GENERATOR_NAME,
            CHECKSUM_GENERATOR_NAME);
    }

    size_t prefix_length = backup_settings.data_file_name_prefix_length;
    if (prefix_length == 0)
        prefix_length = config.getUInt64("backups.data_file_name_prefix_length", generator == CHECKSUM_GENERATOR_NAME ? 3 : 0);

    if (generator == FIRST_FILE_NAME_GENERATOR_NAME && prefix_length != 0)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Non-zero 'data_file_name_prefix_length' ({}) is not allowed when using '{}'",
            prefix_length,
            FIRST_FILE_NAME_GENERATOR_NAME);
    }

    if (generator == FIRST_FILE_NAME_GENERATOR_NAME)
        return std::make_shared<BackupDataFileNameGeneratorFromFirstFileName>();

    return std::make_shared<BackupDataFileNameGeneratorFromChecksum>(prefix_length);
}

}
