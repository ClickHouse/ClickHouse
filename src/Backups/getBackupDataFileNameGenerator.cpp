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
constexpr size_t DEFAULT_BACKUP_PREFIX_LENGTH = 0;

class BackupDataFileNameGeneratorWithChecksumHexPrefix : public DB::IBackupDataFileNameGenerator
{
public:
    explicit BackupDataFileNameGeneratorWithChecksumHexPrefix(size_t prefix_length_)
        : prefix_length(prefix_length_)
    {
        if (prefix_length > 32)
            throw DB::Exception(DB::ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Prefix length {} out of bound", prefix_length);
    }

    std::string getName() const override { return std::string("checksum_hex_") + std::to_string(prefix_length); }

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

class BackupDataFileNameGeneratorDefault : public DB::IBackupDataFileNameGenerator
{
public:
    std::string getName() const override { return "default"; }

    std::string generate(const DB::BackupFileInfo & file_info) override { return file_info.file_name; }
};

}

namespace DB
{

BackupDataFileNameGeneratorPtr
getBackupDataFileNameGenerator(const Poco::Util::AbstractConfiguration & config, const BackupSettings & backup_settings)
{
    /// Plain backup.
    if (!backup_settings.deduplicate_files)
        return std::make_shared<BackupDataFileNameGeneratorDefault>();

    size_t prefix_length = backup_settings.key_prefix_length;

    if (prefix_length == 0)
        prefix_length = config.getUInt64("backups.key_prefix_length", DEFAULT_BACKUP_PREFIX_LENGTH);

    if (prefix_length == 0)
        return std::make_shared<BackupDataFileNameGeneratorDefault>();

    return std::make_shared<BackupDataFileNameGeneratorWithChecksumHexPrefix>(prefix_length);
}

BackupDataFileNameGeneratorPtr getBackupDataFileNameGenerator(const std::string & generator_name)
{
    std::string checksum_hex_prefix = "checksum_hex_";
    if (generator_name.starts_with(checksum_hex_prefix))
    {
        const std::string length_part = generator_name.substr(checksum_hex_prefix.size());
        auto prefix_length = std::stoi(length_part);
        return std::make_shared<BackupDataFileNameGeneratorWithChecksumHexPrefix>(prefix_length);
    }
    else if (generator_name == "default")
    {
        return std::make_shared<BackupDataFileNameGeneratorDefault>();
    }
    throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown data file name generator '{}'", generator_name);
}

}
