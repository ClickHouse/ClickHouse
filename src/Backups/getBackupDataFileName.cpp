#include <Backups/getBackupDataFileName.h>

#include <Common/ErrorCodes.h>
#include <Common/Exception.h>

#include <filesystem>
#include <base/defines.h>
#include <base/hex.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

std::string
getBackupDataFileName(const BackupFileInfo & file_info, BackupDataFileNameGeneratorType data_file_name_generator, size_t prefix_length)
{
    switch (data_file_name_generator)
    {
        case BackupDataFileNameGeneratorType::FirstFileName:
            return file_info.file_name;
        case BackupDataFileNameGeneratorType::Checksum: {
            if (file_info.checksum == 0)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Backup checksum should not be zero ({})", file_info.file_name);

            std::string checksum_hex = getHexUIntLowercase(file_info.checksum);
            if (prefix_length == 0 || prefix_length >= checksum_hex.size())
                return checksum_hex;

            const std::string_view prefix = {checksum_hex.data(), prefix_length};
            const std::string_view suffix = {checksum_hex.data() + prefix_length, checksum_hex.size() - prefix_length};
            return (fs::path(prefix) / suffix).string();
        }
    }
    UNREACHABLE();
}

}
