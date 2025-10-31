#pragma once

#include <memory>
#include <string>

namespace DB
{

struct BackupFileInfo;

/// Ensures consistent file naming in the backup destination during backup creation. Used in backup creation logic.
class IBackupDataFileNameGenerator
{
public:
    virtual ~IBackupDataFileNameGenerator() = default;

    /// Returns the name of this generator (e.g. "first_file_name" or "checksum").
    virtual std::string getName() const = 0;

    /// Generates a unique and deterministic backup file name
    /// based on the given file info.
    virtual std::string generate(const BackupFileInfo & file_info) = 0;
};

using BackupDataFileNameGeneratorPtr = std::shared_ptr<IBackupDataFileNameGenerator>;

}
