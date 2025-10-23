#pragma once

#include <memory>
#include <string>

namespace DB
{

struct BackupFileInfo;

class IBackupDataFileNameGenerator
{
public:
    virtual ~IBackupDataFileNameGenerator() = default;
    virtual std::string getName() const = 0;
    virtual std::string generate(const BackupFileInfo & file_info) = 0;
};

using BackupDataFileNameGeneratorPtr = std::shared_ptr<IBackupDataFileNameGenerator>;

}
