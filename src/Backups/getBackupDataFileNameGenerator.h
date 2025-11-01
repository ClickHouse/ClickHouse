#pragma once

#include <Backups/BackupSettings.h>
#include <Backups/IBackupDataFileNameGenerator.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

BackupDataFileNameGeneratorPtr
getBackupDataFileNameGenerator(const Poco::Util::AbstractConfiguration & config, const BackupSettings & settings);

}
