#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <Backups/BackupSettings.h>
#include <Common/ObjectStorageKeyGenerator.h>

namespace DB
{

ObjectStorageKeysGeneratorPtr getBackupObjectKeyGenerator(const Poco::Util::AbstractConfiguration & config, const BackupSettings & settings);
}
