#pragma once

#include <Core/SettingsEnums.h>


namespace DB
{

/// Defines how backup data file names are generated.
/// - `FirstFileName`: use the original file name from BackupFileInfo.
/// - `Checksum`: derive the name from the file checksum.
enum class BackupDataFileNameGeneratorType : uint8_t
{
    FirstFileName = 0,
    Checksum = 1,
};

DECLARE_SETTING_ENUM(BackupDataFileNameGeneratorType)
}
