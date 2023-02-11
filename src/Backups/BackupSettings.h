#pragma once

#include <Core/BaseSettings.h>


namespace DB
{

#define LIST_OF_BACKUP_SETTINGS(M) \
    M(Bool, dummy, false, "", 0) \

DECLARE_SETTINGS_TRAITS_ALLOW_CUSTOM_SETTINGS(BackupSettingsTraits, LIST_OF_BACKUP_SETTINGS)

struct BackupSettings : public BaseSettings<BackupSettingsTraits> {};

}
