#pragma once

#include <Core/BaseSettings.h>


namespace DB
{

#define LIST_OF_BACKUP_SETTINGS(M) \
    M(String, base_backup, "", "Name of the base backup. Only differences made after the base backup will be included in a newly created backup, so this option allows to make an incremental backup.", 0) \

DECLARE_SETTINGS_TRAITS_ALLOW_CUSTOM_SETTINGS(BackupSettingsTraits, LIST_OF_BACKUP_SETTINGS)

struct BackupSettings : public BaseSettings<BackupSettingsTraits> {};

}
