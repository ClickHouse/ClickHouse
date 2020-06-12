#pragma once

#include <Core/Defines.h>
#include <Core/SettingsCollection.h>

namespace DB
{

class ASTStorage;

/** Settings for the MySQL Database engine(materialize mode).
  * Could be loaded from a CREATE DATABASE query (SETTINGS clause).
  */
struct MaterializeModeSettings : public SettingsCollection<MaterializeModeSettings>
{
#define LIST_OF_MATERIALIZE_MODE_SETTINGS(M) \
    M(SettingBool, locality_data, false, "", 0) \
    M(SettingUInt64, max_rows_in_buffer, DEFAULT_BLOCK_SIZE, "", 0) \
    M(SettingUInt64, max_bytes_in_buffers, DBMS_DEFAULT_BUFFER_SIZE, "", 0) \
    M(SettingUInt64, max_flush_data_time, 1000, "", 0)  \
    M(SettingUInt64, max_wait_time_when_mysql_unavailable, 1000, "", 0) \

    DECLARE_SETTINGS_COLLECTION(LIST_OF_MATERIALIZE_MODE_SETTINGS)

    void loadFromQuery(ASTStorage & storage_def);
};

}
