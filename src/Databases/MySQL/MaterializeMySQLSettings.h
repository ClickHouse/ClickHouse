#pragma once

#include <Core/Defines.h>
#include <Core/BaseSettings.h>

namespace DB
{

class ASTStorage;

#define LIST_OF_MATERIALIZE_MODE_SETTINGS(M) \
    M(UInt64, max_rows_in_buffer, DEFAULT_BLOCK_SIZE, "", 0) \
    M(UInt64, max_bytes_in_buffer, DBMS_DEFAULT_BUFFER_SIZE, "", 0) \
    M(UInt64, max_rows_in_buffers, DEFAULT_BLOCK_SIZE, "", 0) \
    M(UInt64, max_bytes_in_buffers, DBMS_DEFAULT_BUFFER_SIZE, "", 0) \
    M(UInt64, max_flush_data_time, 1000, "", 0)  \
    M(UInt64, max_wait_time_when_mysql_unavailable, 1000, "", 0) \

    DECLARE_SETTINGS_TRAITS(MaterializeMySQLSettingsTraits, LIST_OF_MATERIALIZE_MODE_SETTINGS)


/** Settings for the MaterializeMySQL database engine.
  * Could be loaded from a CREATE DATABASE query (SETTINGS clause).
  */
struct MaterializeMySQLSettings : public BaseSettings<MaterializeMySQLSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
