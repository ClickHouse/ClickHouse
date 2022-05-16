#pragma once

#include <Core/Defines.h>
#include <Core/BaseSettings.h>

namespace DB
{

class ASTStorage;

#define LIST_OF_MATERIALIZE_MODE_SETTINGS(M) \
    M(UInt64, max_rows_in_buffer, DEFAULT_BLOCK_SIZE, "Max rows that data is allowed to cache in memory(for single table and the cache data unable to query). when rows is exceeded, the data will be materialized", 0) \
    M(UInt64, max_bytes_in_buffer, DBMS_DEFAULT_BUFFER_SIZE, "Max bytes that data is allowed to cache in memory(for single table and the cache data unable to query). when rows is exceeded, the data will be materialized", 0) \
    M(UInt64, max_rows_in_buffers, DEFAULT_BLOCK_SIZE, "Max rows that data is allowed to cache in memory(for database and the cache data unable to query). when rows is exceeded, the data will be materialized", 0) \
    M(UInt64, max_bytes_in_buffers, DBMS_DEFAULT_BUFFER_SIZE, "Max bytes that data is allowed to cache in memory(for database and the cache data unable to query). when rows is exceeded, the data will be materialized", 0) \
    M(UInt64, max_flush_data_time, 1000, "Max milliseconds that data is allowed to cache in memory(for database and the cache data unable to query). when this time is exceeded, the data will be materialized", 0)  \
    M(Int64, max_wait_time_when_mysql_unavailable, 1000, "Retry interval when MySQL is not available (milliseconds). Negative value disable retry.", 0) \
    M(Bool, allows_query_when_mysql_lost, false, "Allow query materialized table when mysql is lost.", 0) \
    M(String, materialized_mysql_tables_list, "", "a comma-separated list of mysql database tables, which will be replicated by MaterializedMySQL database engine. Default value: empty list — means whole tables will be replicated.", 0) \

    DECLARE_SETTINGS_TRAITS(MaterializedMySQLSettingsTraits, LIST_OF_MATERIALIZE_MODE_SETTINGS)


/** Settings for the MaterializedMySQL database engine.
  * Could be loaded from a CREATE DATABASE query (SETTINGS clause).
  */
struct MaterializedMySQLSettings : public BaseSettings<MaterializedMySQLSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
