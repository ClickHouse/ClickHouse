#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Databases/MySQL/MaterializedMySQLSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define LIST_OF_MATERIALIZE_MODE_SETTINGS(DECLARE, ALIAS) \
    DECLARE(UInt64, max_rows_in_buffer, DEFAULT_BLOCK_SIZE, "Max rows that data is allowed to cache in memory(for single table and the cache data unable to query). when rows is exceeded, the data will be materialized", 0) \
    DECLARE(UInt64, max_bytes_in_buffer, DBMS_DEFAULT_BUFFER_SIZE, "Max bytes that data is allowed to cache in memory(for single table and the cache data unable to query). when rows is exceeded, the data will be materialized", 0) \
    DECLARE(UInt64, max_rows_in_buffers, DEFAULT_BLOCK_SIZE, "Max rows that data is allowed to cache in memory(for database and the cache data unable to query). when rows is exceeded, the data will be materialized", 0) \
    DECLARE(UInt64, max_bytes_in_buffers, DBMS_DEFAULT_BUFFER_SIZE, "Max bytes that data is allowed to cache in memory(for database and the cache data unable to query). when rows is exceeded, the data will be materialized", 0) \
    DECLARE(UInt64, max_flush_data_time, 1000, "Max milliseconds that data is allowed to cache in memory(for database and the cache data unable to query). when this time is exceeded, the data will be materialized", 0)  \
    DECLARE(Int64, max_wait_time_when_mysql_unavailable, 1000, "Retry interval when MySQL is not available (milliseconds). Negative value disable retry.", 0) \
    DECLARE(Bool, allows_query_when_mysql_lost, false, "Allow query materialized table when mysql is lost.", 0) \
    DECLARE(String, materialized_mysql_tables_list, "", "a comma-separated list of mysql database tables, which will be replicated by MaterializedMySQL database engine. Default value: empty list — means whole tables will be replicated.", 0) \
    DECLARE(Bool, use_binlog_client, false, "Use MySQL Binlog Client.", 0) \
    DECLARE(UInt64, max_bytes_in_binlog_queue, 64 * 1024 * 1024, "Max bytes in binlog's queue created from MySQL Binlog Client.", 0) \
    DECLARE(UInt64, max_milliseconds_to_wait_in_binlog_queue, 10000, "Max milliseconds to wait when max bytes exceeded in a binlog queue.", 0) \
    DECLARE(UInt64, max_bytes_in_binlog_dispatcher_buffer, DBMS_DEFAULT_BUFFER_SIZE, "Max bytes in the binlog dispatcher's buffer before it is flushed to attached binlogs.", 0) \
    DECLARE(UInt64, max_flush_milliseconds_in_binlog_dispatcher, 1000, "Max milliseconds in the binlog dispatcher's buffer to wait before it is flushed to attached binlogs.", 0) \
    DECLARE(Bool, allow_startup_database_without_connection_to_mysql, false, "Allow to create and attach database without available connection to MySQL.", 0) \

DECLARE_SETTINGS_TRAITS(MaterializedMySQLSettingsTraits, LIST_OF_MATERIALIZE_MODE_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(MaterializedMySQLSettingsTraits, LIST_OF_MATERIALIZE_MODE_SETTINGS)

struct MaterializedMySQLSettingsImpl : public BaseSettings<MaterializedMySQLSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
    MaterializedMySQLSettings##TYPE NAME = &MaterializedMySQLSettingsImpl ::NAME;

namespace MaterializedMySQLSetting
{
LIST_OF_MATERIALIZE_MODE_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

MaterializedMySQLSettings::MaterializedMySQLSettings() : impl(std::make_unique<MaterializedMySQLSettingsImpl>())
{
}

MaterializedMySQLSettings::MaterializedMySQLSettings(const MaterializedMySQLSettings & settings)
    : impl(std::make_unique<MaterializedMySQLSettingsImpl>(*settings.impl))
{
}

MaterializedMySQLSettings::MaterializedMySQLSettings(MaterializedMySQLSettings && settings) noexcept
    : impl(std::make_unique<MaterializedMySQLSettingsImpl>(std::move(*settings.impl)))
{
}

MaterializedMySQLSettings::~MaterializedMySQLSettings() = default;

MATERIALIZED_MYSQL_SETTINGS_SUPPORTED_TYPES(MaterializedMySQLSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)


void MaterializedMySQLSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            impl->applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for database " + storage_def.engine->name);
            throw;
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }
}

}
