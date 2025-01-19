#include <Storages/PostgreSQL/MaterializedPostgreSQLSettings.h>

#if USE_LIBPQXX

#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define LIST_OF_MATERIALIZED_POSTGRESQL_SETTINGS(DECLARE, ALIAS) \
    DECLARE(UInt64, materialized_postgresql_max_block_size, 65536, "Number of row collected before flushing data into table.", 0) \
    DECLARE(String, materialized_postgresql_tables_list, "", "List of tables for MaterializedPostgreSQL database engine", 0) \
    DECLARE(String, materialized_postgresql_schema_list, "", "List of schemas for MaterializedPostgreSQL database engine", 0) \
    DECLARE(String, materialized_postgresql_replication_slot, "", "A user-created replication slot", 0) \
    DECLARE(String, materialized_postgresql_snapshot, "", "User provided snapshot in case he manages replication slots himself", 0) \
    DECLARE(String, materialized_postgresql_schema, "", "PostgreSQL schema", 0) \
    DECLARE(Bool, materialized_postgresql_tables_list_with_schema, false, \
        "Consider by default that if there is a dot in tables list 'name.name', " \
        "then the first name is postgres schema and second is postgres table. This setting is needed to allow table names with dots", 0) \
    DECLARE(UInt64, materialized_postgresql_backoff_min_ms, 200, "Poll backoff start point", 0) \
    DECLARE(UInt64, materialized_postgresql_backoff_max_ms, 10000, "Poll backoff max point", 0) \
    DECLARE(UInt64, materialized_postgresql_backoff_factor, 2, "Poll backoff factor", 0) \
    DECLARE(Bool, materialized_postgresql_use_unique_replication_consumer_identifier, false, "Should a unique consumer be registered for table replication", 0) \

DECLARE_SETTINGS_TRAITS(MaterializedPostgreSQLSettingsTraits, LIST_OF_MATERIALIZED_POSTGRESQL_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(MaterializedPostgreSQLSettingsTraits, LIST_OF_MATERIALIZED_POSTGRESQL_SETTINGS)

struct MaterializedPostgreSQLSettingsImpl : public BaseSettings<MaterializedPostgreSQLSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) MaterializedPostgreSQLSettings##TYPE NAME = &MaterializedPostgreSQLSettingsImpl ::NAME;

namespace MaterializedPostgreSQLSetting
{
LIST_OF_MATERIALIZED_POSTGRESQL_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

MaterializedPostgreSQLSettings::MaterializedPostgreSQLSettings() : impl(std::make_unique<MaterializedPostgreSQLSettingsImpl>())
{
}

MaterializedPostgreSQLSettings::MaterializedPostgreSQLSettings(const MaterializedPostgreSQLSettings & settings)
    : impl(std::make_unique<MaterializedPostgreSQLSettingsImpl>(*settings.impl))
{
}

MaterializedPostgreSQLSettings::MaterializedPostgreSQLSettings(MaterializedPostgreSQLSettings && settings) noexcept
    : impl(std::make_unique<MaterializedPostgreSQLSettingsImpl>(std::move(*settings.impl)))
{
}

MaterializedPostgreSQLSettings::~MaterializedPostgreSQLSettings() = default;

MATERIALIZED_POSTGRESQL_SETTINGS_SUPPORTED_TYPES(MaterializedPostgreSQLSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void MaterializedPostgreSQLSettings::applyChange(const SettingChange & change)
{
    impl->applyChange(change);
}

bool MaterializedPostgreSQLSettings::has(std::string_view name) const
{
    return impl->has(name);
}

void MaterializedPostgreSQLSettings::loadFromQuery(ASTStorage & storage_def)
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
                e.addMessage("for storage " + storage_def.engine->name);
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

#endif
