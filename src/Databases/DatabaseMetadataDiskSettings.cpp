#include <Databases/DatabaseMetadataDiskSettings.h>

#include <Parsers/ASTCreateQuery.h>
#include <Disks/DiskFromAST.h>
#include <Disks/IDisk.h>
#include <Interpreters/Context.h>
#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTFunction.h>

namespace DB
{
#define LIST_OF_DATABASE_METADATA_DISK_SETTINGS(DECLARE, DECLARE_WITH_ALIAS) \
    DECLARE(String, disk, "", R"(Name of disk storing table metadata files in the database.)", 0) \
    DECLARE(Bool, lazy_load_tables, false, R"(If enabled, tables are not loaded during database startup. Instead, a lightweight proxy is created and the real table is loaded on first access.)", 0) \

DECLARE_SETTINGS_TRAITS(DatabaseMetadataDiskSettingsTraits, LIST_OF_DATABASE_METADATA_DISK_SETTINGS, DATABASE_METADATA_SETTINGS_SUPPORTED_TYPES)

struct DatabaseMetadataDiskSettingsImpl : public BaseSettings<DatabaseMetadataDiskSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def, ContextPtr context, bool is_loading_from_existing_metadata);
};

IMPLEMENT_SETTINGS_TRAITS_CUSTOM_IMPL(DatabaseMetadataDiskSettingsTraits, LIST_OF_DATABASE_METADATA_DISK_SETTINGS, DatabaseMetadataDiskSettings, DatabaseMetadataDiskSetting)

void DatabaseMetadataDiskSettingsImpl::loadFromQuery(ASTStorage & storage_def, ContextPtr context, bool is_loading_from_existing_metadata)
{
    if (!storage_def.settings)
        return;

    auto changes = storage_def.settings->changes;
    auto * value = changes.tryGet("disk");
    if (!value)
    {
        applyChanges(changes);
        return;
    }

    DiskFromAST::convertCustomDiskField(*value, context, is_loading_from_existing_metadata);
    [[maybe_unused]] auto disk = context->getDisk(value->safeGet<String>());
    chassert(disk);

    applyChanges(changes);
}


DatabaseMetadataDiskSettings::DatabaseMetadataDiskSettings() : impl(std::make_unique<DatabaseMetadataDiskSettingsImpl>())
{
}

DatabaseMetadataDiskSettings::DatabaseMetadataDiskSettings(const DatabaseMetadataDiskSettings & settings) : impl(std::make_unique<DatabaseMetadataDiskSettingsImpl>(*settings.impl))
{
}

DatabaseMetadataDiskSettings::DatabaseMetadataDiskSettings(DatabaseMetadataDiskSettings && settings) noexcept = default;

DatabaseMetadataDiskSettings::~DatabaseMetadataDiskSettings() = default;

DATABASE_METADATA_SETTINGS_SUPPORTED_TYPES(DatabaseMetadataDiskSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void DatabaseMetadataDiskSettings::loadFromQuery(ASTStorage & storage_def, ContextPtr context, bool is_loading_from_existing_metadata)
{
    impl->loadFromQuery(storage_def, context, is_loading_from_existing_metadata);
}
}
