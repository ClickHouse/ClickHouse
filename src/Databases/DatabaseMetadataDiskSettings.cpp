#include <Databases/DatabaseMetadataDiskSettings.h>

#include <Parsers/ASTCreateQuery.h>
#include <Disks/DiskFromAST.h>
#include <Parsers/isDiskFunction.h>
#include <Parsers/FieldFromAST.h>
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

DECLARE_SETTINGS_TRAITS(DatabaseMetadataDiskSettingsTraits, LIST_OF_DATABASE_METADATA_DISK_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(DatabaseMetadataDiskSettingsTraits, LIST_OF_DATABASE_METADATA_DISK_SETTINGS)

struct DatabaseMetadataDiskSettingsImpl : public BaseSettings<DatabaseMetadataDiskSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def, ContextPtr context, bool is_attach);
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, ...) \
    DatabaseMetadataDiskSettings##TYPE NAME = &DatabaseMetadataDiskSettingsImpl ::NAME;

namespace DatabaseMetadataDiskSetting
{
LIST_OF_DATABASE_METADATA_DISK_SETTINGS(INITIALIZE_SETTING_EXTERN, INITIALIZE_SETTING_EXTERN)
}

#undef INITIALIZE_SETTING_EXTERN

void DatabaseMetadataDiskSettingsImpl::loadFromQuery(ASTStorage & storage_def, ContextPtr context, bool is_attach)
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

    ASTPtr value_as_custom_ast = nullptr;
    CustomType custom;
    if (value->tryGet<CustomType>(custom) && 0 == strcmp(custom.getTypeName(), "AST"))
        value_as_custom_ast = dynamic_cast<const FieldFromASTImpl &>(custom.getImpl()).ast;

    if (value_as_custom_ast && isDiskFunction(value_as_custom_ast))
    {
        auto disk_name = DiskFromAST::createCustomDisk(value_as_custom_ast, context, is_attach);
        *value = disk_name;
    }
    else
    {
        DiskFromAST::ensureDiskIsNotCustom(value->safeGet<String>(), context);
    }
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

DatabaseMetadataDiskSettings::DatabaseMetadataDiskSettings(DatabaseMetadataDiskSettings && settings) noexcept
    : impl(std::make_unique<DatabaseMetadataDiskSettingsImpl>(std::move(*settings.impl)))
{
}

DatabaseMetadataDiskSettings::~DatabaseMetadataDiskSettings() = default;

DATABASE_METADATA_SETTINGS_SUPPORTED_TYPES(DatabaseMetadataDiskSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void DatabaseMetadataDiskSettings::loadFromQuery(ASTStorage & storage_def, ContextPtr context, bool is_attach)
{
    impl->loadFromQuery(storage_def, context, is_attach);
}
}
