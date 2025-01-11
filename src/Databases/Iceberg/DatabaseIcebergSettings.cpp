#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Databases/Iceberg/DatabaseIcebergSettings.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define DATABASE_ICEBERG_RELATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(DatabaseIcebergCatalogType, catalog_type, DatabaseIcebergCatalogType::REST, "Catalog type", 0) \
    DECLARE(String, catalog_credential, "", "", 0)             \
    DECLARE(Bool, vended_credentials, true, "Use vended credentials (storage credentials) from catalog", 0)             \
    DECLARE(String, auth_scope, "PRINCIPAL_ROLE:ALL", "Authorization scope for client credentials or token exchange", 0)             \
    DECLARE(String, oauth_server_uri, "", "OAuth server uri", 0)             \
    DECLARE(String, warehouse, "", "Warehouse name inside the catalog", 0)             \
    DECLARE(String, auth_header, "", "Authorization header of format 'Authorization: <scheme> <auth_info>'", 0)             \
    DECLARE(String, storage_endpoint, "", "Object storage endpoint", 0) \

#define LIST_OF_DATABASE_ICEBERG_SETTINGS(M, ALIAS) \
    DATABASE_ICEBERG_RELATED_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(DatabaseIcebergSettingsTraits, LIST_OF_DATABASE_ICEBERG_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(DatabaseIcebergSettingsTraits, LIST_OF_DATABASE_ICEBERG_SETTINGS)

struct DatabaseIcebergSettingsImpl : public BaseSettings<DatabaseIcebergSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
    DatabaseIcebergSettings##TYPE NAME = &DatabaseIcebergSettingsImpl ::NAME;

namespace DatabaseIcebergSetting
{
LIST_OF_DATABASE_ICEBERG_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

DatabaseIcebergSettings::DatabaseIcebergSettings() : impl(std::make_unique<DatabaseIcebergSettingsImpl>())
{
}

DatabaseIcebergSettings::DatabaseIcebergSettings(const DatabaseIcebergSettings & settings)
    : impl(std::make_unique<DatabaseIcebergSettingsImpl>(*settings.impl))
{
}

DatabaseIcebergSettings::DatabaseIcebergSettings(DatabaseIcebergSettings && settings) noexcept
    : impl(std::make_unique<DatabaseIcebergSettingsImpl>(std::move(*settings.impl)))
{
}

DatabaseIcebergSettings::~DatabaseIcebergSettings() = default;

DATABASE_ICEBERG_SETTINGS_SUPPORTED_TYPES(DatabaseIcebergSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)


void DatabaseIcebergSettings::applyChanges(const SettingsChanges & changes)
{
    impl->applyChanges(changes);
}

void DatabaseIcebergSettings::loadFromQuery(const ASTStorage & storage_def)
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
                e.addMessage("for database engine " + storage_def.engine->name);
            throw;
        }
    }
}

}
