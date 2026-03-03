#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Databases/DataLake/DatabaseDataLakeSettings.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define DATABASE_ICEBERG_RELATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(DatabaseDataLakeCatalogType, catalog_type, DatabaseDataLakeCatalogType::ICEBERG_REST, "Catalog type", 0) \
    DECLARE(String, catalog_credential, "", "", 0)             \
    DECLARE(Bool, vended_credentials, true, "Use vended credentials (storage credentials) from catalog", 0)             \
    DECLARE(String, auth_scope, "PRINCIPAL_ROLE:ALL", "Authorization scope for client credentials or token exchange", 0)             \
    DECLARE(String, oauth_server_uri, "", "OAuth server uri", 0)             \
    DECLARE(String, warehouse, "", "Warehouse name inside the catalog", 0)             \
    DECLARE(String, auth_header, "", "Authorization header of format 'Authorization: <scheme> <auth_info>'", 0)           \
    DECLARE(String, aws_access_key_id, "", "Key for AWS connection for Glue catalog", 0)           \
    DECLARE(String, aws_secret_access_key, "", "Key for AWS connection for Glue Catalog'", 0)           \
    DECLARE(String, region, "", "Region for Glue catalog", 0)           \
    DECLARE(String, storage_endpoint, "", "Object storage endpoint", 0) \

#define LIST_OF_DATABASE_ICEBERG_SETTINGS(M, ALIAS) \
    DATABASE_ICEBERG_RELATED_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(DatabaseDataLakeSettingsTraits, LIST_OF_DATABASE_ICEBERG_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(DatabaseDataLakeSettingsTraits, LIST_OF_DATABASE_ICEBERG_SETTINGS)

struct DatabaseDataLakeSettingsImpl : public BaseSettings<DatabaseDataLakeSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
    DatabaseDataLakeSettings##TYPE NAME = &DatabaseDataLakeSettingsImpl ::NAME;

namespace DatabaseDataLakeSetting
{
LIST_OF_DATABASE_ICEBERG_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

DatabaseDataLakeSettings::DatabaseDataLakeSettings() : impl(std::make_unique<DatabaseDataLakeSettingsImpl>())
{
}

DatabaseDataLakeSettings::DatabaseDataLakeSettings(const DatabaseDataLakeSettings & settings)
    : impl(std::make_unique<DatabaseDataLakeSettingsImpl>(*settings.impl))
{
}

DatabaseDataLakeSettings::DatabaseDataLakeSettings(DatabaseDataLakeSettings && settings) noexcept
    : impl(std::make_unique<DatabaseDataLakeSettingsImpl>(std::move(*settings.impl)))
{
}

DatabaseDataLakeSettings::~DatabaseDataLakeSettings() = default;

DATABASE_ICEBERG_SETTINGS_SUPPORTED_TYPES(DatabaseDataLakeSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)


void DatabaseDataLakeSettings::applyChanges(const SettingsChanges & changes)
{
    impl->applyChanges(changes);
}

void DatabaseDataLakeSettings::loadFromQuery(const ASTStorage & storage_def)
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
