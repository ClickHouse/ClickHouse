#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/System/MutableColumnsAndConstraints.h>
#include <Common/Exception.h>

#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

DECLARE_SETTINGS_TRAITS(DataLakeStorageSettingsTraits, LIST_OF_DATA_LAKE_STORAGE_SETTINGS, STORAGE_DATA_LAKE_STORAGE_SETTINGS_SUPPORTED_TYPES)
IMPLEMENT_SETTINGS_TRAITS(DataLakeStorageSettingsTraits, LIST_OF_DATA_LAKE_STORAGE_SETTINGS, DataLakeStorageSettings, DataLakeStorageSetting)

namespace
{
/// Settings that belong to the DataLakeCatalog database engine. Users often pass them to Iceberg/DeltaLake
/// table engines by mistake (especially `catalog_type`), which previously produced a bare UNKNOWN_SETTING.
void throwIfDataLakeCatalogDatabaseSetting(std::string_view name)
{
    static const std::unordered_set<std::string_view> database_catalog_settings = {
        "catalog_type",
        "catalog_credential",
        "vended_credentials",
        "auth_scope",
        "oauth_server_uri",
        "oauth_server_use_request_body",
        "warehouse",
        "auth_header",
        "aws_access_key_id",
        "aws_secret_access_key",
        "region",
        "aws_role_arn",
        "aws_role_session_name",
        "aws_external_id",
        "storage_endpoint",
    };

    if (!database_catalog_settings.contains(name))
        return;

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Setting '{}' is a database engine setting for DataLakeCatalog, not a table engine setting. "
        "To integrate with a data catalog, create a database with ENGINE = DataLakeCatalog and put catalog "
        "settings there. See https://clickhouse.com/docs/engines/database-engines/datalakecatalog",
        String(name));
}
}

DataLakeStorageSettings::DataLakeStorageSettings() : impl(std::make_unique<DataLakeStorageSettingsImpl>())
{
}

DataLakeStorageSettings::DataLakeStorageSettings(const DataLakeStorageSettings & settings)
    : impl(std::make_unique<DataLakeStorageSettingsImpl>(*settings.impl))
{
}

DataLakeStorageSettings::DataLakeStorageSettings(DataLakeStorageSettings && settings) noexcept = default;


DataLakeStorageSettings::~DataLakeStorageSettings() = default;

STORAGE_DATA_LAKE_STORAGE_SETTINGS_SUPPORTED_TYPES(DataLakeStorageSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)


void DataLakeStorageSettings::loadFromQuery(ASTSetQuery & settings_ast)
{
    for (const auto & change : settings_ast.changes)
        throwIfDataLakeCatalogDatabaseSetting(change.name);

    impl->applyChanges(settings_ast.changes);
}

Field DataLakeStorageSettings::get(const std::string & name)
{
    return impl->get(name);
}

bool DataLakeStorageSettings::hasBuiltin(std::string_view name)
{
    return DataLakeStorageSettingsImpl::hasBuiltin(name);
}

void DataLakeStorageSettings::loadFromSettingsChanges(const SettingsChanges & changes)
{
    for (const auto & [name, value, _] : changes)
    {
        if (impl->has(name))
            impl->set(name, value);
    }
}

void DataLakeStorageSettings::serialize(WriteBuffer & out) const
{
    impl->writeChangedBinary(out);
}

DataLakeStorageSettings DataLakeStorageSettings::deserialize(ReadBuffer & in)
{
    DataLakeStorageSettings result;
    result.impl = std::make_unique<DataLakeStorageSettingsImpl>();
    result.impl->readBinary(in);

    return result;
}

}
