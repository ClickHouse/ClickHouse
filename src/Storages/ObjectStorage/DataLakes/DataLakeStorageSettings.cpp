#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Databases/DataLake/DatabaseDataLakeSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/System/MutableColumnsAndConstraints.h>
#include <Common/Exception.h>

#include <array>
#include <vector>

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
/// Catalog settings (e.g. `catalog_type`, `region`, AWS creds) belong to the DataLakeCatalog database
/// engine, not to Iceberg/DeltaLake table engines. Users often pass them to a table by mistake, which
/// previously produced a bare UNKNOWN_SETTING. The set is derived from DatabaseDataLakeSettings so that
/// any catalog setting missing from the table-engine settings is caught, and new ones cannot drift back
/// to UNKNOWN_SETTING.
void throwIfDataLakeCatalogDatabaseSetting(std::string_view name)
{
    if (!DatabaseDataLakeSettings::hasBuiltin(name) || DataLakeStorageSettings::hasBuiltin(name))
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

std::vector<std::string_view> DataLakeStorageSettings::getChangedDeprecatedCatalogSettings() const
{
    /// The `storage_*`/`object_storage_*` aliases that mirror DataLakeCatalog database catalog settings.
    /// Keep in sync with the catalog aliases declared in DATA_LAKE_STORAGE_RELATED_SETTINGS.
    static constexpr std::array deprecated_catalog_settings = {
        "storage_catalog_type",
        "storage_catalog_url",
        "storage_catalog_credential",
        "storage_warehouse",
        "storage_auth_scope",
        "storage_auth_header",
        "storage_oauth_server_uri",
        "storage_oauth_server_use_request_body",
        "storage_aws_access_key_id",
        "storage_aws_secret_access_key",
        "storage_region",
        "storage_aws_role_arn",
        "storage_aws_role_session_name",
        "object_storage_endpoint",
    };

    std::vector<std::string_view> changed;
    for (const auto * name : deprecated_catalog_settings)
    {
        if (impl->isChanged(name))
            changed.emplace_back(name);
    }
    return changed;
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
