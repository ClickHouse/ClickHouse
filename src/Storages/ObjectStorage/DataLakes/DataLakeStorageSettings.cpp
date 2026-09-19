#include <Storages/SettingsWithRecordedOrigin.h>
#include <Storages/enumerateSettingsFromImpl.h>
#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/System/MutableColumnsAndConstraints.h>
#include <Common/Exception.h>

namespace DB
{

DECLARE_SETTINGS_TRAITS(DataLakeStorageSettingsTraits, LIST_OF_DATA_LAKE_STORAGE_SETTINGS, STORAGE_DATA_LAKE_STORAGE_SETTINGS_SUPPORTED_TYPES)
struct DataLakeStorageSettingsImpl : public SettingsWithRecordedOrigin<DataLakeStorageSettingsTraits>
{
};
IMPLEMENT_SETTINGS_TRAITS_CUSTOM_IMPL(DataLakeStorageSettingsTraits, LIST_OF_DATA_LAKE_STORAGE_SETTINGS, DataLakeStorageSettings, DataLakeStorageSetting)

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
    /// The table's own `SETTINGS` clause, recorded as the definition. A data lake catalog's table takes its
    /// settings from the database instead, through `loadFromSettingsChanges`, which records nothing.
    impl->applyChangesWithOrigin(settings_ast.changes, SettingOrigin::Definition);
}

Field DataLakeStorageSettings::get(const std::string & name)
{
    return impl->get(name);
}

bool DataLakeStorageSettings::isChanged(std::string_view name) const
{
    return impl->isChanged(name);
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

IMPLEMENT_SETTINGS_ENUMERATION(DataLakeStorageSettings)

}
