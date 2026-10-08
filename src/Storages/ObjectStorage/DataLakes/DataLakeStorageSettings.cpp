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

DECLARE_SETTINGS_TRAITS(DataLakeStorageSettingsTraits, LIST_OF_DATA_LAKE_STORAGE_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(DataLakeStorageSettingsTraits, LIST_OF_DATA_LAKE_STORAGE_SETTINGS)

struct DataLakeStorageSettingsImpl : public BaseSettings<DataLakeStorageSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, ...) \
    DataLakeStorageSettings##TYPE NAME = &DataLakeStorageSettingsImpl ::NAME;

namespace DataLakeStorageSetting
{
LIST_OF_DATA_LAKE_STORAGE_SETTINGS(INITIALIZE_SETTING_EXTERN, INITIALIZE_SETTING_EXTERN)
}

#undef INITIALIZE_SETTING_EXTERN

DataLakeStorageSettings::DataLakeStorageSettings() : impl(std::make_unique<DataLakeStorageSettingsImpl>())
{
}

DataLakeStorageSettings::DataLakeStorageSettings(const DataLakeStorageSettings & settings)
    : impl(std::make_unique<DataLakeStorageSettingsImpl>(*settings.impl))
{
}

DataLakeStorageSettings::DataLakeStorageSettings(DataLakeStorageSettings && settings) noexcept
    : impl(std::make_unique<DataLakeStorageSettingsImpl>(std::move(*settings.impl)))
{
}


DataLakeStorageSettings::~DataLakeStorageSettings() = default;

STORAGE_DATA_LAKE_STORAGE_SETTINGS_SUPPORTED_TYPES(DataLakeStorageSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)


void DataLakeStorageSettings::loadFromQuery(ASTSetQuery & settings_ast)
{
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
    for (const auto & [name, value] : changes)
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
