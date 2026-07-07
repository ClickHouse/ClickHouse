#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>
#include <Storages/System/MutableColumnsAndConstraints.h>
#include <Common/Exception.h>

namespace DB
{

DECLARE_SETTINGS_TRAITS(StorageObjectStorageSettingsTraits, LIST_OF_STORAGE_OBJECT_STORAGE_SETTINGS, STORAGE_OBJECT_STORAGE_SETTINGS_SUPPORTED_TYPES)
IMPLEMENT_SETTINGS_TRAITS(StorageObjectStorageSettingsTraits, LIST_OF_STORAGE_OBJECT_STORAGE_SETTINGS, StorageObjectStorageSettings, StorageObjectStorageSetting)

StorageObjectStorageSettings::StorageObjectStorageSettings() : impl(std::make_unique<StorageObjectStorageSettingsImpl>())
{
}

StorageObjectStorageSettings::StorageObjectStorageSettings(const StorageObjectStorageSettings & settings)
    : impl(std::make_unique<StorageObjectStorageSettingsImpl>(*settings.impl))
{
}

StorageObjectStorageSettings::StorageObjectStorageSettings(StorageObjectStorageSettings && settings) noexcept = default;


StorageObjectStorageSettings::~StorageObjectStorageSettings() = default;

STORAGE_OBJECT_STORAGE_SETTINGS_SUPPORTED_TYPES(StorageObjectStorageSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)


void StorageObjectStorageSettings::loadFromQuery(ASTSetQuery & settings_ast)
{
    impl->applyChanges(settings_ast.changes);
}

Field StorageObjectStorageSettings::get(const std::string & name)
{
    return impl->get(name);
}

bool StorageObjectStorageSettings::hasBuiltin(std::string_view name)
{
    return StorageObjectStorageSettingsImpl::hasBuiltin(name);
}

void StorageObjectStorageSettings::loadFromSettingsChanges(const SettingsChanges & changes)
{
    for (const auto & [name, value] : changes)
    {
        if (impl->has(name))
            impl->set(name, value);
    }
}

}
