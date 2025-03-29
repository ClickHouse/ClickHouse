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

// clang-format off

#define STORAGE_OBJECT_STORAGE_RELATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Bool, allow_dynamic_metadata_for_data_lakes, false, R"(
If enabled, indicates that metadata is taken from iceberg specification that is pulled from cloud before each query.
)", 0) \
    DECLARE(Bool, allow_experimental_delta_kernel_rs, false, R"(
If enabled, the engine would use delta-kernel-rs for DeltaLake metadata parsing
)", 0) \
    DECLARE(Bool, delta_lake_read_schema_same_as_table_schema, false, R"(
Whether delta-lake read schema is the same as table schema.
)", 0) \
    DECLARE(String, iceberg_metadata_file_path, "", R"(
Explicit path to desired Iceberg metadata file, should be relative to path in object storage. Make sense for table function use case only.
)", 0) \

// clang-format on

#define LIST_OF_STORAGE_OBJECT_STORAGE_SETTINGS(M, ALIAS) \
    STORAGE_OBJECT_STORAGE_RELATED_SETTINGS(M, ALIAS) \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(StorageObjectStorageSettingsTraits, LIST_OF_STORAGE_OBJECT_STORAGE_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(StorageObjectStorageSettingsTraits, LIST_OF_STORAGE_OBJECT_STORAGE_SETTINGS)

struct StorageObjectStorageSettingsImpl : public BaseSettings<StorageObjectStorageSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
    StorageObjectStorageSettings##TYPE NAME = &StorageObjectStorageSettingsImpl ::NAME;

namespace StorageObjectStorageSetting
{
LIST_OF_STORAGE_OBJECT_STORAGE_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

StorageObjectStorageSettings::StorageObjectStorageSettings() : impl(std::make_unique<StorageObjectStorageSettingsImpl>())
{
}

StorageObjectStorageSettings::StorageObjectStorageSettings(const StorageObjectStorageSettings & settings)
    : impl(std::make_unique<StorageObjectStorageSettingsImpl>(*settings.impl))
{
}

StorageObjectStorageSettings::StorageObjectStorageSettings(StorageObjectStorageSettings && settings) noexcept
    : impl(std::make_unique<StorageObjectStorageSettingsImpl>(std::move(*settings.impl)))
{
}


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
}
