#include <Storages/MergeTree/MergeTreeSettingsDisks.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

#include <Common/Exception.h>
#include <Common/SettingsChanges.h>
#include <Common/logger_useful.h>
#include <Disks/DiskFromAST.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Interpreters/Context.h>
#include <Parsers/FieldFromAST.h>
#include <Parsers/isDiskFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static void validateTableDisk(const DiskPtr & disk)
{
    if (!disk)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MergeTree settings `table_disk` requires `disk` setting.");

    const auto description = disk->getDataSourceDescription();
    if (description.type != DataSourceType::ObjectStorage)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MergeTree settings `table_disk` is not supported for non-ObjectStorage disks");

    /// table_disk loads the table straight from the disk root (no database/UUID path), so its metadata must be
    /// reconstructable from the object storage alone; random blob keys (Local, Keeper) keep the map elsewhere.
    const auto metadata_storage = disk->getMetadataStorage();
    if (metadata_storage->areBlobPathsRandom())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "MergeTree settings `table_disk` is not supported for {}: it requires metadata stored on the object storage.",
            description.toString());
}

bool checkMergeTreeDiskSettings(const SettingsChanges & changes, ContextPtr context, bool is_loading_from_existing_metadata)
{
    bool found_disk_setting = false;
    bool found_storage_policy_setting = false;
    bool table_disk = false;
    DiskPtr disk;

    for (const auto & [name, value, _] : changes)
    {
        if (name == "disk")
        {
            disk = context->getDisk(value.safeGet<String>());
            found_disk_setting = true;
        }
        else if (name == "storage_policy")
            found_storage_policy_setting = true;
        else if (name == "table_disk")
            table_disk = value.safeGet<bool>();

        if (!is_loading_from_existing_metadata && found_disk_setting && found_storage_policy_setting)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "MergeTree settings `storage_policy` and `disk` cannot be specified at the same time");
        }
    }

    if (table_disk)
        validateTableDisk(disk);

    return found_disk_setting;
}

void MergeTreeSettings::resolveDiskSetting(
    SettingsChanges & changes, ContextPtr context, bool is_loading_from_existing_metadata, bool for_system_database, bool stored_definition)
{
    for (auto & change : changes)
        resolveDiskSetting(change, context, is_loading_from_existing_metadata, for_system_database, stored_definition);
}

void MergeTreeSettings::resolveDiskSetting(
    SettingChange & change, ContextPtr context, bool is_loading_from_existing_metadata, bool for_system_database, bool stored_definition)
{
    if (change.name != "disk")
        return;

    CustomType custom;
    ASTPtr value_as_custom_ast = nullptr;
    if (change.value.tryGet<CustomType>(custom) && 0 == strcmp(custom.getTypeName(), "AST"))
        value_as_custom_ast = dynamic_cast<const FieldFromASTImpl &>(custom.getImpl()).ast;

    if (value_as_custom_ast && isDiskFunction(value_as_custom_ast))
    {
        auto disk_name = DiskFromAST::createCustomDisk(
            value_as_custom_ast, context, is_loading_from_existing_metadata, for_system_database, stored_definition);
        LOG_DEBUG(getLogger("MergeTreeSettings"), "Created custom disk {}", disk_name);
        change.value = disk_name;
    }
    else if (!is_loading_from_existing_metadata)
    {
        DiskFromAST::ensureDiskIsNotCustom(change.value.safeGet<String>(), context);
    }
}

}
