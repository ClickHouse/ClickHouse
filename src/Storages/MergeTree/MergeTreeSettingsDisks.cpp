#include <Storages/MergeTree/MergeTreeSettingsDisks.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

#include <Common/Exception.h>
#include <Common/SettingsChanges.h>
#include <Common/logger_useful.h>
#include <Disks/DiskFromAST.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/StoragePolicy.h>
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

CustomDiskRegistrationPtr MergeTreeSettings::resolveDiskSetting(
    SettingsChanges & changes, ContextPtr context, bool is_loading_from_existing_metadata, bool for_system_database, bool stored_definition)
{
    /// Only the `disk` and `storage_policy` settings yield a registration, and a setting occurs at most once.
    CustomDiskRegistrationPtr registration;
    for (auto & change : changes)
    {
        if (auto change_registration = resolveDiskSetting(change, context, is_loading_from_existing_metadata, for_system_database, stored_definition))
            registration = std::move(change_registration);
    }
    return registration;
}

CustomDiskRegistrationPtr MergeTreeSettings::resolveDiskSetting(
    SettingChange & change, ContextPtr context, bool is_loading_from_existing_metadata, bool for_system_database, bool stored_definition)
{
    if (change.name == "storage_policy")
    {
        /// The single-disk storage policy implicitly created for a custom disk can be named directly. Keep
        /// the disk registered for as long as the table uses the policy, as if it was named in `disk`.
        /// The policy is required to exist already: if it was looked up separately from the registration,
        /// a table defining the disk could create it in between, and this table would use it unregistered.
        String policy_name;
        if (change.value.tryGet<String>(policy_name) && policy_name.starts_with(StoragePolicySelector::TMP_STORAGE_POLICY_PREFIX))
            return context->getCustomDiskRegistrationOfStoragePolicy(policy_name);
        return nullptr;
    }

    if (change.name != "disk")
        return nullptr;

    CustomType custom;
    ASTPtr value_as_custom_ast = nullptr;
    if (change.value.tryGet<CustomType>(custom) && 0 == strcmp(custom.getTypeName(), "AST"))
        value_as_custom_ast = dynamic_cast<const FieldFromASTImpl &>(custom.getImpl()).ast;

    if (value_as_custom_ast && isDiskFunction(value_as_custom_ast))
    {
        auto custom_disk = DiskFromAST::createCustomDisk(
            value_as_custom_ast, context, is_loading_from_existing_metadata, for_system_database, stored_definition);
        LOG_DEBUG(getLogger("MergeTreeSettings"), "Created custom disk {}", custom_disk.disk_name);
        change.value = custom_disk.disk_name;
        return std::move(custom_disk.registration);
    }

    if (!is_loading_from_existing_metadata)
    {
        DiskFromAST::ensureDiskIsNotCustom(change.value.safeGet<String>(), context);
        return nullptr;
    }

    /// The definition has already been flattened to a disk name, which happens when settings are
    /// re-applied to a table that is already using the disk (`ALTER TABLE ... MODIFY SETTING`, for
    /// one). Take a registration for it, so that the new settings keep the disk alive as the old
    /// ones did.
    return context->tryGetCustomDiskRegistration(change.value.safeGet<String>());
}

}
