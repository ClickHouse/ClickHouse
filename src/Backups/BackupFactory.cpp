#include <Backups/BackupFactory.h>
#include <Common/Exception.h>

#include <fmt/format.h>

#include <iterator>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int BACKUP_ENGINE_NOT_FOUND;
    extern const int LOGICAL_ERROR;
}

namespace
{
    void appendIdentityComponent(String & identity, std::string_view component)
    {
        fmt::format_to(std::back_inserter(identity), ":{}:{}", component.size(), component);
    }
}


BackupFactory::CreateParams BackupFactory::CreateParams::getCreateParamsForBaseBackup(BackupInfo base_backup_info_, String old_password) const
{
    CreateParams read_params;
    read_params.open_mode = OpenMode::READ;
    read_params.backup_info = std::move(base_backup_info_);
    read_params.context = context;
    read_params.is_internal_backup = is_internal_backup;
    read_params.data_file_name_generator = data_file_name_generator;
    read_params.data_file_name_prefix_length = data_file_name_prefix_length;
    read_params.allow_s3_native_copy = allow_s3_native_copy;
    read_params.allow_azure_native_copy = allow_azure_native_copy;
    read_params.use_same_s3_credentials_for_base_backup = use_same_s3_credentials_for_base_backup;
    read_params.use_same_password_for_base_backup = use_same_password_for_base_backup;
    if (read_params.use_same_password_for_base_backup)
        read_params.password = old_password;
    return read_params;
}

BackupFactory & BackupFactory::instance()
{
    static BackupFactory the_instance;
    return the_instance;
}

BackupMutablePtr BackupFactory::createBackup(const CreateParams & params) const
{
    const String & engine_name = params.backup_info.backup_engine_name;
    auto it = engines.find(engine_name);
    if (it == engines.end())
        throw Exception(ErrorCodes::BACKUP_ENGINE_NOT_FOUND, "Not found backup engine '{}'", engine_name);

    CreateParams frozen_params = params;
    frozen_params.backup_info = params.backup_info.freezeNamedCollection(params.context);
    if (params.base_backup_info)
        frozen_params.base_backup_info = params.base_backup_info->freezeNamedCollection(params.context);
    return it->second.creator(frozen_params);
}

String BackupFactory::getDestinationIdentity(const BackupInfo & backup_info, ContextPtr context) const
{
    if (!context)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Context is required to identify a backup destination");
    if (!backup_info.id_arg.empty() && !backup_info.frozen_named_collection)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Named collection '{}' must be frozen before identifying a backup destination",
            backup_info.id_arg);

    const String & engine_name = backup_info.backup_engine_name;
    auto it = engines.find(engine_name);
    if (it == engines.end())
        throw Exception(ErrorCodes::BACKUP_ENGINE_NOT_FOUND, "Not found backup engine '{}'", engine_name);

    String identity = "backup-destination-v1";
    appendIdentityComponent(identity, engine_name);
    for (const auto & component : it->second.destination_identity(backup_info, context))
        appendIdentityComponent(identity, component);
    return identity;
}

BackupInfo BackupFactory::withoutCredentials(const BackupInfo & backup_info, ContextPtr context) const
{
    if (!context)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Context is required to remove credentials from a backup locator");

    auto it = engines.find(backup_info.backup_engine_name);
    if (it == engines.end())
        throw Exception(ErrorCodes::BACKUP_ENGINE_NOT_FOUND, "Not found backup engine '{}'", backup_info.backup_engine_name);

    BackupInfo res = backup_info;
    res.frozen_named_collection.reset();
    res.credentials_source.reset();
    it->second.remove_credentials(res, context);
    return res;
}

bool BackupFactory::copyCredentials(
    const BackupInfo & source,
    BackupInfo & destination,
    ContextPtr context,
    const BackupInfo * expected_credentials) const
{
    if (!context)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Context is required to copy credentials between backup locators");

    if (source.backup_engine_name != destination.backup_engine_name)
        return false;

    auto it = engines.find(destination.backup_engine_name);
    if (it == engines.end())
        throw Exception(ErrorCodes::BACKUP_ENGINE_NOT_FOUND, "Not found backup engine '{}'", destination.backup_engine_name);
    return it->second.copy_credentials(source, destination, context, expected_credentials);
}

void BackupFactory::registerBackupEngine(
    const String & engine_name,
    const CreatorFn & creator_fn,
    const DestinationIdentityFn & destination_identity_fn,
    const RemoveCredentialsFn & remove_credentials_fn,
    const CopyCredentialsFn & copy_credentials_fn)
{
    if (engines.contains(engine_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup engine '{}' was registered twice", engine_name);
    engines.emplace(engine_name, RegisteredEngine{creator_fn, destination_identity_fn, remove_credentials_fn, copy_credentials_fn});
}

void registerBackupEnginesFileAndDisk(BackupFactory &);
void registerBackupEngineMemory(BackupFactory &);
void registerBackupEngineNull(BackupFactory &);
void registerBackupEngineS3(BackupFactory &);
void registerBackupEngineAzureBlobStorage(BackupFactory &);

void registerBackupEngines(BackupFactory & factory);

void registerBackupEngines(BackupFactory & factory)
{
    registerBackupEnginesFileAndDisk(factory);
    registerBackupEngineMemory(factory);
    registerBackupEngineNull(factory);
    registerBackupEngineS3(factory);
    registerBackupEngineAzureBlobStorage(factory);
}

BackupFactory::BackupFactory()
{
    registerBackupEngines(*this);
}

}
