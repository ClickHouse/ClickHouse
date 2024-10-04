#include "config.h"

#include <Backups/BackupFactory.h>
#include <Common/Exception.h>

#if USE_AZURE_BLOB_STORAGE
#include <Backups/BackupIO_AzureBlobStorage.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <Backups/BackupImpl.h>
#include <IO/Archives/hasRegisteredArchiveFileExtension.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <filesystem>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

#if USE_AZURE_BLOB_STORAGE
namespace
{
    String removeFileNameFromURL(String & url)
    {
        Poco::URI url2{url};
        String path = url2.getPath();
        size_t slash_pos = path.find_last_of('/');
        String file_name = path.substr(slash_pos + 1);
        path.resize(slash_pos + 1);
        url2.setPath(path);
        url = url2.toString();
        return file_name;
    }
}
#endif

void registerBackupEngineAzureBlobStorage(BackupFactory & factory)
{
    auto creator_fn = []([[maybe_unused]] const BackupFactory::CreateParams & params) -> std::unique_ptr<IBackup>
    {
#if USE_AZURE_BLOB_STORAGE
        const String & id_arg = params.backup_info.id_arg;
        const auto & args = params.backup_info.args;

        String blob_path;
        AzureBlobStorage::ConnectionParams connection_params;
        auto request_settings = AzureBlobStorage::getRequestSettings(params.context->getSettingsRef());

        if (!id_arg.empty())
        {
            const auto & config = params.context->getConfigRef();
            auto config_prefix = "named_collections." + id_arg;

            if (!config.has(config_prefix))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no collection named `{}` in config", id_arg);

            connection_params =
            {
                .endpoint = AzureBlobStorage::processEndpoint(config, config_prefix),
                .auth_method = AzureBlobStorage::getAuthMethod(config, config_prefix),
                .client_options = AzureBlobStorage::getClientOptions(*request_settings, /*for_disk=*/ true),
            };

            if (args.size() > 1)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                                "Backup AzureBlobStorage requires 1 or 2 arguments: named_collection, [filename]");

            if (args.size() == 1)
                blob_path = args[0].safeGet<String>();
        }
        else
        {
            if (args.size() == 3)
            {
                auto connection_url = args[0].safeGet<String>();
                auto container_name = args[1].safeGet<String>();
                blob_path = args[2].safeGet<String>();

                AzureBlobStorage::processURL(connection_url, container_name, connection_params.endpoint, connection_params.auth_method);
                connection_params.client_options = AzureBlobStorage::getClientOptions(*request_settings, /*for_disk=*/ true);
            }
            else if (args.size() == 5)
            {
                connection_params.endpoint.storage_account_url = args[0].safeGet<String>();
                connection_params.endpoint.container_name = args[1].safeGet<String>();
                blob_path = args[2].safeGet<String>();

                auto account_name = args[3].safeGet<String>();
                auto account_key = args[4].safeGet<String>();

                connection_params.auth_method = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(account_name, account_key);
                connection_params.client_options = AzureBlobStorage::getClientOptions(*request_settings, /*for_disk=*/ true);
            }
            else
            {
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                                    "Backup AzureBlobStorage requires 3 or 5 arguments: connection string>/<url, container, path, [account name], [account key]");
            }
        }

        BackupImpl::ArchiveParams archive_params;
        if (hasRegisteredArchiveFileExtension(blob_path))
        {
            if (params.is_internal_backup)
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Using archives with backups on clusters is disabled");

            archive_params.archive_name = removeFileNameFromURL(blob_path);
            archive_params.compression_method = params.compression_method;
            archive_params.compression_level = params.compression_level;
            archive_params.password = params.password;
        }
        else
        {
            if (!params.password.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Password is not applicable, backup cannot be encrypted");
        }


        if (params.open_mode == IBackup::OpenMode::READ)
        {
            auto reader = std::make_shared<BackupReaderAzureBlobStorage>(
                connection_params,
                blob_path,
                params.allow_azure_native_copy,
                params.read_settings,
                params.write_settings,
                params.context);

            return std::make_unique<BackupImpl>(
                params.backup_info,
                archive_params,
                params.base_backup_info,
                reader,
                params.context,
                params.is_internal_backup,
                /* use_same_s3_credentials_for_base_backup*/ false,
                params.use_same_password_for_base_backup);
        }
        else
        {
            auto writer = std::make_shared<BackupWriterAzureBlobStorage>(
                connection_params,
                blob_path,
                params.allow_azure_native_copy,
                params.read_settings,
                params.write_settings,
                params.context,
                params.azure_attempt_to_create_container);

            return std::make_unique<BackupImpl>(
                params.backup_info,
                archive_params,
                params.base_backup_info,
                writer,
                params.context,
                params.is_internal_backup,
                params.backup_coordination,
                params.backup_uuid,
                params.deduplicate_files,
                /* use_same_s3_credentials_for_base_backup */ false,
                params.use_same_password_for_base_backup);
        }
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "AzureBlobStorage support is disabled");
#endif
    };

    factory.registerBackupEngine("AzureBlobStorage", creator_fn);
}

}
