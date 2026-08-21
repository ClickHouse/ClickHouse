#include "config.h"

#include <Backups/BackupFactory.h>
#include <Core/Settings.h>
#include <Common/Exception.h>

#if USE_AZURE_BLOB_STORAGE

#include <Backups/BackupIO_AzureBlobStorage.h>
#include <Backups/BackupImpl.h>
#include <Backups/BackupInfo.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <IO/Archives/hasRegisteredArchiveFileExtension.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>

#include <Poco/URI.h>

#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace Setting
{
extern const SettingsUInt64 archive_adaptive_buffer_max_size_bytes;
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
    auto creator_fn = []([[maybe_unused]] BackupFactory::CreateParams params) -> std::unique_ptr<IBackup>
    {
#if USE_AZURE_BLOB_STORAGE
        const auto & args = params.backup_info.args;

        String blob_path;
        AzureBlobStorage::ConnectionParams connection_params;

        if (auto collection = params.backup_info.getNamedCollection(params.context))
        {
            String connection_url = collection->getAnyOrDefault<String>({"connection_string", "storage_account_url"}, "");
            String container_name = collection->get<String>("container");
            blob_path = collection->getOrDefault<String>("blob_path", "");

            auto get_optional = [&](const char * key) -> std::optional<String>
            {
                return collection->has(key) ? std::optional<String>(collection->get<String>(key)) : std::nullopt;
            };

            connection_params = getAzureConnectionParams(
                connection_url,
                container_name,
                get_optional("account_name"),
                get_optional("account_key"),
                get_optional("client_id"),
                get_optional("tenant_id"),
                params.context);

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

                connection_params = getAzureConnectionParams(
                    connection_url, container_name, std::nullopt, std::nullopt, std::nullopt, std::nullopt, params.context);
            }
            else if (args.size() == 5)
            {
                auto connection_url = args[0].safeGet<String>();
                auto container_name = args[1].safeGet<String>();
                blob_path = args[2].safeGet<String>();
                auto account_name = args[3].safeGet<String>();
                auto account_key = args[4].safeGet<String>();

                connection_params = getAzureConnectionParams(
                    connection_url, container_name, account_name, account_key, std::nullopt, std::nullopt, params.context);
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
            archive_params.adaptive_buffer_max_size = params.context->getSettingsRef()[Setting::archive_adaptive_buffer_max_size_bytes];
        }
        else
        {
            if (!params.password.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Password is not applicable, backup cannot be encrypted");
        }

        if (params.open_mode == IBackup::OpenMode::UNLOCK)
        {
            auto reader = std::make_shared<BackupReaderAzureBlobStorage>(
                connection_params,
                blob_path,
                params.allow_azure_native_copy,
                params.read_settings,
                params.write_settings,
                params.context);

            auto lightweight_snapshot_writer = std::make_shared<BackupWriterAzureBlobStorage>(
                connection_params,
                "",
                params.allow_azure_native_copy,
                params.read_settings,
                params.write_settings,
                params.context,
                params.azure_attempt_to_create_container);

            return std::make_unique<BackupImpl>(
                params.backup_info,
                archive_params,
                reader,
                lightweight_snapshot_writer);
        }

        params.use_same_s3_credentials_for_base_backup = false;

        if (params.open_mode == IBackup::OpenMode::READ)
        {
            auto reader = std::make_shared<BackupReaderAzureBlobStorage>(
                connection_params,
                blob_path,
                params.allow_azure_native_copy,
                params.read_settings,
                params.write_settings,
                params.context);

            auto snapshot_reader_creator = [&](const String & endpoint, const String & container_name)
            {
                connection_params.endpoint.storage_account_url = endpoint;
                connection_params.endpoint.container_name = container_name;
                return std::make_shared<BackupReaderAzureBlobStorage>(
                    connection_params,
                    "",
                    params.allow_azure_native_copy,
                    params.read_settings,
                    params.write_settings,
                    params.context);
            };

            return std::make_unique<BackupImpl>(params, archive_params, reader, snapshot_reader_creator);
        }

        auto writer = std::make_shared<BackupWriterAzureBlobStorage>(
            connection_params,
            blob_path,
            params.allow_azure_native_copy,
            params.read_settings,
            params.write_settings,
            params.context,
            params.azure_attempt_to_create_container);

        return std::make_unique<BackupImpl>(params, archive_params, writer);

#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "AzureBlobStorage support is disabled");
#endif
    };

    factory.registerBackupEngine("AzureBlobStorage", creator_fn);
}

}
