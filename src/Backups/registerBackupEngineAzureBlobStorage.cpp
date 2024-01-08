#include "config.h"

#include <Backups/BackupFactory.h>
#include <Common/Exception.h>

#if USE_AZURE_BLOB_STORAGE
#include <Backups/BackupIO_AzureBlobStorage.h>
#include <Storages/StorageAzureBlob.h>
#include <Backups/BackupImpl.h>
#include <IO/Archives/hasRegisteredArchiveFileExtension.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
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

        LOG_INFO(&Poco::Logger::get("registerBackupEngineAzureBlobStorage"), "Begin id_arg={} args.size={}", id_arg, args.size());

        StorageAzureBlob::Configuration configuration;

        if (args.size() == 3)
        {
            configuration.connection_url = args[0].safeGet<String>();
            configuration.is_connection_string = true;

            configuration.container =  args[1].safeGet<String>();
            configuration.blob_path = args[2].safeGet<String>();

            LOG_TRACE(&Poco::Logger::get("registerBackupEngineAzureBlobStorage"), "configuration.connection_url = {}"
                                                                                 "configuration.container = {}"
                                                                                 "configuration.blob_path = {}",
                                                                                 configuration.connection_url, configuration.container, configuration.blob_path);
        }
        else if (args.size() == 5)
        {
            configuration.connection_url = args[0].safeGet<String>();
            configuration.is_connection_string = false;

            configuration.container =  args[1].safeGet<String>();
            configuration.blob_path = args[2].safeGet<String>();
            configuration.account_name = args[3].safeGet<String>();
            configuration.account_key = args[4].safeGet<String>();

        }
        else
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                                "Backup AzureBlobStorage requires 3 or 5 arguments: connection string>/<url, container, path, [account name], [account key]");
        }


        BackupImpl::ArchiveParams archive_params;
        if (hasRegisteredArchiveFileExtension(configuration.blob_path))
        {
            if (params.is_internal_backup)
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Using archives with backups on clusters is disabled");

            archive_params.archive_name = removeFileNameFromURL(configuration.blob_path);
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
            auto reader = std::make_shared<BackupReaderAzureBlobStorage>(configuration,
                                                           params.read_settings,
                                                           params.write_settings,
                                                           params.context);

            return std::make_unique<BackupImpl>(
                params.backup_info,
                archive_params,
                params.base_backup_info,
                reader,
                params.context,
                /*params.use_same_s3_credentials_for_base_backup*/ false);
        }
        else
        {
            auto writer = std::make_shared<BackupWriterAzureBlobStorage>(configuration,
                                                           params.read_settings,
                                                           params.write_settings,
                                                           params.context);

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
                /*params.use_same_s3_credentials_for_base_backup*/ false);
        }
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "AzureBlobStorage support is disabled");
#endif
    };

    factory.registerBackupEngine("AzureBlobStorage", creator_fn);
}

}
