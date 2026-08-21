#include "config.h"

#include <Backups/BackupFactory.h>
#include <Core/Settings.h>
#include <Common/Exception.h>

#if USE_AWS_S3
#include <Backups/BackupIO_S3.h>
#include <Backups/BackupImpl.h>
#include <Backups/BackupInfo.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <IO/Archives/hasRegisteredArchiveFileExtension.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <filesystem>

namespace DB::S3AuthSetting
{
    extern const S3AuthSettingsString role_arn;
    extern const S3AuthSettingsString role_session_name;
}

#endif


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SUPPORT_IS_DISABLED;
}

namespace Setting
{
extern const SettingsUInt64 archive_adaptive_buffer_max_size_bytes;
}

#if USE_AWS_S3
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


void registerBackupEngineS3(BackupFactory & factory)
{
    auto creator_fn = []([[maybe_unused]] const BackupFactory::CreateParams & params) -> std::unique_ptr<IBackup>
    {
#if USE_AWS_S3
        const auto & args = params.backup_info.args;

        String s3_uri;
        String access_key_id;
        String secret_access_key;
        String role_arn;
        String role_session_name;

        if (auto collection = params.backup_info.getNamedCollection(params.context))
        {
            s3_uri = collection->get<String>("url");
            access_key_id = collection->getOrDefault<String>("access_key_id", "");
            secret_access_key = collection->getOrDefault<String>("secret_access_key", "");
            role_arn = collection->getOrDefault<String>("role_arn", "");
            role_session_name = collection->getOrDefault<String>("role_session_name", "");

            if (collection->has("filename"))
                s3_uri = std::filesystem::path(s3_uri) / collection->get<String>("filename");

            if (args.size() > 1)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Backup S3 requires 1 or 2 arguments: named_collection, [filename]");

            if (args.size() == 1)
                s3_uri = std::filesystem::path(s3_uri) / args[0].safeGet<String>();
        }
        else
        {
            if ((args.size() != 1) && (args.size() != 3))
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                                "Backup S3 requires 1 or 3 arguments: url, [access_key_id, secret_access_key]");

            s3_uri = args[0].safeGet<String>();
            if (args.size() >= 3)
            {
                access_key_id = args[1].safeGet<String>();
                secret_access_key = args[2].safeGet<String>();
            }

            if (params.backup_info.function_arg)
            {
                S3::S3AuthSettings auth_settings;

                if (!StorageS3Configuration::collectCredentials(params.backup_info.function_arg, auth_settings, params.context))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid argument: {}", params.backup_info.function_arg->formatForErrorMessage());

                role_arn = std::move(auth_settings[S3AuthSetting::role_arn]);
                role_session_name = std::move(auth_settings[S3AuthSetting::role_session_name]);
            }
        }

        BackupImpl::ArchiveParams archive_params;
        if (hasRegisteredArchiveFileExtension(s3_uri))
        {
            if (params.is_internal_backup)
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Using archives with backups on clusters is disabled");

            archive_params.archive_name = removeFileNameFromURL(s3_uri);
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
            auto reader = std::make_shared<BackupReaderS3>(
                S3::URI{s3_uri},
                access_key_id,
                secret_access_key,
                role_arn,
                role_session_name,
                params.allow_s3_native_copy,
                params.read_settings,
                params.write_settings,
                params.context,
                params.is_internal_backup);
            /// We assume object storage of backup files and original disk use same endpoint, bucket and credentials.
            auto uri_for_lightweight = S3::URI{s3_uri};
            /// We set the prefix to "" because in meta file, object key is absolute path.
            uri_for_lightweight.key = "";
            auto lightweight_snapshot_writer = std::make_shared<BackupWriterS3>(
                uri_for_lightweight,
                access_key_id,
                secret_access_key,
                role_arn,
                role_session_name,
                params.allow_s3_native_copy,
                params.s3_storage_class,
                params.read_settings,
                params.write_settings,
                params.context,
                params.is_internal_backup);

            return std::make_unique<BackupImpl>(
                params.backup_info,
                archive_params,
                reader,
                lightweight_snapshot_writer);
        }
        else if (params.open_mode == IBackup::OpenMode::READ)
        {
            auto reader = std::make_shared<BackupReaderS3>(
                S3::URI{s3_uri},
                access_key_id,
                secret_access_key,
                role_arn,
                role_session_name,
                params.allow_s3_native_copy,
                params.read_settings,
                params.write_settings,
                params.context,
                params.is_internal_backup);


            auto snapshot_reader_creator = [&](const String & s3_uri_, const String & s3_bucket_)
            {
                String full_uri = std::filesystem::path(s3_uri_) / s3_bucket_;
                auto uri_for_lightweight = S3::URI{full_uri};
                /// We set the prefix to "" because in meta file, object key is absolute path.
                uri_for_lightweight.key = "";
                return std::make_shared<BackupReaderS3>(
                    uri_for_lightweight,
                    access_key_id,
                    secret_access_key,
                    role_arn,
                    role_session_name,
                    params.allow_s3_native_copy,
                    params.read_settings,
                    params.write_settings,
                    params.context,
                    params.is_internal_backup);
            };

            return std::make_unique<BackupImpl>(params, archive_params, reader, snapshot_reader_creator);
        }
        else
        {
            auto writer = std::make_shared<BackupWriterS3>(
                S3::URI{s3_uri},
                access_key_id,
                secret_access_key,
                std::move(role_arn),
                std::move(role_session_name),
                params.allow_s3_native_copy,
                params.s3_storage_class,
                params.read_settings,
                params.write_settings,
                params.context,
                params.is_internal_backup);

            return std::make_unique<BackupImpl>(params, archive_params, writer);
        }
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "S3 support is disabled");
#endif
    };

    factory.registerBackupEngine("S3", creator_fn);
}

}
