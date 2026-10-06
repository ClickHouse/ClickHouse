#include "config.h"

#include <Backups/BackupFactory.h>
#include <Core/Settings.h>
#include <Common/Exception.h>

#if USE_AZURE_BLOB_STORAGE

#include <Backups/BackupIO_AzureBlobStorage.h>
#include <Backups/BackupImpl.h>
#include <Backups/BackupInfo.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <IO/Archives/ArchiveUtils.h>
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

    /// Beside explicit credentials the destination reads a plain storage account URL only, so anything
    /// that carries a credential of its own (userinfo, a query string, a fragment) or that is no URL at
    /// all is rejected here, before the SDK reports it as a bare `std::exception`.
    void validatePlainStorageAccountURL(const String & connection_url)
    {
        try
        {
            Poco::URI uri(connection_url);
            const String & scheme = uri.getScheme();
            const size_t scheme_end = connection_url.find("://");
            bool has_userinfo = false;
            if (scheme_end != String::npos)
            {
                const size_t authority_start = scheme_end + 3;
                const size_t authority_end = connection_url.find_first_of("/?#", authority_start);
                const size_t userinfo_end = connection_url.find('@', authority_start);
                has_userinfo = userinfo_end != String::npos
                    && (authority_end == String::npos || userinfo_end < authority_end);
            }

            if ((scheme != "http" && scheme != "https")
                || uri.getHost().empty()
                || connection_url.find_first_of("?#") != String::npos
                || has_userinfo)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Azure storage account URL");
            }

            Azure::Core::Url{connection_url};
        }
        catch (const Poco::Exception &)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "AzureBlobStorage with explicit credentials requires a plain storage account URL "
                "without userinfo, query, or fragment");
        }
        catch (const std::logic_error &)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "AzureBlobStorage with explicit credentials requires a plain storage account URL "
                "without userinfo, query, or fragment");
        }
    }
}
#endif

void registerBackupEngineAzureBlobStorage(BackupFactory &);

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

            const auto account_name = get_optional("account_name");
            const auto account_key = get_optional("account_key");
            const auto client_id = get_optional("client_id");
            const auto tenant_id = get_optional("tenant_id");
            if (account_name || account_key || client_id || tenant_id)
                validatePlainStorageAccountURL(connection_url);

            connection_params = getAzureConnectionParams(
                connection_url, container_name, account_name, account_key, client_id, tenant_id, params.context);

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

                validatePlainStorageAccountURL(connection_url);
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
            if (hasSupportedZipExtension(blob_path))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Zip archive format is not supported for AzureBlobStorage backups because zip requires seeking "
                    "which object storage does not support efficiently. "
                    "Use tar.gz or other tar-based formats instead");

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

            return std::make_unique<BackupImpl>(
                params.backup_info,
                archive_params,
                reader);
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
