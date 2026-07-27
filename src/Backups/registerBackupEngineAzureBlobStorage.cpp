#include "config.h"

#include <Backups/BackupFactory.h>
#include <Backups/BackupInfo.h>
#include <Core/Settings.h>
#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollections.h>

#include <map>
#include <optional>

#if USE_AZURE_BLOB_STORAGE

#include <Backups/BackupIO_AzureBlobStorage.h>
#include <Backups/BackupImpl.h>
#include <IO/Archives/ArchiveUtils.h>
#include <IO/Archives/hasRegisteredArchiveFileExtension.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>

#include <Poco/URI.h>

#include <azure/storage/common/storage_credential.hpp>

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

namespace
{
#if USE_AZURE_BLOB_STORAGE
    bool isAzureConnectionKey(const String & key)
    {
        return key == "connection_string" || key == "storage_account_url";
    }

    bool isAzureCredentialKey(const String & key)
    {
        return key == "account_name" || key == "account_key" || key == "client_id" || key == "tenant_id";
    }
#endif

    enum class AzureCredentialsType : uint8_t
    {
        Default,
        ConnectionString,
        SAS,
        SharedKey,
        WorkloadIdentity,
    };

    struct AzureCredentials
    {
        AzureCredentialsType type;
        std::map<String, String> values;

        bool operator==(const AzureCredentials &) const = default;
    };

    std::optional<AzureCredentials> getAzureCredentials(const BackupInfo & backup_info, ContextPtr context)
    {
        String connection;
        std::optional<String> account_name;
        std::optional<String> account_key;
        std::optional<String> client_id;
        std::optional<String> tenant_id;

        if (auto collection = backup_info.getNamedCollection(context))
        {
            connection = collection->getAnyOrDefault<String>({"connection_string", "storage_account_url"}, "");
            auto get_optional = [&](const char * key) -> std::optional<String>
            {
                return collection->has(key) ? std::optional<String>(collection->get<String>(key)) : std::nullopt;
            };
            account_name = get_optional("account_name");
            account_key = get_optional("account_key");
            client_id = get_optional("client_id");
            tenant_id = get_optional("tenant_id");
        }
        else if (backup_info.args.size() == 5)
        {
            connection = backup_info.args[0].safeGet<String>();
            account_name = backup_info.args[3].safeGet<String>();
            account_key = backup_info.args[4].safeGet<String>();
        }
        else if (backup_info.args.size() == 3)
        {
            connection = backup_info.args[0].safeGet<String>();
        }
        else
            return std::nullopt;

        if (connection.empty())
            return std::nullopt;

        const bool has_shared_key = account_name || account_key;
        const bool has_workload_identity = client_id || tenant_id;
        if (has_shared_key || has_workload_identity)
        {
            if (account_name && account_key && !has_workload_identity)
                return AzureCredentials{
                    AzureCredentialsType::SharedKey,
                    {{"account_name", *account_name}, {"account_key", *account_key}}};
            if (client_id && tenant_id && !has_shared_key)
                return AzureCredentials{
                    AzureCredentialsType::WorkloadIdentity,
                    {{"client_id", *client_id}, {"tenant_id", *tenant_id}}};
            return std::nullopt;
        }

        if (!connection.starts_with("http"))
        {
#if USE_AZURE_BLOB_STORAGE
            try
            {
                auto parsed = Azure::Storage::_internal::ParseConnectionString(connection);
                std::map<String, String> values;
                if (!parsed.AccountKey.empty())
                {
                    values.emplace("account_name", std::move(parsed.AccountName));
                    values.emplace("account_key", std::move(parsed.AccountKey));
                }
                for (const auto & [key, value] : parsed.BlobServiceUrl.GetQueryParameters())
                    values.emplace("sas:" + key, value);
                return AzureCredentials{AzureCredentialsType::ConnectionString, std::move(values)};
            }
            catch (const std::logic_error &)
            {
                return std::nullopt;
            }
            catch (const std::runtime_error &)
            {
                return std::nullopt;
            }
#else
            return std::nullopt;
#endif
        }
        if (connection.find('?') != String::npos)
        {
#if USE_AZURE_BLOB_STORAGE
            try
            {
                return AzureCredentials{AzureCredentialsType::SAS, Azure::Core::Url(connection).GetQueryParameters()};
            }
            catch (const std::logic_error &)
            {
                return std::nullopt;
            }
            catch (const std::runtime_error &)
            {
                return std::nullopt;
            }
#else
            return std::nullopt;
#endif
        }
        return AzureCredentials{AzureCredentialsType::Default, {}};
    }

    bool copyAzureCredentials(
        const BackupInfo & source,
        BackupInfo & destination,
        ContextPtr context,
        const BackupInfo * expected_credentials)
    {
        auto make_snapshot = [&](const BackupInfo & backup_info)
        {
            const auto & credentials = backup_info.credentials_source ? *backup_info.credentials_source : backup_info;
            BackupInfo snapshot = credentials.freezeNamedCollection(context);
            snapshot.credentials_source.reset();
            return snapshot;
        };

        BackupInfo source_snapshot = make_snapshot(source);
        const auto source_credentials = getAzureCredentials(source_snapshot, context);
        if (!source_credentials)
            return false;

        if (expected_credentials)
        {
            BackupInfo expected_snapshot = make_snapshot(*expected_credentials);
            const auto expected = getAzureCredentials(expected_snapshot, context);
            if (!expected || *source_credentials != *expected)
                return false;
        }

        if (!destination.id_arg.empty() && !destination.frozen_named_collection)
            destination.frozen_named_collection = destination.getNamedCollection(context)->duplicate();
        destination.credentials_source = std::make_shared<BackupInfo>(std::move(source_snapshot));
        return true;
    }
}

#if USE_AZURE_BLOB_STORAGE
namespace
{
    struct ResolvedAzureBackupLocation
    {
        AzureBlobStorage::ConnectionParams connection_params;
        String blob_path;
        String archive_name;
    };

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

    String stripOneTrailingSlash(String str)
    {
        /// Path joining consumes one trailing slash after a non-root prefix but preserves additional slashes in blob keys.
        if (str.size() > 1 && str.back() == '/' && str.find_first_not_of('/') != String::npos)
            str.pop_back();
        return str;
    }

    struct ResolvedAzureBackupPath
    {
        String blob_path;
        String archive_name;
    };

    ResolvedAzureBackupPath resolveAzureBackupPath(const BackupInfo & backup_info, ContextPtr context)
    {
        ResolvedAzureBackupPath path;
        const auto & args = backup_info.args;
        if (auto collection = backup_info.getNamedCollection(context))
        {
            path.blob_path = collection->getOrDefault<String>("blob_path", "");
            if (args.size() > 1)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Backup AzureBlobStorage requires 1 or 2 arguments: named_collection, [filename]");
            if (args.size() == 1)
                path.blob_path = args[0].safeGet<String>();
        }
        else if (args.size() == 3 || args.size() == 5)
            path.blob_path = args[2].safeGet<String>();
        else
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Backup AzureBlobStorage requires 3 arguments (connection string/url, container, path) or 5 arguments "
                "(storage account URL, container, path, account name, account key)");
        }

        if (hasRegisteredArchiveFileExtension(path.blob_path))
            path.archive_name = removeFileNameFromURL(path.blob_path);
        return path;
    }

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

    ResolvedAzureBackupLocation resolveAzureBackupLocation(const BackupInfo & backup_info, ContextPtr context)
    {
        if (backup_info.credentials_source)
        {
            BackupInfo destination_info = backup_info;
            destination_info.credentials_source.reset();
            BackupInfo source_info = *backup_info.credentials_source;
            source_info.credentials_source.reset();

            auto destination = resolveAzureBackupPath(destination_info, context);
            auto source = resolveAzureBackupLocation(source_info, context);
            source.blob_path = std::move(destination.blob_path);
            source.archive_name = std::move(destination.archive_name);
            return source;
        }

        ResolvedAzureBackupLocation location;
        auto path = resolveAzureBackupPath(backup_info, context);
        location.blob_path = std::move(path.blob_path);
        location.archive_name = std::move(path.archive_name);
        const auto & args = backup_info.args;

        if (auto collection = backup_info.getNamedCollection(context))
        {
            const String connection_url = collection->getAnyOrDefault<String>({"connection_string", "storage_account_url"}, "");
            const String container_name = collection->get<String>("container");

            auto get_optional = [&](const char * key) -> std::optional<String>
            {
                return collection->has(key) ? std::optional<String>(collection->get<String>(key)) : std::nullopt;
            };

            const auto account_name = get_optional("account_name");
            const auto account_key = get_optional("account_key");
            const auto client_id = get_optional("client_id");
            const auto tenant_id = get_optional("tenant_id");
            const bool has_explicit_credentials = account_name || account_key || client_id || tenant_id;
            if (has_explicit_credentials)
                validatePlainStorageAccountURL(connection_url);
            location.connection_params = getAzureConnectionParams(
                connection_url, container_name, account_name, account_key, client_id, tenant_id, context);
        }
        else if (args.size() == 3)
        {
            location.connection_params = getAzureConnectionParams(
                args[0].safeGet<String>(),
                args[1].safeGet<String>(),
                std::nullopt,
                std::nullopt,
                std::nullopt,
                std::nullopt,
                context);
        }
        else if (args.size() == 5)
        {
            const String connection_url = args[0].safeGet<String>();
            validatePlainStorageAccountURL(connection_url);
            location.connection_params = getAzureConnectionParams(
                connection_url,
                args[1].safeGet<String>(),
                args[3].safeGet<String>(),
                args[4].safeGet<String>(),
                std::nullopt,
                std::nullopt,
                context);
        }
        else
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Backup AzureBlobStorage requires 3 arguments (connection string/url, container, path) or 5 arguments "
                "(storage account URL, container, path, account name, account key)");
        }

        return location;
    }

    String removeAzureCredentials(const AzureBlobStorage::ConnectionParams & connection_params)
    {
        try
        {
            const String connection_url = connection_params.getConnectionURL();
            const size_t query_pos = connection_url.find('?');
            const size_t fragment_pos = connection_url.find('#');
            if (fragment_pos != String::npos && (query_pos == String::npos || fragment_pos < query_pos))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Azure backup destination contains an unsupported URL fragment");

            Poco::URI uri(connection_url);
            if (!uri.getUserInfo().empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Azure backup destination URL must not contain userinfo");
            uri.setQuery("");
            uri.setFragment("");
            return Azure::Core::Url(uri.toString()).GetAbsoluteUrl();
        }
        catch (const Poco::Exception &)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to parse Azure backup destination");
        }
        catch (const std::logic_error &)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to parse Azure backup destination");
        }
    }

    String removeAzureCredentials(const String & connection_url)
    {
        AzureBlobStorage::ConnectionParams connection_params;
        AzureBlobStorage::processURL(connection_url, "", connection_params.endpoint, connection_params.auth_method);
        return removeAzureCredentials(connection_params);
    }

    void removeAzureCredentials(BackupInfo & backup_info, ContextPtr context)
    {
        if (backup_info.id_arg.empty())
        {
            auto location = resolveAzureBackupLocation(backup_info, context);
            backup_info.args[0] = removeAzureCredentials(location.connection_params);
            backup_info.args.resize(3);
            backup_info.kv_args.clear();
        }
        else
        {
            ASTs kv_args;
            kv_args.reserve(backup_info.kv_args.size());
            for (const auto & kv_arg : backup_info.kv_args)
            {
                String key = BackupInfo::evaluateKeyValueArgument(kv_arg, 0, context);
                if (isAzureCredentialKey(key))
                    continue;

                if (isAzureConnectionKey(key))
                {
                    ASTPtr cloned = kv_arg->clone();
                    cloned->as<ASTFunction>()->arguments->children[1]
                        = make_intrusive<ASTLiteral>(removeAzureCredentials(BackupInfo::evaluateKeyValueArgument(kv_arg, 1, context)));
                    kv_args.emplace_back(std::move(cloned));
                }
                else
                    kv_args.emplace_back(kv_arg);
            }
            backup_info.kv_args = std::move(kv_args);
        }
        backup_info.function_arg = nullptr;
    }

    Strings getAzureDestinationIdentity(const BackupInfo & backup_info, ContextPtr context)
    {
        auto location = resolveAzureBackupLocation(backup_info, context);
        return {
            "connection_url=" + stripOneTrailingSlash(removeAzureCredentials(location.connection_params)),
            "container=" + location.connection_params.getContainer(),
            "blob_path=" + stripOneTrailingSlash(location.blob_path),
            "archive=" + location.archive_name,
        };
    }
}
#endif

void registerBackupEngineAzureBlobStorage(BackupFactory &);

void registerBackupEngineAzureBlobStorage(BackupFactory & factory)
{
    auto creator_fn = []([[maybe_unused]] BackupFactory::CreateParams params) -> std::unique_ptr<IBackup>
    {
#if USE_AZURE_BLOB_STORAGE
        auto location = resolveAzureBackupLocation(params.backup_info, params.context);
        auto & connection_params = location.connection_params;
        String & blob_path = location.blob_path;

        BackupImpl::ArchiveParams archive_params;
        if (!location.archive_name.empty())
        {
            if (hasSupportedZipExtension(location.archive_name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Zip archive format is not supported for AzureBlobStorage backups because zip requires seeking "
                    "which object storage does not support efficiently. "
                    "Use tar.gz or other tar-based formats instead");

            if (params.is_internal_backup)
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Using archives with backups on clusters is disabled");

            archive_params.archive_name = location.archive_name;
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

    auto destination_identity_fn = []([[maybe_unused]] const BackupInfo & backup_info, [[maybe_unused]] ContextPtr context) -> Strings
    {
#if USE_AZURE_BLOB_STORAGE
        return getAzureDestinationIdentity(backup_info, context);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "AzureBlobStorage support is disabled");
#endif
    };

    auto remove_credentials_fn = []([[maybe_unused]] BackupInfo & backup_info, [[maybe_unused]] ContextPtr context)
    {
#if USE_AZURE_BLOB_STORAGE
        removeAzureCredentials(backup_info, context);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "AzureBlobStorage support is disabled");
#endif
    };

    factory.registerBackupEngine(
        "AzureBlobStorage", creator_fn, destination_identity_fn, remove_credentials_fn, copyAzureCredentials);
}

}
