#include <Backups/BackupFactory.h>
#include <Backups/BackupIO_URL.h>
#include <Backups/BackupImpl.h>
#include <Backups/BackupInfo.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Core/Settings.h>
#include <IO/Archives/hasRegisteredArchiveFileExtension.h>
#include <Interpreters/Context.h>
#include <Access/Common/AccessFlags.h>

#include <Poco/URI.h>

#include <filesystem>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace Setting
{
    extern const SettingsUInt64 archive_adaptive_buffer_max_size_bytes;
}

namespace
{
    String removeFileNameFromURL(String & url)
    {
        Poco::URI uri{url};
        String path = uri.getPath();
        size_t slash_pos = path.find_last_of('/');
        String file_name = path.substr(slash_pos + 1);
        path.resize(slash_pos + 1);
        uri.setPath(path);
        url = uri.toString();
        return file_name;
    }
}


void registerBackupEngineURL(BackupFactory & factory)
{
    auto creator_fn = [](const BackupFactory::CreateParams & params) -> std::unique_ptr<IBackup>
    {
        const auto & args = params.backup_info.args;

        String url;
        String http_user_name;
        String http_password;
        String http_method;

        if (auto collection = params.backup_info.getNamedCollection(params.context))
        {
            url = collection->get<String>("url");
            http_user_name = collection->getOrDefault<String>("user", "");
            http_password = collection->getOrDefault<String>("password", "");
            http_method = collection->getOrDefault<String>("http_method", collection->getOrDefault<String>("method", ""));
            if (!http_method.empty() && http_method != "POST" && http_method != "PUT")
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "HTTP method must be POST or PUT (got: {}). Default is PUT",
                    http_method);

            if (collection->has("filename"))
                url = std::filesystem::path(url) / collection->get<String>("filename");

            if (args.size() > 1)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Backup URL requires 1 or 2 arguments: named_collection, [filename]");

            if (args.size() == 1)
                url = std::filesystem::path(url) / args[0].safeGet<String>();
        }
        else
        {
            if (args.empty() || args.size() > 3)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Backup URL requires 1 to 3 arguments: url, [http_user_name, http_password]");

            url = args[0].safeGet<String>();

            if (args.size() >= 3)
            {
                http_user_name = args[1].safeGet<String>();
                http_password = args[2].safeGet<String>();
            }
        }

        BackupImpl::ArchiveParams archive_params;
        if (hasRegisteredArchiveFileExtension(url))
        {
            if (params.is_internal_backup)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Using archives with backups on clusters is disabled");

            archive_params.archive_name = removeFileNameFromURL(url);
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

        const Poco::URI uri{url};

        const std::string_view source_name = toStringSource(AccessTypeObjects::Source::URL);
        if (params.open_mode == IBackup::OpenMode::READ)
        {
            params.context->checkAccess(AccessType::READ, source_name);

            auto reader = std::make_shared<BackupReaderURL>(
                uri,
                http_user_name,
                http_password,
                params.read_settings,
                params.write_settings,
                params.context);

            return std::make_unique<BackupImpl>(params, archive_params, reader);
        }
        else
        {
            params.context->checkAccess(AccessType::WRITE, source_name);

            auto writer = std::make_shared<BackupWriterURL>(
                uri,
                http_user_name,
                http_password,
                http_method,
                params.read_settings,
                params.write_settings,
                params.context);

            return std::make_unique<BackupImpl>(params, archive_params, writer);
        }
    };

    factory.registerBackupEngine("URL", creator_fn);
}

}
