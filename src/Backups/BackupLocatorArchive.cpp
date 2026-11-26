#include <Backups/BackupLocatorArchive.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>

#include <Poco/URI.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

namespace Setting
{
    extern const SettingsUInt64 archive_adaptive_buffer_max_size_bytes;
}

String removeFileNameFromURL(String & url)
{
    Poco::URI url2{url};
    String path = url2.getPath();
    size_t slash_pos = path.find_last_of('/');
    const size_t file_name_pos = slash_pos == String::npos ? 0 : slash_pos + 1;
    String file_name = path.substr(file_name_pos);
    path.resize(file_name_pos);
    url2.setPath(path);
    url = url2.toString();
    return file_name;
}

BackupImpl::ArchiveParams makeBackupArchiveParams(const BackupFactory::CreateParams & params, const String & archive_name)
{
    BackupImpl::ArchiveParams archive_params;
    if (archive_name.empty())
    {
        if (!params.password.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Password is not applicable, backup cannot be encrypted");
        return archive_params;
    }

    if (params.is_internal_backup)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Using archives with backups on clusters is disabled");

    archive_params.archive_name = archive_name;
    archive_params.compression_method = params.compression_method;
    archive_params.compression_level = params.compression_level;
    archive_params.password = params.password;
    archive_params.adaptive_buffer_max_size = params.context->getSettingsRef()[Setting::archive_adaptive_buffer_max_size_bytes];
    return archive_params;
}

}
