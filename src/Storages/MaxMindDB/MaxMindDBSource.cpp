#include <Storages/MaxMindDB/MaxMindDBSource.h>

#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessType.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/MaxMindDB/MaxMindDBSettings.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/filesystemHelpers.h>

#include <filesystem>
#include <sys/stat.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_STAT;
extern const int DATABASE_ACCESS_DENIED;
}

namespace
{
String localVersion(const String & path)
{
    struct stat info{};
    if (::stat(path.c_str(), &info) != 0)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_STAT, path, "Cannot stat MaxMindDB file");
    if (!S_ISREG(info.st_mode))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB source must be a regular file");
#if defined(__APPLE__)
    const auto & modified = info.st_mtimespec;
#else
    const auto & modified = info.st_mtim;
#endif
    return fmt::format("{}:{}:{}:{}:{}", info.st_dev, info.st_ino, info.st_size, modified.tv_sec, modified.tv_nsec);
}
}

MaxMindDBFile::MaxMindDBFile() = default;
MaxMindDBFile::~MaxMindDBFile() = default;
MaxMindDBSource::~MaxMindDBSource() = default;

MaxMindDBSource::MaxMindDBSource(ASTs & arguments, ContextPtr context_, const StorageID &, bool check_access)
    : WithContext(context_->getGlobalContext())
{
    if (arguments.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB local sources require a single file path");
    arguments.front() = evaluateConstantExpressionOrIdentifierAsLiteral(arguments.front(), context_);
    const String path_string = checkAndGetLiteralArgument<String>(arguments.front(), "source");
    if (path_string.empty() || path_string.contains('\0') || path_string.contains("://"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB local sources require a nonempty file path");

    if (check_access)
        context_->checkAccess(AccessType::READ, toStringSource(AccessTypeObjects::Source::FILE));
    std::filesystem::path path(path_string);
    if (context_->getApplicationType() == Context::ApplicationType::SERVER && path.is_relative())
        path = std::filesystem::path(context_->getUserFilesPath()) / path;
    local_path = std::filesystem::absolute(path).string();
}

std::unique_ptr<MaxMindDBFile> MaxMindDBSource::load(const String & current_version, const MaxMindDBSettings &) const
{
    const auto context = getContext();
    if (context->getApplicationType() == Context::ApplicationType::SERVER && !pathStartsWith(local_path, context->getUserFilesPath()))
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "MaxMindDB file must be inside user_files_path");
    auto file = std::make_unique<MaxMindDBFile>();
    file->path = local_path;
    file->version = localVersion(local_path);
    if (file->version == current_version)
        return {};
    return file;
}

void MaxMindDBSource::checkVersion(const MaxMindDBFile & file) const
{
    if (localVersion(local_path) != file.version)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB source changed while preparing a generation; retry the operation");
}
}
