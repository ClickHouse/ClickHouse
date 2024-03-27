#include <Storages/ObjectStorage/HDFS/Configuration.h>

#if USE_HDFS
#include <Storages/ObjectStorage/HDFS/HDFSCommon.h>
#include <Interpreters/Context.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Parsers/IAST.h>
#include <Disks/ObjectStorages/HDFS/HDFSObjectStorage.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Formats/FormatFactory.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

StorageHDFSConfiguration::StorageHDFSConfiguration(const StorageHDFSConfiguration & other)
    : StorageObjectStorageConfiguration(other)
{
    url = other.url;
    path = other.path;
    paths = other.paths;
}

void StorageHDFSConfiguration::check(ContextPtr context) const
{
    context->getRemoteHostFilter().checkURL(Poco::URI(url));
    checkHDFSURL(fs::path(url) / path.substr(1));
}

ObjectStoragePtr StorageHDFSConfiguration::createObjectStorage( /// NOLINT
    ContextPtr context,
    bool /* is_readonly */)
{
    assertInitialized();
    const auto & settings = context->getSettingsRef();
    auto hdfs_settings = std::make_unique<HDFSObjectStorageSettings>(
        settings.remote_read_min_bytes_for_seek,
        settings.hdfs_replication
    );
    return std::make_shared<HDFSObjectStorage>(url, std::move(hdfs_settings), context->getConfigRef());
}

std::string StorageHDFSConfiguration::getPathWithoutGlob() const
{
    /// Unlike s3 and azure, which are object storages,
    /// hdfs is a filesystem, so it cannot list files by partual prefix,
    /// only by directory.
    auto first_glob_pos = path.find_first_of("*?{");
    auto end_of_path_without_globs = path.substr(0, first_glob_pos).rfind('/');
    if (end_of_path_without_globs == std::string::npos || end_of_path_without_globs == 0)
        return "/";
    return path.substr(0, end_of_path_without_globs);
}

void StorageHDFSConfiguration::fromAST(ASTs & args, ContextPtr context, bool with_structure)
{
    std::string url_str;
    url_str = checkAndGetLiteralArgument<String>(args[0], "url");

    if (args.size() > 1)
    {
        args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], context);
        format = checkAndGetLiteralArgument<String>(args[1], "format_name");
    }

    if (with_structure)
    {
        if (args.size() > 2)
        {
            structure = checkAndGetLiteralArgument<String>(args[2], "structure");
        }
        if (args.size() > 3)
        {
            args[3] = evaluateConstantExpressionOrIdentifierAsLiteral(args[3], context);
            compression_method = checkAndGetLiteralArgument<String>(args[3], "compression_method");
        }
    }
    else if (args.size() > 2)
    {
        args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], context);
        compression_method = checkAndGetLiteralArgument<String>(args[2], "compression_method");
    }

    setURL(url_str);
}

void StorageHDFSConfiguration::fromNamedCollection(const NamedCollection & collection)
{
    std::string url_str;

    auto filename = collection.getOrDefault<String>("filename", "");
    if (!filename.empty())
        url_str = std::filesystem::path(collection.get<String>("url")) / filename;
    else
        url_str = collection.get<String>("url");

    format = collection.getOrDefault<String>("format", "auto");
    compression_method = collection.getOrDefault<String>("compression_method", collection.getOrDefault<String>("compression", "auto"));
    structure = collection.getOrDefault<String>("structure", "auto");

    setURL(url_str);
}

void StorageHDFSConfiguration::setURL(const std::string url_)
{
    auto pos = url_.find("//");
    if (pos == std::string::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad hdfs url: {}", url_);

    pos = url_.find('/', pos + 2);
    if (pos == std::string::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad hdfs url: {}", url_);

    path = url_.substr(pos + 1);
    url = url_.substr(0, pos);
    path = '/' + path;
    paths = {path};

    LOG_TRACE(getLogger("StorageHDFSConfiguration"), "Using url: {}, path: {}", url, path);
}

}

#endif
