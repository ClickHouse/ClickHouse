#include <Storages/ObjectStorage/HDFS/Configuration.h>

#if USE_HDFS
#include <Storages/ObjectStorage/HDFS/HDFSCommon.h>
#include <Interpreters/Context.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Parsers/IAST.h>
#include <Disks/ObjectStorages/HDFS/HDFSObjectStorage.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Formats/FormatFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
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
    checkHDFSURL(fs::path(url) / path);
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

void StorageHDFSConfiguration::fromAST(ASTs & args, ContextPtr context, bool /* with_structure */)
{
    url = checkAndGetLiteralArgument<String>(args[0], "url");

    if (args.size() > 1)
    {
        args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], context);
        format = checkAndGetLiteralArgument<String>(args[1], "format_name");
    }

    if (args.size() == 3)
    {
        args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], context);
        compression_method = checkAndGetLiteralArgument<String>(args[2], "compression_method");
    }

    auto pos = url.find("//");
    if (pos == std::string::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid url: {}", url);

    pos = url.find('/', pos + 2);
    if (pos == std::string::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid url: {}", url);

    path = url.substr(pos + 1);
    url = url.substr(0, pos);
    paths = {path};
}

void StorageHDFSConfiguration::fromNamedCollection(const NamedCollection &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method fromNamedColection() is not implemented");
}

}

#endif
