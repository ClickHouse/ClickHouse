#include <Storages/ObjectStorage/HDFS/Configuration.h>

#if USE_HDFS
#include <Storages/HDFS/HDFSCommon.h>
#include <Interpreters/Context.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Parsers/IAST.h>
#include <Disks/ObjectStorages/HDFS/HDFSObjectStorage.h>
#include <Formats/FormatFactory.h>

namespace DB
{

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

ObjectStoragePtr StorageHDFSConfiguration::createObjectStorage(ContextPtr context, bool is_readonly) /// NOLINT
{
    UNUSED(is_readonly);
    auto settings = std::make_unique<HDFSObjectStorageSettings>();
    chassert(!url.empty());
    return std::make_shared<HDFSObjectStorage>(url, std::move(settings), context->getConfigRef());
}

void StorageHDFSConfiguration::fromAST(ASTs & args, ContextPtr, bool /* with_structure */)
{
    url = checkAndGetLiteralArgument<String>(args[0], "url");

    if (args.size() > 1)
        format = checkAndGetLiteralArgument<String>(args[1], "format_name");
    else
        format = "auto";

    if (args.size() == 3)
        compression_method = checkAndGetLiteralArgument<String>(args[2], "compression_method");
    else
        compression_method = "auto";

    const size_t begin_of_path = url.find('/', url.find("//") + 2);
    path = url.substr(begin_of_path + 1);
    url = url.substr(0, begin_of_path);
    paths = {path};
}

}

#endif
