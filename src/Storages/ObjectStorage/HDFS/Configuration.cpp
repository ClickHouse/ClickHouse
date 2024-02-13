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
{
    url = other.url;
    path = other.path;
    paths = other.paths;
    format = other.format;
    compression_method = other.compression_method;
    structure = other.structure;
}

void StorageHDFSConfiguration::check(ContextPtr context) const
{
    context->getRemoteHostFilter().checkURL(Poco::URI(url));
    checkHDFSURL(url);
}

ObjectStoragePtr StorageHDFSConfiguration::createOrUpdateObjectStorage(ContextPtr context, bool is_readonly) /// NOLINT
{
    UNUSED(is_readonly);
    auto settings = std::make_unique<HDFSObjectStorageSettings>();
    return std::make_shared<HDFSObjectStorage>(url, std::move(settings), context->getConfigRef());
}

void StorageHDFSConfiguration::fromAST(ASTs & args, ContextPtr, bool /* with_structure */)
{
    url = checkAndGetLiteralArgument<String>(args[0], "url");

    String format_name = "auto";
    if (args.size() > 1)
        format_name = checkAndGetLiteralArgument<String>(args[1], "format_name");

    if (format_name == "auto")
        format_name = FormatFactory::instance().getFormatFromFileName(url, true);

    String compression_method;
    if (args.size() == 3)
        compression_method = checkAndGetLiteralArgument<String>(args[2], "compression_method");
    else
        compression_method = "auto";

}
}

#endif
