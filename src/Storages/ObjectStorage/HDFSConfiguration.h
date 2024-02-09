#pragma once
#include "config.h"

#if USE_HDFS

#include <Storages/ObjectStorage/Configuration.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Interpreters/Context.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Formats/FormatFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST.h>
#include <Disks/ObjectStorages/HDFS/HDFSObjectStorage.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class StorageHDFSConfiguration : public StorageObjectStorageConfiguration
{
public:
    Path getPath() const override { return path; }
    void setPath(const Path & path_) override { path = path_; }

    const Paths & getPaths() const override { return paths; }
    Paths & getPaths() override { return paths; }

    String getNamespace() const override { return ""; }
    String getDataSourceDescription() override { return url; }

    void check(ContextPtr context) const override
    {
        context->getRemoteHostFilter().checkURL(Poco::URI(url));
        checkHDFSURL(url);
    }
    StorageObjectStorageConfigurationPtr clone() override
    {
        auto configuration = std::make_shared<StorageHDFSConfiguration>();
        return configuration;
    }

    ObjectStoragePtr createOrUpdateObjectStorage(ContextPtr context, bool is_readonly = true) override /// NOLINT
    {
        UNUSED(is_readonly);
        auto settings = std::make_unique<HDFSObjectStorageSettings>();
        return std::make_shared<HDFSObjectStorage>(url, std::move(settings), context->getConfigRef());
    }

    void fromNamedCollection(const NamedCollection &) override {}
    void fromAST(ASTs & args, ContextPtr, bool /* with_structure */) override
    {
        url = checkAndGetLiteralArgument<String>(args[0], "url");

        String format_name = "auto";
        if (args.size() > 1)
            format_name = checkAndGetLiteralArgument<String>(args[1], "format_name");

        if (format_name == "auto")
            format_name = FormatFactory::instance().getFormatFromFileName(url, true);

        String compression_method;
        if (args.size() == 3)
        {
            compression_method = checkAndGetLiteralArgument<String>(args[2], "compression_method");
        } else compression_method = "auto";

    }
    static void addStructureToArgs(ASTs &, const String &, ContextPtr) {}

private:
    String url;
    String path;
    std::vector<String> paths;
};

}

#endif
