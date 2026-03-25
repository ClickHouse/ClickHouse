#include "config.h"

#if USE_HDFS

#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorageTableOptions.h>

#include <Interpreters/Context.h>
#include <Storages/NamedCollectionsHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

StorageParsedArguments HDFSStorageParsedArguments::extractBaseArguments()
{
    return std::move(static_cast<StorageParsedArguments &>(*this));
}

StorageHDFSConfiguration::StorageHDFSConfiguration(const std::string & url_str)
{
    setURL(url_str);
}

ConfigWithOptions fromHDFSAST(ASTs & args, ContextPtr context, bool with_structure)
{
    HDFSStorageParsedArguments parsed_arguments;
    parsed_arguments.fromAST(args, context, with_structure);
    auto table_options = tableOptionsFromParsedArguments(parsed_arguments.extractBaseArguments());
    auto config = std::make_shared<StorageHDFSConfiguration>(parsed_arguments.url_str);
    return {config, std::move(table_options)};
}

ConfigWithOptions fromHDFSNamedCollection(const NamedCollection & collection, ContextPtr context)
{
    HDFSStorageParsedArguments parsed_arguments;
    parsed_arguments.fromNamedCollection(collection, context);
    auto table_options = tableOptionsFromParsedArguments(parsed_arguments.extractBaseArguments());
    auto config = std::make_shared<StorageHDFSConfiguration>(parsed_arguments.url_str);
    return {config, std::move(table_options)};
}

ConfigWithOptions fromHDFSDisk(const String & /*disk_name*/, ASTs & /*args*/, ContextPtr /*context*/, bool /*with_structure*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "method `fromDisk` is not implemented for HDFS");
}

}

#endif
