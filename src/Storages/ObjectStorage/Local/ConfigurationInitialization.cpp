#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorageTableOptions.h>

#include <Interpreters/Context.h>
#include <Storages/NamedCollectionsHelpers.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

StorageParsedArguments LocalStorageParsedArguments::extractBaseArguments()
{
    return std::move(static_cast<StorageParsedArguments &>(*this));
}

ConfigWithOptions fromLocalAST(ASTs & args, ContextPtr context, bool with_structure)
{
    LocalStorageParsedArguments parsed_arguments;
    parsed_arguments.fromAST(args, context, with_structure);
    auto config = std::make_shared<StorageLocalConfiguration>(parsed_arguments.path);
    auto table_options = tableOptionsFromParsedArguments(parsed_arguments.extractBaseArguments(), config->getRawPath());
    return {config, std::move(table_options)};
}

ConfigWithOptions fromLocalDisk(const String & disk_name_, ASTs & args, ContextPtr context, bool with_structure)
{
    LocalStorageParsedArguments parsed_arguments;
    auto disk = context->getDisk(disk_name_);
    parsed_arguments.fromDisk(disk, args, context, with_structure);
    fs::path root = disk->getPath();
    fs::path suffix = parsed_arguments.path_suffix;
    String full_path = String(root / suffix);
    auto config = std::make_shared<StorageLocalConfiguration>(full_path, disk_name_);
    auto table_options = tableOptionsFromParsedArguments(parsed_arguments.extractBaseArguments(), config->getRawPath());
    return {config, std::move(table_options)};
}

ConfigWithOptions fromLocalNamedCollection(const NamedCollection & collection, ContextPtr context)
{
    LocalStorageParsedArguments parsed_arguments;
    parsed_arguments.fromNamedCollection(collection, context);
    auto config = std::make_shared<StorageLocalConfiguration>(parsed_arguments.path);
    auto table_options = tableOptionsFromParsedArguments(parsed_arguments.extractBaseArguments(), config->getRawPath());
    return {config, std::move(table_options)};
}

}
