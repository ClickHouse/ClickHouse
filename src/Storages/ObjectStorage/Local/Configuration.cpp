#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Storages/ObjectStorage/Utils.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool engine_file_skip_empty_files;
    extern const SettingsBool engine_file_truncate_on_insert;
    extern const SettingsSchemaInferenceMode schema_inference_mode;
    extern const SettingsBool schema_inference_use_cache_for_file;
}

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void StorageLocalConfiguration::fromNamedCollection(const NamedCollection & collection, ContextPtr)
{
    path = collection.get<String>("path");
    format = collection.getOrDefault<String>("format", "auto");
    compression_method = collection.getOrDefault<String>("compression_method", collection.getOrDefault<String>("compression", "auto"));
    structure = collection.getOrDefault<String>("structure", "auto");
    paths = {path};
}

void StorageLocalConfiguration::fromDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure)
{
    auto disk = context->getDisk(disk_name);
    ParseFromDiskResult parsing_result = parseFromDisk(args, with_structure, context, disk->getPath());

    fs::path root = disk->getPath();
    fs::path suffix = parsing_result.path_suffix;
    setPathForRead(String(root / suffix));
    setPaths({String(root / suffix)});
    if (parsing_result.format.has_value())
        format = *parsing_result.format;
    if (parsing_result.compression_method.has_value())
        compression_method = *parsing_result.compression_method;
    if (parsing_result.structure.has_value())
        structure = *parsing_result.structure;
}

void StorageLocalConfiguration::fromAST(ASTs & args, ContextPtr context, bool with_structure)
{
    if (args.empty() || args.size() > getMaxNumberOfArguments(with_structure))
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Storage Local requires 1 to {} arguments. All supported signatures:\n{}",
            getMaxNumberOfArguments(with_structure),
            getSignatures(with_structure));

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    path = checkAndGetLiteralArgument<String>(args[0], "path");

    if (args.size() > 1)
    {
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
            compression_method = checkAndGetLiteralArgument<String>(args[3], "compression_method");
        }
    }
    else if (args.size() > 2)
    {
        compression_method = checkAndGetLiteralArgument<String>(args[2], "compression_method");
    }
    paths = {path};
}

StorageObjectStorageQuerySettings StorageLocalConfiguration::getQuerySettings(const ContextPtr & context) const
{
    const auto & settings = context->getSettingsRef();
    return StorageObjectStorageQuerySettings{
        .truncate_on_insert = settings[Setting::engine_file_truncate_on_insert],
        .create_new_file_on_insert = false,
        .schema_inference_use_cache = settings[Setting::schema_inference_use_cache_for_file],
        .schema_inference_mode = settings[Setting::schema_inference_mode],
        .skip_empty_files = settings[Setting::engine_file_skip_empty_files],
        .list_object_keys_size = 0,
        .throw_on_zero_files_match = false,
        .ignore_non_existent_file = false};
}

}
