#include <Storages/ObjectStorage/Web/Configuration.h>

#include <Common/NamedCollections/NamedCollections.h>
#include <Common/HTTPHeaderFilter.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageURL.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Poco/URI.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool engine_url_skip_empty_files;
    extern const SettingsSchemaInferenceMode schema_inference_mode;
    extern const SettingsBool schema_inference_use_cache_for_url;
    extern const SettingsUInt64 glob_expansion_max_elements;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

void WebStorageParsedArguments::fromNamedCollection(const NamedCollection & collection, ContextPtr context)
{
    url = collection.get<String>("url");
    format = collection.getOrDefault<String>("format", "auto");
    compression_method = collection.getOrDefault<String>("compression_method", collection.getOrDefault<String>("compression", "auto"));
    structure = collection.getOrDefault<String>("structure", "auto");

    headers_from_ast = getHeadersFromNamedCollection(collection);
    (void)context;
}

void WebStorageParsedArguments::fromAST(ASTs & args, ContextPtr context, bool with_structure)
{
    if (args.empty() || args.size() > WebStorageParsedArguments::getMaxNumberOfArguments(with_structure))
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Storage URL requires 1 to {} arguments. All supported signatures:\n{}",
            WebStorageParsedArguments::getMaxNumberOfArguments(with_structure),
            WebStorageParsedArguments::getSignatures(with_structure));

    size_t count = StorageURL::evalArgsAndCollectHeaders(args, headers_from_ast, context);

    if (count == 0)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "URL requires at least 1 argument");

    url = checkAndGetLiteralArgument<String>(args[0], "url");

    if (count > 1)
        format = checkAndGetLiteralArgument<String>(args[1], "format");

    if (with_structure)
    {
        if (count > 2)
            structure = checkAndGetLiteralArgument<String>(args[2], "structure");
        if (count > 3)
            compression_method = checkAndGetLiteralArgument<String>(args[3], "compression_method");
    }
    else if (count > 2)
    {
        compression_method = checkAndGetLiteralArgument<String>(args[2], "compression_method");
    }
}

StorageObjectStorageQuerySettings StorageWebConfiguration::getQuerySettings(const ContextPtr & context) const
{
    const auto & settings = context->getSettingsRef();
    return StorageObjectStorageQuerySettings{
        .truncate_on_insert = false,
        .create_new_file_on_insert = false,
        .schema_inference_use_cache = settings[Setting::schema_inference_use_cache_for_url],
        .schema_inference_mode = settings[Setting::schema_inference_mode],
        .skip_empty_files = settings[Setting::engine_url_skip_empty_files],
        .list_object_keys_size = settings[Setting::glob_expansion_max_elements],
        .throw_on_zero_files_match = false,
        .ignore_non_existent_file = false};
}

void StorageWebConfiguration::check(ContextPtr context)
{
    StorageObjectStorageConfiguration::check(context);
    context->getGlobalContext()->getRemoteHostFilter().checkURL(Poco::URI(raw_url, false));
    context->getGlobalContext()->getHTTPHeaderFilter().checkAndNormalizeHeaders(headers_from_ast);
}

ObjectStoragePtr StorageWebConfiguration::createObjectStorage(ContextPtr context, bool) /// NOLINT
{
    assertInitialized();
    return std::make_shared<WebObjectStorage>(base_url, query_fragment, context, headers_from_ast);
}

void StorageWebConfiguration::addStructureAndFormatToArgsIfNeeded(
    ASTs & args,
    const String & structure_,
    const String & format_,
    ContextPtr context,
    bool with_structure)
{
    if (auto collection = tryGetNamedCollectionWithOverrides(args, context))
    {
        if (collection->getOrDefault<String>("format", "auto") == "auto")
        {
            ASTs format_equal_func_args;
            format_equal_func_args.emplace_back(ASTPtr(new ASTIdentifier("format")));
            format_equal_func_args.emplace_back(ASTPtr(new ASTLiteral(format_)));
            auto format_equal_func = makeASTOperator("equals", std::move(format_equal_func_args));
            args.push_back(format_equal_func);
        }
        if (with_structure && collection->getOrDefault<String>("structure", "auto") == "auto")
        {
            ASTs structure_equal_func_args;
            structure_equal_func_args.emplace_back(ASTPtr(new ASTIdentifier("structure")));
            structure_equal_func_args.emplace_back(ASTPtr(new ASTLiteral(structure_)));
            auto structure_equal_func = makeASTOperator("equals", std::move(structure_equal_func_args));
            args.push_back(structure_equal_func);
        }
        return;
    }

    for (auto & arg : args)
    {
        const auto * func = arg->as<ASTFunction>();
        if (func && func->name == "headers")
        {
            // `headers(...)` is a URL-specific argument and is not a SQL function.
            // It must be preserved for URL argument parsing instead of being evaluated.
            continue;
        }
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);
    }

    size_t count = args.size();
    ASTPtr format_literal = ASTPtr(new ASTLiteral(format_));
    ASTPtr structure_literal = ASTPtr(new ASTLiteral(structure_));

    if (count == 1)
    {
        args.push_back(format_literal);
        if (with_structure)
            args.push_back(structure_literal);
    }
    else if (count == 2)
    {
        if (checkAndGetLiteralArgument<String>(args[1], "format") == "auto")
            args.back() = format_literal;
        if (with_structure)
            args.push_back(structure_literal);
    }
    else if (count == 3 && with_structure)
    {
        if (checkAndGetLiteralArgument<String>(args[1], "format") == "auto")
            args[1] = format_literal;
        if (checkAndGetLiteralArgument<String>(args[2], "structure") == "auto")
            args[2] = structure_literal;
    }
}

void StorageWebConfiguration::initializeFromParsedArguments(WebStorageParsedArguments && parsed_arguments)
{
    StorageObjectStorageConfiguration::initializeFromParsedArguments(parsed_arguments);
    raw_url = parsed_arguments.url;
    headers_from_ast = std::move(parsed_arguments.headers_from_ast);

    setNamespaceFromURL();
    paths = {path};
    setPathForRead(path);
}

void StorageWebConfiguration::fromNamedCollection(const NamedCollection & collection, ContextPtr context)
{
    WebStorageParsedArguments parsed_arguments;
    parsed_arguments.fromNamedCollection(collection, context);
    initializeFromParsedArguments(std::move(parsed_arguments));
}

void StorageWebConfiguration::fromAST(ASTs & args, ContextPtr context, bool with_structure)
{
    WebStorageParsedArguments parsed_arguments;
    parsed_arguments.fromAST(args, context, with_structure);
    initializeFromParsedArguments(std::move(parsed_arguments));
}

void StorageWebConfiguration::fromDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure)
{
    WebStorageParsedArguments parsed_arguments;
    parsed_arguments.fromAST(args, context, with_structure);
    if (parsed_arguments.url.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "URL is required for disk {}", disk_name);
    initializeFromParsedArguments(std::move(parsed_arguments));
}

void StorageWebConfiguration::setNamespaceFromURL()
{
    Poco::URI uri(raw_url, false);

    namespace_prefix = uri.getHost();
    if (uri.getPort())
        namespace_prefix += ":" + std::to_string(uri.getPort());

    base_url = uri.getScheme() + "://" + uri.getAuthority() + "/";
    path.path = uri.getPath();
    if (path.path.starts_with('/'))
        path.path.erase(0, 1);

    query_fragment.clear();
    if (!uri.getRawQuery().empty())
        query_fragment = "?" + uri.getRawQuery();
    if (!uri.getFragment().empty())
        query_fragment += "#" + uri.getFragment();
}

}
