#include <Storages/ObjectStorage/Web/Configuration.h>

#include <Common/NamedCollections/NamedCollections.h>
#include <Common/HTTPHeaderFilter.h>
#include <Common/parseRemoteDescription.h>
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
    extern const SettingsUInt64 url_wildcard_max_directories_to_read;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

using URLFailoverOptions = std::vector<String>;
using URLShardsWithFailover = std::vector<URLFailoverOptions>;

URLShardsWithFailover parseURLShardsWithFailover(const String & uri, size_t max_addresses, const String & func_name = "url")
{
    auto disclosed_urls = parseRemoteDescription(uri, 0, uri.size(), ',', max_addresses, func_name);

    URLShardsWithFailover result;
    result.reserve(disclosed_urls.size());
    size_t url_options_count = 0;

    for (const auto & disclosed_url : disclosed_urls)
    {
        auto failover_options = parseRemoteDescription(disclosed_url, 0, disclosed_url.size(), '|', max_addresses, func_name);
        if (url_options_count + failover_options.size() > max_addresses)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}': first argument generates too many result addresses", func_name);

        url_options_count += failover_options.size();
        result.push_back(std::move(failover_options));
    }

    return result;
}

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
    for (const auto & url_shard : url_shards)
    {
        for (const auto & url_option : url_shard)
            context->getGlobalContext()->getRemoteHostFilter().checkURL(Poco::URI(url_option.base_url + url_option.query_fragment, false));
    }
    context->getGlobalContext()->getHTTPHeaderFilter().checkAndNormalizeHeaders(headers_from_ast);
}

ObjectStoragePtr StorageWebConfiguration::createObjectStorage(ContextPtr context, bool, CredentialsConfigurationCallback) /// NOLINT
{
    assertInitialized();
    return std::make_shared<WebObjectStorage>(
        url_shards,
        context,
        headers_from_ast,
        context->getSettingsRef()[Setting::url_wildcard_max_directories_to_read]);
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
            format_equal_func_args.emplace_back(make_intrusive<ASTIdentifier>("format"));
            format_equal_func_args.emplace_back(make_intrusive<ASTLiteral>(format_));
            auto format_equal_func = makeASTOperator("equals", std::move(format_equal_func_args));
            args.push_back(format_equal_func);
        }
        if (with_structure && collection->getOrDefault<String>("structure", "auto") == "auto")
        {
            ASTs structure_equal_func_args;
            structure_equal_func_args.emplace_back(make_intrusive<ASTIdentifier>("structure"));
            structure_equal_func_args.emplace_back(make_intrusive<ASTLiteral>(structure_));
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
    ASTPtr format_literal = make_intrusive<ASTLiteral>(format_);
    ASTPtr structure_literal = make_intrusive<ASTLiteral>(structure_);

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

void StorageWebConfiguration::initializeFromParsedArguments(WebStorageParsedArguments && parsed_arguments, ContextPtr context)
{
    StorageObjectStorageConfiguration::initializeFromParsedArguments(parsed_arguments);
    raw_url = parsed_arguments.url;
    headers_from_ast = std::move(parsed_arguments.headers_from_ast);

    setNamespaceFromURL(context);
    paths = {path};
    setPathForRead(path);
}

void StorageWebConfiguration::fromNamedCollection(const NamedCollection & collection, ContextPtr context)
{
    WebStorageParsedArguments parsed_arguments;
    parsed_arguments.fromNamedCollection(collection, context);
    initializeFromParsedArguments(std::move(parsed_arguments), context);
}

void StorageWebConfiguration::fromAST(ASTs & args, ContextPtr context, bool with_structure)
{
    WebStorageParsedArguments parsed_arguments;
    parsed_arguments.fromAST(args, context, with_structure);
    initializeFromParsedArguments(std::move(parsed_arguments), context);
}

void StorageWebConfiguration::fromDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure)
{
    WebStorageParsedArguments parsed_arguments;
    parsed_arguments.fromAST(args, context, with_structure);
    if (parsed_arguments.url.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "URL is required for disk {}", disk_name);
    initializeFromParsedArguments(std::move(parsed_arguments), context);
}

void StorageWebConfiguration::setNamespaceFromURL(ContextPtr context)
{
    const auto max_addresses = context->getSettingsRef()[Setting::glob_expansion_max_elements];
    const auto scheme_pos = raw_url.find("://");
    const auto authority_start = scheme_pos == String::npos ? 0 : scheme_pos + 3;
    const auto path_start = raw_url.find('/', authority_start);
    const auto query_or_fragment_pos = raw_url.find_first_of("?#", authority_start);
    const bool has_path = path_start != String::npos && (query_or_fragment_pos == String::npos || path_start < query_or_fragment_pos);
    const auto path_end = query_or_fragment_pos == String::npos ? raw_url.size() : query_or_fragment_pos;

    const auto url_root_for_expansion = has_path ? raw_url.substr(0, path_start + 1) : raw_url.substr(0, path_end) + "/";
    const auto query_fragment_part = query_or_fragment_pos == String::npos ? String{} : raw_url.substr(query_or_fragment_pos);
    const auto url_shards_with_failover = parseURLShardsWithFailover(url_root_for_expansion + query_fragment_part, max_addresses, "url");

    path.path = has_path ? raw_url.substr(path_start, path_end - path_start) : String{};
    while (path.path.starts_with('/'))
        path.path.erase(0, 1);

    url_shards.clear();
    url_shards.reserve(url_shards_with_failover.size());

    for (const auto & failover_url_options : url_shards_with_failover)
    {
        WebObjectStorage::URLOptions url_shard;
        url_shard.reserve(failover_url_options.size());

        for (const auto & url_option : failover_url_options)
        {
            Poco::URI uri(url_option, false);

            if (url_shards.empty() && url_shard.empty())
            {
                namespace_prefix = uri.getHost();
                if (uri.getPort())
                    namespace_prefix += ":" + std::to_string(uri.getPort());
            }

            String query_fragment;
            if (!uri.getRawQuery().empty())
                query_fragment = "?" + uri.getRawQuery();
            if (!uri.getFragment().empty())
                query_fragment += "#" + uri.getFragment();

            url_shard.push_back({.base_url = uri.getScheme() + "://" + uri.getAuthority() + "/", .query_fragment = std::move(query_fragment)});
        }

        url_shards.push_back(std::move(url_shard));
    }
}

}
