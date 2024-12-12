#include <Storages/ObjectStorage/S3/Configuration.h>

#if USE_AWS_S3
#include <Common/HTTPHeaderFilter.h>
#include <Core/Settings.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageURL.h>

#include <IO/S3/getObjectInfo.h>
#include <Formats/FormatFactory.h>

#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/ObjectStorages/S3/diskSettings.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

#include <boost/algorithm/string.hpp>
#include <filesystem>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_archive_path_syntax;
    extern const SettingsBool s3_create_new_file_on_insert;
    extern const SettingsBool s3_ignore_file_doesnt_exist;
    extern const SettingsUInt64 s3_list_object_keys_size;
    extern const SettingsBool s3_skip_empty_files;
    extern const SettingsBool s3_truncate_on_insert;
    extern const SettingsBool s3_throw_on_zero_files_match;
    extern const SettingsBool s3_validate_request_settings;
    extern const SettingsSchemaInferenceMode schema_inference_mode;
    extern const SettingsBool schema_inference_use_cache_for_s3;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

static const std::unordered_set<std::string_view> required_configuration_keys = {
    "url",
};

static const std::unordered_set<std::string_view> optional_configuration_keys = {
    "format",
    "compression",
    "compression_method",
    "structure",
    "access_key_id",
    "secret_access_key",
    "session_token",
    "filename",
    "use_environment_credentials",
    "max_single_read_retries",
    "min_upload_part_size",
    "upload_part_size_multiply_factor",
    "upload_part_size_multiply_parts_count_threshold",
    "max_single_part_upload_size",
    "max_connections",
    "expiration_window_seconds",
    "no_sign_request"
};

String StorageS3Configuration::getDataSourceDescription() const
{
    return std::filesystem::path(url.uri.getHost() + std::to_string(url.uri.getPort())) / url.bucket;
}

std::string StorageS3Configuration::getPathInArchive() const
{
    if (url.archive_pattern.has_value())
        return url.archive_pattern.value();

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} is not an archive", getPath());
}

void StorageS3Configuration::check(ContextPtr context) const
{
    validateNamespace(url.bucket);
    context->getGlobalContext()->getRemoteHostFilter().checkURL(url.uri);
    context->getGlobalContext()->getHTTPHeaderFilter().checkHeaders(headers_from_ast);
    Configuration::check(context);
}

void StorageS3Configuration::validateNamespace(const String & name) const
{
    S3::URI::validateBucket(name, {});
}

StorageS3Configuration::StorageS3Configuration(const StorageS3Configuration & other)
    : Configuration(other)
{
    url = other.url;
    static_configuration = other.static_configuration;
    headers_from_ast = other.headers_from_ast;
    keys = other.keys;
}

StorageObjectStorage::QuerySettings StorageS3Configuration::getQuerySettings(const ContextPtr & context) const
{
    const auto & settings = context->getSettingsRef();
    return StorageObjectStorage::QuerySettings{
        .truncate_on_insert = settings[Setting::s3_truncate_on_insert],
        .create_new_file_on_insert = settings[Setting::s3_create_new_file_on_insert],
        .schema_inference_use_cache = settings[Setting::schema_inference_use_cache_for_s3],
        .schema_inference_mode = settings[Setting::schema_inference_mode],
        .skip_empty_files = settings[Setting::s3_skip_empty_files],
        .list_object_keys_size = settings[Setting::s3_list_object_keys_size],
        .throw_on_zero_files_match = settings[Setting::s3_throw_on_zero_files_match],
        .ignore_non_existent_file = settings[Setting::s3_ignore_file_doesnt_exist],
    };
}

ObjectStoragePtr StorageS3Configuration::createObjectStorage(ContextPtr context, bool /* is_readonly */) /// NOLINT
{
    assertInitialized();

    const auto & config = context->getConfigRef();
    const auto & settings = context->getSettingsRef();

    auto s3_settings = getSettings(config, "s3" /* config_prefix */, context, url.uri_str, settings[Setting::s3_validate_request_settings]);

    if (auto endpoint_settings = context->getStorageS3Settings().getSettings(url.uri.toString(), context->getUserName()))
    {
        s3_settings->auth_settings.updateIfChanged(endpoint_settings->auth_settings);
        s3_settings->request_settings.updateIfChanged(endpoint_settings->request_settings);
    }

    s3_settings->auth_settings.updateIfChanged(auth_settings);
    s3_settings->request_settings.updateIfChanged(request_settings);

    if (!headers_from_ast.empty())
    {
        s3_settings->auth_settings.headers.insert(
            s3_settings->auth_settings.headers.end(),
            headers_from_ast.begin(), headers_from_ast.end());
    }

    auto client = getClient(url, *s3_settings, context, /* for_disk_s3 */false);
    auto key_generator = createObjectStorageKeysGeneratorAsIsWithPrefix(url.key);
    auto s3_capabilities = S3Capabilities
    {
        .support_batch_delete = config.getBool("s3.support_batch_delete", true),
        .support_proxy = config.getBool("s3.support_proxy", config.has("s3.proxy")),
    };

    return std::make_shared<S3ObjectStorage>(
        std::move(client), std::move(s3_settings), url, s3_capabilities,
        key_generator, "StorageS3", false);
}

void StorageS3Configuration::fromNamedCollection(const NamedCollection & collection, ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    validateNamedCollection(collection, required_configuration_keys, optional_configuration_keys);

    auto filename = collection.getOrDefault<String>("filename", "");
    if (!filename.empty())
        url = S3::URI(std::filesystem::path(collection.get<String>("url")) / filename, settings[Setting::allow_archive_path_syntax]);
    else
        url = S3::URI(collection.get<String>("url"), settings[Setting::allow_archive_path_syntax]);

    auth_settings.access_key_id = collection.getOrDefault<String>("access_key_id", "");
    auth_settings.secret_access_key = collection.getOrDefault<String>("secret_access_key", "");
    auth_settings.use_environment_credentials = collection.getOrDefault<UInt64>("use_environment_credentials", 1);
    auth_settings.no_sign_request = collection.getOrDefault<bool>("no_sign_request", false);
    auth_settings.expiration_window_seconds = collection.getOrDefault<UInt64>("expiration_window_seconds", S3::DEFAULT_EXPIRATION_WINDOW_SECONDS);

    format = collection.getOrDefault<String>("format", format);
    compression_method = collection.getOrDefault<String>("compression_method", collection.getOrDefault<String>("compression", "auto"));
    structure = collection.getOrDefault<String>("structure", "auto");

    request_settings = S3::RequestSettings(collection, settings, /* validate_settings */true);

    static_configuration = !auth_settings.access_key_id.value.empty() || auth_settings.no_sign_request.changed;

    keys = {url.key};
}

void StorageS3Configuration::fromAST(ASTs & args, ContextPtr context, bool with_structure)
{
    size_t count = StorageURL::evalArgsAndCollectHeaders(args, headers_from_ast, context);

    if (count == 0 || count > getMaxNumberOfArguments(with_structure))
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Storage S3 requires 1 to {} arguments. All supported signatures:\n{}",
            getMaxNumberOfArguments(with_structure),
            getSignatures(with_structure));

    std::unordered_map<std::string_view, size_t> engine_args_to_idx;
    bool no_sign_request = false;

    /// When adding new arguments in the signature don't forget to update addStructureAndFormatToArgsIfNeeded as well.

    /// For 2 arguments we support:
    /// - s3(source, format)
    /// - s3(source, NOSIGN)
    /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN or not.
    if (count == 2)
    {
        auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
        if (boost::iequals(second_arg, "NOSIGN"))
            no_sign_request = true;
        else
            engine_args_to_idx = {{"format", 1}};
    }
    /// For 3 arguments we support:
    /// if with_structure == 0:
    /// - s3(source, NOSIGN, format)
    /// - s3(source, format, compression_method)
    /// - s3(source, access_key_id, secret_access_key)
    /// if with_structure == 1:
    /// - s3(source, NOSIGN, format)
    /// - s3(source, format, structure)
    /// - s3(source, access_key_id, secret_access_key)
    /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN or format name.
    else if (count == 3)
    {
        auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/access_key_id/NOSIGN");
        if (boost::iequals(second_arg, "NOSIGN"))
        {
            no_sign_request = true;
            engine_args_to_idx = {{"format", 2}};
        }
        else if (second_arg == "auto" || FormatFactory::instance().exists(second_arg))
        {
            if (with_structure)
                engine_args_to_idx = {{"format", 1}, {"structure", 2}};
            else
                engine_args_to_idx = {{"format", 1}, {"compression_method", 2}};
        }
        else
            engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}};
    }
    /// For 4 arguments we support:
    /// if with_structure == 0:
    /// - s3(source, access_key_id, secret_access_key, session_token)
    /// - s3(source, access_key_id, secret_access_key, format)
    /// - s3(source, NOSIGN, format, compression_method)
    /// if with_structure == 1:
    /// - s3(source, format, structure, compression_method),
    /// - s3(source, access_key_id, secret_access_key, format),
    /// - s3(source, access_key_id, secret_access_key, session_token)
    /// - s3(source, NOSIGN, format, structure)
    /// We can distinguish them by looking at the 2-nd argument: check if it's a NOSIGN, format name of something else.
    else if (count == 4)
    {
        auto second_arg = checkAndGetLiteralArgument<String>(args[1], "access_key_id/NOSIGN");
        if (boost::iequals(second_arg, "NOSIGN"))
        {
            no_sign_request = true;
            if (with_structure)
                engine_args_to_idx = {{"format", 2}, {"structure", 3}};
            else
                engine_args_to_idx = {{"format", 2}, {"compression_method", 3}};
        }
        else if (with_structure && (second_arg == "auto" || FormatFactory::instance().exists(second_arg)))
        {
            engine_args_to_idx = {{"format", 1}, {"structure", 2}, {"compression_method", 3}};
        }
        else
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "session_token/format");
            if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
            {
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}};
            }
            else
            {
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}};
            }
        }
    }
    /// For 5 arguments we support:
    /// if with_structure == 0:
    /// - s3(source, access_key_id, secret_access_key, session_token, format)
    /// - s3(source, access_key_id, secret_access_key, format, compression)
    /// if with_structure == 1:
    /// - s3(source, access_key_id, secret_access_key, format, structure)
    /// - s3(source, access_key_id, secret_access_key, session_token, format)
    /// - s3(source, NOSIGN, format, structure, compression_method)
    else if (count == 5)
    {
        if (with_structure)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "NOSIGN/access_key_id");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                no_sign_request = true;
                engine_args_to_idx = {{"format", 2}, {"structure", 3}, {"compression_method", 4}};
            }
            else
            {
                auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/session_token");
                if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
                {
                    engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}};
                }
                else
                {
                    engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}};
                }
            }
        }
        else
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "session_token/format");
            if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
            {
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"compression_method", 4}};
            }
            else
            {
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}};
            }
        }
    }
    /// For 6 arguments we support:
    /// if with_structure == 0:
    /// - s3(source, access_key_id, secret_access_key, session_token, format, compression_method)
    /// if with_structure == 1:
    /// - s3(source, access_key_id, secret_access_key, format, structure, compression_method)
    /// - s3(source, access_key_id, secret_access_key, session_token, format, structure)
    else if (count == 6)
    {
        if (with_structure)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/session_token");
            if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
            {
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}, {"compression_method", 5}};
            }
            else
            {
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}};
            }
        }
        else
        {
            engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"compression_method", 5}};
        }
    }
    /// s3(source, access_key_id, secret_access_key, session_token, format, structure, compression_method)
    else if (with_structure && count == 7)
    {
        engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}, {"compression_method", 6}};
    }

    /// This argument is always the first
    url = S3::URI(checkAndGetLiteralArgument<String>(args[0], "url"), context->getSettingsRef()[Setting::allow_archive_path_syntax]);

    if (engine_args_to_idx.contains("format"))
    {
        format = checkAndGetLiteralArgument<String>(args[engine_args_to_idx["format"]], "format");
        /// Set format to configuration only of it's not 'auto',
        /// because we can have default format set in configuration.
        if (format != "auto")
            format = format;
    }

    if (engine_args_to_idx.contains("structure"))
        structure = checkAndGetLiteralArgument<String>(args[engine_args_to_idx["structure"]], "structure");

    if (engine_args_to_idx.contains("compression_method"))
        compression_method = checkAndGetLiteralArgument<String>(args[engine_args_to_idx["compression_method"]], "compression_method");

    if (engine_args_to_idx.contains("access_key_id"))
        auth_settings.access_key_id = checkAndGetLiteralArgument<String>(args[engine_args_to_idx["access_key_id"]], "access_key_id");

    if (engine_args_to_idx.contains("secret_access_key"))
        auth_settings.secret_access_key = checkAndGetLiteralArgument<String>(args[engine_args_to_idx["secret_access_key"]], "secret_access_key");

    if (engine_args_to_idx.contains("session_token"))
        auth_settings.session_token = checkAndGetLiteralArgument<String>(args[engine_args_to_idx["session_token"]], "session_token");

    if (no_sign_request)
        auth_settings.no_sign_request = no_sign_request;

    static_configuration = !auth_settings.access_key_id.value.empty() || auth_settings.no_sign_request.changed;
    auth_settings.no_sign_request = no_sign_request;

    keys = {url.key};
}

void StorageS3Configuration::addStructureAndFormatToArgsIfNeeded(
    ASTs & args, const String & structure_, const String & format_, ContextPtr context)
{
    if (auto collection = tryGetNamedCollectionWithOverrides(args, context))
    {
        /// In case of named collection, just add key-value pairs "format='...', structure='...'"
        /// at the end of arguments to override existed format and structure with "auto" values.
        if (collection->getOrDefault<String>("format", "auto") == "auto")
        {
            ASTs format_equal_func_args = {std::make_shared<ASTIdentifier>("format"), std::make_shared<ASTLiteral>(format_)};
            auto format_equal_func = makeASTFunction("equals", std::move(format_equal_func_args));
            args.push_back(format_equal_func);
        }
        if (collection->getOrDefault<String>("structure", "auto") == "auto")
        {
            ASTs structure_equal_func_args = {std::make_shared<ASTIdentifier>("structure"), std::make_shared<ASTLiteral>(structure_)};
            auto structure_equal_func = makeASTFunction("equals", std::move(structure_equal_func_args));
            args.push_back(structure_equal_func);
        }
    }
    else
    {
        HTTPHeaderEntries tmp_headers;
        size_t count = StorageURL::evalArgsAndCollectHeaders(args, tmp_headers, context);

        if (count == 0 || count > getMaxNumberOfArguments())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected 1 to {} arguments in table function s3, got {}", getMaxNumberOfArguments(), count);

        auto format_literal = std::make_shared<ASTLiteral>(format_);
        auto structure_literal = std::make_shared<ASTLiteral>(structure_);

        /// s3(s3_url)
        if (count == 1)
        {
            /// Add format=auto before structure argument.
            args.push_back(std::make_shared<ASTLiteral>("auto"));
            args.push_back(structure_literal);
        }
        /// s3(s3_url, format) or
        /// s3(s3_url, NOSIGN)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN or not.
        else if (count == 2)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            /// If there is NOSIGN, add format=auto before structure.
            if (boost::iequals(second_arg, "NOSIGN"))
                args.push_back(format_literal);
            else if (checkAndGetLiteralArgument<String>(args[1], "format") == "auto")
                args[1] = format_literal;

            args.push_back(structure_literal);
        }
        /// s3(source, format, structure) or
        /// s3(source, access_key_id, secret_access_key) or
        /// s3(source, NOSIGN, format)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN, format name or neither.
        else if (count == 3)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                if (checkAndGetLiteralArgument<String>(args[2], "format") == "auto")
                    args[2] = format_literal;
                args.push_back(structure_literal);
            }
            else if (second_arg == "auto" || FormatFactory::instance().exists(second_arg))
            {
                if (second_arg == "auto")
                    args[1] = format_literal;
                if (checkAndGetLiteralArgument<String>(args[2], "structure") == "auto")
                    args[2] = structure_literal;
            }
            else
            {
                /// Add format and structure arguments.
                args.push_back(format_literal);
                args.push_back(structure_literal);
            }
        }
        /// s3(source, format, structure, compression_method) or
        /// s3(source, access_key_id, secret_access_key, format) or
        /// s3(source, access_key_id, secret_access_key, session_token) or
        /// s3(source, NOSIGN, format, structure)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN, format name or neither.
        else if (count == 4)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                if (checkAndGetLiteralArgument<String>(args[2], "format") == "auto")
                    args[2] = format_literal;
                if (checkAndGetLiteralArgument<String>(args[3], "structure") == "auto")
                    args[3] = structure_literal;
            }
            else if (second_arg == "auto" || FormatFactory::instance().exists(second_arg))
            {
                if (second_arg == "auto")
                    args[1] = format_literal;
                if (checkAndGetLiteralArgument<String>(args[2], "structure") == "auto")
                    args[2] = structure_literal;
            }
            else
            {
                auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/session_token");
                if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
                {
                    if (checkAndGetLiteralArgument<String>(args[3], "format") == "auto")
                        args[3] = format_literal;
                    args.push_back(structure_literal);
                }
                else
                {
                    args.push_back(format_literal);
                    args.push_back(structure_literal);
                }
            }
        }
        /// s3(source, access_key_id, secret_access_key, format, structure) or
        /// s3(source, access_key_id, secret_access_key, session_token, format) or
        /// s3(source, NOSIGN, format, structure, compression_method)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a NOSIGN keyword name or not.
        else if (count == 5)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                if (checkAndGetLiteralArgument<String>(args[2], "format") == "auto")
                    args[2] = format_literal;
                if (checkAndGetLiteralArgument<String>(args[2], "structure") == "auto")
                    args[3] = structure_literal;
            }
            else
            {
                auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/session_token");
                if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
                {
                    if (checkAndGetLiteralArgument<String>(args[3], "format") == "auto")
                        args[3] = format_literal;
                    if (checkAndGetLiteralArgument<String>(args[4], "structure") == "auto")
                        args[4] = structure_literal;
                }
                else
                {
                    if (checkAndGetLiteralArgument<String>(args[4], "format") == "auto")
                        args[4] = format_literal;
                    args.push_back(structure_literal);
                }
            }
        }
        /// s3(source, access_key_id, secret_access_key, format, structure, compression) or
        /// s3(source, access_key_id, secret_access_key, session_token, format, structure)
        else if (count == 6)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/session_token");
            if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
            {
                if (checkAndGetLiteralArgument<String>(args[3], "format") == "auto")
                    args[3] = format_literal;
                if (checkAndGetLiteralArgument<String>(args[4], "structure") == "auto")
                    args[4] = structure_literal;
            }
            else
            {
                if (checkAndGetLiteralArgument<String>(args[4], "format") == "auto")
                    args[4] = format_literal;
                if (checkAndGetLiteralArgument<String>(args[5], "format") == "auto")
                    args[5] = structure_literal;
            }
        }
        /// s3(source, access_key_id, secret_access_key, session_token, format, structure, compression_method)
        else if (count == 7)
        {
            if (checkAndGetLiteralArgument<String>(args[4], "format") == "auto")
                args[4] = format_literal;
            if (checkAndGetLiteralArgument<String>(args[5], "format") == "auto")
                args[5] = structure_literal;
        }
    }
}

}

#endif
