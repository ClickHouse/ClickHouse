#include <Storages/ObjectStorage/S3/Configuration.h>

#if USE_AWS_S3

#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageURL.h>
#include <Formats/FormatFactory.h>
#include <boost/algorithm/string.hpp>
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{
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

String StorageS3Configuration::getDataSourceDescription()
{
    return fs::path(url.uri.getHost() + std::to_string(url.uri.getPort())) / url.bucket;
}

void StorageS3Configuration::check(ContextPtr context) const
{
    context->getGlobalContext()->getRemoteHostFilter().checkURL(url.uri);
    context->getGlobalContext()->getHTTPHeaderFilter().checkHeaders(headers_from_ast);
}

StorageS3Configuration::StorageS3Configuration(const StorageS3Configuration & other)
{
    url = other.url;
    auth_settings = other.auth_settings;
    request_settings = other.request_settings;
    static_configuration = other.static_configuration;
    headers_from_ast = other.headers_from_ast;
    keys = other.keys;
    initialized = other.initialized;

    format = other.format;
    compression_method = other.compression_method;
    structure = other.structure;
}

ObjectStoragePtr StorageS3Configuration::createOrUpdateObjectStorage(ContextPtr context, bool /* is_readonly */) /// NOLINT
{
    auto s3_settings = context->getStorageS3Settings().getSettings(url.uri.toString());
    request_settings = s3_settings.request_settings;
    request_settings.updateFromSettings(context->getSettings());

    if (!initialized || (!static_configuration && auth_settings.hasUpdates(s3_settings.auth_settings)))
    {
        auth_settings.updateFrom(s3_settings.auth_settings);
        keys[0] = url.key;
        initialized = true;
    }

    const auto & config = context->getConfigRef();
    auto s3_capabilities = S3Capabilities
    {
        .support_batch_delete = config.getBool("s3.support_batch_delete", true),
        .support_proxy = config.getBool("s3.support_proxy", config.has("s3.proxy")),
    };

    auto s3_storage_settings = std::make_unique<S3ObjectStorageSettings>(
        request_settings,
        config.getUInt64("s3.min_bytes_for_seek", 1024 * 1024),
        config.getInt("s3.list_object_keys_size", 1000),
        config.getInt("s3.objects_chunk_size_to_delete", 1000),
        config.getBool("s3.readonly", false));

    auto key_generator = createObjectStorageKeysGeneratorAsIsWithPrefix(url.key);
    auto client = createClient(context);
    std::string disk_name = "StorageS3";

    return std::make_shared<S3ObjectStorage>(
        std::move(client), std::move(s3_storage_settings), url, s3_capabilities, key_generator, /*disk_name*/disk_name);
}

std::unique_ptr<S3::Client> StorageS3Configuration::createClient(ContextPtr context)
{
    const Settings & global_settings = context->getGlobalContext()->getSettingsRef();
    const Settings & local_settings = context->getSettingsRef();

    auto client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        auth_settings.region,
        context->getRemoteHostFilter(),
        static_cast<unsigned>(global_settings.s3_max_redirects),
        static_cast<unsigned>(global_settings.s3_retry_attempts),
        global_settings.enable_s3_requests_logging,
        /* for_disk_s3 = */ false,
        request_settings.get_request_throttler,
        request_settings.put_request_throttler,
        url.uri.getScheme());

    client_configuration.endpointOverride = url.endpoint;
    client_configuration.maxConnections = static_cast<unsigned>(request_settings.max_connections);
    client_configuration.http_connection_pool_size = global_settings.s3_http_connection_pool_size;

    auto headers = auth_settings.headers;
    if (!headers_from_ast.empty())
        headers.insert(headers.end(), headers_from_ast.begin(), headers_from_ast.end());

    client_configuration.requestTimeoutMs = request_settings.request_timeout_ms;

    S3::ClientSettings client_settings{
        .use_virtual_addressing = url.is_virtual_hosted_style,
        .disable_checksum = local_settings.s3_disable_checksum,
        .gcs_issue_compose_request = context->getConfigRef().getBool("s3.gcs_issue_compose_request", false),
    };

    auto credentials = Aws::Auth::AWSCredentials(auth_settings.access_key_id,
                                                 auth_settings.secret_access_key,
                                                 auth_settings.session_token);

    auto credentials_configuration = S3::CredentialsConfiguration
    {
        auth_settings.use_environment_credentials.value_or(context->getConfigRef().getBool("s3.use_environment_credentials", true)),
        auth_settings.use_insecure_imds_request.value_or(context->getConfigRef().getBool("s3.use_insecure_imds_request", false)),
        auth_settings.expiration_window_seconds.value_or(context->getConfigRef().getUInt64("s3.expiration_window_seconds", S3::DEFAULT_EXPIRATION_WINDOW_SECONDS)),
        auth_settings.no_sign_request.value_or(context->getConfigRef().getBool("s3.no_sign_request", false)),
    };

    return S3::ClientFactory::instance().create(
        client_configuration,
        client_settings,
        credentials.GetAWSAccessKeyId(),
        credentials.GetAWSSecretKey(),
        auth_settings.server_side_encryption_customer_key_base64,
        auth_settings.server_side_encryption_kms_config,
        std::move(headers),
        credentials_configuration);
}

void StorageS3Configuration::fromNamedCollection(const NamedCollection & collection)
{
    validateNamedCollection(collection, required_configuration_keys, optional_configuration_keys);

    auto filename = collection.getOrDefault<String>("filename", "");
    if (!filename.empty())
        url = S3::URI(std::filesystem::path(collection.get<String>("url")) / filename);
    else
        url = S3::URI(collection.get<String>("url"));

    auth_settings.access_key_id = collection.getOrDefault<String>("access_key_id", "");
    auth_settings.secret_access_key = collection.getOrDefault<String>("secret_access_key", "");
    auth_settings.use_environment_credentials = collection.getOrDefault<UInt64>("use_environment_credentials", 1);
    auth_settings.no_sign_request = collection.getOrDefault<bool>("no_sign_request", false);
    auth_settings.expiration_window_seconds = collection.getOrDefault<UInt64>("expiration_window_seconds", S3::DEFAULT_EXPIRATION_WINDOW_SECONDS);

    format = collection.getOrDefault<String>("format", format);
    compression_method = collection.getOrDefault<String>("compression_method", collection.getOrDefault<String>("compression", "auto"));
    structure = collection.getOrDefault<String>("structure", "auto");

    request_settings = S3Settings::RequestSettings(collection);

    static_configuration = !auth_settings.access_key_id.empty() || auth_settings.no_sign_request.has_value();

    keys = {url.key};

    //if (format == "auto" && get_format_from_file)
    if (format == "auto")
        format = FormatFactory::instance().getFormatFromFileName(url.key, true);
}

void StorageS3Configuration::fromAST(ASTs & args, ContextPtr context, bool with_structure)
{
    /// Supported signatures: S3('url') S3('url', 'format') S3('url', 'format', 'compression') S3('url', NOSIGN) S3('url', NOSIGN, 'format') S3('url', NOSIGN, 'format', 'compression') S3('url', 'aws_access_key_id', 'aws_secret_access_key') S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'session_token') S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format') S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'session_token', 'format') S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format', 'compression')
    /// S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'session_token', 'format', 'compression')
    /// with optional headers() function

    size_t count = StorageURL::evalArgsAndCollectHeaders(args, headers_from_ast, context);

    if (count == 0 || count > (with_structure ? 7 : 6))
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Storage S3 requires 1 to 5 arguments: "
                        "url, [NOSIGN | access_key_id, secret_access_key], name of used format and [compression_method]");

    std::unordered_map<std::string_view, size_t> engine_args_to_idx;
    bool no_sign_request = false;

    /// For 2 arguments we support 2 possible variants:
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
    /// For 3 arguments we support 2 possible variants:
    /// - s3(source, format, compression_method)
    /// - s3(source, access_key_id, secret_access_key)
    /// - s3(source, NOSIGN, format)
    /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN or format name.
    else if (count == 3)
    {
        auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/access_key_id/NOSIGN");
        if (boost::iequals(second_arg, "NOSIGN"))
        {
            no_sign_request = true;
            engine_args_to_idx = {{"format", 2}};
        }
        else if (second_arg == "auto" || FormatFactory::instance().getAllFormats().contains(second_arg))
        {
            if (with_structure)
                engine_args_to_idx = {{"format", 1}, {"structure", 2}};
            else
                engine_args_to_idx = {{"format", 1}, {"compression_method", 2}};
        }
        else
            engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}};
    }
    /// For 4 arguments we support 3 possible variants:
    /// if with_structure == 0:
    /// - s3(source, access_key_id, secret_access_key, session_token)
    /// - s3(source, access_key_id, secret_access_key, format)
    /// - s3(source, NOSIGN, format, compression_method)
    /// if with_structure == 1:
    /// - s3(source, format, structure, compression_method),
    /// - s3(source, access_key_id, secret_access_key, format),
    /// - s3(source, access_key_id, secret_access_key, session_token)
    /// - s3(source, NOSIGN, format, structure)
    /// We can distinguish them by looking at the 2-nd argument: check if it's a NOSIGN or not.
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
        else if (with_structure && (second_arg == "auto" || FormatFactory::instance().getAllFormats().contains(second_arg)))
        {
            engine_args_to_idx = {{"format", 1}, {"structure", 2}, {"compression_method", 3}};
        }
        else
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "session_token/format");
            if (fourth_arg == "auto" || FormatFactory::instance().getAllFormats().contains(fourth_arg))
            {
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}};
            }
            else
            {
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}};
            }
        }
    }
    /// For 5 arguments we support 2 possible variants:
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
                if (fourth_arg == "auto" || FormatFactory::instance().getAllFormats().contains(fourth_arg))
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
            if (fourth_arg == "auto" || FormatFactory::instance().getAllFormats().contains(fourth_arg))
            {
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"compression_method", 4}};
            }
            else
            {
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}};
            }
        }
    }
    else if (count == 6)
    {
        if (with_structure)
        {
            /// - s3(source, access_key_id, secret_access_key, format, structure, compression_method)
            /// - s3(source, access_key_id, secret_access_key, session_token, format, structure)
            /// We can distinguish them by looking at the 4-th argument: check if it's a format name or not
            auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/session_token");
            if (fourth_arg == "auto" || FormatFactory::instance().getAllFormats().contains(fourth_arg))
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
    else if (with_structure && count == 7)
    {
        engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}, {"compression_method", 6}};
    }

    /// This argument is always the first
    url = S3::URI(checkAndGetLiteralArgument<String>(args[0], "url"));

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

    static_configuration = !auth_settings.access_key_id.empty() || auth_settings.no_sign_request.has_value();
    auth_settings.no_sign_request = no_sign_request;

    keys = {url.key};

    // if (format == "auto" && get_format_from_file)
    if (format == "auto")
        format = FormatFactory::instance().getFormatFromFileName(url.key, true);
}

void StorageS3Configuration::addStructureToArgs(ASTs & args, const String & structure_, ContextPtr context)
{
    if (tryGetNamedCollectionWithOverrides(args, context))
    {
        /// In case of named collection, just add key-value pair "structure='...'"
        /// at the end of arguments to override existed structure.
        ASTs equal_func_args = {std::make_shared<ASTIdentifier>("structure"), std::make_shared<ASTLiteral>(structure_)};
        auto equal_func = makeASTFunction("equals", std::move(equal_func_args));
        args.push_back(equal_func);
    }
    else
    {
        HTTPHeaderEntries tmp_headers;
        size_t count = StorageURL::evalArgsAndCollectHeaders(args, tmp_headers, context);

        if (count == 0 || count > 6)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected 1 to 6 arguments in table function, got {}", count);

        auto structure_literal = std::make_shared<ASTLiteral>(structure_);

        /// s3(s3_url)
        if (count == 1)
        {
            /// Add format=auto before structure argument.
            args.push_back(std::make_shared<ASTLiteral>("auto"));
            args.push_back(structure_literal);
        }
        /// s3(s3_url, format) or s3(s3_url, NOSIGN)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN or not.
        else if (count == 2)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            /// If there is NOSIGN, add format=auto before structure.
            if (boost::iequals(second_arg, "NOSIGN"))
                args.push_back(std::make_shared<ASTLiteral>("auto"));
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
                args.push_back(structure_literal);
            }
            else if (second_arg == "auto" || FormatFactory::instance().getAllFormats().contains(second_arg))
            {
                args[count - 1] = structure_literal;
            }
            else
            {
                /// Add format=auto before structure argument.
                args.push_back(std::make_shared<ASTLiteral>("auto"));
                args.push_back(structure_literal);
            }
        }
        /// s3(source, format, structure, compression_method) or
        /// s3(source, access_key_id, secret_access_key, format) or
        /// s3(source, NOSIGN, format, structure)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN, format name or neither.
        else if (count == 4)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                args[count - 1] = structure_literal;
            }
            else if (second_arg == "auto" || FormatFactory::instance().getAllFormats().contains(second_arg))
            {
                args[count - 2] = structure_literal;
            }
            else
            {
                args.push_back(structure_literal);
            }
        }
        /// s3(source, access_key_id, secret_access_key, format, structure) or
        /// s3(source, NOSIGN, format, structure, compression_method)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a NOSIGN keyword name or not.
        else if (count == 5)
        {
            auto sedond_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            if (boost::iequals(sedond_arg, "NOSIGN"))
            {
                args[count - 2] = structure_literal;
            }
            else
            {
                args[count - 1] = structure_literal;
            }
        }
        /// s3(source, access_key_id, secret_access_key, format, structure, compression)
        else if (count == 6)
        {
            args[count - 2] = structure_literal;
        }
    }
}

}

#endif
