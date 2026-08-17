#include <memory>
#include <Storages/ObjectStorage/S3/Configuration.h>

#if USE_AWS_S3
#include <Common/HTTPHeaderFilter.h>
#include <Common/logger_useful.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageURL.h>
#include <Interpreters/Context.h>

#include <IO/S3/getObjectInfo.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Formats/FormatFactory.h>

#include <Common/ProxyConfigurationResolverProvider.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/diskSettings.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>

#include <boost/algorithm/string.hpp>
#include <filesystem>
#include <Poco/Util/AbstractConfiguration.h>
#include <Storages/IPartitionStrategy.h>
#include <Storages/ObjectStorage/Utils.h>
#include <IO/S3/URI.h>
#include <IO/S3Defines.h>

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
    extern const SettingsBool compatibility_s3_presigned_url_query_in_path;
    extern const SettingsS3UriStyle s3_uri_style;
    extern const SettingsString s3_base;
}

namespace S3AuthSetting
{
    extern const S3AuthSettingsString access_key_id;
    extern const S3AuthSettingsUInt64 expiration_window_seconds;
    extern const S3AuthSettingsBool no_sign_request;
    extern const S3AuthSettingsString secret_access_key;
    extern const S3AuthSettingsString session_token;
    extern const S3AuthSettingsBool use_environment_credentials;

    extern const S3AuthSettingsString role_arn;
    extern const S3AuthSettingsString role_session_name;
    extern const S3AuthSettingsString external_id;
    extern const S3AuthSettingsString http_client;
    extern const S3AuthSettingsString service_account;
    extern const S3AuthSettingsString metadata_service;
    extern const S3AuthSettingsString request_token_path;
    extern const S3AuthSettingsString google_adc_client_id;
    extern const S3AuthSettingsString google_adc_client_secret;
    extern const S3AuthSettingsString google_adc_refresh_token;
    extern const S3AuthSettingsString impersonate_service_account;
    extern const S3AuthSettingsString impersonation_delegates;
    extern const S3AuthSettingsString impersonation_scopes;
    extern const S3AuthSettingsUInt64 impersonation_lifetime_seconds;
    extern const S3AuthSettingsString iam_credentials_endpoint;
}

namespace S3RequestSetting
{
    extern const S3RequestSettingsString storage_class_name;
}

namespace ServerSetting
{
    extern const ServerSettingsBool s3_load_table_anonymously_if_credentials_restricted;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int ACCESS_DENIED;
}

/// Every GCP impersonation setting, in the named-collection spelling.
static constexpr auto & gcp_impersonation_keys = S3::GCP_IMPERSONATION_SETTING_NAMES;

/// The one entry of `gcp_impersonation_keys` that decides neither *which* identity the impersonated token acts
/// as nor *which host* the source identity's own token is sent to: it only bounds the lifetime of a token the
/// server keeps to itself, with the target, the delegation chain, the scopes and the endpoint all still the
/// operator's, so overriding it escalates nothing. Spelled as the exception rather than as a second list of
/// everything else, so a setting added to `gcp_impersonation_keys` is guarded by default.
static constexpr std::string_view gcp_impersonation_non_identity_key = "impersonation_lifetime_seconds";

void checkQueryOverriddenGcpImpersonation(
    const NamedCollection & collection,
    const ContextPtr & context,
    S3::S3AuthSettings & auth_settings,
    bool is_loading_from_existing_metadata)
{
    if (!context->shouldRestrictUserQueryS3Credentials())
        return;

    if (collection.isQueryOverridden("google_adc_client_id") && collection.isQueryOverridden("google_adc_client_secret")
        && collection.isQueryOverridden("google_adc_refresh_token"))
        return;

    for (const auto & key : gcp_impersonation_keys)
    {
        if (key == gcp_impersonation_non_identity_key)
            continue;

        if (!collection.isQueryOverridden(String(key)))
            continue;

        /// A table created while the restriction was relaxed stores its overrides in its definition, and
        /// `markQueryOverridden` fires again when that definition is re-parsed at startup. Throwing there would
        /// abort the attach and drop the table out of `system.tables`, so defer to
        /// `s3_load_table_anonymously_if_credentials_restricted` exactly as every other restricted-load path
        /// does (`getDiskConfigurationFromAST`, `getCredentialsProvider`, `DatabaseDataLake`): when it is
        /// enabled, drop the whole GCP OAuth block -- no source identity remains, so nothing is impersonated and
        /// the table is merely inaccessible until its credentials are permitted again -- and when the operator
        /// disabled it, fail the load instead of silently downgrading.
        if (is_loading_from_existing_metadata
            && context->getGlobalContext()->getServerSettings()[ServerSetting::s3_load_table_anonymously_if_credentials_restricted])
        {
            LOG_WARNING(
                getLogger("StorageS3Configuration"),
                "Loading this table with an anonymous S3 client: its definition overrides `{}` on a named "
                "collection, which is restricted for user queries "
                "(s3_allow_server_credentials_in_user_queries = 0). The table will be inaccessible until its "
                "credentials resolve to a permitted source. Set the server setting "
                "s3_load_table_anonymously_if_credentials_restricted = 0 to fail loading instead.",
                key);

            auth_settings.clearServerManagedGcpOAuth();
            return;
        }

        throw Exception(
            ErrorCodes::ACCESS_DENIED,
            "`{}` cannot be overridden in a query on a named collection: the GCP service account impersonation it "
            "configures would be performed with the collection's own identity as the source. Supply the Google "
            "Application Default Credentials triple (google_adc_client_id, google_adc_client_secret, "
            "google_adc_refresh_token) in the same query, or enable the setting "
            "`s3_allow_server_credentials_in_user_queries`.",
            key);
    }
}

static const std::unordered_set<std::string_view> required_configuration_keys =
{
    "url",
};

static const std::unordered_set<std::string_view> optional_configuration_keys =
{
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
    "no_sign_request",
    "partition_strategy",
    "partition_columns_in_data_file",
    "storage_class_name",
    "storage_class", /// Interchangeable alias for `storage_class_name`, see issue #68551
    /// Private configuration options
    "role_arn", /// for extra_credentials
    "role_session_name", /// for extra_credentials
    "external_id", /// for extra_credentials
    "http_client", /// For GCP
    "metadata_service", /// For GCP
    "service_account", /// For GCP
    "request_token_path", /// For GCP
    "google_adc_client_id", /// For GCP (explicit Application Default Credentials triple)
    "google_adc_client_secret", /// For GCP
    "google_adc_refresh_token", /// For GCP
    "impersonate_service_account", /// For GCP (service account impersonation), also for extra_credentials
    "impersonation_delegates", /// For GCP, also for extra_credentials
    "impersonation_scopes", /// For GCP, also for extra_credentials
    "impersonation_lifetime_seconds", /// For GCP
    "iam_credentials_endpoint", /// For GCP
};

String StorageS3Configuration::getDataSourceDescription() const
{
    return std::filesystem::path(url.uri.getHost() + std::to_string(url.uri.getPort())) / url.bucket;
}

std::string StorageS3Configuration::getPathInArchive() const
{
    if (url.archive_pattern.has_value())
        return url.archive_pattern.value();

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} is not an archive", getRawPath().path);
}

void StorageS3Configuration::check(ContextPtr context)
{
    validateNamespace(url.bucket);
    context->getGlobalContext()->getRemoteHostFilter().checkURL(url.uri);
    context->getGlobalContext()->getHTTPHeaderFilter().checkAndNormalizeHeaders(headers_from_ast);
    StorageObjectStorageConfiguration::check(context);
}

void StorageS3Configuration::validateNamespace(const String & name) const
{
    S3::URI::validateBucket(name, {});
}

StorageObjectStorageQuerySettings StorageS3Configuration::getQuerySettings(const ContextPtr & context) const
{
    const auto & settings = context->getSettingsRef();
    return StorageObjectStorageQuerySettings{
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

ObjectStoragePtr StorageS3Configuration::createObjectStorage(ContextPtr context, bool /* is_readonly */, CredentialsConfigurationCallback refresh_credentials_callback) /// NOLINT
{
    assertInitialized();

    if (!headers_from_ast.empty())
    {
        s3_settings->auth_settings.headers.insert(
            s3_settings->auth_settings.headers.end(),
            headers_from_ast.begin(), headers_from_ast.end());
    }

    auto client = getClient(
        url, *s3_settings, context, /* for_disk_s3 */ false, /*opt_disk_name*/ {}, /*refresh_credentials_callback*/ std::nullopt,
        is_loading_from_existing_metadata, force_anonymous_load_fallback);

    auto client_refresher = [refresh_credentials_callback, this, context_ = Context::createCopy(context)] () -> std::unique_ptr<S3::Client>
    {
        if (!refresh_credentials_callback)
            return nullptr;
        auto new_client = getClient(
            url, *s3_settings, context_, /* for_disk_s3 */ false, /*opt_disk_name*/ {}, refresh_credentials_callback,
            is_loading_from_existing_metadata, force_anonymous_load_fallback);
        return new_client;
    };
    return std::make_shared<S3ObjectStorage>(
        std::move(client),
        std::make_unique<S3Settings>(*s3_settings),
        url,
        *s3_capabilities,
        /*key_generator=*/nullptr,
        "StorageS3",
        false,
        client_refresher,
        /*client_restricts_server_credentials=*/context->shouldRestrictUserQueryS3Credentials());
}

void S3StorageParsedArguments::fromNamedCollection(
    const NamedCollection & collection, ContextPtr context, bool is_loading_from_existing_metadata)
{
    const auto & settings = context->getSettingsRef();
    validateNamedCollection(collection, required_configuration_keys, optional_configuration_keys);

    /// Resolve relative URLs against the `s3_base` setting. When the setting rewrote the URL,
    /// record the resolved value so that `StorageObjectStorageConfiguration::initialize`
    /// materializes it back into the persisted engine args (`url='...'` override), keeping the
    /// persisted DDL independent of `s3_base` at attach time.
    const String raw_collection_url = collection.get<String>("url");
    const String collection_url = StorageURL::resolveURLBase(raw_collection_url, settings[Setting::s3_base].value, "s3_base");
    if (collection_url != raw_collection_url)
        url_overridden_by_base_setting = collection_url;

    auto filename = collection.getOrDefault<String>("filename", "");
    if (!filename.empty())
        url = S3::URI(
            std::filesystem::path(collection_url) / filename,
            settings[Setting::allow_archive_path_syntax],
            /*keep_presigned_query_parameters*/ !settings[Setting::compatibility_s3_presigned_url_query_in_path],
            /*uri_style*/ settings[Setting::s3_uri_style]);
    else
        url = S3::URI(
            collection_url,
            settings[Setting::allow_archive_path_syntax],
            /*keep_presigned_query_parameters*/ !settings[Setting::compatibility_s3_presigned_url_query_in_path],
            /*uri_style*/ settings[Setting::s3_uri_style]);

    const auto & config = context->getConfigRef();

    s3_settings = std::make_unique<S3Settings>();
    s3_settings->loadFromConfigForObjectStorage(
        config, "s3", context->getSettingsRef(), url.uri.getScheme(), context->getSettingsRef()[Setting::s3_validate_request_settings]);

    if (auto endpoint_settings = context->getStorageS3Settings().getSettings(url.uri.toString(), context->getUserName()))
    {
        s3_settings->auth_settings.updateIfChanged(endpoint_settings->auth_settings);
        s3_settings->request_settings.updateIfChanged(endpoint_settings->request_settings);
    }

    /// Under the restriction the collection must fully define its request-auth material, so drop the
    /// headers/access-headers and SSE-C/SSE-KMS keys merged from the server `<s3>`/endpoint config above.
    /// Otherwise a URL-only collection would still send the server's headers (which can include `Authorization`)
    /// or encryption keys to its endpoint, breaking the contract that such a collection reads anonymously. With
    /// the opt-in (`s3_allow_server_credentials_in_user_queries = 1`) the server material is kept.
    if (context->shouldRestrictUserQueryS3Credentials())
        s3_settings->auth_settings.clearServerManagedRequestAuth();

    s3_settings->auth_settings[S3AuthSetting::access_key_id] = collection.getOrDefault<String>("access_key_id", "");
    s3_settings->auth_settings[S3AuthSetting::secret_access_key] = collection.getOrDefault<String>("secret_access_key", "");
    /// Default to 0 so a URL-only collection reads anonymously instead of using the server's identity; a
    /// collection can still opt in with `use_environment_credentials = 1`.
    s3_settings->auth_settings[S3AuthSetting::use_environment_credentials]
        = collection.getOrDefault<UInt64>("use_environment_credentials", 0);
    s3_settings->auth_settings[S3AuthSetting::no_sign_request] = collection.getOrDefault<bool>("no_sign_request", false);
    s3_settings->auth_settings[S3AuthSetting::expiration_window_seconds]
        = collection.getOrDefault<UInt64>("expiration_window_seconds", S3::DEFAULT_EXPIRATION_WINDOW_SECONDS);
    s3_settings->auth_settings[S3AuthSetting::session_token] = collection.getOrDefault<String>("session_token", "");

    if (collection.has("partition_strategy"))
    {
        const auto partition_strategy_name = collection.get<std::string>("partition_strategy");
        const auto partition_strategy_type_opt = magic_enum::enum_cast<PartitionStrategyFactory::StrategyType>(partition_strategy_name, magic_enum::case_insensitive);

        if (!partition_strategy_type_opt)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Partition strategy {} is not supported", partition_strategy_name);
        }

        partition_strategy_type = partition_strategy_type_opt.value();
    }

    if (collection.has("partition_columns_in_data_file"))
    {
        partition_columns_in_data_file = collection.get<bool>("partition_columns_in_data_file");
        partition_columns_in_data_file_was_set = true;
    }
    else
        partition_columns_in_data_file = partition_strategy_type != PartitionStrategyFactory::StrategyType::HIVE;
    s3_settings->auth_settings[S3AuthSetting::role_arn] = collection.getOrDefault<String>("role_arn", "");
    s3_settings->auth_settings[S3AuthSetting::role_session_name] = collection.getOrDefault<String>("role_session_name", "");
    s3_settings->auth_settings[S3AuthSetting::external_id] = collection.getOrDefault<String>("external_id", "");

    /// A query-overridden `role_arn` (`s3(collection, role_arn = ...)`) is honored even under the restriction
    /// (STS assume-role is the documented way to grant ClickHouse Cloud access to a private bucket), but it
    /// must not be assumed using the collection's operator-provisioned keys as the STS base. When the same
    /// query did not also supply the base key pair, drop the collection keys so the assume-role call is signed
    /// by the server's ambient identity instead; a `role_arn` from the stored collection definition is left
    /// untouched and keeps the collection's own keys as its base.
    if (context->shouldRestrictUserQueryS3Credentials() && collection.isQueryOverridden("role_arn")
        && !(collection.isQueryOverridden("access_key_id") && collection.isQueryOverridden("secret_access_key")))
    {
        s3_settings->auth_settings[S3AuthSetting::access_key_id] = "";
        s3_settings->auth_settings[S3AuthSetting::secret_access_key] = "";
        s3_settings->auth_settings[S3AuthSetting::session_token] = "";
    }

    /// A query-overridden `role_arn` must not silently inherit the collection's `external_id`: it is the
    /// secret half of the STS triple, tied to the collection's own role, and reusing it with a
    /// query-chosen role could unlock any role whose trust policy gates on that ExternalId. Drop it unless
    /// the query supplied its own.
    if (context->shouldRestrictUserQueryS3Credentials() && collection.isQueryOverridden("role_arn")
        && !collection.isQueryOverridden("external_id"))
        s3_settings->auth_settings[S3AuthSetting::external_id] = "";

    /// When the query supplies its own key pair but no `session_token`, drop any token inherited from the
    /// collection: it was issued for the collection's keys and would be sent with the query's keys instead
    /// (and would break the STS base when a query-supplied `role_arn` is also present). Mirrors the
    /// explicit-URL path in `fromAST`.
    if (collection.isQueryOverridden("access_key_id") && collection.isQueryOverridden("secret_access_key")
        && !collection.isQueryOverridden("session_token"))
        s3_settings->auth_settings[S3AuthSetting::session_token] = "";

    s3_settings->auth_settings[S3AuthSetting::http_client] = collection.getOrDefault<String>("http_client", "");
    s3_settings->auth_settings[S3AuthSetting::service_account] = collection.getOrDefault<String>("service_account", "");
    s3_settings->auth_settings[S3AuthSetting::metadata_service] = collection.getOrDefault<String>("metadata_service", "");
    s3_settings->auth_settings[S3AuthSetting::request_token_path] = collection.getOrDefault<String>("request_token_path", "");
    /// An explicit Google ADC triple is a user-supplied credential, so `gcp_oauth` with it is allowed.
    s3_settings->auth_settings[S3AuthSetting::google_adc_client_id] = collection.getOrDefault<String>("google_adc_client_id", "");
    s3_settings->auth_settings[S3AuthSetting::google_adc_client_secret] = collection.getOrDefault<String>("google_adc_client_secret", "");
    s3_settings->auth_settings[S3AuthSetting::google_adc_refresh_token] = collection.getOrDefault<String>("google_adc_refresh_token", "");
    s3_settings->auth_settings[S3AuthSetting::impersonate_service_account]
        = collection.getOrDefault<String>("impersonate_service_account", "");
    s3_settings->auth_settings[S3AuthSetting::impersonation_delegates] = collection.getOrDefault<String>("impersonation_delegates", "");
    s3_settings->auth_settings[S3AuthSetting::impersonation_scopes] = collection.getOrDefault<String>("impersonation_scopes", "");
    s3_settings->auth_settings[S3AuthSetting::impersonation_lifetime_seconds]
        = collection.getOrDefault<UInt64>("impersonation_lifetime_seconds", S3::DEFAULT_GCP_IMPERSONATION_LIFETIME_SECONDS);
    s3_settings->auth_settings[S3AuthSetting::iam_credentials_endpoint] = collection.getOrDefault<String>("iam_credentials_endpoint", "");

    checkQueryOverriddenGcpImpersonation(collection, context, s3_settings->auth_settings, is_loading_from_existing_metadata);

    format = collection.getOrDefault<String>("format", format);
    compression_method = collection.getOrDefault<String>("compression_method", collection.getOrDefault<String>("compression", "auto"));
    structure = collection.getOrDefault<String>("structure", "auto");

    s3_settings->request_settings = S3::S3RequestSettings(collection, settings, /* validate_settings */ true);

    s3_capabilities = std::make_unique<S3Capabilities>(getCapabilitiesFromConfig(config, "s3"));
}

static ASTPtr extractExtraCredentials(ASTs & args)
{
    for (size_t i = 0; i != args.size(); ++i)
    {
        const auto * ast_function = args[i]->as<ASTFunction>();
        if (ast_function && ast_function->name == "extra_credentials")
        {
            auto credentials = args[i];
            args.erase(args.begin() + i);
            return credentials;
        }
    }
    return nullptr;
}

bool StorageS3Configuration::collectCredentials(ASTPtr maybe_credentials, S3::S3AuthSettings & auth_settings_, ContextPtr local_context)
{
    return S3StorageParsedArguments::collectCredentials(maybe_credentials, auth_settings_, local_context);
}

bool S3StorageParsedArguments::collectCredentials(ASTPtr maybe_credentials, S3::S3AuthSettings & auth_settings_, ContextPtr local_context)
{
    if (!maybe_credentials)
        return false;

    const auto * credentials_ast_function = maybe_credentials->as<ASTFunction>();
    if (!credentials_ast_function || credentials_ast_function->name != "extra_credentials")
        return false;

    const auto * credentials_function_args_expr = assert_cast<const ASTExpressionList *>(credentials_ast_function->arguments.get());
    auto credentials_function_args = credentials_function_args_expr->children;

    for (auto & credential_arg : credentials_function_args)
    {
        const auto * credential_ast = credential_arg->as<ASTFunction>();
        if (!credential_ast || credential_ast->name != "equals")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Credentials argument is incorrect");

        auto * credential_args_expr = assert_cast<ASTExpressionList *>(credential_ast->arguments.get());
        auto & credential_args = credential_args_expr->children;
        if (credential_args.size() != 2)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Credentials argument is incorrect: expected 2 arguments, got {}",
                credential_args.size());

        credential_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(credential_args[0], local_context);
        auto arg_name_value = credential_args[0]->as<ASTLiteral>()->value;
        if (arg_name_value.getType() != Field::Types::Which::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected string as credential name");
        auto arg_name = arg_name_value.safeGet<String>();

        credential_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(credential_args[1], local_context);
        auto arg_value = credential_args[1]->as<ASTLiteral>()->value;
        if (arg_value.getType() != Field::Types::Which::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected string as credential value");
        else if (arg_name == "role_arn")
            auth_settings_[S3AuthSetting::role_arn] = arg_value.safeGet<String>();
        else if (arg_name == "role_session_name")
            auth_settings_[S3AuthSetting::role_session_name] = arg_value.safeGet<String>();
        else if (arg_name == "external_id")
            auth_settings_[S3AuthSetting::external_id] = arg_value.safeGet<String>();
        /// GCP service account impersonation, the counterpart of `role_arn` above.
        else if (arg_name == "impersonate_service_account")
            auth_settings_[S3AuthSetting::impersonate_service_account] = arg_value.safeGet<String>();
        else if (arg_name == "impersonation_delegates")
            auth_settings_[S3AuthSetting::impersonation_delegates] = arg_value.safeGet<String>();
        else if (arg_name == "impersonation_scopes")
            auth_settings_[S3AuthSetting::impersonation_scopes] = arg_value.safeGet<String>();
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid credential argument found: {}", arg_name);
    }

    return true;
}

void S3StorageParsedArguments::fromDisk(const DiskPtr & disk, ASTs & args, ContextPtr context, bool with_structure)
{
    auto object_storage = disk->getObjectStorage();
    /// Unwrap decorator object storages (e.g. `CachedObjectStorage`) before the cast.
    /// `assert_cast` checks `typeid` exactly, so calling it on a wrapper would throw a
    /// LOGICAL_ERROR even though the wrapper exposes the same interface and ultimately
    /// holds an `S3ObjectStorage`. See https://github.com/ClickHouse/ClickHouse/issues/89300.
    while (auto inner = object_storage->getUnderlying())
        object_storage = std::move(inner);
    const auto & s3_object_storage = assert_cast<const S3ObjectStorage &>(*object_storage);
    s3_settings = std::make_unique<S3Settings>();
    *s3_settings = s3_object_storage.getS3Settings();

    ParseFromDiskResult parsing_result = parseFromDisk(args, with_structure, context, disk->getPath());
    {
        String path = s3_object_storage.getURI().uri_str;
        fs::path root = path;
        fs::path suffix = parsing_result.path_suffix;
        url = S3::URI(String(root / suffix));
    }
    if (parsing_result.format.has_value())
        format = *parsing_result.format;
    if (parsing_result.compression_method.has_value())
        compression_method = *parsing_result.compression_method;
    if (parsing_result.structure.has_value())
        structure = *parsing_result.structure;
    path_suffix = parsing_result.path_suffix;
}

namespace
{

/// Whether an ambiguous positional literal is a partition strategy (vs a compression method). Exact enum
/// spellings (incl. uppercase `NONE`) match for backward compatibility; matching is also case-insensitive
/// for real strategies (`hive`), but lowercase `none` is left to mean the `compression_method`.
bool looksLikeExplicitPartitionStrategy(const String & arg)
{
    if (magic_enum::enum_contains<PartitionStrategyFactory::StrategyType>(arg))
        return true;
    const auto strategy = magic_enum::enum_cast<PartitionStrategyFactory::StrategyType>(arg, magic_enum::case_insensitive);
    return strategy.has_value() && *strategy != PartitionStrategyFactory::StrategyType::NONE;
}

/// Whether a positional argument is a bool literal (`partition_columns_in_data_file`) rather than a
/// `partition_strategy` string. Matches `checkAndGetLiteralArgument<bool>`: a `Bool` or a `UInt64`.
bool looksLikeBoolArgument(const ASTPtr & arg)
{
    const auto * literal = arg ? arg->as<ASTLiteral>() : nullptr;
    if (!literal)
        return false;
    const auto type = literal->value.getType();
    return type == Field::Types::Which::Bool || type == Field::Types::Which::UInt64;
}

}

void S3StorageParsedArguments::fromAST(ASTs & args, ContextPtr context, bool with_structure)
{
    auto extra_credentials = extractExtraCredentials(args);

    size_t count = StorageURL::evalArgsAndCollectHeaders(args, headers_from_ast, context);

    ASTs key_value_asts;
    if (auto first_key_value_arg_it = getFirstKeyValueArgument(args);
        first_key_value_arg_it != args.end())
    {
        key_value_asts = ASTs(first_key_value_arg_it, args.end());
        count -= key_value_asts.size();
    }

    if (count == 0 || count > S3StorageParsedArguments::getMaxNumberOfArguments(with_structure))
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Storage S3 requires 1 to {} arguments. All supported signatures:\n{}",
            S3StorageParsedArguments::getMaxNumberOfArguments(with_structure),
            S3StorageParsedArguments::getSignatures(with_structure));

    auto key_value_args = parseKeyValueArguments(key_value_asts, context);
    if (key_value_args.contains("structure"))
        with_structure = false;

    /// A key-value argument this form does not know is dropped without a word, and none of the GCP impersonation
    /// settings is read from here (the target and its qualifiers arrive in `extra_credentials`, the two
    /// operator-only ones not at all). Refuse them explicitly: a silently ignored `impersonate_service_account`
    /// would run the read with the source identity's own full-scope token while reporting success -- the same
    /// hazard that is refused in `extra_credentials` and on a named collection.
    for (const auto & key : gcp_impersonation_keys)
        if (key_value_args.contains(String(key)))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "`{}` is not an argument of this form. Configure GCP service account impersonation on a named "
                "collection or in the server `<s3>` configuration; `impersonate_service_account`, "
                "`impersonation_delegates` and `impersonation_scopes` may also be given per query inside "
                "`extra_credentials(...)`.",
                key);

    const auto & config = context->getConfigRef();
    s3_capabilities = std::make_unique<S3Capabilities>(getCapabilitiesFromConfig(config, "s3"));

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
    /// For 7 arguments we support:
    /// if with_structure == 0:
    /// - s3(source, access_key_id, secret_access_key, session_token, format, compression_method, partition_strategy)
    /// if with_structure == 1:
    /// - s3(source, access_key_id, secret_access_key, session_token, format, structure, partition_strategy)
    /// - s3(source, access_key_id, secret_access_key, session_token, format, structure, compression_method)
    else if (count == 7)
    {
        if (with_structure)
        {
            auto sixth_arg = checkAndGetLiteralArgument<String>(args[6], "compression_method/partition_strategy");
            if (looksLikeExplicitPartitionStrategy(sixth_arg))
            {
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}, {"partition_strategy", 6}};
            }
            else
            {
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}, {"compression_method", 6}};
            }
        }
        else
        {
            engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"compression_method", 5}, {"partition_strategy", 6}};
        }
    }
    /// For 8 arguments we support:
    /// if with_structure == 0:
    /// - s3(source, access_key_id, secret_access_key, session_token, format, compression_method, partition_strategy, partition_columns_in_data_file)
    /// if with_structure == 1:
    /// - s3(source, access_key_id, secret_access_key, session_token, format, structure, partition_strategy, partition_columns_in_data_file)
    /// - s3(source, access_key_id, secret_access_key, session_token, format, structure, compression_method, partition_strategy)
    else if (count == 8)
    {
        if (with_structure)
        {
            auto sixth_arg = checkAndGetLiteralArgument<String>(args[6], "compression_method/partition_strategy");
            /// A bool last argument means args[6] is the partition strategy; otherwise inspect args[6].
            /// This keeps the valid `(..., 'NONE', 1)` form working.
            if (looksLikeBoolArgument(args[7]) || looksLikeExplicitPartitionStrategy(sixth_arg))
            {
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}, {"partition_strategy", 6}, {"partition_columns_in_data_file", 7}};
            }
            else
            {
                engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}, {"compression_method", 6}, {"partition_strategy", 7}};
            }
        }
        else
        {
            engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"compression_method", 5}, {"partition_strategy", 6}, {"partition_columns_in_data_file", 7}};
        }
    }
    /// with_structure == 1:
    ///     s3(source, access_key_id, secret_access_key, session_token, format, structure, compression_method, partition_strategy, partition_columns_in_data_file)
    /// with_structure == 0:
    ///     s3(source, access_key_id, secret_access_key, session_token, format, compression_method, partition_strategy, partition_columns_in_data_file, storage_class_name)
    else if (count == 9)
    {
        if (with_structure)
            engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}, {"compression_method", 6}, {"partition_strategy", 7}, {"partition_columns_in_data_file", 8}};
        else
            engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"compression_method", 5}, {"partition_strategy", 6}, {"partition_columns_in_data_file", 7}, {"storage_class_name", 8}};
    }
    /// with_structure == 1:
    ///     s3(source, access_key_id, secret_access_key, session_token, format, structure, compression_method, partition_strategy, partition_columns_in_data_file, storage_class_name)
    else if (count == 10 && with_structure)
    {
        engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}, {"compression_method", 6}, {"partition_strategy", 7}, {"partition_columns_in_data_file", 8}, {"storage_class_name", 9}};
    }

    /// This argument is always the first
    String url_str = checkAndGetLiteralArgument<String>(args[0], "url");

    /// Resolve relative URLs against the `s3_base` setting, and materialize the resolved URL
    /// back into the arguments so that the persisted DDL (`SHOW CREATE TABLE`, DETACH/ATTACH,
    /// server restart) does not depend on the value of `s3_base` at attach time.
    if (String resolved_url = StorageURL::resolveURLBase(url_str, context->getSettingsRef()[Setting::s3_base].value, "s3_base");
        resolved_url != url_str)
    {
        StorageURL::overrideURLInEngineArgs(args, resolved_url, context, /*skip_userinfo=*/ true);
        url_str = std::move(resolved_url);
    }

    url = S3::URI(
        url_str,
        context->getSettingsRef()[Setting::allow_archive_path_syntax],
        /*keep_presigned_query_parameters*/ !context->getSettingsRef()[Setting::compatibility_s3_presigned_url_query_in_path],
        /*uri_style*/ context->getSettingsRef()[Setting::s3_uri_style]);

    s3_settings = std::make_unique<S3Settings>();
    s3_settings->loadFromConfigForObjectStorage(
        config, "s3", context->getSettingsRef(), url.uri.getScheme(), context->getSettingsRef()[Setting::s3_validate_request_settings]);

    /// Drop any role_arn/STS fields from the global `<s3>` config before parsing `extra_credentials`, so only
    /// a query-supplied role_arn remains (a server role_arn would assume the role with the server's identity).
    /// Same for the GCP impersonation target, whose source identity would likewise be the server's.
    const bool restrict_server_credentials = context->shouldRestrictUserQueryS3Credentials();

    /// Read before the clear below wipes it: a target the global `<s3>` supplied is gone by the time the
    /// qualifier check runs, and it is the only place that can tell "the restriction took the target away"
    /// from "there never was one" for a global target.
    const bool config_supplied_impersonation_target
        = !String(s3_settings->auth_settings[S3AuthSetting::impersonate_service_account]).empty();

    if (restrict_server_credentials)
    {
        s3_settings->auth_settings.clearRoleArn();
        s3_settings->auth_settings.clearGcpImpersonation();
    }

    /// Parse `extra_credentials` into a scratch settings object rather than straight into `auth_settings`: it
    /// shares every one of these field names with the `<s3>` config, which is already loaded there, so reading
    /// them back out of `auth_settings` cannot tell a query-supplied value from a server-configured one. Both
    /// the restore after the per-endpoint merge below and the impersonation qualifier check need exactly that
    /// distinction -- without it a global `<s3>` value masquerades as one the query named.
    S3::S3AuthSettings query_auth_settings;
    S3StorageParsedArguments::collectCredentials(extra_credentials, query_auth_settings, context);

    /// The query-supplied role_arn/STS and GCP impersonation fields, so the per-endpoint `<s3>` merge below
    /// cannot replace them. Empty means the query did not name one.
    const String user_role_arn = query_auth_settings[S3AuthSetting::role_arn];
    const String user_role_session_name = query_auth_settings[S3AuthSetting::role_session_name];
    const String user_external_id = query_auth_settings[S3AuthSetting::external_id];
    const String user_impersonate_service_account = query_auth_settings[S3AuthSetting::impersonate_service_account];
    const String user_impersonation_delegates = query_auth_settings[S3AuthSetting::impersonation_delegates];
    const String user_impersonation_scopes = query_auth_settings[S3AuthSetting::impersonation_scopes];

    /// Apply them on top of the config, which is what `collectCredentials` did when it wrote here directly:
    /// `updateIfChanged` copies exactly the fields the query set.
    s3_settings->auth_settings.updateIfChanged(query_auth_settings);

    if (auto endpoint_settings = context->getStorageS3Settings().getSettings(url.uri.toString(), context->getUserName()))
    {
        s3_settings->auth_settings.updateIfChanged(endpoint_settings->auth_settings);
        s3_settings->request_settings.updateIfChanged(endpoint_settings->request_settings);
    }

    /// Whether the restriction is what removed the impersonation target the config had supplied, so the qualifier
    /// check below can name the restriction instead of blaming the query for a target it never had to supply.
    bool restriction_dropped_config_target = false;

    if (restrict_server_credentials)
    {
        s3_settings->auth_settings[S3AuthSetting::role_arn] = user_role_arn;
        s3_settings->auth_settings[S3AuthSetting::role_session_name] = user_role_session_name;
        s3_settings->auth_settings[S3AuthSetting::external_id] = user_external_id;

        /// Either half is a config target the query did not supply: the global one, already cleared above, or
        /// the per-endpoint one, still in place until `clearServerManagedGcpOAuth` below takes it.
        restriction_dropped_config_target = user_impersonate_service_account.empty()
            && (config_supplied_impersonation_target
                || !String(s3_settings->auth_settings[S3AuthSetting::impersonate_service_account]).empty());

        /// Drop any GCP OAuth mechanism inherited from `<s3>` config: the bare-URL `s3(...)` form cannot supply
        /// these fields, so a value here is always server-configured and would mint a server-identity token.
        s3_settings->auth_settings.clearServerManagedGcpOAuth();

        /// The bare-URL `s3(...)` form likewise cannot supply request-auth material, so the headers/access
        /// headers and SSE-C/SSE-KMS keys here come from the server `<s3>`/endpoint config. Drop them so an
        /// anonymous/NOSIGN request does not send the server's `Authorization` header or encryption keys to the
        /// user-chosen endpoint.
        s3_settings->auth_settings.clearServerManagedRequestAuth();
    }

    /// `extra_credentials` *can* supply an impersonation target, so a query-supplied one wins over both the
    /// per-endpoint `<s3>` merge above and (when restricted) the `clearServerManagedGcpOAuth` that dropped it
    /// along with the rest of the GCP block. Without this the operator's target would silently replace the one
    /// the query named, running the request as an identity the user did not ask for. These values come from
    /// `extra_credentials` alone, so a query that names no target leaves the endpoint-configured one in force
    /// rather than having it overwritten by the global `<s3>` value.
    ///
    /// Under the restriction the restored target has no source identity to impersonate from (this form cannot
    /// supply the ADC triple, and the server's `gcp_oauth` was just dropped), so it is rejected by
    /// `getCredentialsProvider` with an explicit error instead of being silently ignored.
    ///
    /// The qualifiers travel with the target they qualify. A query that names its own target therefore takes
    /// its `impersonation_delegates` and `impersonation_scopes` from the query as well, empty ones included:
    /// restoring the target alone would leave it paired with the delegation chain and the scopes the operator
    /// provisioned for the *endpoint's* target, so the account the query named would be reached through a
    /// chain, and granted a scope set, meant for a different account entirely. A query that names no target
    /// qualifies the configured one instead, which is what the check below spells out.
    if (!user_impersonate_service_account.empty())
    {
        s3_settings->auth_settings[S3AuthSetting::impersonate_service_account] = user_impersonate_service_account;
        s3_settings->auth_settings[S3AuthSetting::impersonation_delegates] = user_impersonation_delegates;
        s3_settings->auth_settings[S3AuthSetting::impersonation_scopes] = user_impersonation_scopes;
    }
    else
    {
        if (!user_impersonation_delegates.empty())
            s3_settings->auth_settings[S3AuthSetting::impersonation_delegates] = user_impersonation_delegates;
        if (!user_impersonation_scopes.empty())
            s3_settings->auth_settings[S3AuthSetting::impersonation_scopes] = user_impersonation_scopes;
    }

    /// `impersonation_delegates` and `impersonation_scopes` only ever qualify a target. Supplied in
    /// `extra_credentials` with no target in force, every consumer downstream drops them silently -- the read
    /// would then run with the source identity's full-scope token while the query says otherwise -- so refuse
    /// the query instead. A target inherited from the config counts, hence the check on the resolved value.
    if (String(s3_settings->auth_settings[S3AuthSetting::impersonate_service_account]).empty()
        && (!user_impersonation_delegates.empty() || !user_impersonation_scopes.empty()))
    {
        /// The query did name a target to qualify -- the endpoint's -- and the restriction is what took it away,
        /// so report that and not a missing argument: naming the target in `extra_credentials` instead would only
        /// trade this error for the one about a target with no source identity to impersonate from.
        if (restriction_dropped_config_target)
            throw Exception(
                ErrorCodes::ACCESS_DENIED,
                "`impersonation_delegates` and `impersonation_scopes` in `extra_credentials` qualify the "
                "`impersonate_service_account` configured for this endpoint, and S3 access from user queries is "
                "not allowed to impersonate from the server's own GCP identity. Enable the setting "
                "`s3_allow_server_credentials_in_user_queries` to use it.");

        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "`impersonation_delegates` and `impersonation_scopes` in `extra_credentials` require "
            "`impersonate_service_account`, which names the service account they apply to.");
    }

    /// Re-apply user/profile/query-level settings on top, so they take priority over the global <s3> config section.
    s3_settings->request_settings.updateFromSettings(
        context->getSettingsRef(),
        /* if_changed */ true,
        context->getSettingsRef()[Setting::s3_validate_request_settings]);

    if (auto format_value = getFromPositionOrKeyValue<String>("format", args, engine_args_to_idx, key_value_args);
        format_value.has_value())
    {
        format = format_value.value();
    }

    if (auto structure_value = getFromPositionOrKeyValue<String>("structure", args, engine_args_to_idx, key_value_args);
        structure_value.has_value())
    {
        structure = structure_value.value();
    }

    if (auto compression_method_value = getFromPositionOrKeyValue<String>("compression_method", args, engine_args_to_idx, key_value_args);
        compression_method_value.has_value())
    {
        compression_method = compression_method_value.value();
    }

    if (auto partition_strategy_value = getFromPositionOrKeyValue<String>("partition_strategy", args, engine_args_to_idx, key_value_args);
        partition_strategy_value.has_value())
    {
        const auto & partition_strategy_name = partition_strategy_value.value();
        const auto partition_strategy_type_opt = magic_enum::enum_cast<PartitionStrategyFactory::StrategyType>(partition_strategy_name, magic_enum::case_insensitive);

        if (!partition_strategy_type_opt.has_value())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Partition strategy {} is not supported", partition_strategy_name);
        }

        partition_strategy_type = partition_strategy_type_opt.value();
    }

    if (auto partition_columns_in_data_file_value = getFromPositionOrKeyValue<bool>("partition_columns_in_data_file", args, engine_args_to_idx, key_value_args);
        partition_columns_in_data_file_value.has_value())
    {
        partition_columns_in_data_file = partition_columns_in_data_file_value.value();
        partition_columns_in_data_file_was_set = true;
    }
    else
        partition_columns_in_data_file = partition_strategy_type != PartitionStrategyFactory::StrategyType::HIVE;

    bool query_provided_access_key_id = false;
    if (auto access_key_id_value = getFromPositionOrKeyValue<String>("access_key_id", args, engine_args_to_idx, key_value_args);
        access_key_id_value.has_value())
    {
        s3_settings->auth_settings[S3AuthSetting::access_key_id] = access_key_id_value.value();
        query_provided_access_key_id = true;
    }

    bool query_provided_secret_access_key = false;
    if (auto secret_access_key_value = getFromPositionOrKeyValue<String>("secret_access_key", args, engine_args_to_idx, key_value_args);
        secret_access_key_value.has_value())
    {
        s3_settings->auth_settings[S3AuthSetting::secret_access_key] = secret_access_key_value.value();
        query_provided_secret_access_key = true;
    }

    bool query_provided_session_token = false;
    if (auto session_token_value = getFromPositionOrKeyValue<String>("session_token", args, engine_args_to_idx, key_value_args);
        session_token_value.has_value())
    {
        s3_settings->auth_settings[S3AuthSetting::session_token] = session_token_value.value();
        query_provided_session_token = true;
    }

    /// When the query supplies its own key pair but no `session_token`, drop any token inherited from the
    /// global/per-endpoint `<s3>` config: the server's temporary token does not belong with the query's keys
    /// (it would be sent to a user-chosen endpoint and breaks otherwise-valid explicit credentials).
    if (query_provided_access_key_id && query_provided_secret_access_key && !query_provided_session_token)
        s3_settings->auth_settings[S3AuthSetting::session_token] = "";

    /// A query-supplied `role_arn` is honored even under the restriction (STS assume-role is the documented
    /// way to grant ClickHouse Cloud access to a private bucket), but it must assume the role with the query's
    /// own base keys or the server's ambient identity, never the server `<s3>`/endpoint static keys (a
    /// confused-deputy STS path). Drop the inherited keys when the query supplied a `role_arn` without its own
    /// key pair, so the assume-role call falls through to the ambient provider chain.
    if (restrict_server_credentials && !String(s3_settings->auth_settings[S3AuthSetting::role_arn]).empty()
        && !(query_provided_access_key_id && query_provided_secret_access_key))
    {
        s3_settings->auth_settings[S3AuthSetting::access_key_id] = "";
        s3_settings->auth_settings[S3AuthSetting::secret_access_key] = "";
        s3_settings->auth_settings[S3AuthSetting::session_token] = "";
    }

    if (no_sign_request)
    {
        s3_settings->auth_settings[S3AuthSetting::no_sign_request] = no_sign_request;
    }
    else if (auto no_sign_value = getFromPositionOrKeyValue<bool>("no_sign", args, {}, key_value_args);
        no_sign_value.has_value())
    {
        s3_settings->auth_settings[S3AuthSetting::no_sign_request] = no_sign_value.value();
    }

    /// `storage_class` is an interchangeable alias for `storage_class_name` (see issue #68551).
    auto storage_class_name = getFromPositionOrKeyValue<String>("storage_class_name", args, engine_args_to_idx, key_value_args);
    if (!storage_class_name.has_value())
        storage_class_name = getFromPositionOrKeyValue<String>("storage_class", args, {}, key_value_args);
    if (storage_class_name.has_value())
    {
        s3_settings->request_settings[S3RequestSetting::storage_class_name] = storage_class_name.value();
    }

    if (extra_credentials)
        args.push_back(extra_credentials);

     if (context->getSettingsRef()[Setting::s3_validate_request_settings])
         s3_settings->request_settings.validateUploadSettings();
}

static void addStructureAndFormatToArgsIfNeededS3(
    ASTs & args, const String & structure_, const String & format_, ContextPtr context, bool with_structure, size_t max_number_of_arguments)
{
    if (auto collection = tryGetNamedCollectionWithOverrides(args, context))
    {
        /// In case of named collection, just add key-value pairs "format='...', structure='...'"
        /// at the end of arguments to override existed format and structure with "auto" values.
        if (collection->getOrDefault<String>("format", "auto") == "auto")
        {
            ASTs format_equal_func_args = {make_intrusive<ASTIdentifier>("format"), make_intrusive<ASTLiteral>(format_)};
            auto format_equal_func = makeASTOperator("equals", std::move(format_equal_func_args));
            args.push_back(format_equal_func);
        }
        if (with_structure && collection->getOrDefault<String>("structure", "auto") == "auto")
        {
            ASTs structure_equal_func_args = {make_intrusive<ASTIdentifier>("structure"), make_intrusive<ASTLiteral>(structure_)};
            auto structure_equal_func = makeASTOperator("equals", std::move(structure_equal_func_args));
            args.push_back(structure_equal_func);
        }
    }
    else
    {
        auto extra_credentials = extractExtraCredentials(args);

        HTTPHeaderEntries tmp_headers;

        size_t count = StorageURL::evalArgsAndCollectHeaders(args, tmp_headers, context);

        ASTs key_value_asts;
        auto first_key_value_arg_it = getFirstKeyValueArgument(args);
        if (first_key_value_arg_it != args.end())
        {
            key_value_asts = ASTs(first_key_value_arg_it, args.end());
            count -= key_value_asts.size();
        }

        if (!count)
            return;

        if (count > max_number_of_arguments)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Expected 1 to {} arguments in table function s3, got {}",
                max_number_of_arguments,
                count);
        }

        auto format_literal = make_intrusive<ASTLiteral>(format_);
        auto structure_literal = make_intrusive<ASTLiteral>(structure_);

        bool format_in_key_value = false;
        bool structure_in_key_value = false;
        for (auto it = first_key_value_arg_it; it != args.end(); ++it)
        {
            const auto & arg = *it;
            const auto * function_ast = arg->as<ASTFunction>();
            if (!function_ast || function_ast->name != "equals")
                continue;

            auto * args_expr = assert_cast<ASTExpressionList *>(function_ast->arguments.get());
            auto & children = args_expr->children;
            if (children.size() != 2)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Key value argument is incorrect: expected 2 arguments, got {}", children.size());
            }

            auto literal = evaluateConstantExpressionOrIdentifierAsLiteral(children[0], context);

            auto arg_name_value = literal->as<ASTLiteral>()->value;
            if (arg_name_value.getType() != Field::Types::Which::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected string as credential name");
            auto arg_name = arg_name_value.safeGet<String>();

            if (arg_name == "format")
            {
                children[1] = format_literal;
                format_in_key_value = true;
            }
            else if (arg_name == "structure")
            {
                children[1] = structure_literal;
                structure_in_key_value = true;
            }
        }

        if (format_in_key_value && structure_in_key_value)
        {
            /// Add extracted extra credentials to the end of the args.
            if (extra_credentials)
                args.push_back(extra_credentials);
            return;
        }
        else if (format_in_key_value && with_structure)
        {
            /// Structure goes right after format, so if format is in key-value,
            /// then structure is required to be key-value.
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected positional arguments to go before key-value arguments");
        }
        else if (structure_in_key_value)
        {
            with_structure = false;
        }

        /// We will return it back at the end.
        args.erase(first_key_value_arg_it, args.end());

        /// s3(s3_url)
        if (count == 1)
        {
            /// Add format=auto before structure argument.
            args.push_back(format_literal);
            if (with_structure)
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

            if (with_structure)
                args.push_back(structure_literal);
        }
        /// s3(source, format, structure) or
        /// s3(source, access_key_id, secret_access_key) or
        /// s3(source, NOSIGN, format) or
        /// s3(source, format, compression_method)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN, format name or neither.
        else if (count == 3)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                if (checkAndGetLiteralArgument<String>(args[2], "format") == "auto")
                    args[2] = format_literal;
                if (with_structure)
                    args.push_back(structure_literal);
            }
            else if (second_arg == "auto" || FormatFactory::instance().exists(second_arg))
            {
                if (second_arg == "auto")
                    args[1] = format_literal;
                if (with_structure && checkAndGetLiteralArgument<String>(args[2], "structure") == "auto")
                    args[2] = structure_literal;
            }
            else
            {
                /// Add format and structure arguments.
                args.push_back(format_literal);
                if (with_structure)
                    args.push_back(structure_literal);
            }
        }
        /// s3(source, format, structure, compression_method) or
        /// s3(source, access_key_id, secret_access_key, format) or
        /// s3(source, access_key_id, secret_access_key, session_token) or
        /// s3(source, NOSIGN, format, structure) or
        /// s3(source, NOSIGN, format, compression_method)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN, format name or neither.
        else if (count == 4)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                if (checkAndGetLiteralArgument<String>(args[2], "format") == "auto")
                    args[2] = format_literal;
                if (with_structure && checkAndGetLiteralArgument<String>(args[3], "structure") == "auto")
                    args[3] = structure_literal;
            }
            else if (second_arg == "auto" || FormatFactory::instance().exists(second_arg))
            {
                if (second_arg == "auto")
                    args[1] = format_literal;
                if (with_structure && checkAndGetLiteralArgument<String>(args[2], "structure") == "auto")
                    args[2] = structure_literal;
            }
            else
            {
                auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/session_token");
                if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
                {
                    if (checkAndGetLiteralArgument<String>(args[3], "format") == "auto")
                        args[3] = format_literal;
                    if (with_structure)
                        args.push_back(structure_literal);
                }
                else
                {
                    args.push_back(format_literal);
                    if (with_structure)
                        args.push_back(structure_literal);
                }
            }
        }
        /// s3(source, access_key_id, secret_access_key, format, structure) or
        /// s3(source, access_key_id, secret_access_key, session_token, format) or
        /// s3(source, NOSIGN, format, structure, compression_method) or
        /// s3(source, access_key_id, secret_access_key, format, compression)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a NOSIGN keyword name or not.
        else if (count == 5)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                if (checkAndGetLiteralArgument<String>(args[2], "format") == "auto")
                    args[2] = format_literal;
                if (with_structure && checkAndGetLiteralArgument<String>(args[2], "structure") == "auto")
                    args[3] = structure_literal;
            }
            else
            {
                auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/session_token");
                if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
                {
                    if (checkAndGetLiteralArgument<String>(args[3], "format") == "auto")
                        args[3] = format_literal;
                    if (with_structure && checkAndGetLiteralArgument<String>(args[4], "structure") == "auto")
                        args[4] = structure_literal;
                }
                else
                {
                    if (checkAndGetLiteralArgument<String>(args[4], "format") == "auto")
                        args[4] = format_literal;
                    if (with_structure)
                        args.push_back(structure_literal);
                }
            }
        }
        /// s3(source, access_key_id, secret_access_key, format, structure, compression) or
        /// s3(source, access_key_id, secret_access_key, session_token, format, structure) or
        /// s3(source, access_key_id, secret_access_key, session_token, format, compression_method)
        else if (count == 6)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/session_token");
            if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
            {
                if (checkAndGetLiteralArgument<String>(args[3], "format") == "auto")
                    args[3] = format_literal;
                if (with_structure && checkAndGetLiteralArgument<String>(args[4], "structure") == "auto")
                    args[4] = structure_literal;
            }
            else
            {
                if (checkAndGetLiteralArgument<String>(args[4], "format") == "auto")
                    args[4] = format_literal;
                if (with_structure && checkAndGetLiteralArgument<String>(args[5], "format") == "auto")
                    args[5] = structure_literal;
            }
        }
        /// s3(source, access_key_id, secret_access_key, session_token, format, structure, compression_method)
        else
        {
            if (checkAndGetLiteralArgument<String>(args[4], "format") == "auto")
                args[4] = format_literal;
            if (with_structure && checkAndGetLiteralArgument<String>(args[5], "format") == "auto")
                args[5] = structure_literal;
        }

        if (!key_value_asts.empty())
            args.insert(args.end(), std::make_move_iterator(key_value_asts.begin()), std::make_move_iterator(key_value_asts.end()));

        /// Add extracted extra credentials to the end of the args.
        if (extra_credentials)
            args.push_back(extra_credentials);
    }
}

void StorageS3Configuration::initializeFromParsedArguments(S3StorageParsedArguments && parsed_arguments)
{
    StorageObjectStorageConfiguration::initializeFromParsedArguments(parsed_arguments);
    url = std::move(parsed_arguments.url);
    s3_settings = std::move(parsed_arguments.s3_settings);
    s3_capabilities = std::move(parsed_arguments.s3_capabilities);
    headers_from_ast = std::move(parsed_arguments.headers_from_ast);
}


void StorageS3Configuration::fromNamedCollection(const NamedCollection & collection, ContextPtr context)
{
    S3StorageParsedArguments parsed_arguments;
    parsed_arguments.fromNamedCollection(collection, context, is_loading_from_existing_metadata);
    initializeFromParsedArguments(std::move(parsed_arguments));
    keys = {url.key};
    static_configuration = !s3_settings->auth_settings[S3AuthSetting::access_key_id].value.empty()
        || s3_settings->auth_settings[S3AuthSetting::no_sign_request].changed;
}

void StorageS3Configuration::fromDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure)
{
    S3StorageParsedArguments parsed_arguments;
    auto disk = context->getDisk(disk_name);
    parsed_arguments.fromDisk(disk, args, context, with_structure);
    fs::path suffix = parsed_arguments.path_suffix;
    initializeFromParsedArguments(std::move(parsed_arguments));
    if (auto object_storage_disk = std::static_pointer_cast<DiskObjectStorage>(disk); object_storage_disk)
    {
        String path = object_storage_disk->getObjectStorage()->getCommonKeyPrefix();
        fs::path root = path;
        setPathForRead(String(root / suffix));
        keys = {String(root / suffix)};
    }
}

void StorageS3Configuration::fromAST(ASTs & args, ContextPtr context, bool with_structure)
{
    S3StorageParsedArguments parsed_arguments;
    parsed_arguments.fromAST(args, context, with_structure);
    initializeFromParsedArguments(std::move(parsed_arguments));
    keys = {url.key};
    chassert(s3_settings != nullptr);
    if (!biglake_adc_client_id.empty())
    {
        s3_settings->auth_settings[S3AuthSetting::http_client] = "gcp_oauth";
        s3_settings->auth_settings[S3AuthSetting::google_adc_client_id] = biglake_adc_client_id;
        s3_settings->auth_settings[S3AuthSetting::google_adc_client_secret] = biglake_adc_client_secret;
        s3_settings->auth_settings[S3AuthSetting::google_adc_refresh_token] = biglake_adc_refresh_token;
    }
    static_configuration = !s3_settings->auth_settings[S3AuthSetting::access_key_id].value.empty()
        || s3_settings->auth_settings[S3AuthSetting::no_sign_request].changed;
}

void StorageS3Configuration::addStructureAndFormatToArgsIfNeeded(
    ASTs & args, const String & structure_, const String & format_, ContextPtr context, bool with_structure)
{
    addStructureAndFormatToArgsIfNeededS3(
        args, structure_, format_, context, with_structure, S3StorageParsedArguments::getMaxNumberOfArguments(with_structure));
}
}

#endif
