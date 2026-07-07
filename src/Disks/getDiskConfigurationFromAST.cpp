#include <Disks/getDiskConfigurationFromAST.h>

#include <Common/assert_cast.h>
#include <Common/FieldVisitorToString.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Disks/DiskFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/Element.h>
#include <Poco/DOM/Text.h>
#include <Common/StringUtils.h>
#include <boost/algorithm/string/predicate.hpp>

#include <vector>
#include <boost/algorithm/string/trim.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ACCESS_DENIED;
}

namespace Setting
{
    extern const SettingsBool dynamic_disk_allow_from_env;
    extern const SettingsBool dynamic_disk_allow_include;
    extern const SettingsBool dynamic_disk_allow_from_zk;
}

namespace ServerSetting
{
    extern const ServerSettingsBool s3_load_table_anonymously_if_credentials_restricted;
    extern const ServerSettingsBool s3_allow_server_credentials_for_system_table_disks;
}

[[noreturn]] static void throwBadConfiguration(const std::string & message = "")
{
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Incorrect configuration{}. Example of expected configuration: `(type=s3 ...`)`",
        message.empty() ? "" : ": " + message);
}

Poco::AutoPtr<Poco::XML::Document> getDiskConfigurationFromASTImpl(const ASTs & disk_args, ContextPtr context, bool is_loading_from_existing_metadata, DynamicS3DiskCredentialInfo * info, bool for_system_database)
{
    if (disk_args.empty())
        throwBadConfiguration("expected non-empty list of arguments");

    Poco::AutoPtr<Poco::XML::Document> xml_document(new Poco::XML::Document());
    Poco::AutoPtr<Poco::XML::Element> root(xml_document->createElement("disk"));
    xml_document->appendChild(root);

    const auto & settings = context->getSettingsRef();

    /// Track credential-related keys so a user-created S3 disk cannot resolve the server's own
    /// credentials. A restricted dynamic S3 disk must carry a complete explicit key pair or NOSIGN (and, for
    /// `gcp_oauth`, a complete explicit ADC triple); anything else would fall back to the server's
    /// environment / IMDS / instance-profile / AWS-config-file / GCP-metadata credentials.
    /// See Context::shouldRestrictUserQueryS3Credentials.
    bool is_s3_disk = false;
    bool type_is_indirect = false;
    /// A literal, concrete non-S3 `type` (e.g. `encrypted`, `cache`, `local`) -- but NOT `object_storage`,
    /// which is only a wrapper whose backend is chosen by `object_storage_type`.
    bool has_concrete_non_s3_type = false;
    bool type_is_object_storage = false;
    bool has_explicit_non_s3_object_storage_type = false;
    bool has_include = false;
    bool has_indirect_auth_field = false;
    bool has_access_key_id = false;
    bool has_secret_access_key = false;
    bool has_no_sign_request = false;
    bool wants_gcp_oauth = false;
    bool has_google_adc_client_id = false;
    bool has_google_adc_client_secret = false;
    bool has_google_adc_refresh_token = false;
    bool use_environment_credentials_off = false;
    bool has_role_arn = false;
    /// A marker persisted in the stored definition when the disk was created with the credential opt-in (see
    /// the write side below); honored only on metadata load, so a user cannot self-grant on a fresh create.
    bool has_server_credentials_marker = false;

    /// `from_env`/`from_zk` substitute the value from a server-side source (an environment variable or a
    /// ZooKeeper node), so for credential/auth/type fields the literal placeholder must not be treated as a
    /// user-supplied value.
    auto is_indirect_value = [](const std::string & v) { return startsWith(v, "from_env") || startsWith(v, "from_zk"); };
    auto is_s3_credential_or_auth_field = [](const std::string & k)
    {
        return k == "access_key_id" || k == "secret_access_key" || k == "session_token" || k == "role_arn"
            || k == "role_session_name" || k == "use_environment_credentials" || k == "http_client"
            || k == "service_account" || k == "metadata_service" || k == "request_token_path"
            || k == "google_adc_client_id" || k == "google_adc_client_secret" || k == "google_adc_refresh_token"
            || k == "server_side_encryption_customer_key_base64" || k == "server_side_encryption_kms_key_id"
            || k == "server_side_encryption_kms_encryption_context" || startsWith(k, "header") || startsWith(k, "access_header");
    };

    for (const auto & arg : disk_args)
    {
        const auto * setting_function = arg->as<const ASTFunction>();
        if (!setting_function || setting_function->name != "equals")
            throwBadConfiguration("expected configuration arguments as key=value pairs");

        const auto * function_args_expr = assert_cast<const ASTExpressionList *>(setting_function->arguments.get());
        if (!function_args_expr)
            throwBadConfiguration("expected a list of key=value arguments");

        auto function_args = function_args_expr->children;
        if (function_args.empty())
            throwBadConfiguration("expected a non-empty list of key=value arguments");

        auto * key_identifier = function_args[0]->as<ASTIdentifier>();
        if (!key_identifier)
            throwBadConfiguration("expected the key (key=value) to be identifier");

        std::string key = key_identifier->name();
        Poco::AutoPtr<Poco::XML::Element> key_element(xml_document->createElement(key));
        root->appendChild(key_element);

        if (!function_args[1]->as<ASTLiteral>() && !function_args[1]->as<ASTIdentifier>())
            throwBadConfiguration("expected values to be literals or identifiers");

        auto value = evaluateConstantExpressionOrIdentifierAsLiteral(function_args[1], context);
        auto value_str = convertFieldToString(value->as<ASTLiteral>()->value);

        const bool indirect = is_indirect_value(value_str);
        if (indirect && is_s3_credential_or_auth_field(key))
            has_indirect_auth_field = true;

        if (key == "include")
            has_include = true;
        else if (key == "type" || key == "object_storage_type")
        {
            const bool is_s3_value = (value_str == "s3" || value_str.starts_with("s3_"));
            is_s3_disk |= is_s3_value;
            type_is_indirect |= indirect;

            if (key == "type")
            {
                if (!indirect && value_str == "object_storage")
                    type_is_object_storage = true;
                else if (!indirect && !is_s3_value)
                    has_concrete_non_s3_type = true;
            }
            else if (!indirect && !is_s3_value) /// object_storage_type
                has_explicit_non_s3_object_storage_type = true;
        }
        else if (key == "access_key_id")
            has_access_key_id = !value_str.empty() && !indirect;
        else if (key == "secret_access_key")
            has_secret_access_key = !value_str.empty() && !indirect;
        else if (key == "no_sign_request")
            has_no_sign_request = !indirect && value_str != "0" && !boost::iequals(value_str, "false");
        else if (key == "http_client")
            wants_gcp_oauth = !indirect && boost::iequals(value_str, "gcp_oauth");
        else if (key == "google_adc_client_id")
            has_google_adc_client_id = !value_str.empty() && !indirect;
        else if (key == "google_adc_client_secret")
            has_google_adc_client_secret = !value_str.empty() && !indirect;
        else if (key == "google_adc_refresh_token")
            has_google_adc_refresh_token = !value_str.empty() && !indirect;
        else if (key == "use_environment_credentials")
            use_environment_credentials_off = !indirect && (value_str == "0" || boost::iequals(value_str, "false"));
        else if (key == "role_arn")
            has_role_arn = !value_str.empty() && !indirect;
        else if (key == "_server_credentials_allowed")
            has_server_credentials_marker = !indirect && value_str != "0" && !boost::iequals(value_str, "false");

        if (key == "include")
        {
            if (!is_loading_from_existing_metadata && !settings[Setting::dynamic_disk_allow_include])
                throw Exception(
                    ErrorCodes::ACCESS_DENIED,
                    "Using `include` in dynamic disk configuration is disabled by the setting `dynamic_disk_allow_include`");
            key_element->setAttribute("incl", value_str);
        }
        else if (startsWith(value_str, "from_env"))
        {
            if (!is_loading_from_existing_metadata && !settings[Setting::dynamic_disk_allow_from_env])
                throw Exception(
                    ErrorCodes::ACCESS_DENIED,
                    "Using `from_env` in dynamic disk configuration is disabled by the setting `dynamic_disk_allow_from_env`");
            value_str = value_str.substr(std::strlen("from_env"));
            boost::trim(value_str);
            key_element->setAttribute("from_env", value_str);
        }
        else if (startsWith(value_str, "from_zk"))
        {
            if (!is_loading_from_existing_metadata && !settings[Setting::dynamic_disk_allow_from_zk])
                throw Exception(
                    ErrorCodes::ACCESS_DENIED,
                    "Using `from_zk` in dynamic disk configuration is disabled by the setting `dynamic_disk_allow_from_zk`");
            value_str = value_str.substr(std::strlen("from_zk"));
            boost::trim(value_str);
            key_element->setAttribute("from_zk", value_str);
        }
        else
        {
            Poco::AutoPtr<Poco::XML::Text> value_element(xml_document->createTextNode(value_str));
            key_element->appendChild(value_element);
        }
    }

    /// A user-created S3 disk must not resolve the server's own credentials. Indirection (`from_env`/`from_zk`
    /// on the type or auth fields, or an `include`) is treated as potentially-S3 unless the backend is an
    /// explicit literal non-S3 type. `type = object_storage` is a wrapper: non-S3 only with a literal non-S3
    /// `object_storage_type`.
    const bool type_explicitly_non_s3
        = has_concrete_non_s3_type || (type_is_object_storage && has_explicit_non_s3_object_storage_type);
    const bool maybe_s3_disk = is_s3_disk || type_is_indirect || (has_include && !type_explicitly_non_s3);

    const bool has_explicit_credentials = has_access_key_id && has_secret_access_key;
    const bool has_explicit_gcp_adc
        = has_google_adc_client_id && has_google_adc_client_secret && has_google_adc_refresh_token;
    /// `use_environment_credentials = 0` without a `role_arn` goes unsigned, so it is a safe anonymous form.
    const bool allowed_anonymous = use_environment_credentials_off && !has_role_arn;
    /// A safe credential form supplied by the AST itself (literal values); a substitution/`include` value does not count.
    const bool ast_has_explicit_credentials = wants_gcp_oauth
        ? has_explicit_gcp_adc
        : (has_explicit_credentials || has_no_sign_request || allowed_anonymous);

    /// Whether the disk relies on server-managed credentials (it would be refused under the restriction).
    const bool relies_on_server_credentials
        = maybe_s3_disk && (type_is_indirect || has_include || has_indirect_auth_field || !ast_has_explicit_credentials);

    /// A disk of a table in the `system` database is server-internal infrastructure (attached by the operator
    /// to ship system tables to S3 with the server's identity), exempt from the user-query restriction when the
    /// server setting allows it. Unlike the session setting this also applies on metadata reload, and `system`
    /// is not user-writable, so it does not relax the restriction for user queries.
    const bool system_disk_exempt = for_system_database
        && context->getGlobalContext()->getServerSettings()[ServerSetting::s3_allow_server_credentials_for_system_table_disks];

    /// A disk created with the credential opt-in records it in its stored definition (see the write side and
    /// createCustomDisk). The marker is honored only on metadata load -- on a fresh create it is ignored and
    /// the session setting decides -- so a user cannot self-grant by adding it to a `CREATE`.
    const bool marker_exempt = is_loading_from_existing_metadata && has_server_credentials_marker;
    const bool restriction_exempt = system_disk_exempt || marker_exempt;

    if (info)
    {
        info->has_include = has_include;
        info->ast_has_explicit_key_pair = has_explicit_credentials;
        info->ast_has_no_sign_request = has_no_sign_request;
        info->ast_has_use_environment_credentials_off = allowed_anonymous;
        info->ast_has_explicit_gcp_adc = has_explicit_gcp_adc;
        info->restriction_exempt = restriction_exempt;
        /// Persist the opt-in for a fresh create that resolves server credentials and is currently allowed
        /// (the session opted in), so the disk is not re-restricted on reload.
        info->persist_server_credentials_allowance = !is_loading_from_existing_metadata && relies_on_server_credentials
            && !has_server_credentials_marker && !context->shouldRestrictUserQueryS3Credentials();
    }

    if (relies_on_server_credentials && context->shouldRestrictUserQueryS3Credentials() && !restriction_exempt)
    {
        if (is_loading_from_existing_metadata
            && context->getGlobalContext()->getServerSettings()[ServerSetting::s3_load_table_anonymously_if_credentials_restricted])
        {
            LOG_WARNING(
                getLogger("getDiskConfigurationFromAST"),
                "Loading a dynamic S3 disk anonymously: it resolves server-managed credentials that are "
                "restricted for user queries (s3_allow_server_credentials_in_user_queries = 0). The disk will "
                "be inaccessible until its credentials resolve to a permitted source. Set the server setting "
                "s3_load_table_anonymously_if_credentials_restricted = 0 to fail loading instead.");

            /// Force the disk anonymous once `include` is resolved (see forceAnonymousS3DiskConfig); we
            /// cannot rewrite the DOM here because DiskFromAST processes `include` after this returns.
            if (info)
                info->load_anonymously = true;
        }
        else
            throw Exception(
                ErrorCodes::ACCESS_DENIED,
                "A dynamic S3 disk created from user SQL must provide a complete explicit "
                "`access_key_id`/`secret_access_key` pair, `no_sign_request`, or `use_environment_credentials = 0` "
                "(or, for `http_client = gcp_oauth`, a complete explicit Google ADC triple), with literal values "
                "and no `include`. It may not fall back to the server's own credentials. Enable the setting "
                "`s3_allow_server_credentials_in_user_queries` to allow it.");
    }

    return xml_document;
}

void forceAnonymousS3DiskConfig(Poco::Util::AbstractConfiguration & config)
{
    /// Force the disk root and every `locations.<name>` child anonymous: an `include` can resolve a
    /// `locations.<name>` S3 child with its own server-managed auth, and this pre-resolution fallback runs
    /// instead of the post-resolution per-prefix check, so a root-only rewrite would leave such a child with
    /// the server identity (built with `for_disk_s3 = true`, bypassing the restriction).
    forceAnonymousS3DiskConfigAtPrefix(config, "");
    if (config.has("locations"))
    {
        Poco::Util::AbstractConfiguration::Keys locations;
        config.keys("locations", locations);
        for (const auto & location : locations)
            forceAnonymousS3DiskConfigAtPrefix(config, "locations." + location + ".");
    }
}

void forceAnonymousS3DiskConfigAtPrefix(Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    /// Force the S3 client unsigned: clear `http_client` (which would mint a GCP token regardless of the
    /// credentials provider) and set `no_sign_request = 1`. `setString` overwrites any inherited value. The
    /// `prefix` selects the backend: empty for the disk root, or `locations.<name>.` for a multi-location child.
    config.setString(prefix + "http_client", "");
    config.setString(prefix + "no_sign_request", "1");

    /// The disk is intentionally inaccessible, so skip the startup access check: the table must load (and
    /// merely be inaccessible on query) rather than fail to attach. `skip_access_check` is read from the disk
    /// root and applied to every location child, so set it once at the root regardless of `prefix`.
    config.setString("skip_access_check", "1");

    /// An anonymous disk must not still send request-auth material inherited from the server `<s3>`/endpoint
    /// config: generic/per-request headers (either can carry an `Authorization`) and the SSE-C key / SSE-KMS
    /// config. Remove them from the resolved config here -- at the single point that forces a disk anonymous --
    /// rather than in `getClient`, which cannot tell a forced-anonymous disk from an operator-configured
    /// `no_sign_request` disk that legitimately keeps its headers and encryption keys.
    const String enum_root = prefix.empty() ? "" : prefix.substr(0, prefix.size() - 1);
    Poco::Util::AbstractConfiguration::Keys backend_keys;
    config.keys(enum_root, backend_keys);
    for (const auto & key : backend_keys)
    {
        if (key.starts_with("header") || key.starts_with("access_header"))
            config.remove(prefix + key);
    }
    for (const auto * sse_key : {"server_side_encryption_customer_key_base64",
                                 "server_side_encryption_kms_key_id",
                                 "server_side_encryption_kms_encryption_context"})
    {
        if (config.has(prefix + sse_key))
            config.remove(prefix + sse_key);
    }
}

namespace
{

/// Whether one resolved object-storage backend (the disk root, or a single `locations.<name>` child selected by
/// `prefix`) is an S3 backend that resolved to credentials the AST did not prove explicit, and so must be
/// downgraded to anonymous or rejected. Returns false for a non-S3 backend or one whose resolved auth is safe.
bool resolvedS3BackendIsRestricted(
    const Poco::Util::AbstractConfiguration & config, const String & prefix, bool is_root, const DynamicS3DiskCredentialInfo & info)
{
    auto key = [&](const String & k) { return prefix + k; };
    auto is_s3_type = [](const String & t) { return t == "s3" || t.starts_with("s3_"); };
    const bool resolved_backend_is_s3
        = is_s3_type(config.getString(key("type"), "")) || is_s3_type(config.getString(key("object_storage_type"), ""));
    if (!resolved_backend_is_s3)
        return false;

    const bool resolved_wants_gcp_oauth = boost::iequals(config.getString(key("http_client"), ""), "gcp_oauth");
    const bool resolved_has_role_arn = !config.getString(key("role_arn"), "").empty();
    const bool resolved_has_key_pair
        = !config.getString(key("access_key_id"), "").empty() && !config.getString(key("secret_access_key"), "").empty();
    const bool resolved_no_sign = config.getBool(key("no_sign_request"), false);
    const bool resolved_use_env_off
        = config.has(key("use_environment_credentials")) && !config.getBool(key("use_environment_credentials"), true);

    bool safe = false;
    if (resolved_wants_gcp_oauth)
    {
        /// `gcp_oauth` mints a bearer token (server GCP metadata unless a complete ADC triple is given). It is
        /// checked before NOSIGN because NOSIGN only disables the AWS credentials provider: a child that is both
        /// NOSIGN and `gcp_oauth` still mints and sends the server GCP token, so it is not anonymous.
        safe = is_root && info.ast_has_explicit_gcp_adc;
    }
    else if (resolved_no_sign)
    {
        /// NOSIGN sends no credentials at all, so it is anonymous regardless of where it was configured. For the
        /// root this still requires the AST itself to have proved NOSIGN (an `include` could otherwise inject it).
        safe = is_root ? info.ast_has_no_sign_request : true;
    }
    else if (resolved_has_role_arn || resolved_has_key_pair)
    {
        /// A `role_arn` assumes a role using an explicit base key pair; an explicit key pair is itself
        /// credentials. These are trusted only when they came from the literal SQL of *this* backend. The AST
        /// flags describe the disk root only, so a `locations.<name>` child whose key pair / role was resolved
        /// from the `include` (or server config) is not vouched for by a key pair on the root wrapper.
        safe = is_root && info.ast_has_explicit_key_pair;
    }
    else if (resolved_use_env_off)
    {
        /// `use_environment_credentials = 0` with no key pair is anonymous. For the root the AST must have
        /// proved it; a child resolving to it (no keys, no role, no gcp) is genuinely anonymous either way.
        safe = is_root ? (info.ast_has_use_environment_credentials_off || info.ast_has_no_sign_request) : true;
    }
    else
        safe = false; /// resolved relies on the default `use_environment_credentials = 1` (server credentials)

    return !safe;
}

}

void validateResolvedS3DiskCredentials(
    Poco::Util::AbstractConfiguration & config, ContextPtr context, bool is_loading_from_existing_metadata, const DynamicS3DiskCredentialInfo & info)
{
    /// Re-check after `include` is resolved: an `include` can inject an S3 type and credentials past the
    /// pre-resolution check (the disk is then built with `for_disk_s3 = true`, bypassing `getClient`). Only an
    /// `include`d disk needs this; the resolved auth mode is validated against the form the AST itself proved.
    /// `restriction_exempt` carries a pre-resolution exemption (e.g. a server-internal `system`-database disk).
    if (!info.has_include || !context->shouldRestrictUserQueryS3Credentials() || info.restriction_exempt)
        return;

    /// Check the disk root and every `locations.<name>` child: a multi-location `DiskObjectStorage` builds one
    /// object storage per child, each with its own `object_storage_type`, so an `include` can hide an S3 child
    /// (built with `for_disk_s3 = true`) behind a root that proves non-S3.
    std::vector<String> prefixes{""};
    if (config.has("locations"))
    {
        Poco::Util::AbstractConfiguration::Keys locations;
        config.keys("locations", locations);
        for (const auto & location : locations)
            prefixes.push_back("locations." + location + ".");
    }

    std::vector<String> restricted_prefixes;
    for (const auto & prefix : prefixes)
        if (resolvedS3BackendIsRestricted(config, prefix, /* is_root */ prefix.empty(), info))
            restricted_prefixes.push_back(prefix);

    if (restricted_prefixes.empty())
        return;

    if (is_loading_from_existing_metadata
        && context->getGlobalContext()->getServerSettings()[ServerSetting::s3_load_table_anonymously_if_credentials_restricted])
    {
        LOG_WARNING(
            getLogger("getDiskConfigurationFromAST"),
            "Loading a dynamic S3 disk anonymously: an `include` resolves to an S3 backend whose credentials are "
            "not provided explicitly in the SQL definition, so they may be server-managed, which is restricted for "
            "user queries (s3_allow_server_credentials_in_user_queries = 0). The disk will be inaccessible. Set the "
            "server setting s3_load_table_anonymously_if_credentials_restricted = 0 to fail loading instead.");
        for (const auto & prefix : restricted_prefixes)
            forceAnonymousS3DiskConfigAtPrefix(config, prefix);
    }
    else
        throw Exception(
            ErrorCodes::ACCESS_DENIED,
            "A dynamic S3 disk created from user SQL that uses `include` must provide its credentials explicitly "
            "in the SQL definition (a complete `access_key_id`/`secret_access_key` pair, `no_sign_request`, or "
            "`use_environment_credentials = 0`; or, for `http_client = gcp_oauth`, a complete explicit Google ADC "
            "triple). It may not take the S3 type or credentials from the included configuration, which could "
            "resolve the server's own credentials. Enable `s3_allow_server_credentials_in_user_queries` to allow it.");
}

DiskConfigurationPtr getDiskConfigurationFromAST(const ASTs & disk_args, ContextPtr context)
{
    DynamicS3DiskCredentialInfo info;
    auto xml_document = getDiskConfigurationFromASTImpl(disk_args, context, /* is_loading_from_existing_metadata = */ false, &info);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> conf(new Poco::Util::XMLConfiguration());
    conf->load(xml_document);
    /// This path does not resolve `include`, so there is no post-resolution check; only honor the
    /// pre-resolution decision.
    if (info.load_anonymously)
        forceAnonymousS3DiskConfig(*conf);
    return conf;
}

ASTs convertDiskConfigurationToAST(const Poco::Util::AbstractConfiguration & configuration, const std::string & config_path)
{
    ASTs result;

    Poco::Util::AbstractConfiguration::Keys keys;
    configuration.keys(config_path, keys);

    for (const auto & key : keys)
    {
        result.push_back(
            makeASTOperator(
                "equals",
                make_intrusive<ASTIdentifier>(key),
                make_intrusive<ASTLiteral>(configuration.getString(config_path + "." + key))));
    }

    return result;
}

}
