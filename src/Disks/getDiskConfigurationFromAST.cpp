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
}

[[noreturn]] static void throwBadConfiguration(const std::string & message = "")
{
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Incorrect configuration{}. Example of expected configuration: `(type=s3 ...`)`",
        message.empty() ? "" : ": " + message);
}

Poco::AutoPtr<Poco::XML::Document> getDiskConfigurationFromASTImpl(const ASTs & disk_args, ContextPtr context, bool is_loading_from_existing_metadata, DynamicS3DiskCredentialInfo * info)
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

    /// A user-created S3 disk must not resolve the server's own credentials (environment/IMDS/IRSA,
    /// role_arn-based STS, AWS config files, or the GCP metadata service). The check is bypassed for trusted
    /// clients via the `s3_allow_server_credentials_in_user_queries` setting (see
    /// shouldRestrictUserQueryS3Credentials).
    ///
    /// The check is also applied when loading from existing metadata (server startup or RESTORE): a disk
    /// definition that resolves server-managed credentials -- e.g. one created before this restriction existed,
    /// or while it was allowed -- must not silently keep using the server's identity on restart, since a user
    /// `CREATE`/`ATTACH` of the same definition would be refused. To avoid aborting startup, such a disk is
    /// forced anonymous on load (it becomes inaccessible for a private bucket) rather than throwing, governed by
    /// the server setting `s3_load_table_anonymously_if_credentials_restricted`.
    ///
    /// A `from_env`/`from_zk` substitution on the disk type or on a credential/auth field could hide an S3
    /// disk or resolve a server-side value, so an indirect type is treated as potentially-S3. An `include`
    /// could pull S3 fields (including the type) from server config, so it is also potentially-S3 -- unless
    /// the disk's backend is explicitly a literal non-S3 type, in which case the include cannot turn it into
    /// an S3 disk and the check must not break that unrelated dynamic-disk feature. `type = object_storage`
    /// is only a wrapper: its backend comes from `object_storage_type` (which an `include` could inject as
    /// `s3`), so it is non-S3 only when an explicit literal non-S3 `object_storage_type` is also present.
    const bool type_explicitly_non_s3
        = has_concrete_non_s3_type || (type_is_object_storage && has_explicit_non_s3_object_storage_type);
    const bool maybe_s3_disk = is_s3_disk || type_is_indirect || (has_include && !type_explicitly_non_s3);

    const bool has_explicit_credentials = has_access_key_id && has_secret_access_key;
    const bool has_explicit_gcp_adc
        = has_google_adc_client_id && has_google_adc_client_secret && has_google_adc_refresh_token;
    /// An explicit anonymous form is allowed: an explicit key pair, `no_sign_request`, or an explicit
    /// `use_environment_credentials = 0` with no `role_arn` (the request then goes unsigned, the same as
    /// any other user query that asks for no server-managed credentials). `gcp_oauth` authenticates with a
    /// bearer token rather than S3 keys, so it needs a complete explicit ADC triple, otherwise the token
    /// comes from the server's GCP metadata service.
    const bool allowed_anonymous = use_environment_credentials_off && !has_role_arn;
    /// Whether the AST itself (literal values, no substitution/include) supplied a safe credential form.
    /// Only this counts when the resolved configuration is re-checked after `include`: a value an `include`
    /// or a `from_env`/`from_zk` substitution injects must not be trusted as a user-provided credential.
    const bool ast_has_explicit_credentials = wants_gcp_oauth
        ? has_explicit_gcp_adc
        : (has_explicit_credentials || has_no_sign_request || allowed_anonymous);

    if (info)
    {
        info->has_include = has_include;
        info->ast_has_explicit_key_pair = has_explicit_credentials;
        info->ast_has_no_sign_request = has_no_sign_request;
        info->ast_has_use_environment_credentials_off = allowed_anonymous;
        info->ast_has_explicit_gcp_adc = has_explicit_gcp_adc;
    }

    if (maybe_s3_disk && context->shouldRestrictUserQueryS3Credentials())
    {
        /// Everything that is not a safe explicit form (no credentials with the default
        /// `use_environment_credentials = 1`, a `role_arn` without explicit keys, substitutions on the type
        /// or credential/auth fields, or `include`) could resolve a server-managed credential and is refused.
        const bool denied = type_is_indirect || has_include || has_indirect_auth_field || !ast_has_explicit_credentials;
        if (denied)
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

                /// Do not abort startup: tell the caller to force the disk anonymous once the configuration is
                /// loaded (and any `include` is resolved), see forceAnonymousS3DiskConfig. We cannot rewrite the
                /// DOM here because in DiskFromAST an `include` is processed after this function returns and
                /// could otherwise re-introduce server credentials.
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
    }

    return xml_document;
}

void forceAnonymousS3DiskConfig(Poco::Util::AbstractConfiguration & config)
{
    /// Clear `http_client` (which would mint a GCP token at the HTTP layer regardless of the credentials
    /// provider) and force `no_sign_request = 1`, so the S3 client is built unsigned regardless of
    /// `use_environment_credentials`/`role_arn` (see getCredentialsProvider) and never resolves the server's
    /// own credentials. `setString` overwrites any value the definition (or an `include`) supplied.
    config.setString("http_client", "");
    config.setString("no_sign_request", "1");
}

void validateResolvedS3DiskCredentials(
    Poco::Util::AbstractConfiguration & config, ContextPtr context, bool is_loading_from_existing_metadata, const DynamicS3DiskCredentialInfo & info)
{
    /// Re-check the credential restriction AFTER `include`/`from_env`/`from_zk` are resolved, but only for an
    /// `include`d disk: an `include` can supply the type and credentials (e.g. `type = s3` plus
    /// `access_key_id`/`secret_access_key` via `from_env`/`from_zk`, or `http_client = gcp_oauth`), evading the
    /// pre-resolution check that ran on a literal non-S3 type in the AST; the disk would then be built with
    /// `for_disk_s3 = true`, so the central `getClient` restriction never applies. A disk without `include` is
    /// fully decided by the pre-resolution check. The resolved values here are just concrete strings whose
    /// provenance cannot be trusted, so the resolved auth MODE is validated against the specific safe form the
    /// AST itself proved: a value an `include`/substitution injected does not count.
    if (!info.has_include || !context->shouldRestrictUserQueryS3Credentials())
        return;

    auto is_s3_type = [](const String & t) { return t == "s3" || t.starts_with("s3_"); };
    const bool resolved_backend_is_s3
        = is_s3_type(config.getString("type", "")) || is_s3_type(config.getString("object_storage_type", ""));
    if (!resolved_backend_is_s3)
        return;

    /// Match the resolved auth mode to the form the AST proved. `gcp_oauth` mints a bearer token (server GCP
    /// metadata unless a complete ADC triple is given); a `role_arn` assumes a role and needs an explicit base
    /// key pair; otherwise the disk is safe only if it is explicitly anonymous or carries an explicit key pair.
    const bool resolved_wants_gcp_oauth = boost::iequals(config.getString("http_client", ""), "gcp_oauth");
    const bool resolved_has_role_arn = !config.getString("role_arn", "").empty();
    const bool resolved_has_key_pair
        = !config.getString("access_key_id", "").empty() && !config.getString("secret_access_key", "").empty();
    const bool resolved_no_sign = config.getBool("no_sign_request", false);
    const bool resolved_use_env_off
        = config.has("use_environment_credentials") && !config.getBool("use_environment_credentials", true);

    bool safe;
    if (resolved_wants_gcp_oauth)
        safe = info.ast_has_explicit_gcp_adc;
    else if (resolved_has_role_arn)
        safe = info.ast_has_explicit_key_pair;
    else if (resolved_has_key_pair)
        safe = info.ast_has_explicit_key_pair;
    else if (resolved_no_sign)
        safe = info.ast_has_no_sign_request;
    else if (resolved_use_env_off)
        safe = info.ast_has_use_environment_credentials_off || info.ast_has_no_sign_request;
    else
        safe = false; /// resolved relies on the default `use_environment_credentials = 1` (server credentials)

    if (safe)
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
        forceAnonymousS3DiskConfig(config);
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
