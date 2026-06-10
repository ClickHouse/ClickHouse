#include <Disks/getDiskConfigurationFromAST.h>

#include <Common/assert_cast.h>
#include <Common/FieldVisitorToString.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
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

[[noreturn]] static void throwBadConfiguration(const std::string & message = "")
{
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Incorrect configuration{}. Example of expected configuration: `(type=s3 ...`)`",
        message.empty() ? "" : ": " + message);
}

Poco::AutoPtr<Poco::XML::Document> getDiskConfigurationFromASTImpl(const ASTs & disk_args, ContextPtr context, bool is_loading_from_existing_metadata)
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
            is_s3_disk |= (value_str == "s3" || value_str.starts_with("s3_"));
            type_is_indirect |= indirect;
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
    /// role_arn-based STS, AWS config files, or the GCP metadata service). Skip the check when loading from
    /// existing metadata, so a disk that was created while it was allowed keeps loading after a restart. The
    /// check is bypassed for trusted clients via the `s3_allow_server_credentials_in_user_queries` setting
    /// (see shouldRestrictUserQueryS3Credentials).
    ///
    /// `include` could pull S3 fields from server config, and a `from_env`/`from_zk` substitution on the
    /// disk type or on a credential/auth field could hide an S3 disk or resolve a server-side value, so any
    /// of those is treated as potentially-S3 and rejected.
    const bool maybe_s3_disk = is_s3_disk || type_is_indirect || has_include;
    if (!is_loading_from_existing_metadata && maybe_s3_disk && context->shouldRestrictUserQueryS3Credentials())
    {
        const bool has_explicit_credentials = has_access_key_id && has_secret_access_key;
        const bool has_explicit_gcp_adc
            = has_google_adc_client_id && has_google_adc_client_secret && has_google_adc_refresh_token;

        /// An explicit anonymous form is allowed: an explicit key pair, `no_sign_request`, or an explicit
        /// `use_environment_credentials = 0` with no `role_arn` (the request then goes unsigned, the same as
        /// any other user query that asks for no server-managed credentials). `gcp_oauth` authenticates with a
        /// bearer token rather than S3 keys, so it needs a complete explicit ADC triple, otherwise the token
        /// comes from the server's GCP metadata service. Everything else (no credentials with the default
        /// `use_environment_credentials = 1`, a `role_arn` without explicit keys, substitutions on the type or
        /// credential/auth fields, or `include`) could resolve a server-managed credential and is refused.
        const bool allowed_anonymous = use_environment_credentials_off && !has_role_arn;
        const bool denied = type_is_indirect || has_include || has_indirect_auth_field
            || (wants_gcp_oauth ? !has_explicit_gcp_adc
                                : (!has_explicit_credentials && !has_no_sign_request && !allowed_anonymous));
        if (denied)
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

DiskConfigurationPtr getDiskConfigurationFromAST(const ASTs & disk_args, ContextPtr context)
{
    auto xml_document = getDiskConfigurationFromASTImpl(disk_args, context);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> conf(new Poco::Util::XMLConfiguration());
    conf->load(xml_document);
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
