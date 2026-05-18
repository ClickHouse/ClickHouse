#include <Disks/getDiskConfigurationFromAST.h>

#include <Common/assert_cast.h>
#include <Common/FieldVisitorToString.h>
#include <Common/logger_useful.h>
#include <Core/SettingsFields.h>
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
    bool is_s3_configuration = false;
    /// True if the resolved disk type cannot be determined statically here
    /// because it comes from `from_env`/`from_zk` substitution on the `type`
    /// key, or because the config is augmented by an `include` directive that
    /// could supply (or override) the type after this function returns. In
    /// either case we conservatively treat the disk as potentially S3 so the
    /// credential hardening below cannot be bypassed by indirection.
    bool type_resolution_is_indirect = false;
    bool has_use_environment_credentials = false;
    bool use_environment_credentials = false;
    bool has_role_arn = false;
    /// "Explicit" here means the value is a literal in the SQL — not a
    /// `from_env`/`from_zk` reference. Indirect values must not count as the
    /// user-supplied keys that authorize a `role_arn`, since they could
    /// resolve to credentials configured by the operator.
    bool has_explicit_access_key_id = false;
    bool has_explicit_secret_access_key = false;

    auto is_indirect_value = [](const std::string & v)
    {
        return startsWith(v, "from_env") || startsWith(v, "from_zk");
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

        if (key == "type" || key == "object_storage_type")
        {
            if (value_str == "s3" || value_str.starts_with("s3_"))
                is_s3_configuration = true;
            else if (is_indirect_value(value_str))
                type_resolution_is_indirect = true;
        }
        else if (key == "include")
            type_resolution_is_indirect = true;
        else if (key == "use_environment_credentials")
        {
            has_use_environment_credentials = true;
            use_environment_credentials = is_indirect_value(value_str) || stringToBool(value_str);
        }
        else if (key == "role_arn" && !value_str.empty())
            has_role_arn = true;
        else if (key == "access_key_id" && !value_str.empty() && !is_indirect_value(value_str))
            has_explicit_access_key_id = true;
        else if (key == "secret_access_key" && !value_str.empty() && !is_indirect_value(value_str))
            has_explicit_secret_access_key = true;

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

    /// Apply the S3 hardening checks whenever this disk is, or could resolve
    /// to, an S3 disk. `type_resolution_is_indirect` covers the case where
    /// the literal `s3`/`s3_*` value never appears here because `type` is
    /// fetched from an env variable / ZooKeeper, or the configuration is
    /// augmented by an `include` that could supply the type itself.
    const bool apply_s3_credential_checks = is_s3_configuration || type_resolution_is_indirect;

    if (!is_loading_from_existing_metadata && apply_s3_credential_checks && use_environment_credentials)
        throw Exception(
            ErrorCodes::ACCESS_DENIED,
            "Using `use_environment_credentials` in dynamic S3 disk configuration is not allowed");

    if (!is_loading_from_existing_metadata && apply_s3_credential_checks && !has_use_environment_credentials)
    {
        Poco::AutoPtr<Poco::XML::Element> key_element(xml_document->createElement("use_environment_credentials"));
        root->appendChild(key_element);
        Poco::AutoPtr<Poco::XML::Text> value_element(xml_document->createTextNode("false"));
        key_element->appendChild(value_element);
    }

    if (!is_loading_from_existing_metadata && apply_s3_credential_checks && has_role_arn
        && (!has_explicit_access_key_id || !has_explicit_secret_access_key))
        throw Exception(
            ErrorCodes::ACCESS_DENIED,
            "Using `role_arn` without explicit S3 credentials in dynamic disk configuration is not allowed");

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
