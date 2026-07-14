#include <Disks/getDiskConfigurationFromAST.h>

#include <Common/assert_cast.h>
#include <Common/FieldVisitorToString.h>
#include <Common/logger_useful.h>
#include <Disks/DiskFactory.h>
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
}

[[noreturn]] static void throwBadConfiguration(const std::string & message = "")
{
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Incorrect configuration{}. Example of expected configuration: `(type=s3 ...`)`",
        message.empty() ? "" : ": " + message);
}

Poco::AutoPtr<Poco::XML::Document> getDiskConfigurationFromASTImpl(const ASTs & disk_args, ContextPtr context)
{
    if (disk_args.empty())
        throwBadConfiguration("expected non-empty list of arguments");

    Poco::AutoPtr<Poco::XML::Document> xml_document(new Poco::XML::Document());
    Poco::AutoPtr<Poco::XML::Element> root(xml_document->createElement("disk"));
    xml_document->appendChild(root);

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
        if (key == "include")
        {
            key_element->setAttribute("incl", value_str);
        }
        else if (startsWith(value_str, "from_env"))
        {
            value_str = value_str.substr(std::strlen("from_env"));
            boost::trim(value_str);
            key_element->setAttribute("from_env", value_str);
        }
        else if (startsWith(value_str, "from_zk"))
        {
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
            makeASTFunction(
                "equals",
                std::make_shared<ASTIdentifier>(key),
                std::make_shared<ASTLiteral>(configuration.getString(config_path + "." + key))));
    }

    return result;
}

}
