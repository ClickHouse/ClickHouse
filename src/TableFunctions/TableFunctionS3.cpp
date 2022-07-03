#include <Common/config.h>

#if USE_AWS_S3

#include <IO/S3Common.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageS3.h>
#include <Formats/FormatFactory.h>
#include "registerTableFunctions.h"

#include <list>
#include <map>
#include <set>

#include <filesystem>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/// This is needed to avoid copy-pase. Because s3Cluster arguments only differ in additional argument (first) - cluster name
void TableFunctionS3::parseArgumentsImpl(const String & error_message, ASTs & args, ContextPtr context, StorageS3Configuration & s3_configuration)
{
    if (auto named_collection = getURLBasedDataSourceConfiguration(args, context))
    {
        auto [common_configuration, storage_specific_args] = named_collection.value();
        s3_configuration.set(common_configuration);
        StorageS3::processNamedCollectionResult(s3_configuration, storage_specific_args);
    }
    else
    {
        if (args.empty() || args.size() > 7)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, error_message);

        for (auto & arg : args)
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

        /// parameter -> (can_be_skipped, is_valid)
        static std::map<String, std::pair<bool, std::function<bool (const String & value)>>> parameters_info =
        {
            {"access_key_id", {true, [](const String &){ return true; }}},
            {"secret_access_key", {true, [](const String &){ return true; }}},
            {"session_token", {true, [](const String &){ return true; }}},
            {"format", {false, [](const String & value){ return FormatFactory::instance().getAllFormats().contains(value); }}},
            {"structure", {false, [](const String &){ return true; }}},
            {"compression_method", {false, [](const String &){ return true; }}}
        };

        std::list<std::pair<String, size_t>> matches;
        std::set<std::pair<String, size_t>> wrong_matches;

        auto parameters_it = parameters_info.begin();
        for (size_t index = 1; index < args.size(); )
        {
            const auto & arg = args[index]->as<ASTLiteral &>().value.safeGet<String>();
            if (wrong_matches.count({parameters_it->first, index}) == 0 && parameters_it->second.second(arg))
            {
                /// Valid.
                matches.push_back({parameters_it->first, index});
                ++index;
                ++parameters_it;
            }
            else if (parameters_it->second.first && std::next(parameters_it) != parameters_info.end())
            {
                /// Invalid and can be skipped.
                ++parameters_it;
            }
            else if (!matches.empty())
            {
                /// Invalid and can't be skipped.
                auto [last_parameter, last_index] = matches.back();
                parameters_it = parameters_info.find(last_parameter);
                index = last_index;
                wrong_matches.emplace(std::move(last_parameter), last_index);
                matches.pop_back();
            }
            else
            {
                /// No possible combinations left.
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, error_message);
            }
        }

        std::map<String, size_t> args_to_idx(matches.begin(), matches.end());

        /// This argument is always the first
        s3_configuration.url = args[0]->as<ASTLiteral &>().value.safeGet<String>();

        if (args_to_idx.contains("format"))
            s3_configuration.format = args[args_to_idx["format"]]->as<ASTLiteral &>().value.safeGet<String>();

        if (args_to_idx.contains("structure"))
            s3_configuration.structure = args[args_to_idx["structure"]]->as<ASTLiteral &>().value.safeGet<String>();

        if (args_to_idx.contains("compression_method"))
            s3_configuration.compression_method = args[args_to_idx["compression_method"]]->as<ASTLiteral &>().value.safeGet<String>();

        if (args_to_idx.contains("access_key_id"))
            s3_configuration.auth_settings.access_key_id = args[args_to_idx["access_key_id"]]->as<ASTLiteral &>().value.safeGet<String>();

        if (args_to_idx.contains("secret_access_key"))
            s3_configuration.auth_settings.secret_access_key = args[args_to_idx["secret_access_key"]]->as<ASTLiteral &>().value.safeGet<String>();

        if (args_to_idx.contains("session_token"))
            s3_configuration.auth_settings.session_token = args[args_to_idx["session_token"]]->as<ASTLiteral &>().value.safeGet<String>();
    }

    if (s3_configuration.format == "auto")
        s3_configuration.format = FormatFactory::instance().getFormatFromFileName(s3_configuration.url, true);
}

void TableFunctionS3::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    const auto message = fmt::format(
        "The signature of table function {} could be the following:\n"
        " - url\n"
        " - url, format\n"
        " - url, format, structure\n"
        " - url, access_key_id, secret_access_key\n"
        " - url, format, structure, compression_method\n"
        " - url, access_key_id, secret_access_key, session_token\n"
        " - url, access_key_id, secret_access_key, format\n"
        " - url, access_key_id, secret_access_key, session_token, format\n"
        " - url, access_key_id, secret_access_key, format, structure\n"
        " - url, access_key_id, secret_access_key, session_token, format, structure\n"
        " - url, access_key_id, secret_access_key, format, structure, compression_method\n"
        " - url, access_key_id, secret_access_key, session_token, format, structure, compression_method",
        getName());

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    auto & args = args_func.at(0)->children;

    parseArgumentsImpl(message, args, context, configuration);
}

ColumnsDescription TableFunctionS3::getActualTableStructure(ContextPtr context) const
{
    if (configuration.structure == "auto")
    {
        return StorageS3::getTableStructureFromData(
            configuration.format,
            S3::URI(Poco::URI(configuration.url)),
            configuration.auth_settings.access_key_id,
            configuration.auth_settings.secret_access_key,
            configuration.compression_method,
            false,
            std::nullopt,
            context);
    }

    return parseColumnsListFromString(configuration.structure, context);
}

StoragePtr TableFunctionS3::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    Poco::URI uri (configuration.url);
    S3::URI s3_uri (uri);

    ColumnsDescription columns;
    if (configuration.structure != "auto")
        columns = parseColumnsListFromString(configuration.structure, context);
    else if (!structure_hint.empty())
        columns = structure_hint;

    StoragePtr storage = std::make_shared<StorageS3>(
        s3_uri,
        configuration.auth_settings.access_key_id,
        configuration.auth_settings.secret_access_key,
        StorageID(getDatabaseName(), table_name),
        configuration.format,
        configuration.rw_settings,
        columns,
        ConstraintsDescription{},
        String{},
        context,
        /// No format_settings for table function S3
        std::nullopt,
        configuration.compression_method);

    storage->startup();

    return storage;
}


void registerTableFunctionS3(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionS3>();
}

void registerTableFunctionCOS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionCOS>();
}

}

#endif
