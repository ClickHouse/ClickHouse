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

#include <array>
#include <filesystem>
#include <functional>
#include <list>
#include <set>
#include <tuple>
#include <utility>


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

        using IsValid = std::function<bool (const String & value)>;
        using ParameterInfo = std::tuple<bool, String, IsValid>; /// (can_be_skipped, parameter, is_valid).
        using ParametersInfo = std::array<ParameterInfo, 6>;
        static const ParametersInfo parameters_info =
        {{
            {true, "access_key_id", [](const String &){ return true; }},
            {true, "secret_access_key", [](const String &){ return true; }},
            {true, "session_token", [](const String &){ return true; }},
            {false, "format", [](const String & value){ return FormatFactory::instance().getAllFormats().contains(value); }},
            {false, "structure", [](const String &){ return true; }},
            {false, "compression_method", [](const String &){ return true; }}
        }};

        using Match = std::pair<ParametersInfo::const_iterator, size_t>;
        std::list<Match> matches;
        std::set<Match> wrong_matches;

        for (Match match = {parameters_info.begin(), 1}; match.second < args.size(); )
        {
            const auto & arg = args[match.second]->as<ASTLiteral &>().value.safeGet<String>();
            if (match.first != parameters_info.end() && !wrong_matches.contains(match) && std::get<IsValid>(*match.first)(arg))
            {
                /// Valid.
                matches.emplace_back(match);
                ++match.first;
                ++match.second;
            }
            else if (match.first != parameters_info.end() && std::get<bool>(*match.first) && std::next(match.first) != parameters_info.end())
            {
                /// Invalid and can be skipped.
                ++match.first;
            }
            else if (!matches.empty())
            {
                /// Invalid and can't be skipped.
                wrong_matches.emplace(match = std::move(matches.back()));
                matches.pop_back();
            }
            else
            {
                /// No possible combinations left.
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, error_message);
            }
        }

        std::map<String, size_t> args_to_idx;
        for (const auto & [parameter_it, index] : matches)
            args_to_idx.emplace(std::move(std::get<String>(*parameter_it)), index);

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
