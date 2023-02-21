#include "config.h"

#if USE_AWS_S3

#    include <filesystem>
#    include <Access/Common/AccessFlags.h>
#    include <Formats/FormatFactory.h>
#    include <IO/S3Common.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/evaluateConstantExpression.h>
#    include <Interpreters/parseColumnsListForTableFunction.h>
#    include <Parsers/ASTLiteral.h>
#    include <Storages/StorageHudi.h>
#    include <Storages/StorageURL.h>
#    include <Storages/checkAndGetLiteralArgument.h>
#    include <TableFunctions/TableFunctionFactory.h>
#    include <TableFunctions/TableFunctionHudi.h>
#    include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


void TableFunctionHudi::parseArgumentsImpl(
    const String & error_message, ASTs & args, ContextPtr context, StorageS3Configuration & base_configuration)
{
    if (args.empty() || args.size() > 6)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, error_message);

    auto header_it = StorageURL::collectHeaders(args, base_configuration, context);
    if (header_it != args.end())
        args.erase(header_it);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    /// Size -> argument indexes
    static auto size_to_args = std::map<size_t, std::map<String, size_t>>{
        {1, {{}}},
        {2, {{"format", 1}}},
        {5, {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}}},
        {6, {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}, {"compression_method", 5}}}};

    std::map<String, size_t> args_to_idx;
    /// For 4 arguments we support 2 possible variants:
    /// hudi(source, format, structure, compression_method) and hudi(source, access_key_id, access_key_id, format)
    /// We can distinguish them by looking at the 2-nd argument: check if it's a format name or not.
    if (args.size() == 4)
    {
        auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/access_key_id");
        if (second_arg == "auto" || FormatFactory::instance().getAllFormats().contains(second_arg))
            args_to_idx = {{"format", 1}, {"structure", 2}, {"compression_method", 3}};

        else
            args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}};
    }
    /// For 3 arguments we support 2 possible variants:
    /// hudi(source, format, structure) and hudi(source, access_key_id, access_key_id)
    /// We can distinguish them by looking at the 2-nd argument: check if it's a format name or not.
    else if (args.size() == 3)
    {
        auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/access_key_id");
        if (second_arg == "auto" || FormatFactory::instance().getAllFormats().contains(second_arg))
            args_to_idx = {{"format", 1}, {"structure", 2}};
        else
            args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}};
    }
    else
    {
        args_to_idx = size_to_args[args.size()];
    }

    /// This argument is always the first
    base_configuration.url = checkAndGetLiteralArgument<String>(args[0], "url");

    if (args_to_idx.contains("format"))
        base_configuration.format = checkAndGetLiteralArgument<String>(args[args_to_idx["format"]], "format");
    else
        base_configuration.format = "Parquet";

    if (args_to_idx.contains("structure"))
        base_configuration.structure = checkAndGetLiteralArgument<String>(args[args_to_idx["structure"]], "structure");

    if (args_to_idx.contains("compression_method"))
        base_configuration.compression_method
            = checkAndGetLiteralArgument<String>(args[args_to_idx["compression_method"]], "compression_method");

    if (args_to_idx.contains("access_key_id"))
        base_configuration.auth_settings.access_key_id
            = checkAndGetLiteralArgument<String>(args[args_to_idx["access_key_id"]], "access_key_id");

    if (args_to_idx.contains("secret_access_key"))
        base_configuration.auth_settings.secret_access_key
            = checkAndGetLiteralArgument<String>(args[args_to_idx["secret_access_key"]], "secret_access_key");
}

void TableFunctionHudi::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    const auto message = fmt::format(
        "The signature of table function {} could be the following:\n" \
        " - url\n" \
        " - url, format\n" \
        " - url, format, structure\n" \
        " - url, access_key_id, secret_access_key\n" \
        " - url, format, structure, compression_method\n" \
        " - url, access_key_id, secret_access_key, format\n" \
        " - url, access_key_id, secret_access_key, format, structure\n" \
        " - url, access_key_id, secret_access_key, format, structure, compression_method",
        getName());

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments", getName());

    auto & args = args_func.at(0)->children;

    parseArgumentsImpl(message, args, context, configuration);
}

ColumnsDescription TableFunctionHudi::getActualTableStructure(ContextPtr context) const
{
    if (configuration.structure == "auto")
    {
        context->checkAccess(getSourceAccessType());
        return StorageS3::getTableStructureFromData(configuration, false, std::nullopt, context);
    }

    return parseColumnsListFromString(configuration.structure, context);
}

StoragePtr TableFunctionHudi::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    Poco::URI uri(configuration.url);
    S3::URI s3_uri(uri);

    ColumnsDescription columns;
    if (configuration.structure != "auto")
        columns = parseColumnsListFromString(configuration.structure, context);

    StoragePtr storage = std::make_shared<StorageHudi>(
        configuration, StorageID(getDatabaseName(), table_name), columns, ConstraintsDescription{}, String{}, context, std::nullopt);

    storage->startup();

    return storage;
}


void registerTableFunctionHudi(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHudi>(
        {.documentation
         = {R"(The table function can be used to read the Hudi table stored on object store.)",
            Documentation::Examples{{"hudi", "SELECT * FROM hudi(url, access_key_id, secret_access_key)"}},
            Documentation::Categories{"DataLake"}},
         .allow_readonly = false});
}
}

#endif
