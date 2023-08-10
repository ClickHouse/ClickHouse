#include "config.h"

#if USE_AWS_S3

#include <IO/S3Common.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Access/Common/AccessFlags.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageS3.h>
#include <Storages/StorageURL.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Formats/FormatFactory.h>
#include "registerTableFunctions.h"
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableFunctionNode.h>

#include <boost/algorithm/string.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}


std::vector<size_t> TableFunctionS3::skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr) const
{
    auto & table_function_node = query_node_table_function->as<TableFunctionNode &>();
    auto & table_function_arguments_nodes = table_function_node.getArguments().getNodes();
    size_t table_function_arguments_size = table_function_arguments_nodes.size();

    std::vector<size_t> result;

    for (size_t i = 0; i < table_function_arguments_size; ++i)
    {
        auto * function_node = table_function_arguments_nodes[i]->as<FunctionNode>();
        if (function_node && function_node->getFunctionName() == "headers")
            result.push_back(i);
    }

    return result;
}

/// This is needed to avoid copy-pase. Because s3Cluster arguments only differ in additional argument (first) - cluster name
void TableFunctionS3::parseArgumentsImpl(ASTs & args, const ContextPtr & context)
{
    if (auto named_collection = tryGetNamedCollectionWithOverrides(args, context))
    {
        StorageS3::processNamedCollectionResult(configuration, *named_collection);
    }
    else
    {

        auto * header_it = StorageURL::collectHeaders(args, configuration.headers_from_ast, context);
        if (header_it != args.end())
            args.erase(header_it);

        if (args.empty() || args.size() > 6)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "The signature of table function {} shall be the following:\n{}", getName(), getSignature());

        for (auto & arg : args)
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

        /// Size -> argument indexes
        static std::unordered_map<size_t, std::unordered_map<std::string_view, size_t>> size_to_args
        {
            {1, {{}}},
            {6, {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}, {"compression_method", 5}}}
        };

        std::unordered_map<std::string_view, size_t> args_to_idx;

        bool no_sign_request = false;

        /// For 2 arguments we support 2 possible variants:
        /// - s3(source, format)
        /// - s3(source, NOSIGN)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN or not.
        if (args.size() == 2)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
                no_sign_request = true;
            else
                args_to_idx = {{"format", 1}};
        }
        /// For 3 arguments we support 3 possible variants:
        /// - s3(source, format, structure)
        /// - s3(source, access_key_id, access_key_id)
        /// - s3(source, NOSIGN, format)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a format name or not.
        else if (args.size() == 3)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/access_key_id/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                no_sign_request = true;
                args_to_idx = {{"format", 2}};
            }
            else if (second_arg == "auto" || FormatFactory::instance().getAllFormats().contains(second_arg))
                args_to_idx = {{"format", 1}, {"structure", 2}};
            else
                args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}};
        }
        /// For 4 arguments we support 3 possible variants:
        /// - s3(source, format, structure, compression_method),
        /// - s3(source, access_key_id, access_key_id, format)
        /// - s3(source, NOSIGN, format, structure)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a format name or not.
        else if (args.size() == 4)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/access_key_id/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                no_sign_request = true;
                args_to_idx = {{"format", 2}, {"structure", 3}};
            }
            else if (second_arg == "auto" || FormatFactory::instance().getAllFormats().contains(second_arg))
                args_to_idx = {{"format", 1}, {"structure", 2}, {"compression_method", 3}};
            else
                args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}};
        }
        /// For 5 arguments we support 2 possible variants:
        /// - s3(source, access_key_id, access_key_id, format, structure)
        /// - s3(source, NOSIGN, format, structure, compression_method)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a NOSIGN keyword name or not.
        else if (args.size() == 5)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "NOSIGN/access_key_id");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                no_sign_request = true;
                args_to_idx = {{"format", 2}, {"structure", 3}, {"compression_method", 4}};
            }
            else
                args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}};
        }
        else
        {
            args_to_idx = size_to_args[args.size()];
        }

        /// This argument is always the first
        configuration.url = S3::URI(checkAndGetLiteralArgument<String>(args[0], "url"));

        if (args_to_idx.contains("format"))
        {
            auto format = checkAndGetLiteralArgument<String>(args[args_to_idx["format"]], "format");
            /// Set format to configuration only of it's not 'auto',
            /// because we can have default format set in configuration.
            if (format != "auto")
                configuration.format = format;
        }

        if (args_to_idx.contains("structure"))
            configuration.structure = checkAndGetLiteralArgument<String>(args[args_to_idx["structure"]], "structure");

        if (args_to_idx.contains("compression_method"))
            configuration.compression_method = checkAndGetLiteralArgument<String>(args[args_to_idx["compression_method"]], "compression_method");

        if (args_to_idx.contains("access_key_id"))
            configuration.auth_settings.access_key_id = checkAndGetLiteralArgument<String>(args[args_to_idx["access_key_id"]], "access_key_id");

        if (args_to_idx.contains("secret_access_key"))
            configuration.auth_settings.secret_access_key = checkAndGetLiteralArgument<String>(args[args_to_idx["secret_access_key"]], "secret_access_key");

        configuration.auth_settings.no_sign_request = no_sign_request;
    }

    configuration.keys = {configuration.url.key};

    if (configuration.format == "auto")
        configuration.format = FormatFactory::instance().getFormatFromFileName(configuration.url.uri.getPath(), true);
}

void TableFunctionS3::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Clone ast function, because we can modify its arguments like removing headers.
    auto ast_copy = ast_function->clone();

    /// Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    auto & args = args_func.at(0)->children;

    parseArgumentsImpl(args, context);
}

void TableFunctionS3::addColumnsStructureToArguments(ASTs & args, const String & structure, const ContextPtr & context)
{
    if (tryGetNamedCollectionWithOverrides(args, context))
    {
        /// In case of named collection, just add key-value pair "structure='...'"
        /// at the end of arguments to override existed structure.
        ASTs equal_func_args = {std::make_shared<ASTIdentifier>("structure"), std::make_shared<ASTLiteral>(structure)};
        auto equal_func = makeASTFunction("equals", std::move(equal_func_args));
        args.push_back(equal_func);
    }
    else
    {
        /// If arguments contain headers, just remove it and add to the end of arguments later
        /// (header argument can be at any position).
        HTTPHeaderEntries tmp_headers;
        auto * headers_it = StorageURL::collectHeaders(args, tmp_headers, context);
        ASTPtr headers_ast;
        if (headers_it != args.end())
        {
            headers_ast = *headers_it;
            args.erase(headers_it);
        }

        if (args.empty() || args.size() > getMaxNumberOfArguments())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected 1 to {} arguments in table function, got {}", getMaxNumberOfArguments(), args.size());

        auto structure_literal = std::make_shared<ASTLiteral>(structure);

        /// s3(s3_url)
        if (args.size() == 1)
        {
            /// Add format=auto before structure argument.
            args.push_back(std::make_shared<ASTLiteral>("auto"));
            args.push_back(structure_literal);
        }
        /// s3(s3_url, format) or s3(s3_url, NOSIGN)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN or not.
        else if (args.size() == 2)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            /// If there is NOSIGN, add format=auto before structure.
            if (boost::iequals(second_arg, "NOSIGN"))
                args.push_back(std::make_shared<ASTLiteral>("auto"));
            args.push_back(structure_literal);
        }
        /// s3(source, format, structure) or
        /// s3(source, access_key_id, access_key_id) or
        /// s3(source, NOSIGN, format)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN, format name or neither.
        else if (args.size() == 3)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                args.push_back(structure_literal);
            }
            else if (second_arg == "auto" || FormatFactory::instance().getAllFormats().contains(second_arg))
            {
                args.back() = structure_literal;
            }
            else
            {
                /// Add format=auto before structure argument.
                args.push_back(std::make_shared<ASTLiteral>("auto"));
                args.push_back(structure_literal);
            }
        }
        /// s3(source, format, structure, compression_method) or
        /// s3(source, access_key_id, access_key_id, format) or
        /// s3(source, NOSIGN, format, structure)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN, format name or neither.
        else if (args.size() == 4)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                args.back() = structure_literal;
            }
            else if (second_arg == "auto" || FormatFactory::instance().getAllFormats().contains(second_arg))
            {
                args[args.size() - 2] = structure_literal;
            }
            else
            {
                args.push_back(structure_literal);
            }
        }
        /// s3(source, access_key_id, access_key_id, format, structure) or
        /// s3(source, NOSIGN, format, structure, compression_method)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a NOSIGN keyword name or not.
        else if (args.size() == 5)
        {
            auto sedond_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            if (boost::iequals(sedond_arg, "NOSIGN"))
            {
                args[args.size() - 2] = structure_literal;
            }
            else
            {
                args.back() = structure_literal;
            }
        }
        /// s3(source, access_key_id, access_key_id, format, structure, compression)
        else if (args.size() == 6)
        {
            args[args.size() - 2] = structure_literal;
        }

        if (headers_ast)
            args.push_back(headers_ast);
    }
}

ColumnsDescription TableFunctionS3::getActualTableStructure(ContextPtr context) const
{
    if (configuration.structure == "auto")
    {
        context->checkAccess(getSourceAccessType());
        configuration.update(context);
        return StorageS3::getTableStructureFromData(configuration, std::nullopt, context);
    }

    return parseColumnsListFromString(configuration.structure, context);
}

bool TableFunctionS3::supportsReadingSubsetOfColumns()
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(configuration.format);
}

StoragePtr TableFunctionS3::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    S3::URI s3_uri (configuration.url);

    ColumnsDescription columns;
    if (configuration.structure != "auto")
        columns = parseColumnsListFromString(configuration.structure, context);
    else if (!structure_hint.empty())
        columns = structure_hint;

    StoragePtr storage = std::make_shared<StorageS3>(
        configuration,
        context,
        StorageID(getDatabaseName(), table_name),
        columns,
        ConstraintsDescription{},
        String{},
        /// No format_settings for table function S3
        std::nullopt);

    storage->startup();

    return storage;
}


class TableFunctionGCS : public TableFunctionS3
{
public:
    static constexpr auto name = "gcs";
    std::string getName() const override
    {
        return name;
    }
private:
    const char * getStorageTypeName() const override { return "GCS"; }
};

class TableFunctionCOS : public TableFunctionS3
{
public:
    static constexpr auto name = "cosn";
    std::string getName() const override
    {
        return name;
    }
private:
    const char * getStorageTypeName() const override { return "COSN"; }
};

class TableFunctionOSS : public TableFunctionS3
{
public:
    static constexpr auto name = "oss";
    std::string getName() const override
    {
        return name;
    }
private:
    const char * getStorageTypeName() const override { return "OSS"; }
};


void registerTableFunctionGCS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionGCS>(
        {.documentation
         = {.description=R"(The table function can be used to read the data stored on Google Cloud Storage.)",
            .examples{{"gcs", "SELECT * FROM gcs(url, hmac_key, hmac_secret)", ""}},
            .categories{"DataLake"}},
         .allow_readonly = false});
}

void registerTableFunctionS3(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionS3>(
        {.documentation
         = {.description=R"(The table function can be used to read the data stored on AWS S3.)",
            .examples{{"s3", "SELECT * FROM s3(url, access_key_id, secret_access_key)", ""}},
            .categories{"DataLake"}},
         .allow_readonly = false});
}


void registerTableFunctionCOS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionCOS>();
}

void registerTableFunctionOSS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionOSS>();
}

}

#endif
