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
#include <Storages/VirtualColumnUtils.h>
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

/// This is needed to avoid copy-paste. Because s3Cluster arguments only differ in additional argument (first) - cluster name
void TableFunctionS3::parseArgumentsImpl(ASTs & args, const ContextPtr & context)
{
    if (auto named_collection = tryGetNamedCollectionWithOverrides(args, context))
    {
        StorageS3::processNamedCollectionResult(configuration, *named_collection);
        if (configuration.format == "auto")
        {
            String file_path = named_collection->getOrDefault<String>("filename", Poco::URI(named_collection->get<String>("url")).getPath());
            configuration.format = FormatFactory::instance().tryGetFormatFromFileName(file_path).value_or("auto");
        }
    }
    else
    {
        size_t count = StorageURL::evalArgsAndCollectHeaders(args, configuration.headers_from_ast, context);

        if (count == 0 || count > 7)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "The signature of table function {} shall be the following:\n{}", getName(), getSignature());

        std::unordered_map<std::string_view, size_t> args_to_idx;

        bool no_sign_request = false;

        /// For 2 arguments we support 2 possible variants:
        /// - s3(source, format)
        /// - s3(source, NOSIGN)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN or not.
        if (count == 2)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
                no_sign_request = true;
            else
                args_to_idx = {{"format", 1}};
        }
        /// For 3 arguments we support 3 possible variants:
        /// - s3(source, format, structure)
        /// - s3(source, access_key_id, secret_access_key)
        /// - s3(source, NOSIGN, format)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a format name or not.
        else if (count == 3)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/access_key_id/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                no_sign_request = true;
                args_to_idx = {{"format", 2}};
            }
            else if (second_arg == "auto" || FormatFactory::instance().exists(second_arg))
                args_to_idx = {{"format", 1}, {"structure", 2}};
            else
                args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}};
        }
        /// For 4 arguments we support 4 possible variants:
        /// - s3(source, format, structure, compression_method),
        /// - s3(source, access_key_id, secret_access_key, format),
        /// - s3(source, access_key_id, secret_access_key, session_token)
        /// - s3(source, NOSIGN, format, structure)
        /// We can distinguish them by looking at the 2-nd and 4-th argument: check if it's a format name or not.
        else if (count == 4)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/access_key_id/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                no_sign_request = true;
                args_to_idx = {{"format", 2}, {"structure", 3}};
            }
            else if (second_arg == "auto" || FormatFactory::instance().exists(second_arg))
            {
                args_to_idx = {{"format", 1}, {"structure", 2}, {"compression_method", 3}};
            }
            else
            {
                auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/session_token");
                if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
                {
                    args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}};
                }
                else
                {
                    args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}};
                }
            }
        }
        /// For 5 arguments we support 3 possible variants:
        /// - s3(source, access_key_id, secret_access_key, format, structure)
        /// - s3(source, access_key_id, secret_access_key, session_token, format)
        /// - s3(source, NOSIGN, format, structure, compression_method)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a NOSIGN keyword name or no,
        /// and by the 4-th argument, check if it's a format name or not
        else if (count == 5)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "NOSIGN/access_key_id");
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                no_sign_request = true;
                args_to_idx = {{"format", 2}, {"structure", 3}, {"compression_method", 4}};
            }
            else
            {
                auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/session_token");
                if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
                {
                    args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}};
                }
                else
                {
                    args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}};
                }
            }
        }
        // For 6 arguments we support 2 possible variants:
        /// - s3(source, access_key_id, secret_access_key, format, structure, compression_method)
        /// - s3(source, access_key_id, secret_access_key, session_token, format, structure)
        /// We can distinguish them by looking at the 4-th argument: check if it's a format name or not
        else if (count == 6)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/session_token");
            if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
            {
                args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}, {"compression_method", 5}};
            }
            else
            {
                args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}};
            }
        }
        else if (count == 7)
        {
            args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}, {"compression_method", 6}};
        }

        /// This argument is always the first
        String url = checkAndGetLiteralArgument<String>(args[0], "url");
        configuration.url = S3::URI(url);

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

        if (args_to_idx.contains("session_token"))
            configuration.auth_settings.session_token = checkAndGetLiteralArgument<String>(args[args_to_idx["session_token"]], "session_token");

        configuration.auth_settings.no_sign_request = no_sign_request;

        if (configuration.format == "auto")
            configuration.format = FormatFactory::instance().tryGetFormatFromFileName(Poco::URI(url).getPath()).value_or("auto");
    }

    configuration.keys = {configuration.url.key};
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

void TableFunctionS3::updateStructureAndFormatArgumentsIfNeeded(ASTs & args, const String & structure, const String & format, const ContextPtr & context)
{
    if (auto collection = tryGetNamedCollectionWithOverrides(args, context))
    {
        /// In case of named collection, just add key-value pairs "format='...', structure='...'"
        /// at the end of arguments to override existed format and structure with "auto" values.
        if (collection->getOrDefault<String>("format", "auto") == "auto")
        {
            ASTs format_equal_func_args = {std::make_shared<ASTIdentifier>("format"), std::make_shared<ASTLiteral>(format)};
            auto format_equal_func = makeASTFunction("equals", std::move(format_equal_func_args));
            args.push_back(format_equal_func);
        }
        if (collection->getOrDefault<String>("structure", "auto") == "auto")
        {
            ASTs structure_equal_func_args = {std::make_shared<ASTIdentifier>("structure"), std::make_shared<ASTLiteral>(structure)};
            auto structure_equal_func = makeASTFunction("equals", std::move(structure_equal_func_args));
            args.push_back(structure_equal_func);
        }
    }
    else
    {
        HTTPHeaderEntries tmp_headers;
        size_t count = StorageURL::evalArgsAndCollectHeaders(args, tmp_headers, context);

        if (count == 0 || count > getMaxNumberOfArguments())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected 1 to {} arguments in table function, got {}", getMaxNumberOfArguments(), count);

        auto format_literal = std::make_shared<ASTLiteral>(format);
        auto structure_literal = std::make_shared<ASTLiteral>(structure);

        /// s3(s3_url) -> s3(s3_url, format, structure)
        if (count == 1)
        {
            args.push_back(format_literal);
            args.push_back(structure_literal);
        }
        /// s3(s3_url, format) -> s3(s3_url, format, structure) or
        /// s3(s3_url, NOSIGN) -> s3(s3_url, NOSIGN, format, structure)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN or not.
        else if (count == 2)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
                args.push_back(format_literal);
            else if (second_arg == "auto")
                args.back() = format_literal;
            args.push_back(structure_literal);
        }
        /// s3(source, format, structure) or
        /// s3(source, access_key_id, secret_access_key) or
        /// s3(source, NOSIGN, format)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN, format name or neither.
        else if (count == 3)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            /// s3(source, NOSIGN, format) -> s3(source, NOSIGN, format, structure)
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                if (checkAndGetLiteralArgument<String>(args[2], "format") == "auto")
                    args.back() = format_literal;
                args.push_back(structure_literal);
            }
            /// s3(source, format, structure)
            else if (second_arg == "auto" || FormatFactory::instance().exists(second_arg))
            {
                if (second_arg == "auto")
                    args[1] = format_literal;
                if (checkAndGetLiteralArgument<String>(args[2], "structure") == "auto")
                    args[2] = structure_literal;
            }
            /// s3(source, access_key_id, access_key_id) -> s3(source, access_key_id, access_key_id, format, structure)
            else
            {
                args.push_back(format_literal);
                args.push_back(structure_literal);
            }
        }
        /// s3(source, format, structure, compression_method) or
        /// s3(source, access_key_id, secret_access_key, format) or
        /// s3(source, NOSIGN, format, structure)
        /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN, format name or neither.
        else if (count == 4)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            /// s3(source, NOSIGN, format, structure)
            if (boost::iequals(second_arg, "NOSIGN"))
            {
                if (checkAndGetLiteralArgument<String>(args[2], "format") == "auto")
                    args[2] = format_literal;
                if (checkAndGetLiteralArgument<String>(args[3], "structure") == "auto")
                    args[3] = structure_literal;
            }
            /// s3(source, format, structure, compression_method)
            else if (second_arg == "auto" || FormatFactory::instance().exists(second_arg))
            {
                if (second_arg == "auto")
                    args[1] = format_literal;
                if (checkAndGetLiteralArgument<String>(args[2], "structure") == "auto")
                    args[2] = structure_literal;
            }
            /// s3(source, access_key_id, access_key_id, format) -> s3(source, access_key_id, access_key_id, format, structure)
            else
            {
                if (checkAndGetLiteralArgument<String>(args[3], "format") == "auto")
                    args[3] = format_literal;
                args.push_back(structure_literal);
            }
        }
        /// s3(source, access_key_id, secret_access_key, format, structure) or
        /// s3(source, NOSIGN, format, structure, compression_method)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a NOSIGN keyword name or not.
        else if (count == 5)
        {
            auto sedond_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
            /// s3(source, NOSIGN, format, structure, compression_method)
            if (boost::iequals(sedond_arg, "NOSIGN"))
            {
                if (checkAndGetLiteralArgument<String>(args[2], "format") == "auto")
                    args[2] = format_literal;
                if (checkAndGetLiteralArgument<String>(args[3], "structure") == "auto")
                    args[3] = structure_literal;
            }
            /// s3(source, access_key_id, access_key_id, format, structure)
            else
            {
                if (checkAndGetLiteralArgument<String>(args[3], "format") == "auto")
                    args[3] = format_literal;
                if (checkAndGetLiteralArgument<String>(args[4], "structure") == "auto")
                    args[4] = structure_literal;
            }
        }
        /// s3(source, access_key_id, secret_access_key, format, structure, compression)
        else if (count == 6)
        {
            if (checkAndGetLiteralArgument<String>(args[3], "format") == "auto")
                args[3] = format_literal;
            if (checkAndGetLiteralArgument<String>(args[4], "structure") == "auto")
                args[4] = structure_literal;
        }
    }
}

ColumnsDescription TableFunctionS3::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (configuration.structure == "auto")
    {
        context->checkAccess(getSourceAccessType());
        configuration.update(context);
        if (configuration.format == "auto")
            return StorageS3::getTableStructureAndFormatFromData(configuration, std::nullopt, context).first;

        return StorageS3::getTableStructureFromData(configuration, std::nullopt, context);
    }

    return parseColumnsListFromString(configuration.structure, context);
}

bool TableFunctionS3::supportsReadingSubsetOfColumns(const ContextPtr & context)
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(configuration.format, context);
}

std::unordered_set<String> TableFunctionS3::getVirtualsToCheckBeforeUsingStructureHint() const
{
    return VirtualColumnUtils::getVirtualNamesForFileLikeStorage();
}

StoragePtr TableFunctionS3::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool /*is_insert_query*/) const
{
    S3::URI s3_uri (configuration.url);

    ColumnsDescription columns;
    if (configuration.structure != "auto")
        columns = parseColumnsListFromString(configuration.structure, context);
    else if (!structure_hint.empty())
        columns = structure_hint;
    else if (!cached_columns.empty())
        columns = cached_columns;

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
