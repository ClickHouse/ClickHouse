#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterCreateFunctionQuery.h>

#include <Access/ContextAccess.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionDriverInvoker.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionDriverRegistry.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Parsers/ASTCreateFunctionWithDriverQuery.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTCreateWasmFunctionQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Common/FieldVisitorToString.h>
#include <Common/escapeForFileName.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Access/Common/AccessRightsElement.h>
#include <Functions/UserDefined/ExternalUserDefinedExecutableFunctionsLoader.h>

#include <filesystem>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int BAD_ARGUMENTS;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int UDF_EXECUTION_FAILED;
}

namespace
{
    String formatArgsSignature(const ASTPtr & arguments_ast)
    {
        if (!arguments_ast)
            return {};

        WriteBufferFromOwnString out;
        bool first = true;
        for (const auto & child : arguments_ast->children)
        {
            if (!first)
                out << ", ";
            first = false;

            if (const auto * name_type_pair = child->as<ASTNameTypePair>())
            {
                out << name_type_pair->name << ' ';
                IAST::FormatSettings settings(/*one_line=*/true);
                IAST::FormatState state;
                IAST::FormatStateStacked frame;
                name_type_pair->type->format(out, settings, state, frame);
            }
            else
            {
                IAST::FormatSettings settings(/*one_line=*/true);
                IAST::FormatState state;
                IAST::FormatStateStacked frame;
                child->format(out, settings, state, frame);
            }
        }
        return out.str();
    }

    String formatReturnType(const ASTPtr & return_type_ast)
    {
        if (!return_type_ast)
            return {};
        WriteBufferFromOwnString out;
        IAST::FormatSettings settings(/*one_line=*/true);
        IAST::FormatState state;
        IAST::FormatStateStacked frame;
        return_type_ast->format(out, settings, state, frame);
        return out.str();
    }

    std::vector<std::pair<String, String>> formatEngineArguments(
        const ASTCreateFunctionWithDriverQuery & query,
        const UserDefinedExecutableFunctionDriver & driver)
    {
        std::vector<std::pair<String, String>> result;
        std::unordered_map<String, bool> seen;
        for (const auto & [name, value_ast] : query.engine_arguments)
        {
            auto it = driver.engine_arguments.find(name);
            if (it == driver.engine_arguments.end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Driver '{}' does not declare engine argument '{}'", driver.name, name);

            const auto * literal = value_ast->as<ASTLiteral>();
            if (!literal)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Engine argument '{}' must be a literal", name);

            result.emplace_back(name, applyVisitor(FieldVisitorToString(), literal->value));
            seen[name] = true;
        }
        for (const auto & [name, spec] : driver.engine_arguments)
            if (spec.required && !seen[name])
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Engine argument '{}' is required by driver '{}' but was not provided", name, driver.name);
        return result;
    }

    /// Pick an extension by inspecting the first non-whitespace character of the generated config.
    String pickExtensionForGeneratedConfig(const String & content)
    {
        size_t i = 0;
        while (i < content.size() && (content[i] == ' ' || content[i] == '\t' || content[i] == '\n' || content[i] == '\r'))
            ++i;
        if (i < content.size() && content[i] == '<')
            return ".xml";
        return ".yaml";
    }

    String driverDynamicConfigPath(const ContextPtr & context, const String & function_name, const String & extension)
    {
        String path = context->getDynamicUserDefinedExecutableFunctionsPath();
        if (path.empty())
            throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST,
                "Server setting `dynamic_user_defined_executable_functions_path` is not set");
        if (!path.ends_with('/'))
            path.push_back('/');
        return path + escapeForFileName(function_name) + extension;
    }

    String driverWorkingDirectory(const ContextPtr & context, const String & function_name)
    {
        String path = context->getDynamicUserDefinedExecutableFunctionsPath();
        if (!path.ends_with('/'))
            path.push_back('/');
        return path + escapeForFileName(function_name) + ".d";
    }

    /// Run the driver's create_command and write the generated config to disk atomically.
    /// `is_attach_mode` skips the driver invocation when the dynamic config already exists.
    BlockIO executeCreateFunctionWithDriver(
        const ASTCreateFunctionWithDriverQuery & query, ContextMutablePtr current_context)
    {
        const String function_name = query.getFunctionName();
        const auto driver = UserDefinedExecutableFunctionDriverRegistry::instance().get(query.engine_name);

        const String dynamic_dir = current_context->getDynamicUserDefinedExecutableFunctionsPath();
        if (dynamic_dir.empty())
            throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST,
                "Server setting `dynamic_user_defined_executable_functions_path` is not set");
        std::filesystem::create_directories(dynamic_dir);

        const String working_dir = driverWorkingDirectory(current_context, function_name);

        const String args_signature = formatArgsSignature(query.arguments_ast);
        const String return_type = formatReturnType(query.return_type_ast);
        const auto engine_argument_values = formatEngineArguments(query, *driver);

        const String xml_existing = driverDynamicConfigPath(current_context, function_name, ".xml");
        const String yaml_existing = driverDynamicConfigPath(current_context, function_name, ".yaml");
        const bool config_exists = std::filesystem::exists(xml_existing) || std::filesystem::exists(yaml_existing);

        if (!(query.is_attach && config_exists))
        {
            /// Make sure no stale variants from previous attempts confuse the loader.
            std::error_code ec;
            std::filesystem::remove(xml_existing, ec);
            std::filesystem::remove(yaml_existing, ec);

            std::filesystem::create_directories(working_dir);

            String generated_config;
            try
            {
                generated_config = UserDefinedExecutableFunctionDriverInvoker::runCreateCommand(
                    *driver,
                    function_name,
                    return_type,
                    args_signature,
                    query.source_code,
                    working_dir,
                    engine_argument_values);
            }
            catch (...)
            {
                /// Driver failed - leave no half-baked state behind on disk.
                std::error_code ec_cleanup;
                std::filesystem::remove_all(working_dir, ec_cleanup);
                throw;
            }

            const String extension = pickExtensionForGeneratedConfig(generated_config);
            const String final_path = driverDynamicConfigPath(current_context, function_name, extension);
            const String tmp_path = final_path + ".tmp";

            try
            {
                WriteBufferFromFile out(tmp_path);
                writeString(generated_config, out);
                out.finalize();
                std::filesystem::rename(tmp_path, final_path);
            }
            catch (...)
            {
                std::error_code ec_cleanup;
                std::filesystem::remove(tmp_path, ec_cleanup);
                std::filesystem::remove_all(working_dir, ec_cleanup);
                throw;
            }
        }

        /// Trigger a reload so the new function is picked up by the executable UDF loader.
        try
        {
            auto & loader = current_context->getExternalUserDefinedExecutableFunctionsLoader();
            loader.reloadFunction(function_name);
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// Ignore reload errors here - they will be surfaced when the user tries to call the function.
            tryLogCurrentException("InterpreterCreateFunctionQuery");
        }

        return BlockIO();
    }
}

template <typename T>
std::optional<BlockIO> tryExecute(const ASTPtr & query_ptr, ContextMutablePtr current_context)
{
    if (std::is_same_v<ASTCreateSQLFunctionQuery, T>)
    {
        /// Normalize function names in substituted SQL expression
        FunctionNameNormalizer::visit(query_ptr.get());
    }
    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, current_context);
    auto * create_function_query = updated_query_ptr->as<T>();
    if (!create_function_query)
        return std::nullopt;

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_FUNCTION);

    if (create_function_query->or_replace)
        access_rights_elements.emplace_back(AccessType::DROP_FUNCTION);


    if (!create_function_query->cluster.empty())
    {
        if (current_context->getUserDefinedSQLObjectsStorage().isReplicated())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "ON CLUSTER is not allowed because used-defined functions are replicated automatically");

        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(updated_query_ptr, current_context, params);
    }

    current_context->checkAccess(access_rights_elements);

    auto function_name = create_function_query->getFunctionName();
    bool throw_if_exists = !create_function_query->if_not_exists && !create_function_query->or_replace;
    bool replace_if_exists = create_function_query->or_replace;

    UserDefinedSQLFunctionFactory::instance().registerFunction(current_context, function_name, updated_query_ptr, throw_if_exists, replace_if_exists);

    return BlockIO();
}


static std::optional<BlockIO> tryExecuteWithDriver(const ASTPtr & query_ptr, ContextMutablePtr current_context)
{
    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, current_context);
    auto * create_function_query = updated_query_ptr->as<ASTCreateFunctionWithDriverQuery>();
    if (!create_function_query)
        return std::nullopt;

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_FUNCTION);
    if (create_function_query->or_replace)
        access_rights_elements.emplace_back(AccessType::DROP_FUNCTION);

    if (!create_function_query->cluster.empty())
    {
        if (current_context->getUserDefinedSQLObjectsStorage().isReplicated())
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "ON CLUSTER is not allowed because user-defined functions are replicated automatically");

        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(updated_query_ptr, current_context, params);
    }

    current_context->checkAccess(access_rights_elements);

    /// 1. Invoke driver, write generated config to dynamic_path.
    executeCreateFunctionWithDriver(*create_function_query, current_context);

    /// 2. Persist the SQL AST (in ATTACH form) in user-defined SQL objects storage.
    auto stored_ast_intrusive = make_intrusive<ASTCreateFunctionWithDriverQuery>(*create_function_query);
    /// Replace children that were copied by value with deep clones to keep ownership consistent.
    stored_ast_intrusive->children.clear();
    stored_ast_intrusive->engine_arguments.clear();
    stored_ast_intrusive->function_name_ast = create_function_query->function_name_ast->clone();
    stored_ast_intrusive->children.push_back(stored_ast_intrusive->function_name_ast);
    if (create_function_query->arguments_ast)
    {
        stored_ast_intrusive->arguments_ast = create_function_query->arguments_ast->clone();
        stored_ast_intrusive->children.push_back(stored_ast_intrusive->arguments_ast);
    }
    if (create_function_query->return_type_ast)
    {
        stored_ast_intrusive->return_type_ast = create_function_query->return_type_ast->clone();
        stored_ast_intrusive->children.push_back(stored_ast_intrusive->return_type_ast);
    }
    for (const auto & [name, value] : create_function_query->engine_arguments)
    {
        auto cloned_value = value->clone();
        stored_ast_intrusive->children.push_back(cloned_value);
        stored_ast_intrusive->engine_arguments.emplace_back(name, cloned_value);
    }
    stored_ast_intrusive->is_attach = true;
    stored_ast_intrusive->or_replace = false;
    stored_ast_intrusive->if_not_exists = false;

    bool throw_if_exists = !create_function_query->if_not_exists && !create_function_query->or_replace;
    bool replace_if_exists = create_function_query->or_replace;

    auto & storage = current_context->getUserDefinedSQLObjectsStorage();
    storage.storeObject(
        current_context,
        UserDefinedSQLObjectType::Function,
        create_function_query->getFunctionName(),
        ASTPtr(stored_ast_intrusive),
        throw_if_exists,
        replace_if_exists,
        current_context->getSettingsRef());

    return BlockIO();
}

BlockIO InterpreterCreateFunctionQuery::execute()
{
    if (auto res = tryExecute<ASTCreateSQLFunctionQuery>(query_ptr, getContext()))
        return std::move(res.value());

    if (auto res = tryExecute<ASTCreateWasmFunctionQuery>(query_ptr, getContext()))
        return std::move(res.value());

    if (auto res = tryExecuteWithDriver(query_ptr, getContext()))
        return std::move(res.value());

    throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot execute query, got unexpected AST type: {}", query_ptr->getID());
}

void registerInterpreterCreateFunctionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateFunctionQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateFunctionQuery", create_fn);
}

}
