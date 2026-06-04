#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterCreateFunctionQuery.h>

#include <Access/ContextAccess.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionDriverInvoker.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionDriverRegistry.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedWebAssembly.h>
#include <Core/UUID.h>
#include <Core/ServerSettings.h>
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
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
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
    extern const int CANNOT_CREATE_DIRECTORY;
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int SUPPORT_IS_DISABLED;
}

namespace ServerSetting
{
    extern const ServerSettingsBool allow_experimental_executable_udf_drivers;
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

    String driverWorkingDirectoryMetadataPath(const ContextPtr & context, const String & function_name)
    {
        return driverDynamicConfigPath(context, function_name, ".workdir");
    }

    String driverWorkingDirectory(const ContextPtr & context, const String & directory_name)
    {
        String path = context->getDynamicUserDefinedExecutableFunctionsPath();
        if (!path.ends_with('/'))
            path.push_back('/');
        return path + directory_name;
    }

    void validateDriverWorkingDirectoryName(const String & directory_name, const String & function_name)
    {
        UUID uuid;
        if (!tryParseUUID({reinterpret_cast<const UInt8 *>(directory_name.data()), directory_name.size()}, uuid))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Invalid executable UDF driver working directory name '{}' for function '{}'",
                directory_name, function_name);
    }

    String readDriverWorkingDirectory(const ContextPtr & context, const String & function_name)
    {
        const String metadata_path = driverWorkingDirectoryMetadataPath(context, function_name);
        if (!std::filesystem::exists(metadata_path))
            return {};

        String directory_name;
        ReadBufferFromFile in(metadata_path);
        readStringUntilEOF(directory_name, in);
        if (directory_name.empty())
            return {};

        validateDriverWorkingDirectoryName(directory_name, function_name);
        return driverWorkingDirectory(context, directory_name);
    }

    String createDriverWorkingDirectory(const ContextPtr & context)
    {
        /// The RFC requires a random UUID directory for the driver-owned state.
        for (size_t attempt = 0; attempt != 100; ++attempt)
        {
            const String directory_name = toString(UUIDHelpers::generateV4());
            const String path = driverWorkingDirectory(context, directory_name);
            if (std::filesystem::create_directory(path))
                return path;
        }

        throw Exception(ErrorCodes::CANNOT_CREATE_DIRECTORY, "Cannot create a unique executable UDF driver working directory");
    }

    bool shouldRunCreateFunctionWithDriver(
        const ASTCreateFunctionWithDriverQuery & query,
        const ContextMutablePtr & current_context,
        const String & function_name,
        bool throw_if_exists,
        bool replace_if_exists)
    {
        if (FunctionFactory::instance().hasNameOrAlias(function_name))
            throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The function '{}' already exists", function_name);

        if (AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
            throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The aggregate function '{}' already exists", function_name);

        auto & storage = current_context->getUserDefinedSQLObjectsStorage();
        if (storage.has(function_name))
        {
            if (throw_if_exists)
                throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User-defined object '{}' already exists", function_name);
            return replace_if_exists;
        }

        if (throw_if_exists && UserDefinedExecutableFunctionFactory::instance().has(function_name, current_context)) /// NOLINT(readability-static-accessed-through-instance)
            throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User defined executable function '{}' already exists", function_name);

        if (throw_if_exists && UserDefinedWebAssemblyFunctionFactory::instance().has(function_name)) /// NOLINT(readability-static-accessed-through-instance)
            throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User defined wasm function '{}' already exists", function_name);

        /// This keeps validation of engine arguments and driver availability before any filesystem side effects.
        const auto driver = UserDefinedExecutableFunctionDriverRegistry::instance().get(query.engine_name);
        formatEngineArguments(query, *driver);

        return true;
    }

    void removeDriverArtifacts(const ContextPtr & context, const String & function_name)
    {
        std::error_code ec;
        const String working_dir = readDriverWorkingDirectory(context, function_name);
        std::filesystem::remove(driverDynamicConfigPath(context, function_name, ".xml"), ec);
        std::filesystem::remove(driverDynamicConfigPath(context, function_name, ".yaml"), ec);
        std::filesystem::remove(driverWorkingDirectoryMetadataPath(context, function_name), ec);
        if (!working_dir.empty())
            std::filesystem::remove_all(working_dir, ec);
    }

    struct DriverCreateResult
    {
        String working_dir;
        String previous_working_dir;
    };

    /// Run the driver's create_command and write the generated config to disk atomically.
    /// `is_attach_mode` skips the driver invocation when the dynamic config already exists.
    DriverCreateResult executeCreateFunctionWithDriver(
        const ASTCreateFunctionWithDriverQuery & query, ContextMutablePtr current_context)
    {
        const String function_name = query.getFunctionName();
        const auto driver = UserDefinedExecutableFunctionDriverRegistry::instance().get(query.engine_name);

        const String dynamic_dir = current_context->getDynamicUserDefinedExecutableFunctionsPath();
        if (dynamic_dir.empty())
            throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST,
                "Server setting `dynamic_user_defined_executable_functions_path` is not set");
        std::filesystem::create_directories(dynamic_dir);

        const String args_signature = formatArgsSignature(query.arguments_ast);
        const String return_type = formatReturnType(query.return_type_ast);
        const auto engine_argument_values = formatEngineArguments(query, *driver);

        const String xml_existing = driverDynamicConfigPath(current_context, function_name, ".xml");
        const String yaml_existing = driverDynamicConfigPath(current_context, function_name, ".yaml");
        const bool config_exists = std::filesystem::exists(xml_existing) || std::filesystem::exists(yaml_existing);

        DriverCreateResult result;
        if (!(query.is_attach && config_exists))
        {
            result.previous_working_dir = readDriverWorkingDirectory(current_context, function_name);
            result.working_dir = createDriverWorkingDirectory(current_context);

            String generated_config;
            try
            {
                generated_config = UserDefinedExecutableFunctionDriverInvoker::runCreateCommand(
                    *driver,
                    function_name,
                    return_type,
                    args_signature,
                    query.source_code,
                    result.working_dir,
                    engine_argument_values);
            }
            catch (...)
            {
                /// Driver failed - leave no half-baked state behind on disk.
                std::error_code ec_cleanup;
                std::filesystem::remove_all(result.working_dir, ec_cleanup);
                throw;
            }

            const String extension = pickExtensionForGeneratedConfig(generated_config);
            const String final_path = driverDynamicConfigPath(current_context, function_name, extension);
            const String tmp_path = final_path + ".tmp";
            const String working_dir_metadata_path = driverWorkingDirectoryMetadataPath(current_context, function_name);
            const String working_dir_metadata_tmp_path = working_dir_metadata_path + ".tmp";

            try
            {
                WriteBufferFromFile out(tmp_path);
                writeString(generated_config, out);
                out.finalize();

                WriteBufferFromFile working_dir_out(working_dir_metadata_tmp_path);
                writeString(std::filesystem::path(result.working_dir).filename().string(), working_dir_out);
                working_dir_out.finalize();

                std::filesystem::rename(tmp_path, final_path);
                std::filesystem::rename(working_dir_metadata_tmp_path, working_dir_metadata_path);

                std::error_code ec_cleanup;
                if (extension == ".xml")
                    std::filesystem::remove(yaml_existing, ec_cleanup);
                else
                    std::filesystem::remove(xml_existing, ec_cleanup);
            }
            catch (...)
            {
                std::error_code ec_cleanup;
                std::filesystem::remove(tmp_path, ec_cleanup);
                std::filesystem::remove(working_dir_metadata_tmp_path, ec_cleanup);
                if (!config_exists)
                    std::filesystem::remove(final_path, ec_cleanup);
                std::filesystem::remove_all(result.working_dir, ec_cleanup);
                throw;
            }
        }

        return result;
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

    if (!current_context->getServerSettings()[ServerSetting::allow_experimental_executable_udf_drivers])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Drivers for executable user-defined functions are an experimental feature. "
            "Enable the server setting `allow_experimental_executable_udf_drivers` to use them");

    /// Driver-based functions are materialized (driver invocation + dynamic config/working directory) only on the
    /// replica that receives the query; the replicated SQL-object storage propagates just the `ATTACH FUNCTION`
    /// statement, so other replicas (and `RESTORE`) would not have the runnable artifacts. Reject this combination
    /// until the artifact lifecycle is replicated too.
    if (current_context->getUserDefinedSQLObjectsStorage().isReplicated())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Drivers for executable user-defined functions are not supported with replicated user-defined function storage");

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

    bool throw_if_exists = !create_function_query->if_not_exists && !create_function_query->or_replace;
    bool replace_if_exists = create_function_query->or_replace;
    const String function_name = create_function_query->getFunctionName();

    if (!shouldRunCreateFunctionWithDriver(
            *create_function_query,
            current_context,
            function_name,
            throw_if_exists,
            replace_if_exists))
        return BlockIO();

    /// 1. Invoke driver, write generated config to dynamic_path.
    DriverCreateResult create_result = executeCreateFunctionWithDriver(*create_function_query, current_context);

    /// 2. Persist the SQL AST (in ATTACH form) in user-defined SQL objects storage.
    ASTPtr stored_ast = normalizeCreateFunctionQuery(*create_function_query, current_context);

    auto & storage = current_context->getUserDefinedSQLObjectsStorage();
    bool stored = false;
    try
    {
        stored = storage.storeObject(
            current_context,
            UserDefinedSQLObjectType::Function,
            function_name,
            stored_ast,
            throw_if_exists,
            replace_if_exists,
            current_context->getSettingsRef());
    }
    catch (...)
    {
        removeDriverArtifacts(current_context, function_name);
        throw;
    }

    if (!stored)
    {
        removeDriverArtifacts(current_context, function_name);
        return BlockIO();
    }

    if (!create_result.previous_working_dir.empty() && create_result.previous_working_dir != create_result.working_dir)
    {
        std::error_code ec_cleanup;
        std::filesystem::remove_all(create_result.previous_working_dir, ec_cleanup);
    }

    if (replace_if_exists)
        UserDefinedWebAssemblyFunctionFactory::instance().dropIfExists(function_name);

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

void registerInterpreterCreateFunctionQuery(InterpreterFactory & factory);
void registerInterpreterCreateFunctionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateFunctionQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateFunctionQuery", create_fn);
}

}
