#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterCreateFunctionQuery.h>

#include <Access/ContextAccess.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionDriverInvoker.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionDriverRegistry.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionDriverUtils.h>
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
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Access/Common/AccessRightsElement.h>
#include <Functions/UserDefined/ExternalUserDefinedExecutableFunctionsLoader.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>

#include <Poco/DOM/DOMParser.h>
#include <Poco/Util/XMLConfiguration.h>

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
    namespace DriverUtils = UserDefinedExecutableFunctionDriverUtils;

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

            result.emplace_back(name, DriverUtils::engineArgumentToString(*literal));
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

    /// Verify that the configuration produced by the driver defines exactly the function being created.
    /// The executable UDF loader (`ExternalUserDefinedExecutableFunctionsLoader`) indexes objects by the
    /// `<function><name>` inside the generated XML/YAML (using `uuid`/`database` when present), not by the file
    /// name. A buggy driver invoked for `CREATE FUNCTION foo ...` could emit a valid configuration for `bar`:
    /// the loader would then register `bar`, while `reloadFunction("foo")` reports `foo` as missing, leaving the
    /// persisted SQL metadata for `foo` pointing at artifacts that define a different function. Reject this here
    /// before the configuration is published into the dynamic directory.
    void validateGeneratedConfig(const String & config_path, const String & driver_name, const String & function_name)
    {
        Poco::XML::DOMParser dom_parser;
        XMLDocumentPtr document = ConfigProcessor::parseConfig(config_path, dom_parser);
        Poco::AutoPtr<Poco::Util::XMLConfiguration> configuration(new Poco::Util::XMLConfiguration(document));

        Poco::Util::AbstractConfiguration::Keys keys;
        configuration->keys(keys);

        size_t functions_defined = 0;
        for (const auto & key : keys)
        {
            if (!startsWith(key, "function"))
                continue;

            ++functions_defined;

            /// Mirror `ExternalLoader`: the effective object name is the `uuid` if present, otherwise `[database.]name`.
            String object_name = configuration->getString(key + ".uuid", "");
            if (object_name.empty())
            {
                object_name = configuration->getString(key + ".name", "");
                const String database = configuration->getString(key + ".database", "");
                if (!database.empty())
                    object_name = database + "." + object_name;
            }

            if (object_name != function_name)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Driver '{}' generated a configuration for function '{}', but the function being created is '{}'. "
                    "A driver must produce a configuration for exactly the function it is invoked for",
                    driver_name, object_name, function_name);
        }

        if (functions_defined == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Driver '{}' generated a configuration that does not define any function (expected '{}')",
                driver_name, function_name);
    }

    String createDriverWorkingDirectory(const ContextPtr & context)
    {
        /// The RFC requires a random UUID directory for the driver-owned state.
        for (size_t attempt = 0; attempt != 100; ++attempt)
        {
            const String directory_name = toString(UUIDHelpers::generateV4());
            const String path = DriverUtils::driverWorkingDirectory(context, directory_name);
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

        /// An executable function with this name may also exist outside the SQL-object storage:
        /// defined statically in the server configuration, or left from a driver-created function
        /// whose SQL metadata was lost. `IF NOT EXISTS` must be a no-op in that case, and
        /// `OR REPLACE` cannot replace a function defined in the server configuration.
        if (UserDefinedExecutableFunctionFactory::instance().has(function_name, current_context)) /// NOLINT(readability-static-accessed-through-instance)
        {
            if (throw_if_exists)
                throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User defined executable function '{}' already exists", function_name);
            if (!replace_if_exists)
                return false;

            const bool has_dynamic_config = std::filesystem::exists(DriverUtils::driverDynamicConfigPath(current_context, function_name, ".xml"))
                || std::filesystem::exists(DriverUtils::driverDynamicConfigPath(current_context, function_name, ".yaml"));
            if (!has_dynamic_config)
                throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS,
                    "User defined executable function '{}' is defined in the server configuration and cannot be replaced", function_name);
        }

        if (UserDefinedWebAssemblyFunctionFactory::instance().has(function_name)) /// NOLINT(readability-static-accessed-through-instance)
        {
            if (throw_if_exists)
                throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User defined wasm function '{}' already exists", function_name);
            if (!replace_if_exists)
                return false;
        }

        /// This keeps validation of engine arguments and driver availability before any filesystem side effects.
        const auto driver = UserDefinedExecutableFunctionDriverRegistry::instance().get(query.engine_name);
        formatEngineArguments(query, *driver);

        return true;
    }

    void removeDriverArtifacts(const ContextPtr & context, const String & function_name)
    {
        std::error_code ec;
        const String working_dir = DriverUtils::readDriverWorkingDirectory(context, function_name);
        std::filesystem::remove(DriverUtils::driverDynamicConfigPath(context, function_name, ".xml"), ec);
        std::filesystem::remove(DriverUtils::driverDynamicConfigPath(context, function_name, ".yaml"), ec);
        std::filesystem::remove(DriverUtils::driverWorkingDirectoryMetadataPath(context, function_name), ec);
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

        const String args_signature = DriverUtils::formatArgsSignature(query.arguments_ast);
        const String return_type = DriverUtils::formatReturnType(query.return_type_ast);
        const auto engine_argument_values = formatEngineArguments(query, *driver);

        const String xml_existing = DriverUtils::driverDynamicConfigPath(current_context, function_name, ".xml");
        const String yaml_existing = DriverUtils::driverDynamicConfigPath(current_context, function_name, ".yaml");
        const bool config_exists = std::filesystem::exists(xml_existing) || std::filesystem::exists(yaml_existing);

        DriverCreateResult result;
        if (!(query.is_attach && config_exists))
        {
            /// The previous working directory is only read so it can be cleaned up after a successful
            /// replace below. Reading it must be best-effort: a missing or invalid (corrupted) `.workdir`
            /// sidecar must not abort the create. This is exactly the corrupted-state case that startup
            /// recovery (`reloadDriverBasedFunctions`) relies on this path to repair - the fresh
            /// config/workdir/sidecar written below overwrites the broken state, so an unreadable previous
            /// sidecar is treated as "no previous working directory to clean up".
            try
            {
                result.previous_working_dir = DriverUtils::readDriverWorkingDirectory(current_context, function_name);
            }
            catch (...)
            {
                tryLogCurrentException(
                    "InterpreterCreateFunctionQuery",
                    "Cannot read the previous working directory of driver-based function " + backQuote(function_name)
                        + "; it will be recreated");
            }
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
            const String final_path = DriverUtils::driverDynamicConfigPath(current_context, function_name, extension);
            const String tmp_path = final_path + ".tmp";
            const String working_dir_metadata_path = DriverUtils::driverWorkingDirectoryMetadataPath(current_context, function_name);
            const String working_dir_metadata_tmp_path = working_dir_metadata_path + ".tmp";

            try
            {
                WriteBufferFromFile out(tmp_path);
                writeString(generated_config, out);
                out.finalize();

                /// Reject a configuration that does not define exactly `function_name` before publishing it,
                /// while the temporary file is not yet visible to the executable UDF loader.
                validateGeneratedConfig(tmp_path, driver->name, function_name);

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

    /// Read the experimental gate from the live configuration rather than from the startup-time `ServerSettings`
    /// returned by `getServerSettings` (which `SYSTEM RELOAD CONFIG` does not refresh), so that toggling
    /// `allow_experimental_executable_udf_drivers` takes effect on reload, consistently with
    /// `Context::loadUserDefinedExecutableFunctionDrivers` loading the driver registry.
    ServerSettings reloaded_server_settings;
    reloaded_server_settings.loadSettingsFromConfig(current_context->getConfigRef());
    if (!reloaded_server_settings[ServerSetting::allow_experimental_executable_udf_drivers])
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
