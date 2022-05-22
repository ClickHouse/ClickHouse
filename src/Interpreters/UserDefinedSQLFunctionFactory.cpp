#include "UserDefinedSQLFunctionFactory.h"

#include <Common/quoteString.h>

#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Interpreters/UserDefinedSQLObjectsLoader.h>
#include <Interpreters/UserDefinedExecutableFunctionFactory.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int UNKNOWN_FUNCTION;
    extern const int CANNOT_DROP_FUNCTION;
}

UserDefinedSQLFunctionFactory & UserDefinedSQLFunctionFactory::instance()
{
    static UserDefinedSQLFunctionFactory result;
    return result;
}

void UserDefinedSQLFunctionFactory::registerFunction(ContextPtr context, const String & function_name, ASTPtr create_function_query, bool replace, bool if_not_exists, bool persist)
{
    if (FunctionFactory::instance().hasNameOrAlias(function_name))
    {
        if (if_not_exists)
            return;

        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The function '{}' already exists", function_name);
    }

    if (AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
    {
        if (if_not_exists)
            return;

        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The aggregate function '{}' already exists", function_name);
    }

    if (UserDefinedExecutableFunctionFactory::instance().has(function_name, context))
    {
        if (if_not_exists)
            return;

        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User defined executable function '{}' already exists", function_name);
    }

    std::lock_guard lock(mutex);

    auto [it, inserted] = function_name_to_create_query.emplace(function_name, create_function_query);

    if (!inserted)
    {
        if (if_not_exists)
            return;

        if (replace)
        {
            it->second = create_function_query;
        }
        else
            throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS,
                "The function name '{}' is not unique",
                function_name);
    }

    if (persist)
    {
        try
        {
            UserDefinedSQLObjectsLoader::instance().storeObject(context, UserDefinedSQLObjectType::Function, function_name, *create_function_query, replace);
        }
        catch (Exception & exception)
        {
            function_name_to_create_query.erase(it);
            exception.addMessage(fmt::format("while storing user defined function {} on disk", backQuote(function_name)));
            throw;
        }
    }
}

void UserDefinedSQLFunctionFactory::unregisterFunction(ContextPtr context, const String & function_name, bool if_exists)
{
    if (FunctionFactory::instance().hasNameOrAlias(function_name) ||
        AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop system function '{}'", function_name);

    if (UserDefinedExecutableFunctionFactory::instance().has(function_name, context))
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop user defined executable function '{}'", function_name);

    std::lock_guard lock(mutex);

    auto it = function_name_to_create_query.find(function_name);
    if (it == function_name_to_create_query.end())
    {
        if (if_exists)
            return;

        throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
            "The function name '{}' is not registered",
            function_name);
    }

    try
    {
        UserDefinedSQLObjectsLoader::instance().removeObject(context, UserDefinedSQLObjectType::Function, function_name);
    }
    catch (Exception & exception)
    {
        exception.addMessage(fmt::format("while removing user defined function {} from disk", backQuote(function_name)));
        throw;
    }

    function_name_to_create_query.erase(it);
}

ASTPtr UserDefinedSQLFunctionFactory::get(const String & function_name) const
{
    std::lock_guard lock(mutex);

    auto it = function_name_to_create_query.find(function_name);
    if (it == function_name_to_create_query.end())
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
            "The function name '{}' is not registered",
            function_name);

    return it->second;
}

ASTPtr UserDefinedSQLFunctionFactory::tryGet(const std::string & function_name) const
{
    std::lock_guard lock(mutex);

    auto it = function_name_to_create_query.find(function_name);
    if (it == function_name_to_create_query.end())
        return nullptr;

    return it->second;
}

bool UserDefinedSQLFunctionFactory::has(const String & function_name) const
{
    return tryGet(function_name) != nullptr;
}

std::vector<std::string> UserDefinedSQLFunctionFactory::getAllRegisteredNames() const
{
    std::vector<std::string> registered_names;

    std::lock_guard lock(mutex);
    registered_names.reserve(function_name_to_create_query.size());

    for (const auto & [name, _] : function_name_to_create_query)
        registered_names.emplace_back(name);

    return registered_names;
}

class UserDefinedInterpFunction final : public IFunction
{
public:

    explicit UserDefinedInterpFunction(
        std::shared_ptr<ASTCreateInterpFunctionQuery> query_,
        ContextPtr context_)
        : query(query_)  // !! not optimal?
        , context(context_)
    {
    }

    String getName() const override { return query->getFunctionName(); }

    bool isVariadic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return query->function_args->children.size(); }

    bool useDefaultImplementationForConstants() const override { return false; }  // !! make configurable (new keyword in query?)
    bool useDefaultImplementationForNulls() const override { return true; }  // should be ok (?)
    bool isDeterministic() const override { return false; }  // !! make configurable?
    bool isDeterministicInScopeOfQuery() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        // !! make configurable
        //    Is it possible to detect type dynamically? (execute func with empty input?)
        return std::make_shared<DataTypeUInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & /*arguments*/, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Do not start user defined script during query analysis. Because user script startup could be heavy.
        if (input_rows_count == 0)
            return result_type->createColumn();
        return result_type->createColumnConstWithDefaultValue(input_rows_count);
        // !! keep interpreter running after executing the function?
//
//        auto coordinator = executable_function->getCoordinator();
//        const auto & coordinator_configuration = coordinator->getConfiguration();
//        const auto & configuration = executable_function->getConfiguration();
//
//        String command = configuration.command;
//
//        if (coordinator_configuration.execute_direct)
//        {
//            auto user_scripts_path = context->getUserScriptsPath();
//            auto script_path = user_scripts_path + '/' + command;
//
//            if (!fileOrSymlinkPathStartsWith(script_path, user_scripts_path))
//                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
//                                "Executable file {} must be inside user scripts folder {}",
//                                command,
//                                user_scripts_path);
//
//            if (!FS::exists(script_path))
//                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
//                                "Executable file {} does not exist inside user scripts folder {}",
//                                command,
//                                user_scripts_path);
//
//            if (!FS::canExecute(script_path))
//                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
//                                "Executable file {} is not executable inside user scripts folder {}",
//                                command,
//                                user_scripts_path);
//
//            command = std::move(script_path);
//        }
//
//        size_t argument_size = arguments.size();
//        auto arguments_copy = arguments;
//
//        for (size_t i = 0; i < argument_size; ++i)
//        {
//            auto & column_with_type = arguments_copy[i];
//            column_with_type.column = column_with_type.column->convertToFullColumnIfConst();
//
//            const auto & argument = configuration.arguments[i];
//            column_with_type.name = argument.name;
//
//            const auto & argument_type = argument.type;
//
//            if (areTypesEqual(arguments_copy[i].type, argument_type))
//                continue;
//
//            ColumnWithTypeAndName column_to_cast = {column_with_type.column, column_with_type.type, column_with_type.name};
//            column_with_type.column = castColumnAccurate(column_to_cast, argument_type);
//            column_with_type.type = argument_type;
//
//            column_with_type = std::move(column_to_cast);
//        }
//
//        ColumnWithTypeAndName result(result_type, configuration.result_name);
//        Block result_block({result});
//
//        Block arguments_block(arguments_copy);
//        auto source = std::make_shared<SourceFromSingleChunk>(std::move(arguments_block));
//        auto shell_input_pipe = Pipe(std::move(source));
//
//        ShellCommandSourceConfiguration shell_command_source_configuration;
//
//        if (coordinator_configuration.is_executable_pool)
//        {
//            shell_command_source_configuration.read_fixed_number_of_rows = true;
//            shell_command_source_configuration.number_of_rows_to_read = input_rows_count;
//        }
//
//        Pipes shell_input_pipes;
//        shell_input_pipes.emplace_back(std::move(shell_input_pipe));
//
//        Pipe pipe = coordinator->createPipe(
//            command,
//            configuration.command_arguments,
//            std::move(shell_input_pipes),
//            result_block,
//            context,
//            shell_command_source_configuration);
//
//        QueryPipeline pipeline(std::move(pipe));
//        PullingPipelineExecutor executor(pipeline);
//
//        auto result_column = result_type->createColumn();
//        result_column->reserve(input_rows_count);
//
//        Block block;
//        while (executor.pull(block))
//        {
//            const auto & result_column_to_add = *block.safeGetByPosition(0).column;
//            result_column->insertRangeFrom(result_column_to_add, 0, result_column_to_add.size());
//        }
//
//        size_t result_column_size = result_column->size();
//        if (result_column_size != input_rows_count)
//            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
//                            "Function {}: wrong result, expected {} row(s), actual {}",
//                            quoteString(getName()),
//                            input_rows_count,
//                            result_column_size);
//
//        return result_column;
    }

private:

    std::shared_ptr<ASTCreateInterpFunctionQuery> query;
    ContextPtr context;
};

// getExec?

FunctionOverloadResolverPtr UserDefinedSQLFunctionFactory::tryGetExec(const String & function_name, ContextPtr context) const
{
    std::lock_guard lock(mutex);

    auto it = function_name_to_create_query.find(function_name);
    if (it == function_name_to_create_query.end())
        return nullptr;
    auto query = std::dynamic_pointer_cast<ASTCreateInterpFunctionQuery>(it->second);
    if (!query)
        return nullptr;

    auto function = std::make_shared<UserDefinedInterpFunction>(query, context);
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
}

}
