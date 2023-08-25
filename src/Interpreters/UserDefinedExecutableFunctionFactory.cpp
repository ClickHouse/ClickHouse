#include "UserDefinedExecutableFunctionFactory.h"

#include <filesystem>

#include <Common/filesystemHelpers.h>
#include <Common/FieldVisitorToString.h>
#include <DataTypes/FieldToDataType.h>

#include <Processors/Sources/ShellCommandSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Formats/formatBlock.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/ExternalUserDefinedExecutableFunctionsLoader.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int BAD_ARGUMENTS;
}

class UserDefinedFunction final : public IFunction
{
public:

    explicit UserDefinedFunction(
        ExternalUserDefinedExecutableFunctionsLoader::UserDefinedExecutableFunctionPtr executable_function_,
        ContextPtr context_,
        Array parameters_)
        : executable_function(std::move(executable_function_))
        , context(context_)
    {
        const auto & configuration = executable_function->getConfiguration();
        size_t command_parameters_size = configuration.parameters.size();
        if (command_parameters_size != parameters_.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Executable user defined function {} number of parameters does not match. Expected {}. Actual {}",
                configuration.name,
                command_parameters_size,
                parameters_.size());

        command_with_parameters = configuration.command;
        command_arguments_with_parameters = configuration.command_arguments;

        for (size_t i = 0; i < command_parameters_size; ++i)
        {
            const auto & command_parameter = configuration.parameters[i];
            const auto & parameter_value = parameters_[i];
            auto converted_parameter = convertFieldToTypeOrThrow(parameter_value, *command_parameter.type);
            auto parameter_placeholder = "{" + command_parameter.name + "}";

            auto parameter_value_string = applyVisitor(FieldVisitorToString(), converted_parameter);
            bool find_placedholder = false;

            auto try_replace_parameter_placeholder_with_value = [&](std::string & command_part)
            {
                size_t previous_parameter_placeholder_position = 0;

                while (true)
                {
                    auto parameter_placeholder_position = command_part.find(parameter_placeholder, previous_parameter_placeholder_position);
                    if (parameter_placeholder_position == std::string::npos)
                        break;

                    size_t parameter_placeholder_size = parameter_placeholder.size();
                    command_part.replace(parameter_placeholder_position, parameter_placeholder_size, parameter_value_string);
                    previous_parameter_placeholder_position = parameter_placeholder_position + parameter_value_string.size();
                    find_placedholder = true;
                }

                find_placedholder = true;
            };

            for (auto & command_argument : command_arguments_with_parameters)
                try_replace_parameter_placeholder_with_value(command_argument);

            try_replace_parameter_placeholder_with_value(command_with_parameters);

            if (!find_placedholder)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Executable user defined function {} no placeholder for parameter {}",
                    configuration.name,
                    command_parameter.name);
            }
        }
    }

    String getName() const override { return executable_function->getConfiguration().name; }

    bool isVariadic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return executable_function->getConfiguration().arguments.size(); }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        const auto & configuration = executable_function->getConfiguration();
        return configuration.result_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Do not start user defined script during query analysis. Because user script startup could be heavy.
        if (input_rows_count == 0)
            return result_type->createColumn();

        auto coordinator = executable_function->getCoordinator();
        const auto & coordinator_configuration = coordinator->getConfiguration();
        const auto & configuration = executable_function->getConfiguration();

        String command = command_with_parameters;

        if (coordinator_configuration.execute_direct)
        {
            auto user_scripts_path = context->getUserScriptsPath();
            auto script_path = user_scripts_path + '/' + command;

            if (!fileOrSymlinkPathStartsWith(script_path, user_scripts_path))
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Executable file {} must be inside user scripts folder {}",
                    command,
                    user_scripts_path);

            if (!FS::exists(script_path))
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Executable file {} does not exist inside user scripts folder {}",
                    command,
                    user_scripts_path);

            if (!FS::canExecute(script_path))
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Executable file {} is not executable inside user scripts folder {}",
                    command,
                    user_scripts_path);

            command = std::move(script_path);
        }

        size_t argument_size = arguments.size();
        auto arguments_copy = arguments;

        for (size_t i = 0; i < argument_size; ++i)
        {
            auto & column_with_type = arguments_copy[i];
            column_with_type.column = column_with_type.column->convertToFullColumnIfConst();

            const auto & argument = configuration.arguments[i];
            column_with_type.name = argument.name;

            const auto & argument_type = argument.type;

            if (areTypesEqual(arguments_copy[i].type, argument_type))
                continue;

            ColumnWithTypeAndName column_to_cast = {column_with_type.column, column_with_type.type, column_with_type.name};
            column_with_type.column = castColumnAccurate(column_to_cast, argument_type);
            column_with_type.type = argument_type;

            column_with_type = std::move(column_to_cast);
        }

        ColumnWithTypeAndName result(result_type, configuration.result_name);
        Block result_block({result});

        Block arguments_block(arguments_copy);
        auto source = std::make_shared<SourceFromSingleChunk>(std::move(arguments_block));
        auto shell_input_pipe = Pipe(std::move(source));

        ShellCommandSourceConfiguration shell_command_source_configuration;

        if (coordinator_configuration.is_executable_pool)
        {
            shell_command_source_configuration.read_fixed_number_of_rows = true;
            shell_command_source_configuration.number_of_rows_to_read = input_rows_count;
        }

        Pipes shell_input_pipes;
        shell_input_pipes.emplace_back(std::move(shell_input_pipe));

        Pipe pipe = coordinator->createPipe(
            command,
            command_arguments_with_parameters,
            std::move(shell_input_pipes),
            result_block,
            context,
            shell_command_source_configuration);

        QueryPipeline pipeline(std::move(pipe));
        PullingPipelineExecutor executor(pipeline);

        auto result_column = result_type->createColumn();
        result_column->reserve(input_rows_count);

        Block block;
        while (executor.pull(block))
        {
            const auto & result_column_to_add = *block.safeGetByPosition(0).column;
            result_column->insertRangeFrom(result_column_to_add, 0, result_column_to_add.size());
        }

        size_t result_column_size = result_column->size();
        if (result_column_size != input_rows_count)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Function {}: wrong result, expected {} row(s), actual {}",
                quoteString(getName()),
                input_rows_count,
                result_column_size);

        return result_column;
    }

private:
    ExternalUserDefinedExecutableFunctionsLoader::UserDefinedExecutableFunctionPtr executable_function;
    ContextPtr context;
    String command_with_parameters;
    std::vector<std::string> command_arguments_with_parameters;
};

UserDefinedExecutableFunctionFactory & UserDefinedExecutableFunctionFactory::instance()
{
    static UserDefinedExecutableFunctionFactory result;
    return result;
}

FunctionOverloadResolverPtr UserDefinedExecutableFunctionFactory::get(const String & function_name, ContextPtr context, Array parameters)
{
    const auto & loader = context->getExternalUserDefinedExecutableFunctionsLoader();
    auto executable_function = std::static_pointer_cast<const UserDefinedExecutableFunction>(loader.load(function_name));
    auto function = std::make_shared<UserDefinedFunction>(std::move(executable_function), std::move(context), std::move(parameters));
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
}

FunctionOverloadResolverPtr UserDefinedExecutableFunctionFactory::tryGet(const String & function_name, ContextPtr context, Array parameters)
{
    const auto & loader = context->getExternalUserDefinedExecutableFunctionsLoader();
    auto load_result = loader.getLoadResult(function_name);

    if (load_result.object)
    {
        auto executable_function = std::static_pointer_cast<const UserDefinedExecutableFunction>(load_result.object);
        auto function = std::make_shared<UserDefinedFunction>(std::move(executable_function), std::move(context), std::move(parameters));
        return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
    }

    return nullptr;
}

bool UserDefinedExecutableFunctionFactory::has(const String & function_name, ContextPtr context)
{
    const auto & loader = context->getExternalUserDefinedExecutableFunctionsLoader();
    auto load_result = loader.getLoadResult(function_name);

    bool result = load_result.object != nullptr;
    return result;
}

std::vector<String> UserDefinedExecutableFunctionFactory::getRegisteredNames(ContextPtr context)
{
    const auto & loader = context->getExternalUserDefinedExecutableFunctionsLoader();
    auto loaded_objects = loader.getLoadedObjects();

    std::vector<std::string> registered_names;
    registered_names.reserve(loaded_objects.size());

    for (auto & loaded_object : loaded_objects)
        registered_names.emplace_back(loaded_object->getLoadableName());

    return registered_names;
}

}
