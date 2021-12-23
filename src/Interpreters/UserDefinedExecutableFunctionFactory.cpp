#include "UserDefinedExecutableFunctionFactory.h"

#include <IO/WriteHelpers.h>

#include <Processors/Sources/ShellCommandSource.h>
#include <Formats/formatBlock.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Interpreters/ExternalUserDefinedExecutableFunctionsLoader.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int TIMEOUT_EXCEEDED;
}

class UserDefinedFunction final : public IFunction
{
public:

    explicit UserDefinedFunction(
        ExternalUserDefinedExecutableFunctionsLoader::UserDefinedExecutableFunctionPtr executable_function_,
        ContextPtr context_)
        : executable_function(std::move(executable_function_))
        , context(context_)
    {
    }

    String getName() const override { return executable_function->getConfiguration().name; }

    bool isVariadic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return executable_function->getConfiguration().argument_types.size(); }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return true; }
    bool isDeterministic() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        const auto & configuration = executable_function->getConfiguration();
        return configuration.result_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & configuration = executable_function->getConfiguration();
        auto arguments_copy = arguments;

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            auto & column_with_type = arguments_copy[i];
            column_with_type.column = column_with_type.column->convertToFullColumnIfConst();

            const auto & argument_type = configuration.argument_types[i];
            if (areTypesEqual(arguments_copy[i].type, argument_type))
                continue;

            ColumnWithTypeAndName column_to_cast = {column_with_type.column, column_with_type.type, column_with_type.name};
            column_with_type.column = castColumnAccurate(column_to_cast, argument_type);
            column_with_type.type = argument_type;

            column_with_type = column_to_cast;
        }

        std::unique_ptr<ShellCommand> process = getProcess();

        ColumnWithTypeAndName result(result_type, "result");
        Block result_block({result});

        Block arguments_block(arguments_copy);
        auto * process_in = &process->in;

        auto process_pool = executable_function->getProcessPool();
        bool is_executable_pool_function = (process_pool != nullptr);

        ShellCommandSourceConfiguration shell_command_source_configuration;

        if (is_executable_pool_function)
        {
            shell_command_source_configuration.read_fixed_number_of_rows = true;
            shell_command_source_configuration.number_of_rows_to_read = input_rows_count;
        }

        ShellCommandSource::SendDataTask task = {[process_in, arguments_block, &configuration, is_executable_pool_function, this]()
        {
            auto & out = *process_in;

            if (configuration.send_chunk_header)
            {
                writeText(arguments_block.rows(), out);
                writeChar('\n', out);
            }

            auto output_format = context->getOutputFormat(configuration.format, out, arguments_block.cloneEmpty());
            formatBlock(output_format, arguments_block);
            if (!is_executable_pool_function)
                out.close();
        }};
        std::vector<ShellCommandSource::SendDataTask> tasks = {std::move(task)};

        Pipe pipe(std::make_unique<ShellCommandSource>(
            context,
            configuration.format,
            result_block.cloneEmpty(),
            std::move(process),
            std::move(tasks),
            shell_command_source_configuration,
            process_pool));

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
                "Function {} wrong result rows count expected {} actual {}",
                getName(),
                input_rows_count,
                result_column_size);

        return result_column;
    }

private:

    std::unique_ptr<ShellCommand> getProcess() const
    {
        auto process_pool = executable_function->getProcessPool();
        auto executable_function_configuration = executable_function->getConfiguration();

        std::unique_ptr<ShellCommand> process;
        bool is_executable_pool_function = (process_pool != nullptr);
        if (is_executable_pool_function)
        {
            bool result = process_pool->tryBorrowObject(process, [&]()
            {
                ShellCommand::Config process_config(executable_function_configuration.script_path);
                process_config.terminate_in_destructor_strategy = ShellCommand::DestructorStrategy{ true /*terminate_in_destructor*/, executable_function_configuration.command_termination_timeout };
                auto shell_command = ShellCommand::execute(process_config);
                return shell_command;
            }, executable_function_configuration.max_command_execution_time * 1000);

            if (!result)
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                    "Could not get process from pool, max command execution timeout exceeded {} seconds",
                    executable_function_configuration.max_command_execution_time);
        }
        else
        {
            process = ShellCommand::execute(executable_function_configuration.script_path);
        }

        return process;
    }

    ExternalUserDefinedExecutableFunctionsLoader::UserDefinedExecutableFunctionPtr executable_function;
    ContextPtr context;
};

UserDefinedExecutableFunctionFactory & UserDefinedExecutableFunctionFactory::instance()
{
    static UserDefinedExecutableFunctionFactory result;
    return result;
}

FunctionOverloadResolverPtr UserDefinedExecutableFunctionFactory::get(const String & function_name, ContextPtr context)
{
    const auto & loader = context->getExternalUserDefinedExecutableFunctionsLoader();
    auto executable_function = std::static_pointer_cast<const UserDefinedExecutableFunction>(loader.load(function_name));
    auto function = std::make_shared<UserDefinedFunction>(std::move(executable_function), std::move(context));
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
}

FunctionOverloadResolverPtr UserDefinedExecutableFunctionFactory::tryGet(const String & function_name, ContextPtr context)
{
    const auto & loader = context->getExternalUserDefinedExecutableFunctionsLoader();
    auto load_result = loader.getLoadResult(function_name);

    if (load_result.object)
    {
        auto executable_function = std::static_pointer_cast<const UserDefinedExecutableFunction>(load_result.object);
        auto function = std::make_shared<UserDefinedFunction>(std::move(executable_function), std::move(context));
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
