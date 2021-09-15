#include "ExternalUserDefinedExecutableFunctionsLoader.h"

#include <DataStreams/ShellCommandSource.h>
#include <DataStreams/formatBlock.h>

#include <DataTypes/DataTypeFactory.h>

#include <IO/WriteHelpers.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <Interpreters/UserDefinedExecutableFunction.h>
#include <Interpreters/UserDefinedExecutableFunctionFactory.h>


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
    using GetProcessFunction = std::function<std::unique_ptr<ShellCommand> (void)>;

    explicit UserDefinedFunction(
        ContextPtr context_,
        const UserDefinedExecutableFunctionConfiguration & configuration_,
        GetProcessFunction get_process_function_,
        std::shared_ptr<ProcessPool> process_pool_)
        : context(context_)
        , configuration(configuration_)
        , get_process_function(std::move(get_process_function_))
        , process_pool(std::move(process_pool_))
    {
    }

    String getName() const override { return configuration.name; }

    bool isVariadic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return configuration.argument_types.size(); }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return true; }
    bool isDeterministic() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const auto & expected_argument_type = configuration.argument_types[i];
            if (!areTypesEqual(expected_argument_type, arguments[i]))
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Function {} for {} argument expected {} actual {}",
                    getName(),
                    i,
                    expected_argument_type->getName(),
                    arguments[i]->getName());
        }

        return configuration.result_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        std::unique_ptr<ShellCommand> process = get_process_function();

        ColumnWithTypeAndName result(result_type, "result");
        Block result_block({result});

        Block arguments_block(arguments);
        auto * process_in = &process->in;

        bool is_executable_pool_function = (process_pool != nullptr);

        ShellCommandSourceConfiguration shell_command_source_configuration;

        if (is_executable_pool_function)
        {
            shell_command_source_configuration.read_fixed_number_of_rows = true;
            shell_command_source_configuration.number_of_rows_to_read = input_rows_count;
        }

        ShellCommandSource::SendDataTask task = {[process_in, arguments_block, is_executable_pool_function, this]()
        {
            auto & out = *process_in;

            if (configuration.send_chunk_header)
            {
                writeText(arguments_block.rows(), out);
                writeChar('\n', out);
            }

            auto output_stream = context->getOutputStream(configuration.format, out, arguments_block.cloneEmpty());
            formatBlock(output_stream, arguments_block);
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

        QueryPipeline pipeline;
        pipeline.init(std::move(pipe));

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
    ContextPtr context;
    UserDefinedExecutableFunctionConfiguration configuration;
    GetProcessFunction get_process_function;
    mutable std::shared_ptr<ProcessPool> process_pool;
};

ExternalUserDefinedExecutableFunctionsLoader::ExternalUserDefinedExecutableFunctionsLoader(ContextPtr global_context_)
    : ExternalLoader("external user defined function", &Poco::Logger::get("ExternalUserDefinedExecutableFunctionsLoader"))
    , WithContext(global_context_)
{
    setConfigSettings({"function", "name", "database", "uuid"});
    enableAsyncLoading(false);
    enablePeriodicUpdates(true);
}

ExternalUserDefinedExecutableFunctionsLoader::UserDefinedExecutableFunctionPtr ExternalUserDefinedExecutableFunctionsLoader::getUserDefinedFunction(const std::string & user_defined_function_name) const
{
    return std::static_pointer_cast<const UserDefinedExecutableFunction>(load(user_defined_function_name));
}

ExternalUserDefinedExecutableFunctionsLoader::UserDefinedExecutableFunctionPtr ExternalUserDefinedExecutableFunctionsLoader::tryGetUserDefinedFunction(const std::string & user_defined_function_name) const
{
    return std::static_pointer_cast<const UserDefinedExecutableFunction>(tryLoad(user_defined_function_name));
}

ExternalLoader::LoadablePtr ExternalUserDefinedExecutableFunctionsLoader::create(const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & key_in_config,
    const std::string &) const
{
    String type = config.getString(key_in_config + ".type");
    UserDefinedExecutableFunctionType function_type;

    if (type == "executable")
        function_type = UserDefinedExecutableFunctionType::executable;
    else if (type == "executable_pool")
        function_type = UserDefinedExecutableFunctionType::executable_pool;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Wrong user defined function type expected 'executable' or 'executable_pool' actual {}",
            function_type);

    String command = config.getString(key_in_config + ".command");
    String format = config.getString(key_in_config + ".format");
    DataTypePtr result_type = DataTypeFactory::instance().get(config.getString(key_in_config + ".return_type"));
    bool send_chunk_header = config.getBool(key_in_config + ".send_chunk_header", false);

    size_t pool_size = 0;
    size_t command_termination_timeout = 0;
    size_t max_command_execution_time = 0;
    if (function_type == UserDefinedExecutableFunctionType::executable_pool)
    {
        pool_size = config.getUInt64(key_in_config + ".pool_size", 16);
        command_termination_timeout = config.getUInt64(key_in_config + ".command_termination_timeout", 10);
        max_command_execution_time = config.getUInt64(key_in_config + ".max_command_execution_time", 10);

        size_t max_execution_time_seconds = static_cast<size_t>(getContext()->getSettings().max_execution_time.totalSeconds());
        if (max_execution_time_seconds != 0 && max_command_execution_time > max_execution_time_seconds)
            max_command_execution_time = max_execution_time_seconds;
    }

    ExternalLoadableLifetime lifetime;

    if (config.has(key_in_config + ".lifetime"))
        lifetime = ExternalLoadableLifetime(config, key_in_config + ".lifetime");

    std::vector<DataTypePtr> argument_types;

    Poco::Util::AbstractConfiguration::Keys config_elems;
    config.keys(key_in_config, config_elems);

    for (const auto & config_elem : config_elems)
    {
        if (!startsWith(config_elem, "argument"))
            continue;

        const auto argument_prefix = key_in_config + '.' + config_elem + '.';
        auto argument_type = DataTypeFactory::instance().get(config.getString(argument_prefix + "type"));
        argument_types.emplace_back(std::move(argument_type));
    }

    UserDefinedExecutableFunctionConfiguration function_configuration
    {
        .type = function_type,
        .name = std::move(name),
        .script_path = std::move(command),
        .format = std::move(format),
        .argument_types = std::move(argument_types),
        .result_type = std::move(result_type),
        .pool_size = pool_size,
        .command_termination_timeout = command_termination_timeout,
        .max_command_execution_time = max_command_execution_time,
        .send_chunk_header = send_chunk_header
    };

    std::shared_ptr<scope_guard> function_deregister_ptr = std::make_shared<scope_guard>([function_name = function_configuration.name]()
    {
        UserDefinedExecutableFunctionFactory::instance().unregisterFunction(function_name);
    });

    auto function = std::make_shared<UserDefinedExecutableFunction>(function_configuration, std::move(function_deregister_ptr), lifetime);

    std::shared_ptr<ProcessPool> process_pool;
    if (function_configuration.type == UserDefinedExecutableFunctionType::executable_pool)
        process_pool = std::make_shared<ProcessPool>(function_configuration.pool_size == 0 ? std::numeric_limits<int>::max() : function_configuration.pool_size);

    auto get_process_function = [function, process_pool]()
    {
        const auto & executable_function_config = function->getConfiguration();

        std::unique_ptr<ShellCommand> process;
        bool is_executable_pool_function = (process_pool != nullptr);
        if (is_executable_pool_function)
        {
            bool result = process_pool->tryBorrowObject(process, [&]()
            {
                ShellCommand::Config process_config(function->getConfiguration().script_path);
                process_config.terminate_in_destructor_strategy = ShellCommand::DestructorStrategy{ true /*terminate_in_destructor*/, executable_function_config.command_termination_timeout };
                auto shell_command = ShellCommand::execute(process_config);
                return shell_command;
            }, executable_function_config.max_command_execution_time * 1000);

            if (!result)
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                    "Could not get process from pool, max command execution timeout exceeded {} seconds",
                    executable_function_config.max_command_execution_time);
        }
        else
        {
            process = ShellCommand::execute(executable_function_config.script_path);
        }

        return process;
    };

    UserDefinedExecutableFunctionFactory::instance().registerFunction(function_configuration.name, [get_process_function, function, process_pool](ContextPtr function_context)
    {
        std::shared_ptr<UserDefinedFunction> user_defined_function = std::make_shared<UserDefinedFunction>(function_context, function->getConfiguration(), std::move(get_process_function), process_pool);
        return std::make_unique<FunctionToOverloadResolverAdaptor>(user_defined_function);
    });

    return function;
}

}
