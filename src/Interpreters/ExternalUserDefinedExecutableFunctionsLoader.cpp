#include "ExternalUserDefinedExecutableFunctionsLoader.h"

#include <Interpreters/UserDefinedExecutableFunction.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <DataStreams/ShellCommandSource.h>
#include <DataStreams/formatBlock.h>

#include <DataTypes/DataTypeFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

class UserDefinedFunction final : public IFunction
{
public:
    explicit UserDefinedFunction(
        ContextPtr context_,
        const UserDefinedExecutableFunction::Config & config_,
        std::unique_ptr<ShellCommand> process_)
        : context(context_)
        , config(config_)
        , process(std::move(process_))
    {
        std::cerr << "UserDefinedFunction::UserDefinedFunction " << config.argument_types.size() << " ";
        std::cerr << " config format " << config.format << std::endl;
    }

    String getName() const override { return config.name; }

    bool isVariadic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return config.argument_types.size(); }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return true; }
    bool isDeterministic() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const auto & expected_argument_type = config.argument_types[i];
            if (!areTypesEqual(expected_argument_type, arguments[i]))
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Function {} for {} argument expected {} actual {}",
                    getName(),
                    i,
                    expected_argument_type->getName(),
                    arguments[i]->getName());
        }

        return config.result_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        std::cerr << "UserDefinedFunction::executeImpl " << input_rows_count << " result type " << result_type->getName();
        std::cerr << " arguments " << arguments.size() << std::endl;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const auto & argument = arguments[i];
            std::cerr << "Index " << i << " structure " << argument.dumpStructure() << std::endl;
        }

        Block arguments_block(arguments);

        ColumnWithTypeAndName result(result_type, "result");
        Block result_block({result});

        auto * process_in = &process->in;

        ShellCommandSource::SendDataTask task = {[process_in, arguments_block, this]()
        {
            auto & out = *process_in;
            auto output_stream = context->getOutputStream(config.format, out, arguments_block.cloneEmpty());
            formatBlock(output_stream, arguments_block);
            out.close();
        }};

        std::vector<ShellCommandSource::SendDataTask> tasks = {std::move(task)};

        // auto & buffer = process->out;
        // char buffer_data[4096] {};
        // size_t read_size = buffer.read(buffer_data, sizeof(buffer_data));
        // buffer_data[read_size] = '\0';
        // std::cerr << "Buffer data read size " << read_size << " data " << buffer_data << std::endl;

        Pipe pipe(std::make_unique<ShellCommandSource>(context, config.format, result_block.cloneEmpty(), std::move(process), nullptr, std::move(tasks)));

        QueryPipeline pipeline;
        pipeline.init(std::move(pipe));

        PullingPipelineExecutor executor(pipeline);

        // std::cerr << "Executor pull blocks" << std::endl;
        auto result_column = result_type->createColumn();
        Block block;
        while (executor.pull(block))
        {
            std::cerr << "Executor pull block " << block.rows() << std::endl;
            result_column->insertFrom(*block.safeGetByPosition(0).column, block.rows());
        }

        std::cerr << "Result column size " << result_column->size() << std::endl;
        Field value;
        for (size_t i = 0; i < result_column->size(); ++i)
        {
            result_column->get(i, value);
            std::cerr << "Index " << i << " value " << value.dump() << std::endl;
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
    UserDefinedExecutableFunction::Config config;
    mutable std::unique_ptr<ShellCommand> process;
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
    std::cerr << "ExternalUserDefinedExecutableFunctionsLoader::create name " << name << " key in config " << key_in_config << std::endl;

    String command = config.getString(key_in_config + ".command");
    String format = config.getString(key_in_config + ".format");
    DataTypePtr result_type = DataTypeFactory::instance().get(config.getString(key_in_config + ".return_type"));
    ExternalLoadableLifetime lifetime(config, key_in_config + ".lifetime");

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

    UserDefinedExecutableFunction::Config function_config
    {
        .name = std::move(name),
        .script_path = std::move(command),
        .format = std::move(format),
        .argument_types = std::move(argument_types),
        .result_type = std::move(result_type),
    };

    auto function = std::make_shared<UserDefinedExecutableFunction>(function_config, lifetime);

    FunctionFactory::instance().registerFunction(function_config.name, [function](ContextPtr function_context)
    {
        auto shell_command = ShellCommand::execute(function->getConfig().script_path);
        std::shared_ptr<UserDefinedFunction> user_defined_function = std::make_shared<UserDefinedFunction>(function_context, function->getConfig(), std::move(shell_command));
        return std::make_unique<FunctionToOverloadResolverAdaptor>(user_defined_function);
    });

    return function;
}

}
