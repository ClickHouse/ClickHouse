#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class FunctionZookeeperFourLetterWordCommand : public IFunction
{
public:
    static constexpr auto name = "zookeeperFourLetterWordCommand";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionZookeeperFourLetterWordCommand>(); }

    String getName() const override { return name; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} requires exactly 2 arguments (keeper_host:port, command), got {}",
                            String{name}, arguments.size());

        if (!isString(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "The first argument of function {} should be a string with keeper host:port",
                            String{name});

        if (!isString(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "The second argument of function {} should be a string with the command",
                            String{name});

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * host_port_column = checkAndGetColumnConstStringOrFixedString(arguments[0].column.get());
        if (!host_port_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "The first argument of function {} should be a constant string with keeper host:port",
                            String{name});

        const auto * command_column = checkAndGetColumnConstStringOrFixedString(arguments[1].column.get());
        if (!command_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "The second argument of function {} should be a constant string with the command",
                            String{name});

        String host_port = String{host_port_column->getDataAt(0).toView()};
        String command = String{command_column->getDataAt(0).toView()};

        String result = executeFourLetterCommand(host_port, command);

        auto result_column = ColumnString::create();
        for (size_t i = 0; i < input_rows_count; ++i)
            result_column->insert(result);

        return result_column;
    }

private:
    String executeFourLetterCommand(const String & host_port, const String & command) const
    {
        try
        {
            // Parse host:port
            auto colon_pos = host_port.find(':');
            if (colon_pos == String::npos)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Invalid host:port format '{}', expected format: 'host:port'",
                                host_port);

            // Create socket connection
            Poco::Net::StreamSocket socket;

            // Default timeouts: 10 seconds (10000 ms)
            constexpr int default_timeout_ms = 10000;

            socket.connect(Poco::Net::SocketAddress{host_port}, default_timeout_ms * 1000);
            socket.setReceiveTimeout(default_timeout_ms * 1000);
            socket.setSendTimeout(default_timeout_ms * 1000);
            socket.setNoDelay(true);

            ReadBufferFromPocoSocket in(socket);
            WriteBufferFromPocoSocket out(socket);

            // Send command
            out.write(command.data(), command.size());
            out.next();
            out.finalize();

            // Read response
            String result;
            readStringUntilEOF(result, in);
            in.next();

            return result;
        }
        catch (const Exception &)
        {
            throw;
        }
        catch (const Poco::Exception & e)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Failed to execute four letter word command '{}' on '{}': {}",
                            command, host_port, e.displayText());
        }
        catch (...)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Failed to execute four letter word command '{}' on '{}': {}",
                            command, host_port, getCurrentExceptionMessage(false));
        }
    }
};

}

REGISTER_FUNCTION(ZookeeperFourLetterWordCommand)
{
    FunctionDocumentation::Description description = R"(
Executes a four-letter word command on the specified ZooKeeper/Keeper server.
Four-letter word commands are special administrative commands that ZooKeeper supports.
)";
    FunctionDocumentation::Syntax syntax = "zookeeperFourLetterWordCommand(keeper_host_port, command)";
    FunctionDocumentation::Arguments arguments = {
        {"keeper_host_port", "The keeper server address in format 'host:port' (e.g., 'localhost:9181').", {"String"}},
        {"command", "The four-letter word command to execute (e.g., 'ruok', 'stat', 'conf').", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the command response as a string.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Check if keeper is running",
        R"(
SELECT zookeeperFourLetterWordCommand('localhost:9181', 'ruok');
        )",
        R"(
┌─zookeeperFourLetterWordCommand('localhost:9181', 'ruok')─┐
│ imok                                                      │
└───────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Get keeper statistics",
        R"(
SELECT zookeeperFourLetterWordCommand('localhost:9181', 'stat');
        )",
        R"(
┌─zookeeperFourLetterWordCommand('localhost:9181', 'stat')─┐
│ ZooKeeper version: 3.8.0                                 │
│ Latency min/avg/max: 0/0/0                               │
│ ...                                                       │
└───────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {24, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionZookeeperFourLetterWordCommand>(documentation, FunctionFactory::Case::Sensitive);
}

}
