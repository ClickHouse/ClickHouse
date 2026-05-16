#include <Functions/UserDefined/UserDefinedExecutableFunctionDriverInvoker.h>

#include <Common/Exception.h>
#include <Common/ShellCommand.h>
#include <Common/logger_useful.h>
#include <IO/copyData.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <future>


namespace DB
{

namespace ErrorCodes
{
    extern const int UDF_EXECUTION_FAILED;
}

namespace
{
    /// Shell-quote a single argument so it can be embedded into a `/bin/sh -c` command.
    String shellQuote(const String & str)
    {
        String out;
        out.reserve(str.size() + 2);
        out.push_back('\'');
        for (char c : str)
        {
            if (c == '\'')
                out += "'\\''";
            else
                out.push_back(c);
        }
        out.push_back('\'');
        return out;
    }

    String buildCommand(
        const UserDefinedExecutableFunctionDriver & driver,
        const String & action_command,
        const String & function_name,
        const String & return_type,
        const String & args_signature,
        const String & working_directory,
        const std::vector<std::pair<String, String>> & engine_argument_values)
    {
        WriteBufferFromOwnString out;

        if (!working_directory.empty())
            out << "cd " << shellQuote(working_directory) << " && ";

        out << "exec env";
        for (const auto & [name, value] : driver.env)
            out << ' ' << shellQuote(name + '=' + value);

        out << ' ' << shellQuote(action_command);
        out << " --name " << shellQuote(function_name);
        if (!return_type.empty())
            out << " --return " << shellQuote(return_type);
        if (!args_signature.empty())
            out << " --args " << shellQuote(args_signature);

        for (const auto & [name, value] : engine_argument_values)
            out << " --" << name << ' ' << shellQuote(value);

        return out.str();
    }

    String readPipeToString(ReadBuffer & pipe)
    {
        String output;
        WriteBufferFromString output_buf(output);
        copyData(pipe, output_buf);
        output_buf.finalize();
        return output;
    }

    struct CommandResult
    {
        int retcode = 0;
        String stdout_output;
        String stderr_output;
        std::exception_ptr wait_exception;
    };

    CommandResult waitAndRead(ShellCommand & process)
    {
        auto stdout_future = std::async(std::launch::async, [&process] { return readPipeToString(process.out); });
        auto stderr_future = std::async(std::launch::async, [&process] { return readPipeToString(process.err); });

        CommandResult result;
        result.stdout_output = stdout_future.get();
        result.stderr_output = stderr_future.get();
        try
        {
            result.retcode = process.tryWait();
        }
        catch (...)
        {
            result.wait_exception = std::current_exception();
        }

        return result;
    }
}

String UserDefinedExecutableFunctionDriverInvoker::runCreateCommand(
    const UserDefinedExecutableFunctionDriver & driver,
    const String & function_name,
    const String & return_type,
    const String & args_signature,
    const String & source_code,
    const String & working_directory,
    const std::vector<std::pair<String, String>> & engine_argument_values)
{
    if (driver.create_command.empty())
        throw Exception(ErrorCodes::UDF_EXECUTION_FAILED,
            "Driver '{}' is missing create_command", driver.name);

    String shell_command = buildCommand(
        driver, driver.create_command, function_name, return_type, args_signature,
        working_directory, engine_argument_values);

    auto log = getLogger("UserDefinedExecutableFunctionDriverInvoker");
    LOG_DEBUG(log, "Invoking driver '{}' create_command: {}", driver.name, shell_command);

    ShellCommand::Config config(shell_command);
    auto process = ShellCommand::execute(config);

    if (!source_code.empty())
    {
        WriteBufferFromOwnString src_buf;
        writeString(source_code, src_buf);
        if (!source_code.ends_with('\n'))
            writeChar('\n', src_buf);
        writeString(src_buf.str(), process->in);
    }
    process->in.close();

    /// `tryWait` returns the actual exit code without throwing on non-zero codes,
    /// so we can decorate the resulting exception with the full driver stderr.
    CommandResult result;
    try
    {
        result = waitAndRead(*process);
        if (result.wait_exception)
            std::rethrow_exception(result.wait_exception);
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format(
            "while waiting for driver '{}' create_command for function '{}'. Stderr: {}",
            driver.name, function_name, result.stderr_output));
        throw;
    }

    if (result.retcode != 0)
        throw Exception(ErrorCodes::UDF_EXECUTION_FAILED,
            "Driver '{}' create_command for function '{}' exited with code {}. Stderr: {}",
            driver.name, function_name, result.retcode, result.stderr_output);

    if (result.stdout_output.empty())
        throw Exception(ErrorCodes::UDF_EXECUTION_FAILED,
            "Driver '{}' produced empty configuration for function '{}'. Stderr: {}",
            driver.name, function_name, result.stderr_output);

    return result.stdout_output;
}

void UserDefinedExecutableFunctionDriverInvoker::runDropCommand(
    const UserDefinedExecutableFunctionDriver & driver,
    const String & function_name,
    const String & return_type,
    const String & args_signature,
    const String & working_directory,
    const std::vector<std::pair<String, String>> & engine_argument_values)
{
    if (driver.drop_command.empty())
        return;

    String shell_command = buildCommand(
        driver, driver.drop_command, function_name, return_type, args_signature,
        working_directory, engine_argument_values);

    auto log = getLogger("UserDefinedExecutableFunctionDriverInvoker");
    LOG_DEBUG(log, "Invoking driver '{}' drop_command: {}", driver.name, shell_command);

    ShellCommand::Config config(shell_command);
    auto process = ShellCommand::execute(config);
    process->in.close();

    CommandResult result;
    try
    {
        result = waitAndRead(*process);
        if (result.wait_exception)
            std::rethrow_exception(result.wait_exception);
    }
    catch (...)
    {
        tryLogCurrentException(log,
            fmt::format("while waiting for driver '{}' drop_command for function '{}'. Stderr: {}",
                driver.name, function_name, result.stderr_output));
        return;
    }

    if (result.retcode != 0)
        throw Exception(ErrorCodes::UDF_EXECUTION_FAILED,
            "Driver '{}' drop_command for function '{}' exited with code {}. Stderr: {}",
            driver.name, function_name, result.retcode, result.stderr_output);
}

}
