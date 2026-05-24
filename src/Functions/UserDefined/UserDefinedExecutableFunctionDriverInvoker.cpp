#include <Functions/UserDefined/UserDefinedExecutableFunctionDriverInvoker.h>

#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/ShellCommand.h>
#include <Common/logger_useful.h>
#include <IO/copyData.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <cerrno>
#include <future>


namespace DB
{

namespace ErrorCodes
{
    extern const int UDF_EXECUTION_FAILED;
}

namespace
{
    /// `/usr/bin/env` is exec'd directly (no shell), so command arguments are passed as
    /// distinct `argv` entries and are not subject to shell quoting/expansion rules.
    /// `env -C` sets the child's working directory, and `KEY=VAL` entries set environment
    /// variables before exec'ing `action_command`.
    constexpr auto ENV_BINARY = "/usr/bin/env";

    struct DirectCommand
    {
        /// Arguments after `argv[0]`. The binary is always `ENV_BINARY`.
        std::vector<String> arguments; // STYLE_CHECK_ALLOW_STD_CONTAINERS

        String describe() const
        {
            WriteBufferFromOwnString out;
            out << ENV_BINARY;
            for (const auto & arg : arguments)
                out << ' ' << arg;
            return out.str();
        }
    };

    DirectCommand buildCommand(
        const UserDefinedExecutableFunctionDriver & driver,
        const String & action_command,
        const String & function_name,
        const String & return_type,
        const String & args_signature,
        const String & working_directory,
        const std::vector<std::pair<String, String>> & engine_argument_values) // STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        DirectCommand cmd;

        if (!working_directory.empty())
        {
            cmd.arguments.emplace_back("-C");
            cmd.arguments.emplace_back(working_directory);
        }

        for (const auto & [name, value] : driver.env)
            cmd.arguments.emplace_back(name + '=' + value);

        cmd.arguments.emplace_back(action_command);

        cmd.arguments.emplace_back("--name");
        cmd.arguments.emplace_back(function_name);
        if (!return_type.empty())
        {
            cmd.arguments.emplace_back("--return");
            cmd.arguments.emplace_back(return_type);
        }
        if (!args_signature.empty())
        {
            cmd.arguments.emplace_back("--args");
            cmd.arguments.emplace_back(args_signature);
        }

        for (const auto & [name, value] : engine_argument_values)
        {
            cmd.arguments.emplace_back("--" + name);
            cmd.arguments.emplace_back(value);
        }

        return cmd;
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
        std::exception_ptr write_exception;
    };

    /// Write `source_code` to the child's stdin. If the driver exits before consuming
    /// all of stdin, the write returns `EPIPE` (which `WriteBufferFromFileDescriptor`
    /// surfaces as `CANNOT_WRITE_TO_FILE_DESCRIPTOR` with `saved_errno == EPIPE`).
    /// That is not a fatal condition - the driver's exit code and stderr describe
    /// the real failure, so we suppress the broken-pipe exception here and let the
    /// caller report the driver's own error.
    std::exception_ptr writeStdinSafely(WriteBufferFromFile & in, const String & source_code)
    {
        try
        {
            if (!source_code.empty())
            {
                writeString(source_code, in);
                if (!source_code.ends_with('\n'))
                    writeChar('\n', in);
            }
            in.close();
        }
        catch (const ErrnoException & e)
        {
            if (e.getErrno() == EPIPE)
                return nullptr;
            return std::current_exception();
        }
        catch (...)
        {
            return std::current_exception();
        }
        return nullptr;
    }

    CommandResult writeAndWait(ShellCommand & process, const String & source_code)
    {
        /// All three pipes must be serviced concurrently: a driver that prints
        /// enough on stderr while waiting for stdin would otherwise deadlock if
        /// we wrote stdin synchronously before draining stdout/stderr.
        auto stdout_future = std::async(std::launch::async, [&process] { return readPipeToString(process.out); });
        auto stderr_future = std::async(std::launch::async, [&process] { return readPipeToString(process.err); });
        auto stdin_future = std::async(std::launch::async, [&process, &source_code] { return writeStdinSafely(process.in, source_code); });

        CommandResult result;
        result.stdout_output = stdout_future.get();
        result.stderr_output = stderr_future.get();
        result.write_exception = stdin_future.get();
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
    const std::vector<std::pair<String, String>> & engine_argument_values) // STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    if (driver.create_command.empty())
        throw Exception(ErrorCodes::UDF_EXECUTION_FAILED,
            "Driver '{}' is missing create_command", driver.name);

    DirectCommand cmd = buildCommand(
        driver, driver.create_command, function_name, return_type, args_signature,
        working_directory, engine_argument_values);

    auto log = getLogger("UserDefinedExecutableFunctionDriverInvoker");
    LOG_DEBUG(log, "Invoking driver '{}' create_command: {}", driver.name, cmd.describe());

    ShellCommand::Config config(ENV_BINARY);
    for (const auto & arg : cmd.arguments)
        config.arguments.emplace_back(arg);
    auto process = ShellCommand::executeDirect(config);

    /// `tryWait` returns the actual exit code without throwing on non-zero codes,
    /// so we can decorate the resulting exception with the full driver stderr.
    CommandResult result = writeAndWait(*process, source_code);

    if (result.retcode != 0)
        throw Exception(ErrorCodes::UDF_EXECUTION_FAILED,
            "Driver '{}' create_command for function '{}' exited with code {}. Stderr: {}",
            driver.name, function_name, result.retcode, result.stderr_output);

    /// The driver exited cleanly but we could not deliver the function body to it.
    if (result.write_exception)
    {
        try
        {
            std::rethrow_exception(result.write_exception);
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format(
                "while writing function body to driver '{}' create_command for function '{}'. Stderr: {}",
                driver.name, function_name, result.stderr_output));
            throw;
        }
    }

    if (result.wait_exception)
    {
        try
        {
            std::rethrow_exception(result.wait_exception);
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format(
                "while waiting for driver '{}' create_command for function '{}'. Stderr: {}",
                driver.name, function_name, result.stderr_output));
            throw;
        }
    }

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
    const std::vector<std::pair<String, String>> & engine_argument_values) // STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    if (driver.drop_command.empty())
        return;

    DirectCommand cmd = buildCommand(
        driver, driver.drop_command, function_name, return_type, args_signature,
        working_directory, engine_argument_values);

    auto log = getLogger("UserDefinedExecutableFunctionDriverInvoker");
    LOG_DEBUG(log, "Invoking driver '{}' drop_command: {}", driver.name, cmd.describe());

    ShellCommand::Config config(ENV_BINARY);
    for (const auto & arg : cmd.arguments)
        config.arguments.emplace_back(arg);
    auto process = ShellCommand::executeDirect(config);

    CommandResult result = writeAndWait(*process, /*source_code=*/ "");

    if (result.wait_exception)
    {
        try
        {
            std::rethrow_exception(result.wait_exception);
        }
        catch (...)
        {
            tryLogCurrentException(log,
                fmt::format("while waiting for driver '{}' drop_command for function '{}'. Stderr: {}",
                    driver.name, function_name, result.stderr_output));
            return;
        }
    }

    if (result.retcode != 0)
        throw Exception(ErrorCodes::UDF_EXECUTION_FAILED,
            "Driver '{}' drop_command for function '{}' exited with code {}. Stderr: {}",
            driver.name, function_name, result.retcode, result.stderr_output);
}

}
