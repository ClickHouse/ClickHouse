#include <Functions/UserDefined/UserDefinedExecutableFunctionDriverInvoker.h>

#include <Common/CurrentMetrics.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/ShellCommand.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/threadPoolCallbackRunner.h>
#include <IO/copyData.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <cerrno>


namespace CurrentMetrics
{
    extern const Metric UDFDriverInvokerThreads;
    extern const Metric UDFDriverInvokerThreadsActive;
    extern const Metric UDFDriverInvokerThreadsScheduled;
}

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

    /// Marker used in logs in place of secret values.
    constexpr auto REDACTED_VALUE = "[HIDDEN]";

    struct DirectCommand
    {
        /// Arguments after `argv[0]`. The binary is always `ENV_BINARY`.
        std::vector<String> arguments; // STYLE_CHECK_ALLOW_STD_CONTAINERS

        /// A human-readable form of the command for logging. Driver `env` values and engine
        /// argument values are an operator/user-facing surface that can carry credentials or
        /// tokens, so only their names (and the non-secret structural arguments) are recorded;
        /// the values are replaced with `[HIDDEN]`. The actual `arguments` passed to the driver
        /// keep the real values and are never logged.
        String redacted_description;
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
        WriteBufferFromOwnString redacted;
        redacted << ENV_BINARY;

        if (!working_directory.empty())
        {
            cmd.arguments.emplace_back("-C");
            cmd.arguments.emplace_back(working_directory);
            redacted << " -C " << working_directory;
        }

        for (const auto & [name, value] : driver.env)
        {
            cmd.arguments.emplace_back(name + '=' + value);
            redacted << ' ' << name << '=' << REDACTED_VALUE;
        }

        cmd.arguments.emplace_back(action_command);
        redacted << ' ' << action_command;

        cmd.arguments.emplace_back("--name");
        cmd.arguments.emplace_back(function_name);
        redacted << " --name " << function_name;
        if (!return_type.empty())
        {
            cmd.arguments.emplace_back("--return");
            cmd.arguments.emplace_back(return_type);
            redacted << " --return " << return_type;
        }
        if (!args_signature.empty())
        {
            cmd.arguments.emplace_back("--args");
            cmd.arguments.emplace_back(args_signature);
            redacted << " --args " << args_signature;
        }

        for (const auto & [name, value] : engine_argument_values)
        {
            cmd.arguments.emplace_back("--" + name);
            cmd.arguments.emplace_back(value);
            redacted << " --" << name << ' ' << REDACTED_VALUE;
        }

        cmd.redacted_description = redacted.str();
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
        CommandResult result;

        /// All three pipes must be serviced concurrently: a driver that prints
        /// enough on stderr while waiting for stdin would otherwise deadlock if
        /// we wrote stdin synchronously before draining stdout/stderr.
        /// stdout/stderr are drained in pool threads while stdin is written in this thread.
        ThreadPool pool(
            CurrentMetrics::UDFDriverInvokerThreads,
            CurrentMetrics::UDFDriverInvokerThreadsActive,
            CurrentMetrics::UDFDriverInvokerThreadsScheduled,
            /*max_threads=*/ 2);
        ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::UDF_DRIVER);

        runner.enqueueAndKeepTrack([&process, &result] { result.stdout_output = readPipeToString(process.out); });
        runner.enqueueAndKeepTrack([&process, &result] { result.stderr_output = readPipeToString(process.err); });

        result.write_exception = writeStdinSafely(process.in, source_code);
        runner.waitForAllToFinishAndRethrowFirstError();

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
    LOG_DEBUG(log, "Invoking driver '{}' create_command: {}", driver.name, cmd.redacted_description);

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
        throw Exception(ErrorCodes::UDF_EXECUTION_FAILED,
            "Failed while writing function body to driver '{}' create_command for function '{}': {}. Stderr: {}",
            driver.name, function_name,
            getExceptionMessage(result.write_exception, /*with_stacktrace=*/ false),
            result.stderr_output);

    if (result.wait_exception)
        throw Exception(ErrorCodes::UDF_EXECUTION_FAILED,
            "Failed while waiting for driver '{}' create_command for function '{}': {}. Stderr: {}",
            driver.name, function_name,
            getExceptionMessage(result.wait_exception, /*with_stacktrace=*/ false),
            result.stderr_output);

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
    LOG_DEBUG(log, "Invoking driver '{}' drop_command: {}", driver.name, cmd.redacted_description);

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
