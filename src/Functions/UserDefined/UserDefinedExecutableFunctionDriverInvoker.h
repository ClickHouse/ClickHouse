#pragma once

#include <Functions/UserDefined/UserDefinedExecutableFunctionDriver.h>
#include <Parsers/IAST_fwd.h>

#include <base/types.h>
#include <utility>
#include <vector>


namespace DB
{

/** Helpers that invoke the create_command/drop_command of a driver.
  *
  * The driver receives the function signature via well-known command-line arguments and the
  * user-provided source code via stdin. The driver's stdout is the textual configuration of
  * the resulting executable user-defined function (XML or YAML).
  */
struct UserDefinedExecutableFunctionDriverInvoker
{
    /// Invokes driver->create_command with the appropriate command-line arguments and stdin.
    /// Returns the generated function configuration as a string (the driver's stdout).
    /// `args_signature` is the human-readable list of arguments (e.g. "x UInt8, y DateTime"),
    /// or an empty string if no arguments were specified by the user.
    /// `working_directory` is set as the process's cwd.
    static String runCreateCommand(
        const UserDefinedExecutableFunctionDriver & driver,
        const String & function_name,
        const String & return_type,
        const String & args_signature,
        const String & source_code,
        const String & working_directory,
        const std::vector<std::pair<String, String>> & engine_argument_values); // STYLE_CHECK_ALLOW_STD_CONTAINERS

    /// Invokes driver->drop_command if present. Mirrors runCreateCommand's argument layout
    /// but does not send anything on stdin.
    static void runDropCommand(
        const UserDefinedExecutableFunctionDriver & driver,
        const String & function_name,
        const String & return_type,
        const String & args_signature,
        const String & working_directory,
        const std::vector<std::pair<String, String>> & engine_argument_values); // STYLE_CHECK_ALLOW_STD_CONTAINERS
};

}
