#pragma once

#include <base/types.h>
#include <map>
#include <memory>


namespace DB
{

/** Description of a driver for executable user-defined functions.
  *
  * A driver is a small adapter program that, given a function signature and a body of code,
  * produces a configuration file for an "ordinary" executable user-defined function.
  * The driver itself is described by an XML configuration file with a top-level `<driver>`
  * element containing:
  *
  *   - `name`             - name of the driver (not of any function).
  *   - `create_command`   - path to a shell script used to create a UDF from a code snippet.
  *   - `drop_command`     - optional, called when a function based on this driver is dropped.
  *   - `engine_arguments` - optional, declares supported `ENGINE = DriverName(...)` arguments.
  *   - `env`              - optional, environment variables to set when invoking driver commands.
  *
  * The driver is invoked when CREATE FUNCTION ... ENGINE = DriverName(...) AS '...code...' is executed.
  */
struct UserDefinedExecutableFunctionDriver
{
    struct EngineArgument
    {
        bool required = false;
    };

    String name;
    String create_command;
    String drop_command;
    std::map<String, EngineArgument> engine_arguments;
    std::map<String, String> env;
};

using UserDefinedExecutableFunctionDriverPtr = std::shared_ptr<const UserDefinedExecutableFunctionDriver>;

}
