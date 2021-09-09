#pragma once

#include <Core/Defines.h>
#include <Core/BaseSettings.h>

namespace DB
{

class ASTStorage;

#define LIST_OF_EXECUTABLE_POOL_SETTINGS(M) \
    M(UInt64, pool_size, 16, "Processes pool size. If size == 0, then there is no size restrictions", 0) \
    M(UInt64, max_command_execution_time, 10, "Max command execution time in seconds.", 0) \
    M(UInt64, command_termination_timeout, 10, "Command termination timeout in seconds.", 0) \

DECLARE_SETTINGS_TRAITS(ExecutablePoolSettingsTraits, LIST_OF_EXECUTABLE_POOL_SETTINGS)

/// Settings for ExecutablePool engine.
struct ExecutablePoolSettings : public BaseSettings<ExecutablePoolSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
