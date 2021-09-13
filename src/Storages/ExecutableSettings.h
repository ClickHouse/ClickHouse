#pragma once

#include <Core/Defines.h>
#include <Core/BaseSettings.h>

namespace DB
{

class ASTStorage;

#define LIST_OF_EXECUTABLE_SETTINGS(M) \
    M(UInt64, send_chunk_header, false, "Send number_of_rows\n before sending chunk to process", 0) \
    M(UInt64, pool_size, 16, "Processes pool size. If size == 0, then there is no size restrictions", 0) \
    M(UInt64, max_command_execution_time, 10, "Max command execution time in seconds.", 0) \
    M(UInt64, command_termination_timeout, 10, "Command termination timeout in seconds.", 0) \

DECLARE_SETTINGS_TRAITS(ExecutableSettingsTraits, LIST_OF_EXECUTABLE_SETTINGS)

/// Settings for ExecutablePool engine.
struct ExecutableSettings : public BaseSettings<ExecutableSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
