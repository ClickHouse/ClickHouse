#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/ExecutableSettings.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define LIST_OF_EXECUTABLE_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Bool, send_chunk_header, false, "Send number_of_rows\n before sending chunk to process.", 0) \
    DECLARE(UInt64, pool_size, 16, "Processes pool size. If size == 0, then there is no size restrictions.", 0) \
    DECLARE(UInt64, max_command_execution_time, 10, "Max command execution time in seconds.", 0) \
    DECLARE(UInt64, command_termination_timeout, 10, "Command termination timeout in seconds.", 0) \
    DECLARE(UInt64, command_read_timeout, 10000, "Timeout for reading data from command stdout in milliseconds.", 0) \
    DECLARE(UInt64, command_write_timeout, 10000, "Timeout for writing data to command stdin in milliseconds.", 0) \
    DECLARE(ExternalCommandStderrReaction, stderr_reaction, ExternalCommandStderrReaction::NONE, "Reaction when external command outputs data to its stderr.", 0) \
    DECLARE(Bool, check_exit_code, false, "Throw exception if the command exited with non-zero status code.", 0) \

DECLARE_SETTINGS_TRAITS(ExecutableSettingsTraits, LIST_OF_EXECUTABLE_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(ExecutableSettingsTraits, LIST_OF_EXECUTABLE_SETTINGS)

struct ExecutableSettingsImpl : public BaseSettings<ExecutableSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) ExecutableSettings##TYPE NAME = &ExecutableSettingsImpl ::NAME;

namespace ExecutableSetting
{
LIST_OF_EXECUTABLE_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

ExecutableSettings::ExecutableSettings()
    : script_name({})
    , script_arguments({})
    , is_executable_pool(false)
    , impl(std::make_unique<ExecutableSettingsImpl>())
{
}

ExecutableSettings::ExecutableSettings(const ExecutableSettings & settings)
    : script_name(settings.script_name)
    , script_arguments(settings.script_arguments)
    , is_executable_pool(settings.is_executable_pool)
    , impl(std::make_unique<ExecutableSettingsImpl>(*settings.impl))
{
}

ExecutableSettings::ExecutableSettings(ExecutableSettings && settings) noexcept
    : script_name(std::move(settings.script_name))
    , script_arguments(std::move(settings.script_arguments))
    , is_executable_pool(settings.is_executable_pool)
    , impl(std::make_unique<ExecutableSettingsImpl>(std::move(*settings.impl)))
{
}

ExecutableSettings::~ExecutableSettings() = default;

EXECUTABLE_SETTINGS_SUPPORTED_TYPES(ExecutableSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void ExecutableSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            impl->applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }
}

void ExecutableSettings::applyChanges(const SettingsChanges & changes)
{
    impl->applyChanges(changes);
}
}
