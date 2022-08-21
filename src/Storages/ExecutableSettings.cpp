#include "ExecutableSettings.h"

#include <Common/Exception.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

// limit max custom executable environ size to 4K
size_t MAX_EXECUTABLE_ENVIRON_SIZE = 4 * 1024;

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
    extern const int CANNOT_CREATE_CHILD_PROCESS;
}

IMPLEMENT_SETTINGS_TRAITS(ExecutableSettingsTraits, LIST_OF_EXECUTABLE_SETTINGS)

void ExecutableSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            applyChanges(storage_def.settings->changes);
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

void ExecutableSettings::applyEnvVars(const SettingsChanges & changes, const std::vector<String> & allow_list)
{
    size_t cur_env_size = 0;
    for (auto change : changes)
    {
        for (const auto & allowed : allow_list)
        {
            re2::RE2 matcher(allowed);
            if (re2::RE2::FullMatch(change.name, matcher))
            {
                // env var values are always strings
                auto value = DB::toString(change.value);
                // environ is built using an array of pointers to `NAME=VALUE\0` C strings
                cur_env_size += sizeof(char *) + change.name.size() + sizeof('=') + value.size() + 1;
                if (cur_env_size > MAX_EXECUTABLE_ENVIRON_SIZE)
                    throw Exception("Max executable environment size exceeded", ErrorCodes::CANNOT_CREATE_CHILD_PROCESS);
                env_vars.emplace_back(std::move(change.name));
                env_vars.emplace_back(std::move(value));
                break;
            }
        }
    }
}

}
