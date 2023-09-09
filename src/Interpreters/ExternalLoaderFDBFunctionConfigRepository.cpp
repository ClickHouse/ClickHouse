#include <Interpreters/ExternalLoaderFDBFunctionConfigRepository.h>
#include <vector>
#include <Interpreters/Context.h>

DB::ExternalLoaderFDBFunctionConfigRepository::ExternalLoaderFDBFunctionConfigRepository(std::string func_name_, ContextPtr context_)
    : WithContext(context_)
    , name(func_name_)
    , func_name(func_name_)
    , fdb_function_repositories(getContext()->getFDBFunctionLoaderUnlocked())
{
}

std::string DB::ExternalLoaderFDBFunctionConfigRepository::getName() const
{
    return func_name;
}

std::set<std::string> DB::ExternalLoaderFDBFunctionConfigRepository::getAllLoadablesDefinitionNames()
{
    std::set<std::string> re;
    re.insert(func_name);
    return re;
}

/// Checks that function with name exists on fdb
bool DB::ExternalLoaderFDBFunctionConfigRepository::exists(const std::string & definition_entity_name)
{
    return fdb_function_repositories->exist(definition_entity_name);
}

/// Return modification time via stat call
Poco::Timestamp DB::ExternalLoaderFDBFunctionConfigRepository::getUpdateTime(const std::string & definition_entity_name)
{
    return fdb_function_repositories->getUpdateTime(definition_entity_name);
}

/// Load metadata by name from fdb
DB::LoadablesConfigurationPtr DB::ExternalLoaderFDBFunctionConfigRepository::load(const std::string & func_name_)
{
    return fdb_function_repositories->getOneFuncConfig(func_name_);
}
