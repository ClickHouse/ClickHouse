#include <Interpreters/ExternalLoaderTempConfigRepository.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


ExternalLoaderTempConfigRepository::ExternalLoaderTempConfigRepository(String repository_name_, String path_, LoadablesConfigurationPtr config_)
    : name(std::move(repository_name_)), path(std::move(path_)), config(std::move(config_)) {}


std::set<String> ExternalLoaderTempConfigRepository::getAllLoadablesDefinitionNames()
{
    std::set<String> paths;
    paths.insert(path);
    return paths;
}


bool ExternalLoaderTempConfigRepository::exists(const String & path_)
{
    return path == path_;
}


Poco::Timestamp ExternalLoaderTempConfigRepository::getUpdateTime(const String & path_)
{
    if (!exists(path_))
        throw Exception("Loadable " + path_ + " not found", ErrorCodes::BAD_ARGUMENTS);
    return creation_time;
}


LoadablesConfigurationPtr ExternalLoaderTempConfigRepository::load(const String & path_)
{
    if (!exists(path_))
        throw Exception("Loadable " + path_ + " not found", ErrorCodes::BAD_ARGUMENTS);
    return config;
}

}
