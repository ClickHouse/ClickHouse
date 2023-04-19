#include <Interpreters/ExternalLoaderTempConfigRepository.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


ExternalLoaderTempConfigRepository::ExternalLoaderTempConfigRepository(const String & repository_name_, const String & path_, const LoadablesConfigurationPtr & config_)
    : name(repository_name_), path(path_), config(config_) {}


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
