#include <Interpreters/ExternalLoaderPresetConfigRepository.h>
#include <Common/Exception.h>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/adaptor/map.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ExternalLoaderPresetConfigRepository::ExternalLoaderPresetConfigRepository(const std::vector<std::pair<String, LoadablesConfigurationPtr>> & preset_)
{
    boost::range::copy(preset_, std::inserter(preset, preset.end()));
}

ExternalLoaderPresetConfigRepository::~ExternalLoaderPresetConfigRepository() = default;

std::set<String> ExternalLoaderPresetConfigRepository::getAllLoadablesDefinitionNames() const
{
    std::set<String> paths;
    boost::range::copy(preset | boost::adaptors::map_keys, std::inserter(paths, paths.end()));
    return paths;
}

bool ExternalLoaderPresetConfigRepository::exists(const String& path) const
{
    return preset.count(path);
}

Poco::Timestamp ExternalLoaderPresetConfigRepository::getUpdateTime(const String & path)
{
    if (!exists(path))
        throw Exception("Loadable " + path + " not found", ErrorCodes::BAD_ARGUMENTS);
    return creation_time;
}

/// May contain definition about several entities (several dictionaries in one .xml file)
LoadablesConfigurationPtr ExternalLoaderPresetConfigRepository::load(const String & path) const
{
    auto it = preset.find(path);
    if (it == preset.end())
        throw Exception("Loadable " + path + " not found", ErrorCodes::BAD_ARGUMENTS);
    return it->second;
}

}
