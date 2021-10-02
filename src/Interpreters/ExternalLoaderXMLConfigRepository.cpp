#include <Interpreters/ExternalLoaderXMLConfigRepository.h>

#include <filesystem>

#include <Common/StringUtils/StringUtils.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Poco/Glob.h>
#include <Common/filesystemHelpers.h>


namespace fs = std::filesystem;

namespace DB
{

ExternalLoaderXMLConfigRepository::ExternalLoaderXMLConfigRepository(
    const std::string & app_path_,
    const std::string & main_config_path_,
    const std::unordered_set<std::string> & patterns_)
    : app_path(app_path_)
    , main_config_path(main_config_path_)
    , patterns(patterns_)
{
}

Poco::Timestamp ExternalLoaderXMLConfigRepository::getUpdateTime(const std::string & definition_entity_name)
{
    return FS::getModificationTimestamp(definition_entity_name);
}

std::set<std::string> ExternalLoaderXMLConfigRepository::getAllLoadablesDefinitionNames()
{
    std::unordered_set<std::string> patterns_copy;

    {
        std::lock_guard<std::mutex> lock(patterns_mutex);
        patterns_copy = patterns;
    }

    const String config_dir = fs::path(main_config_path).parent_path();
    std::set<std::string> files;

    for (const auto & pattern : patterns_copy)
    {
        if (pattern.empty())
            continue;

        if (pattern[0] != '/')
        {
            const String absolute_path = fs::path(config_dir) / pattern;

            Poco::Glob::glob(absolute_path, files, 0);
            if (!files.empty())
                continue;
        }

        Poco::Glob::glob(pattern, files, 0);
    }

    for (auto it = files.begin(); it != files.end();)
    {
        if (ConfigProcessor::isPreprocessedFile(*it))
            it = files.erase(it);
        else
            ++it;
    }

    return files;
}

void ExternalLoaderXMLConfigRepository::updatePatterns(const std::unordered_set<std::string> & patterns_)
{
    std::lock_guard<std::mutex> lock(patterns_mutex);

    if (patterns == patterns_)
        return;

    patterns = patterns_;
}

bool ExternalLoaderXMLConfigRepository::exists(const std::string & definition_entity_name)
{
    return fs::exists(fs::path(definition_entity_name));
}

Poco::AutoPtr<Poco::Util::AbstractConfiguration> ExternalLoaderXMLConfigRepository::load(
    const std::string & config_file)
{
    ConfigProcessor config_processor{config_file};
    ConfigProcessor::LoadedConfig preprocessed = config_processor.loadConfig();
    config_processor.savePreprocessedConfig(preprocessed, app_path);
    return preprocessed.configuration;
}

}
