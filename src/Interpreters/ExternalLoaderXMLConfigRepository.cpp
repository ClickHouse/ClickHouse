#include <Interpreters/ExternalLoaderXMLConfigRepository.h>

#include <Common/StringUtils/StringUtils.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Poco/Glob.h>
#include <Common/filesystemHelpers.h>
#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{
ExternalLoaderXMLConfigRepository::ExternalLoaderXMLConfigRepository(
    const Poco::Util::AbstractConfiguration & main_config_, const std::string & config_key_)
    : main_config(main_config_), config_key(config_key_)
{
}

Poco::Timestamp ExternalLoaderXMLConfigRepository::getUpdateTime(const std::string & definition_entity_name)
{
    return FS::getModificationTimestamp(definition_entity_name);
}

std::set<std::string> ExternalLoaderXMLConfigRepository::getAllLoadablesDefinitionNames()
{
    std::set<std::string> files;

    auto patterns = getMultipleValuesFromConfig(main_config, "", config_key);

    for (auto & pattern : patterns)
    {
        if (pattern.empty())
            continue;

        if (pattern[0] != '/')
        {
            const auto app_config_path = main_config.getString("config-file", "config.xml");
            const String config_dir = fs::path(app_config_path).parent_path();
            const String absolute_path = fs::path(config_dir) / pattern;
            Poco::Glob::glob(absolute_path, files, 0);
            if (!files.empty())
                continue;
        }

        Poco::Glob::glob(pattern, files, 0);
    }

    for (std::set<std::string>::iterator it = files.begin(); it != files.end();)
    {
        if (ConfigProcessor::isPreprocessedFile(*it))
            files.erase(it++);
        else
            ++it;
    }

    return files;
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
    config_processor.savePreprocessedConfig(preprocessed, main_config.getString("path", DBMS_DEFAULT_PATH));
    return preprocessed.configuration;
}

}
