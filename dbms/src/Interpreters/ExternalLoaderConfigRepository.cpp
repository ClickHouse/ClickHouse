#include <Interpreters/ExternalLoaderConfigRepository.h>

#include <Common/StringUtils/StringUtils.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/getMultipleKeysFromConfig.h>

#include <Poco/Glob.h>
#include <Poco/File.h>
#include <Poco/Path.h>


namespace DB
{

ExternalLoaderConfigRepository::Files ExternalLoaderConfigRepository::list(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & path_key) const
{
    Files files;

    auto patterns = getMultipleValuesFromConfig(config, "", path_key);

    for (auto & pattern : patterns)
    {
        if (pattern.empty())
            continue;

        if (pattern[0] != '/')
        {
            const auto app_config_path = config.getString("config-file", "config.xml");
            const auto config_dir = Poco::Path{app_config_path}.parent().toString();
            const auto absolute_path = config_dir + pattern;
            Poco::Glob::glob(absolute_path, files, 0);
            if (!files.empty())
                continue;
        }

        Poco::Glob::glob(pattern, files, 0);
    }

    for (Files::iterator it = files.begin(); it != files.end();)
    {
        if (ConfigProcessor::isPreprocessedFile(*it))
            files.erase(it++);
        else
            ++it;
    }

    return files;
}

bool ExternalLoaderConfigRepository::exists(const std::string & config_file) const
{
    return Poco::File(config_file).exists();
}

Poco::Timestamp ExternalLoaderConfigRepository::getLastModificationTime(
    const std::string & config_file) const
{
    return Poco::File(config_file).getLastModified();
}

Poco::AutoPtr<Poco::Util::AbstractConfiguration> ExternalLoaderConfigRepository::load(
    const std::string & config_file) const
{
    ConfigProcessor config_processor{config_file};
    ConfigProcessor::LoadedConfig preprocessed = config_processor.loadConfig();
    config_processor.savePreprocessedConfig(preprocessed);
    return preprocessed.configuration;
}

}
