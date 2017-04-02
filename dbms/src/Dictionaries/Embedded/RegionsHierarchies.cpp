#include <Dictionaries/Embedded/RegionsHierarchies.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <common/logger_useful.h>


static constexpr auto config_key = "path_to_regions_hierarchy_file";


RegionsHierarchies::RegionsHierarchies()
: RegionsHierarchies(Poco::Util::Application::instance().config().getString(config_key))
{
}


RegionsHierarchies::RegionsHierarchies(const std::string & path)
{
    Logger * log = &Logger::get("RegionsHierarchies");

    LOG_DEBUG(log, "Adding default regions hierarchy from " << path);

    data.emplace(std::piecewise_construct,
        std::forward_as_tuple(""),
        std::forward_as_tuple(path));

    std::string basename = Poco::Path(path).getBaseName();

    Poco::Path dir_path = Poco::Path(path).absolute().parent();

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it(dir_path); dir_it != dir_end; ++dir_it)
    {
        std::string other_basename = dir_it.path().getBaseName();

        if (0 == other_basename.compare(0, basename.size(), basename) && other_basename.size() > basename.size() + 1)
        {
            if (other_basename[basename.size()] != '_')
                continue;

            std::string suffix = other_basename.substr(basename.size() + 1);

            LOG_DEBUG(log, "Adding regions hierarchy from " << dir_it->path() << ", key: " << suffix);

            data.emplace(std::piecewise_construct,
                std::forward_as_tuple(suffix),
                std::forward_as_tuple(dir_it->path()));
        }
    }
}


bool RegionsHierarchies::isConfigured()
{
    return Poco::Util::Application::instance().config().has(config_key);
}
