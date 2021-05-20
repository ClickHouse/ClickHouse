#include <common/addDatabasePrefixToZooKeeperPath.h>
#include <Poco/Util/Application.h>
#include <iostream>

namespace DB
{

static void addDatabaseToPrefix(std::string & prefix, const std::string database)
{
    if (prefix.empty() || prefix.back() != '/')
        prefix += '/';

    if (prefix[0] != '/')
        prefix = '/' + prefix;

    prefix += database;
}

bool addDatabasePrefixToZooKeeperPath(std::string & path, const std::string & database)
{
    const Poco::Util::LayeredConfiguration & config = Poco::Util::Application::instance().config();
    if (config.has("testmode_zk_path_prefix"))
    {
        auto prefix = config.getString("testmode_zk_path_prefix");
        addDatabaseToPrefix(prefix, database);
        path = prefix + path;
        return true;
    }

    return false;
}

bool removeDatabasePrefixToZooKeeperPath(std::string & path)
{
    const Poco::Util::LayeredConfiguration & config = Poco::Util::Application::instance().config();
    if (config.has("testmode_zk_path_prefix"))
    {
        auto prefix = config.getString("testmode_zk_path_prefix");
        addDatabaseToPrefix(prefix, "");

        if (path.starts_with(prefix))
        {
            path = path.substr(prefix.size());

            auto pos = path.find('/');
            if (pos != std::string::npos)
                path = path.substr(pos);

            return true;
        }
    }

    return false;
}

}
