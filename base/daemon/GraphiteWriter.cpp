#include <daemon/GraphiteWriter.h>
#include <daemon/BaseDaemon.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/Util/Application.h>
#include <base/getFQDNOrHostName.h>

#include <mutex>
#include <iomanip>


GraphiteWriter::GraphiteWriter(const std::string & config_name, const std::string & sub_path)
{
    Poco::Util::LayeredConfiguration & config = Poco::Util::Application::instance().config();
    port = config.getInt(config_name + ".port", 42000);
    host = config.getString(config_name + ".host", "localhost");
    timeout = config.getDouble(config_name + ".timeout", 0.1);

    root_path = config.getString(config_name + ".root_path", "one_min");

    if (config.getBool(config_name + ".hostname_in_path", true))
    {
        if (!root_path.empty())
            root_path += ".";

        std::string hostname_in_path = getFQDNOrHostName();

        /// Replace dots to underscores so that Graphite does not interpret them as path separators
        std::replace(std::begin(hostname_in_path), std::end(hostname_in_path), '.', '_');

        root_path += hostname_in_path;
    }

    if (!sub_path.empty())
    {
        if (!root_path.empty())
            root_path += ".";
        root_path += sub_path;
    }
}
