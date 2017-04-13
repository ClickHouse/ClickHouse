#include <Common/getFQDNOrHostName.h>

#include <Interpreters/DDLWorker.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/executeQuery.h>

#include <zkutil/ZooKeeper.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace {

/// Helper class which extracts from the ClickHouse configuration file
/// the parameters we need for operating the resharding thread.
class Arguments final
{
public:
    Arguments(const Poco::Util::AbstractConfiguration & config, const std::string & config_name)
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(config_name, keys);

        for (const auto & key : keys)
        {
            if (key == "task_queue_path")
                task_queue_path = config.getString(config_name + "." + key);
            else
                throw Exception{"Unknown parameter in resharding configuration", ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG};
        }

        if (task_queue_path.empty())
            throw Exception{"Resharding: missing parameter task_queue_path", ErrorCodes::INVALID_CONFIG_PARAMETER};
    }

    Arguments(const Arguments &) = delete;
    Arguments & operator=(const Arguments &) = delete;

    std::string getTaskQueuePath() const
    {
        return task_queue_path;
    }

private:
    std::string task_queue_path;
};

}

DDLWorker::DDLWorker(const Poco::Util::AbstractConfiguration & config,
                      const std::string & config_name, Context & context_)
    : context(context_)
    , stop_flag(false)
{
    Arguments arguments(config, config_name);
    auto zookeeper = context.getZooKeeper();

    std::string root = arguments.getTaskQueuePath();
    if (root.back() != '/')
        root += "/";

    auto current_host = getFQDNOrHostName();
    host_task_queue_path = "/clickhouse/task_queue/ddl/" + current_host;

    thread = std::thread(&DDLWorker::run, this);
}

DDLWorker::~DDLWorker()
{
    stop_flag = true;
    cond_var.notify_one();
    thread.join();
}

void DDLWorker::processTasks()
{
    processCreate(host_task_queue_path + "/create");
}

void DDLWorker::processCreate(const std::string & path)
{
    auto zookeeper = context.getZooKeeper();

    if (!zookeeper->exists(path))
        return;

    const Strings & children = zookeeper->getChildren(path);

    for (const auto & name : children)
    {
        const std::string & query_path = path + "/" + name;

        try
        {
            std::string value;

            if (zookeeper->tryGet(query_path, value))
            {
                if (!value.empty())
                    executeQuery(value, context);

                zookeeper->remove(query_path);
            }
        }
        catch (const std::exception & ex)
        {
            LOG_ERROR(log, ex.what() + std::string(" on ") + query_path);
        }
    }
}

void DDLWorker::run()
{
    using namespace std::chrono_literals;

    while (!stop_flag)
    {
        try
        {
            processTasks();
        }
        catch (const std::exception & ex)
        {
            LOG_ERROR(log, ex.what());
        }

        std::unique_lock<std::mutex> g(lock);
        cond_var.wait_for(g, 10s);
    }
}

}
