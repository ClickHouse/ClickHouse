#include <Disks/DiskObjectStorage/IOSchedulingSettings.h>
#include <Common/CurrentThread.h>
#include <Common/Scheduler/IResourceManager.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace
{

template <class Settings>
Settings updateIOSchedulingSettingsImpl(const Settings & settings, const std::string & read_resource_name, const std::string & write_resource_name)
{
    if (read_resource_name.empty() && write_resource_name.empty())
        return settings;
    if (auto query_context = CurrentThread::tryGetQueryContext())
    {
        Settings result(settings);
        if (!read_resource_name.empty())
            result.io_scheduling.read_resource_link = query_context->getWorkloadClassifier()->get(read_resource_name);
        if (!write_resource_name.empty())
            result.io_scheduling.write_resource_link = query_context->getWorkloadClassifier()->get(write_resource_name);
        return result;
    }
    return settings;
}

}

ReadSettings updateIOSchedulingSettings(const ReadSettings & settings, const std::string & read_resource_name, const std::string & write_resource_name)
{
    return updateIOSchedulingSettingsImpl(settings, read_resource_name, write_resource_name);
}

WriteSettings updateIOSchedulingSettings(const WriteSettings & settings, const std::string & read_resource_name, const std::string & write_resource_name)
{
    return updateIOSchedulingSettingsImpl(settings, read_resource_name, write_resource_name);
}

}
