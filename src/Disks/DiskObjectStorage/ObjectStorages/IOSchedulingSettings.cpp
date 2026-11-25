#include <Disks/DiskObjectStorage/ObjectStorages/IOSchedulingSettings.h>
#include <Common/CurrentThread.h>
#include <Common/Scheduler/IResourceManager.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{
    extern const SettingsString workload;
}

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
        auto & io_scheduling = result.io_scheduling;
        const auto classifier = query_context->getWorkloadClassifier();
        if (!read_resource_name.empty())
            io_scheduling.read_resource_link = classifier->get(read_resource_name);
        if (!write_resource_name.empty())
            io_scheduling.write_resource_link = classifier->get(write_resource_name);
        io_scheduling.workload.reset();
        if (io_scheduling.read_resource_link || io_scheduling.write_resource_link)
            io_scheduling.workload = std::make_shared<const IOSchedulingWorkload>(IOSchedulingWorkload{
                .workload = query_context->getSettingsRef()[Setting::workload],
                .read_resource = io_scheduling.read_resource_link ? read_resource_name : "",
                .write_resource = io_scheduling.write_resource_link ? write_resource_name : "",
            });
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
