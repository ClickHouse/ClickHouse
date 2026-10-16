#include <Disks/CustomDiskRegistration.h>

#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>

namespace DB
{

namespace FailPoints
{
    extern const char custom_disk_registration_pause_before_release[];
}

CustomDiskRegistration::~CustomDiskRegistration()
{
    /// The global context outlives every registration, but it is not available in all the
    /// applications that link the disk code, so tolerate its absence.
    auto context = Context::getGlobalContextInstance();
    if (!context)
        return;

    try
    {
        FailPointInjection::pauseFailPoint(FailPoints::custom_disk_registration_pause_before_release);
        context->releaseCustomDisk(disk_name);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "while releasing custom disk " + disk_name);
    }
}

}
