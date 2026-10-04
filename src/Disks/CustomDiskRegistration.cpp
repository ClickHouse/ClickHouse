#include <Disks/CustomDiskRegistration.h>

#include <Interpreters/Context.h>
#include <Common/Exception.h>

namespace DB
{

CustomDiskRegistration::~CustomDiskRegistration()
{
    /// The global context outlives every registration, but it is not available in all the
    /// applications that link the disk code, so tolerate its absence.
    auto context = Context::getGlobalContextInstance();
    if (!context)
        return;

    try
    {
        context->releaseCustomDisk(disk_name);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "while releasing custom disk " + disk_name);
    }
}

}
