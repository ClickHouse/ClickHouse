#if defined(OS_LINUX)
#include <Interpreters/getOSKernelVersion.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
    
String getOSKernelVersion()
{
    struct utsname os_kernel_version;
    int buf = uname(&os_kernel_version);
    if (buf < 0)
    {
        throw Exception(
            "EFAULT buf is not valid.",
            ErrorCodes::BAD_ARGUMENTS);
    }
    else
    {
        // std::cout <<"sysname: " << os_kernel_version.sysname << " nodename: " << os_kernel_version.nodename 
        //           << " release: " << os_kernel_version.release << " version: " << os_kernel_version.version 
        //           << " machine: " << os_kernel_version.machine << std::endl;

        return "sysname: " + String(os_kernel_version.sysname) + " nodename: " + String(os_kernel_version.nodename) 
                  + " release: " + String(os_kernel_version.release) + " version: " + String(os_kernel_version.version) 
                  + " machine: " + String(os_kernel_version.machine);
    }
}

}

#endif