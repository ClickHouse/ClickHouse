#if defined(OS_LINUX)
#include <Interpreters/getOSKernelVersion.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
}
    
String getOSKernelVersion()
{
    struct utsname os_kernel_info;
    int buf = uname(&os_kernel_info);
    if (buf < 0)
    {
        throw Exception(
            "EFAULT buffer is not valid.",
            ErrorCodes::SYSTEM_ERROR);
    }
    else
    {
        return String(os_kernel_info.sysname) + " " + String(os_kernel_info.release);
    }
}

}

#endif