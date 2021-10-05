#pragma once
#if defined(OS_LINUX)

#include <Common/typeid_cast.h>

#include <string>
#include <sys/utsname.h>

namespace DB
{

/// Returns String with OS Kernel version.
/* To get name and information about current kernel.
   For simplicity, the function can be implemented only for Linux. 
*/
    
String getOSKernelVersion();

}

#endif