#include <Common/getNumberOfPhysicalCPUCores.h>
#include <thread>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#else
#    include <libcpuid/libcpuid.h>
#endif


unsigned getNumberOfPhysicalCPUCores()
{
  return 8;
}
