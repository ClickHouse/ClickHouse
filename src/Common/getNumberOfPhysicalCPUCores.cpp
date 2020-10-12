#include "getNumberOfPhysicalCPUCores.h"

#if USE_CPUID
#    include <libcpuid/libcpuid.h>
#endif

#include <thread>


unsigned getNumberOfPhysicalCPUCores()
{
    static const unsigned number = []
    {
#       if USE_CPUID
            cpu_raw_data_t raw_data;
            cpu_id_t data;

            /// On Xen VMs, libcpuid returns wrong info (zero number of cores). Fallback to alternative method.
            /// Also, libcpuid does not support some CPUs like AMD Hygon C86 7151.
            if (0 != cpuid_get_raw_data(&raw_data) || 0 != cpu_identify(&raw_data, &data) || data.num_logical_cpus == 0)
                return std::thread::hardware_concurrency();

            unsigned res = data.num_cores * data.total_logical_cpus / data.num_logical_cpus;

            /// Also, libcpuid gives strange result on Google Compute Engine VMs.
            /// Example:
            ///  num_cores = 12,            /// number of physical cores on current CPU socket
            ///  total_logical_cpus = 1,    /// total number of logical cores on all sockets
            ///  num_logical_cpus = 24.     /// number of logical cores on current CPU socket
            /// It means two-way hyper-threading (24 / 12), but contradictory, 'total_logical_cpus' == 1.

            if (res != 0)
                return res;
#       endif

        /// As a fallback (also for non-x86 architectures) assume there are no hyper-threading on the system.
        /// (Actually, only Aarch64 is supported).
        return std::thread::hardware_concurrency();
    }();
    return number;
}
