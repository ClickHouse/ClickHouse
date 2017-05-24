#include <Common/getNumberOfPhysicalCPUCores.h>
#include <thread>

#if defined(__x86_64__)

    #include <cpuid/libcpuid.h>
    #include <Common/Exception.h>

    namespace DB { namespace ErrorCodes { extern const int CPUID_ERROR; }}

#endif


unsigned getNumberOfPhysicalCPUCores()
{
#if defined(__x86_64__)

    cpu_raw_data_t raw_data;
    if (0 != cpuid_get_raw_data(&raw_data))
        throw DB::Exception("Cannot cpuid_get_raw_data: " + std::string(cpuid_error()), DB::ErrorCodes::CPUID_ERROR);

    cpu_id_t data;
    if (0 != cpu_identify(&raw_data, &data))
        throw DB::Exception("Cannot cpu_identify: " + std::string(cpuid_error()), DB::ErrorCodes::CPUID_ERROR);

    /// On Xen VMs, libcpuid returns wrong info (zero number of cores). Fallback to alternative method.
    if (data.num_logical_cpus == 0)
        return std::thread::hardware_concurrency();

    unsigned res = data.num_cores * data.total_logical_cpus / data.num_logical_cpus;

    /// Also, libcpuid gives strange result on Google Compute Engine VMs.
    if (res != 0)
        return res;
#endif

    /// As a fallback (also for non-x86 architectures) assume there are no hyper-threading on the system.
    /// (Actually, only Aarch64 is supported).
    return std::thread::hardware_concurrency();
}
