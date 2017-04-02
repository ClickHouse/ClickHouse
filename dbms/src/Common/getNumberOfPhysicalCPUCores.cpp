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
    if (data.num_cores == 0 || data.total_logical_cpus == 0 || data.num_logical_cpus == 0)
        return std::thread::hardware_concurrency();

    return data.num_cores * data.total_logical_cpus / data.num_logical_cpus;

#elif defined(__aarch64__)
    /// Assuming there are no hyper-threading on the system.
    return std::thread::hardware_concurrency();
#endif
}
