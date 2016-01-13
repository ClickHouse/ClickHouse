#include <DB/Common/getNumberOfPhysicalCPUCores.h>

#if defined(__x86_64__)

	#include <cpuid/libcpuid.h>
	#include <DB/Common/Exception.h>

	namespace DB { namespace ErrorCodes { extern const int CPUID_ERROR; }}

#elif defined(__aarch64__)

	#include <thread>

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

	return data.num_cores * data.total_logical_cpus / data.num_logical_cpus;

#elif defined(__aarch64__)
	/// Считаем, что на этой системе нет hyper-threading.
	return std::thread::hardware_concurrency();
#endif
}
