#if defined(OS_LINUX)
#include <Common/ProcfsMetricsProvider.h>

#include <iostream>
#include <linux/taskstats.h>
#endif


#if defined(OS_LINUX)
int main(int argc, char ** argv)
{
    using namespace DB;

    size_t num_iterations = argc >= 2 ? std::stoull(argv[1]) : 1000000;

    if (!ProcfsMetricsProvider::isAvailable())
    {
        std::cerr << "Procfs statistics is not available on this system" << std::endl;
        return -1;
    }

    ProcfsMetricsProvider stats_provider(0);

    ::taskstats stats;
    stats_provider.getTaskStats(stats);

    const auto start_cpu_time = stats.cpu_run_virtual_total;
    for (size_t i = 0; i < num_iterations; ++i)
    {
        stats_provider.getTaskStats(stats);
    }

    if (num_iterations)
        std::cerr << stats.cpu_run_virtual_total - start_cpu_time << '\n';
    return 0;
}
#else
int main()
{
}
#endif
