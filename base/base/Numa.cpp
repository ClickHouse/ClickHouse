#include <base/Numa.h>

#include "config.h"

#if USE_NUMACTL
#    include <numa.h>
#endif

namespace DB
{

std::optional<size_t> getNumaNodesTotalMemory()
{
    std::optional<size_t> total_memory;
#if USE_NUMACTL
    if (numa_available() != -1)
    {
        auto * membind = numa_get_membind();
        if (!numa_bitmask_equal(membind, numa_all_nodes_ptr))
        {
            total_memory.emplace(0);
            auto max_node = numa_max_node();
            for (int i = 0; i <= max_node; ++i)
            {
                if (numa_bitmask_isbitset(membind, i))
                    *total_memory += numa_node_size(i, nullptr);
            }
        }

        numa_bitmask_free(membind);
    }

#endif
    return total_memory;
}

}
