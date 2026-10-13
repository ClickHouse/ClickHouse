#include <base/Numa.h>

#if defined(OS_LINUX)
#    include <charconv>
#    include <cstdint>
#    include <filesystem>
#    include <fstream>
#    include <stdexcept>
#    include <string>

#    include <linux/mempolicy.h>
#    include <sys/syscall.h>
#    include <unistd.h>
#endif

namespace DB
{

std::optional<size_t> getNumaNodesTotalMemory()
{
#if defined(OS_LINUX)
    /// The kernel supports at most 1024 NUMA nodes (`CONFIG_NODES_SHIFT` <= 10).
    static constexpr size_t max_nodes = 1024;
    static constexpr size_t bits_per_word = 64;
    uint64_t nodemask[max_nodes / bits_per_word] = {};
    int mode = 0;

    /// Fails with `ENOSYS` if the kernel is built without NUMA support.
    if (syscall(SYS_get_mempolicy, &mode, nodemask, max_nodes, nullptr, 0) != 0)
        return {};

    /// With mode flags such as `MPOL_F_RELATIVE_NODES` the kernel returns the requested mask, not the effective nodes.
    if (mode != MPOL_BIND)
        return {};

    size_t total_memory = 0;
    bool excludes_node_with_memory = false;
    for (const auto & entry : std::filesystem::directory_iterator("/sys/devices/system/node"))
    {
        const std::string name = entry.path().filename();
        size_t node = 0;
        const char * number_end = name.data() + name.size();
        if (!name.starts_with("node") || std::from_chars(name.data() + 4, number_end, node).ptr != number_end || node >= max_nodes)
            continue;

        /// The first line is "Node <N> MemTotal:   <size> kB".
        std::ifstream meminfo(entry.path() / "meminfo");
        std::string node_word;
        std::string key;
        size_t node_id = 0;
        size_t memory_kb = 0;
        if (!(meminfo >> node_word >> node_id >> key >> memory_kb) || key != "MemTotal:")
            throw std::runtime_error("Cannot read MemTotal from " + (entry.path() / "meminfo").string());

        if ((nodemask[node / bits_per_word] >> (node % bits_per_word)) & 1)
            total_memory += memory_kb * 1024;
        else if (memory_kb)
            excludes_node_with_memory = true;
    }

    if (excludes_node_with_memory)
        return total_memory;
#endif
    return {};
}

}
