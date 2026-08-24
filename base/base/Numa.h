#pragma once

#include <optional>

namespace DB
{

/// return total memory of NUMA nodes the process is bound to
/// if NUMA is not supported or process can use all nodes, std::nullopt is returned
std::optional<size_t> getNumaNodesTotalMemory();

}
