#pragma once
#include <base/defines.h>
#include <fmt/format.h>

namespace DB
{
struct VFSTraits // TODO myrrc implies disk name being consistent across replicas
{
    explicit VFSTraits(std::string_view disk_name)
        : base_node(fmt::format("/vfs_log/{}", disk_name))
        , locks_node(base_node + "/locks")
        , log_base_node(base_node + "/ops")
        , log_item(log_base_node + "/log-")
    {
    }

    VFSTraits(const VFSTraits & other) = default;
    VFSTraits(VFSTraits && other) = default;

    String base_node;
    String locks_node;
    String log_base_node;
    String log_item;
};
}
