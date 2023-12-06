#pragma once
#include <base/defines.h>

namespace DB
{

struct VFSTraits
{
    VFSTraits(std::string_view disk_name)
        : VFS_BASE_NODE(fmt::format("/vfs_log/{}", disk_name))
        , VFS_LOCKS_NODE(VFS_BASE_NODE + "/locks")
        , VFS_LOG_BASE_NODE(VFS_BASE_NODE + "/ops")
        , VFS_LOG_ITEM(VFS_LOG_BASE_NODE + "/log-")
    {
    }

    VFSTraits(const VFSTraits & other) =default;

    String VFS_BASE_NODE;
    String VFS_LOCKS_NODE;
    String VFS_LOG_BASE_NODE;
    String VFS_LOG_ITEM;
};

}
