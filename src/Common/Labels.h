#pragma once

namespace Labels
{
    enum class KeeperOperation
    {
        Create,
        Remove,
        RemoveRecursive,
        Exists,
        Get,
        Set,
        List,
        Check,
        Sync,
        Reconfig,
        Multi,
    };
}
