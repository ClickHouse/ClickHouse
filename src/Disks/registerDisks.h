#pragma once

namespace DB
{

/// @param global_skip_access_check - skip access check regardless regardless
///                                   .skip_access_check config directive (used
///                                   for clickhouse-disks)
void registerDisks(bool global_skip_access_check, bool allow_vfs = false);

}
