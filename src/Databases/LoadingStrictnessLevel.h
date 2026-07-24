#pragma once

#include <cstdint>


namespace DB
{

/// Strictness mode for loading a table or database
enum class LoadingStrictnessLevel : uint8_t
{
    /// Do all possible sanity checks
    CREATE = 0,
    /// Skip some sanity checks (for internal queries in DatabaseReplicated; for RESTORE)
    SECONDARY_CREATE = 1,
    /// Expect existing paths on FS and in ZK for ATTACH query
    ATTACH = 2,
    /// We ignore some error on server startup
    FORCE_ATTACH = 3,
    /// Skip all sanity checks (if force_restore_data flag exists)
    FORCE_RESTORE = 4,
};

LoadingStrictnessLevel getLoadingStrictnessLevel(bool attach, bool force_attach, bool force_restore, bool secondary);

}
