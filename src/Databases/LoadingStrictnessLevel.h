#pragma once

namespace DB
{

/// Strictness mode for loading a table or database
enum class LoadingStrictnessLevel
{
    /// Do all possible sanity checks
    CREATE = 0,
    /// Expect existing paths on FS and in ZK for ATTACH query
    ATTACH = 1,
    /// We ignore some error on server startup
    FORCE_ATTACH = 2,
    /// Skip all sanity checks (if force_restore_data flag exists)
    FORCE_RESTORE = 3,
};

LoadingStrictnessLevel getLoadingStrictnessLevel(bool attach, bool force_attach, bool force_restore);

}
