#pragma once

#include <cstdint>


namespace DB
{

/// Strictness mode for loading a table or database.
/// Not ordered: use the predicates below or compare with `==`.
enum class LoadingStrictnessLevel : uint8_t
{
    /// Fresh definition, no existing state: do all sanity checks
    CREATE = 0,
    /// Replay of a definition validated elsewhere (internal queries in DatabaseReplicated)
    SECONDARY_CREATE = 1,
    /// Restore from a backup: user input that creates new state
    RESTORE = 2,
    /// ATTACH stating the whole definition: user input, existing state expected
    ATTACH_FULL_DEFINITION = 3,
    /// Replay of a stored definition, existing state expected
    ATTACH = 4,
    /// We ignore some error on server startup
    FORCE_ATTACH = 5,
    /// Skip all sanity checks (if force_restore_data flag exists)
    FORCE_RESTORE = 6,
};

bool operator<(LoadingStrictnessLevel, LoadingStrictnessLevel) = delete;
bool operator<=(LoadingStrictnessLevel, LoadingStrictnessLevel) = delete;
bool operator>(LoadingStrictnessLevel, LoadingStrictnessLevel) = delete;
bool operator>=(LoadingStrictnessLevel, LoadingStrictnessLevel) = delete;

LoadingStrictnessLevel getLoadingStrictnessLevel(bool attach, bool force_attach, bool force_restore, bool secondary, bool full_definition, bool restore);

/// The definition is user input now, so it gets every check `CREATE` runs.
inline bool isFreshDefinition(LoadingStrictnessLevel level)
{
    return level == LoadingStrictnessLevel::CREATE || level == LoadingStrictnessLevel::ATTACH_FULL_DEFINITION
        || level == LoadingStrictnessLevel::RESTORE;
}

/// Data, paths and ZooKeeper nodes of the object are expected to exist already.
inline bool expectsExistingState(LoadingStrictnessLevel level)
{
    return level == LoadingStrictnessLevel::ATTACH_FULL_DEFINITION || level == LoadingStrictnessLevel::ATTACH
        || level == LoadingStrictnessLevel::FORCE_ATTACH || level == LoadingStrictnessLevel::FORCE_RESTORE;
}

/// Returns true when a table or database is being loaded from previously-validated metadata
/// (server startup, force-restore, or UNDROP TABLE) rather than from a fresh user-supplied
/// query. Only in this case should security checks (e.g. `dynamic_disk_allow_*`) be skipped,
/// because the disk configuration was already validated when the object was originally created.
inline bool isLoadingFromExistingMetadata(LoadingStrictnessLevel level)
{
    return level == LoadingStrictnessLevel::FORCE_ATTACH || level == LoadingStrictnessLevel::FORCE_RESTORE;
}

}
