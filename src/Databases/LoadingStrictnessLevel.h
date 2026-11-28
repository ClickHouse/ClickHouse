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

/// Every decision that depends on how an object is loaded, made once per level. Callers use the predicates below
/// instead of comparing levels, so a new or split level is handled by filling its row here.
struct LoadingTraits
{
    /// The definition is user input now, so it gets every check `CREATE` runs.
    bool fresh_definition;
    /// Data, paths and ZooKeeper nodes of the object are expected to exist already.
    bool expects_existing_state;
    /// An `ATTACH` query while the server runs: the object may take over state that a dropped table or another
    /// replica used, so caches keyed by its UUID or part names can be stale.
    bool attach_query;
    /// Loaded from previously-validated metadata (server startup, force-restore, or `UNDROP TABLE`).
    bool loading_from_existing_metadata;
    /// External services are contacted on first use, so an unreachable one cannot block loading.
    bool defers_external_connections;
};

constexpr LoadingTraits getLoadingTraits(LoadingStrictnessLevel level)
{
    switch (level)
    {
        case LoadingStrictnessLevel::CREATE:
            return {.fresh_definition = true, .expects_existing_state = false, .attach_query = false,
                    .loading_from_existing_metadata = false, .defers_external_connections = false};
        case LoadingStrictnessLevel::SECONDARY_CREATE:
            return {.fresh_definition = false, .expects_existing_state = false, .attach_query = false,
                    .loading_from_existing_metadata = false, .defers_external_connections = false};
        case LoadingStrictnessLevel::RESTORE:
            return {.fresh_definition = true, .expects_existing_state = false, .attach_query = false,
                    .loading_from_existing_metadata = false, .defers_external_connections = true};
        case LoadingStrictnessLevel::ATTACH_FULL_DEFINITION:
            return {.fresh_definition = true, .expects_existing_state = true, .attach_query = true,
                    .loading_from_existing_metadata = false, .defers_external_connections = true};
        case LoadingStrictnessLevel::ATTACH:
            return {.fresh_definition = false, .expects_existing_state = true, .attach_query = true,
                    .loading_from_existing_metadata = false, .defers_external_connections = true};
        case LoadingStrictnessLevel::FORCE_ATTACH:
        case LoadingStrictnessLevel::FORCE_RESTORE:
            return {.fresh_definition = false, .expects_existing_state = true, .attach_query = false,
                    .loading_from_existing_metadata = true, .defers_external_connections = true};
    }
}

inline bool isFreshDefinition(LoadingStrictnessLevel level) { return getLoadingTraits(level).fresh_definition; }
inline bool expectsExistingState(LoadingStrictnessLevel level) { return getLoadingTraits(level).expects_existing_state; }
inline bool isAttachQuery(LoadingStrictnessLevel level) { return getLoadingTraits(level).attach_query; }
/// Only in this case should security checks (e.g. `dynamic_disk_allow_*`) be skipped, because the disk configuration
/// was already validated when the object was originally created.
inline bool isLoadingFromExistingMetadata(LoadingStrictnessLevel level) { return getLoadingTraits(level).loading_from_existing_metadata; }
inline bool defersExternalConnections(LoadingStrictnessLevel level) { return getLoadingTraits(level).defers_external_connections; }

}
