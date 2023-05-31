#pragma once

namespace DB
{

/// Indices of `AsyncLoader` pools.
/// Note that pools that have different `AsyncLoader` priorities do NOT run jobs simultaneously.
/// (It is possible only for the short transition period after dynamic prioritization due to waiting query).
/// So the following pools cannot be considered independent.
enum AsyncLoaderPoolId
{
    /// Used for executing load jobs that are waited for by queries or in case of synchronous table loading.
    Foreground,

    /// Has lower priority and is used by table load jobs.
    BackgroundLoad,

    /// Has even lower priority and is used by startup jobs.
    /// NOTE: This pool is required to begin table startup only after all tables are loaded.
    /// NOTE: Which is needed to prevent heavy merges/mutations from consuming all the resources, slowing table loading down.
    BackgroundStartup,
};

}
