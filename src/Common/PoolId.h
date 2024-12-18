#pragma once

#include <Common/Priority.h>

namespace DB
{

/// Indices and priorities of `AsyncLoader` pools.

/// The most important difference from regular ThreadPools is priorities of pools:
///  * Pools that have different priorities do NOT run jobs simultaneously (with small exception due to dynamic prioritization).
///  * Pools with lower priority wait for all jobs in higher priority pools to be done.

/// Note that pools also have different configurable sizes not listed here. See `Context::getAsyncLoader()` for details.

/// WARNING: `*PoolId` values must be unique and sequential w/o gaps.

/// Used for executing load jobs that are waited for by queries or in case of synchronous table loading.
constexpr size_t TablesLoaderForegroundPoolId = 0;
constexpr Priority TablesLoaderForegroundPriority{0};

/// Has lower priority and is used by table load jobs.
constexpr size_t TablesLoaderBackgroundLoadPoolId = 1;
constexpr Priority TablesLoaderBackgroundLoadPriority{1};

/// Has even lower priority and is used by startup jobs.
/// NOTE: This pool is required to begin table startup only after all tables are loaded.
/// NOTE: Which is needed to prevent heavy merges/mutations from consuming all the resources, slowing table loading down.
constexpr size_t TablesLoaderBackgroundStartupPoolId = 2;
constexpr Priority TablesLoaderBackgroundStartupPriority{2};

}
