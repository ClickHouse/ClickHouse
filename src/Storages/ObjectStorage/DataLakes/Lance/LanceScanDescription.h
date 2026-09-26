#pragma once

#include "config.h"

#if USE_LANCE

#include <Core/Names.h>
#include <Core/Types.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceTableStateSnapshot.h>

#include <optional>
#include <vector>

namespace DB::Lance
{

struct ScanDescription
{
    TableStateSnapshot snapshot;
    Names projection;
    std::optional<String> predicate;
    /// True when every conjunct of the filter was translated into `predicate` (or there is no filter).
    /// Partial AND pushdown sets this to false; LIMIT and countRows fast paths require true.
    bool predicate_is_complete = true;
    size_t max_block_size = 0;
    /// Soft upper bound on rows returned by the Lance scanner (limit + offset from the plan).
    /// 0 / nullopt means unlimited. Safe only when `predicate_is_complete`.
    std::optional<UInt64> limit;
    bool need_only_count = false;
    /// When true (default), Lance returns batches in deterministic fragment order.
    bool scan_in_order = true;
    /// 0 = Lance SDK default; >0 → Scanner::fragment_readahead (effective when unordered).
    UInt32 fragment_readahead = 0;
    /// 0 = Lance SDK default; >0 → Scanner::batch_readahead.
    UInt32 batch_readahead = 0;
    /// 0 = Lance SDK default; >0 → Scanner::io_buffer_size.
    UInt64 io_buffer_size = 0;
    /// 0 selects the bounded automatic default derived by the caller.
    UInt64 queue_capacity = 0;
    /// 0 selects the bounded automatic byte limit derived by the caller.
    UInt64 queue_bytes = 0;
    /// Empty = scan all fragments; otherwise restrict the Lance scanner to these fragment ids.
    std::vector<UInt64> fragment_ids;
};

}

#endif
