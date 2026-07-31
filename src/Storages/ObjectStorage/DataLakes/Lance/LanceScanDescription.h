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
    bool discard_output_columns = false;
};

}

#endif
