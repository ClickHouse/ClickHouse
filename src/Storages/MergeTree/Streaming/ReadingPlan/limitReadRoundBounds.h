#pragma once

#include <Storages/MergeTree/Streaming/ReadingPlan/ReadRoundContext.h>
#include <Storages/MergeTree/Streaming/ReadState.h>

#include <map>

namespace DB
{

/// Lowers the per-partition upper bounds of the next read round to about `max_rows_to_read` rows and `max_bytes_to_read` bytes.
/// Partitions are taken in the order of the age of their next part to read, oldest first.
std::map<String, Int64> limitReadRoundBounds(
    const ReadRoundContext & reading_context,
    const ReadState & state,
    std::map<String, Int64> safe_block_numbers);

}
