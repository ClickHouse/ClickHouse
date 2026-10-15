#pragma once

#include <QueryPipeline/QueryPipeline.h>

#include <vector>


namespace DB
{

class ReadBuffer;
class WriteBuffer;
class TemporaryDataBuffer;

/// Copies `rows_sources` from `in` to `out` with the rows dropped outside the merging algorithm. Either may be absent:
/// - `row_exists_per_part`: each part's `_row_exists`; writes a skipped row source for each dropped row. Every row source of part k,
///   skipped or not, stands for part k's next row, so a dropped row's row source goes right before part k's next one.
/// - `ttl_kept_rows`: sets the skip flag on the row sources of dropped emitted rows. The Nth row source without
///   the skip flag is emitted row N for every merging algorithm (`ColumnGathererTransform` relies on it too).
/// Returns the number of row sources written without the skip flag.
size_t rewriteRowsSources(
    ReadBuffer & in, WriteBuffer & out, std::vector<QueryPipeline> row_exists_per_part, TemporaryDataBuffer * ttl_kept_rows);

}
