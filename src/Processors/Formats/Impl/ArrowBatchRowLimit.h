#pragma once

#include "config.h"

#if USE_ARROW || USE_PARQUET

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>

namespace DB
{

/// The largest `n` such that rows [begin, begin + n) of `column` stay within `max_batch_bytes`; writers
/// split an oversized chunk accordingly. Returns 0 only when the first row does not fit on its own, which
/// the writer then lets Arrow reject. `ColumnConst` and the sparse/replicated representations are not
/// handled; callers materialize first.
///
/// `LowCardinality` is measured as materialized, which only over-splits when it is written as an Arrow
/// dictionary. A dictionary larger than one buffer is not covered at all: its bytes do not depend on the
/// row count, because `ColumnLowCardinality::insertRangeFrom` keeps a shared source dictionary whole, so
/// every slice re-emits it. Arrow rejects such a chunk.
///
/// A type with no Arrow equivalent is not covered either, since only serializing its values reveals their
/// encoded size. Both uncovered shapes are rejected by the writer instead of being split.
///
/// With `variant_json_settings`, a `Variant` is measured as written with `output_format_arrow_variant_as_json_string`:
/// the JSON text of each value, serialized with these settings. This serializes the values once more.
size_t maxRowsFittingOneArrowBatch(
    const IColumn & column,
    const DataTypePtr & type,
    size_t begin,
    size_t end,
    bool fixed_string_as_fixed_byte_array,
    UInt64 max_batch_bytes = MAX_ARROW_BUFFER_SIZE,
    const FormatSettings * variant_json_settings = nullptr);

/// The largest `n` such that rows [begin, begin + n) of every column stay within `max_batch_bytes`: the
/// number of rows of the next record batch of a chunk. The work is proportional to the returned number of
/// rows rather than to `end - begin`, so splitting a chunk into many batches stays linear.
size_t maxRowsFittingOneArrowBatch(
    const Columns & columns,
    const DataTypes & types,
    size_t begin,
    size_t end,
    bool fixed_string_as_fixed_byte_array,
    UInt64 max_batch_bytes,
    const FormatSettings * variant_json_settings = nullptr);

}

#endif
