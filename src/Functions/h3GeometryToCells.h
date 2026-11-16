#pragma once

#include "config.h"

#if USE_H3

#include <Columns/ColumnsNumber.h>
#include <Functions/CancellationBudget.h>
#include <Functions/geometryConverters.h>

namespace DB
{

/// Appends the H3 cells covering `multi_polygon` to `dst_data`, charging `budget` for every candidate cell
/// examined so that it can check for a timeout or `KILL QUERY` mid-row, including while the search rejects
/// candidates without producing any. `flags` is an H3 containment mode, 0 being CONTAINMENT_CENTER.
void appendH3Cells(
    const SphericalMultiPolygon & multi_polygon,
    UInt8 resolution,
    UInt32 flags,
    std::string_view function_name,
    CancellationBudget & budget,
    ColumnUInt64 & dst_data);

}

#endif
