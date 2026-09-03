#pragma once

#include "config.h"

#if USE_VORTEX

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/Impl/Vortex/VortexFFIHelpers.h>
#include <Common/Logger.h>

#include <string>
#include <vector>

namespace arrow
{
class Schema;
}

namespace DB
{
struct FormatFilterInfo;
}

namespace DB::Vortex
{

/// What one Vortex scan is asked to do: which columns to read and which rows to keep. Everything
/// the query pipeline pushes into the format ends up here, translated into the library's terms,
/// before `FFI_VortexScanOptions` is filled from it.
///
/// Future pushdowns plug in as new fields: a row range (`FFI_VortexScanOptions` already has one)
/// for buckets and LIMIT, a row selection for lazy materialization
/// (`FormatFilterInfo::rows_to_read`), and PREWHERE steps evaluated on the scan's output.
struct VortexScanPlan
{
    /// The top-level file fields to read, in header order. Empty means no column of the file is
    /// needed and only the row count matters.
    std::vector<std::string> column_names;

    /// The translated part of the WHERE condition, or null. The scan drops the rows it rules out
    /// and skips the statistics zones it excludes; ClickHouse reapplies the full condition to the
    /// result, so this may keep more rows than the condition - never fewer.
    VortexExpressionPtr filter;

    /// How much of the WHERE condition the filter carries, for logs and `ProfileEvents`.
    size_t filter_conjuncts_total = 0;
    size_t filter_conjuncts_pushed = 0;
};

VortexScanPlan planVortexScan(
    const Block & header,
    const arrow::Schema & file_schema,
    const FormatFilterInfo * filter_info,
    const FormatSettings & format_settings,
    const LoggerPtr & log);

}

#endif
