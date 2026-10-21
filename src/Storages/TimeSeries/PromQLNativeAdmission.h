#pragma once

#include <base/arithmeticOverflow.h>
#include <base/types.h>

#include <optional>


namespace DB
{

enum class PromQLNativeAdmissionReason
{
    Admitted,
    SeriesLimit,
    GridCellLimit,
    GridCellOverflow,
};

struct PromQLNativeAdmissionResult
{
    PromQLNativeAdmissionReason reason;
    std::optional<UInt64> grid_cells;
};

/// Workload admission only; this does not estimate the query's peak memory.
/// Zero disables the grid-cell limit, not storage/AST eligibility checks.
inline PromQLNativeAdmissionResult evaluatePromQLNativeWorkloadAdmission(
    UInt64 selected_series, UInt64 evaluation_points, UInt64 max_series, UInt64 max_grid_cells)
{
    if (selected_series > max_series)
        return {PromQLNativeAdmissionReason::SeriesLimit, {}};

    if (!max_grid_cells)
        return {PromQLNativeAdmissionReason::Admitted, {}};

    UInt64 grid_cells = 0;
    if (common::mulOverflow(selected_series, evaluation_points, grid_cells))
        return {PromQLNativeAdmissionReason::GridCellOverflow, {}};

    if (grid_cells > max_grid_cells)
        return {PromQLNativeAdmissionReason::GridCellLimit, grid_cells};

    return {PromQLNativeAdmissionReason::Admitted, grid_cells};
}

}
