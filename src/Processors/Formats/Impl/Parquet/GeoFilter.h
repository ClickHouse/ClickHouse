#pragma once

#include <Processors/Formats/Impl/Parquet/Reader.h>

#include <memory>
#include <vector>

#include <Common/GeoBbox.h>
#include <Common/StringUtils.h>
#include <Interpreters/Context_fwd.h>

namespace DB { class ActionsDAG; class Block; class KeyCondition; }

namespace DB::Parquet
{

/// A spatial predicate extracted from a WHERE clause that can be used to skip Parquet row groups.
struct SpatialFilter
{
    /// Name of the geometry column being filtered (Parquet column name).
    String geometry_column_name;

    /// Bounding box of the constant query geometry.
    double query_xmin, query_ymin, query_xmax, query_ymax;
};

/// Walk filter_actions_dag looking for calls to spatial functions with isSpatialPredicate()==true
/// where one argument is a column reference and the other is a compile-time constant geometry.
/// Returns one SpatialFilter per qualifying call. Silently skips non-constant arguments.
std::vector<SpatialFilter> extractSpatialFilters(
    const DB::ActionsDAG & filter_dag,
    const DB::Block & sample_block);

/// Returns true if this row group is provably excluded by at least one spatial filter,
/// using only geospatial_statistics.bbox baked into the geometry column's ColumnMetaData.
/// covering.bbox is handled via the standard KeyCondition hyperrectangle path.
bool rowGroupFailsSpatialFilters(
    const parq::RowGroup & rg_meta,
    const std::vector<Reader::PrimitiveColumnInfo> & primitive_columns,
    const std::vector<SpatialFilter> & filters);

/// Build a KeyCondition for: (xmin <= q_xmax) AND (xmax >= q_xmin) AND (ymin <= q_ymax) AND (ymax >= q_ymin).
/// The 4 bbox columns must already be present in extended_sample_block.
/// Returns nullptr if the condition cannot be built (context expired, columns missing).
std::shared_ptr<DB::KeyCondition> buildBboxKeyCondition(
    const SpatialFilter & filter,
    const String & xmin_col, const String & ymin_col,
    const String & xmax_col, const String & ymax_col,
    const DB::ContextPtr & context,
    const DB::Block & extended_sample_block);

}
