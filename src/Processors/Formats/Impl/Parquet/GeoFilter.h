#pragma once

#include <Processors/Formats/Impl/Parquet/Reader.h>

#include <string>
#include <vector>

namespace DB { class ActionsDAG; class Block; }

namespace DB::Parquet
{

/// A spatial predicate extracted from a WHERE clause that can be used to skip Parquet row groups.
struct SpatialFilter
{
    /// Name of the WKB geometry column being filtered (Parquet column name).
    std::string geometry_column_name;

    /// Bounding box of the constant query geometry.
    double query_xmin, query_ymin, query_xmax, query_ymax;
};

/// Walk filter_actions_dag looking for calls to known spatial functions
/// (st_intersects, st_intersects_extent, st_contains, st_within, st_covers, st_coveredby,
/// st_containsproperly, st_touches, st_crosses, st_overlaps) where one argument
/// is a column reference and the other is a compile-time constant WKB blob.
/// Returns one SpatialFilter per qualifying call. Silently skips non-constant arguments.
std::vector<SpatialFilter> extractSpatialFilters(
    const DB::ActionsDAG & filter_dag,
    const DB::Block & sample_block);

/// Returns true if this row group is provably excluded by at least one spatial filter.
/// Uses geospatial_statistics.bbox from ColumnMetaData when available; falls back to
/// covering.bbox column statistics (Reader::PrimitiveColumnInfo::covering_bbox_indices) otherwise.
bool rowGroupFailsSpatialFilters(
    const parq::RowGroup & rg_meta,
    const std::vector<Reader::PrimitiveColumnInfo> & primitive_columns,
    const std::vector<SpatialFilter> & filters);

}
