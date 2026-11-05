#pragma once

#include <base/types.h>

namespace DB
{

class IMergeTreeDataPart;
class ColumnsDescription;
struct ProjectionDescription;

/// Whether the default of `column_name`, missing from `projection_part`, can be evaluated there with the
/// same result as on `parent_part`: every column or subcolumn it reads must be stored in the projection
/// part and still current, virtual, or itself a late-add (missing from the parent part too) that is
/// fillable in turn. Shared by the projection read and merge paths.
bool projectionPartCanFillDefault(
    const IMergeTreeDataPart & projection_part,
    const IMergeTreeDataPart & parent_part,
    const ProjectionDescription & projection,
    const ColumnsDescription & parent_table_columns,
    const String & column_name);

}
