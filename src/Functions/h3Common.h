#pragma once

#include "config.h"

#if USE_H3

#include <constants.h>
#include <h3api.h>

#include <Interpreters/Context_fwd.h>

#include <base/types.h>

#include <string_view>

namespace DB
{

/// Converts a vertex of a geometry - longitude and latitude in degrees, as ClickHouse geometry types store
/// them - to the H3 `LatLng` in radians, rejecting coordinates outside the domain H3 is defined on.
///
/// The check is not cosmetic. H3 itself only rejects coordinates that are not finite, and its polyfill
/// algorithms derive the part of the grid they have to walk from the polygon's bounding box, so one
/// out-of-domain vertex is enough to defeat that heuristic and make the walk cover the whole grid at the
/// target resolution. The walk happens inside a single library call, which observes neither
/// `max_execution_time` nor `KILL QUERY`.
LatLng h3LatLngFromDegrees(Float64 longitude_degrees, Float64 latitude_degrees, std::string_view function_name);

/// H3 functions that take a cell index don't behave well when given an invalid index.
/// Let's be careful to never call h3 functions on invalid cell indices.
/// Setting functions_h3_default_if_invalid controls whether our functions should throw or return
/// default value on invalid inputs.
/// This struct is a convenience utility for checking the setting and checking whether h3 cell index
/// is valid (isValidCell(h)).
/// Use it before calling h3 functions that take cells as input.
///
/// (Specifically, I've seen h3 functions:
///  * hit an `assert` in gridPathCells on this query:
///      SELECT h3Line(7258718383300032567, 7258718383300032567)
///  * get stuck forever in gridDisk on h3kRing(a, 20), where a took some 128 random values;
///    it didn't get stuck when ran on any one of those values, only on the whole column at once.
///  * segfault in getIcosahedronFaces.
/// Some of these failures were inconsistent or depended on other values in the input IColumn,
/// which means there's probably memory corruption.)
struct H3Validator
{
    bool throw_on_error;

    explicit H3Validator(const ContextPtr & context);

    bool validateCell(uint64_t h) const;
    bool validateEdge(uint64_t h) const;
};

}

#endif
