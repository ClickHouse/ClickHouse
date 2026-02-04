#pragma once

#include "config.h"

#if USE_H3

#include <constants.h>
#include <h3api.h>

namespace DB
{

/// H3 functions that take a cell index don't behave well when given an invalid index.
/// Let's be careful to never call h3 functions on invalid cell indices.
/// This function throws if !isValidCell(h).
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
void validateH3Cell(uint64_t h);

}

#endif
