#pragma once

#include <Functions/IFunction.h>

namespace DB
{

/// Integer implementation selected by `divide` when `sql_compatible_division_for_integers` is enabled.
FunctionPtr createFunctionExactDivide();

/// Return whether two constant integer arguments make checked exact division throw.
bool constantExactIntegerDivisionCanThrow(const ColumnsWithTypeAndName & arguments);

}
