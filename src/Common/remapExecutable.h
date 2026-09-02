#pragma once

#include <cstdlib>

namespace DB
{

/// This function tries to reallocate the code of the running program in a more efficient way.
/// @return size of remapped area.
size_t remapExecutable();

}
