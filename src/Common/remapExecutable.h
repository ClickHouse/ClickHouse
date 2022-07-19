#pragma once
namespace DB
{

/// This function tries to reallocate the code of the running program in a more efficient way.
void remapExecutable();

}
