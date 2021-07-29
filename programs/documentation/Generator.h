#pragma once

#include <cstddef>
#include <ostream>
#include <string>
#include <Documentation/SimpleGenerator.h>

namespace DB
{


// Print documentation
// Format of input is: group name path
// where 
// - group means number of functions, types etc, with common part (for example table_functions)
// for some objects group means system database, which they belong to
// - name is a name of function, type etc.
// -path (optional) shows where documentation should be put (be default it is printed)
int entryDocumentationGenerator(int argc, char** argv);

}
