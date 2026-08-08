#pragma once

#include <Core/Names.h>

namespace DB
{

class Block;

/// Builds the names and type names for a CSV-family WithNames(AndTypes) header.
/// When `flatten` is true, every (named or unnamed) Tuple column is expanded recursively into
/// its leaf fields (dotted names, e.g. `User.ID`, and leaf type names), mirroring how
/// SerializationTuple::serializeTextCSV writes tuple values into separate columns. This keeps the
/// header width equal to the data width. When `flatten` is false, top-level names and types are
/// returned unchanged.
void getCSVHeaderNamesAndTypes(const Block & sample, bool flatten, Names & names, Names & type_names);

}
