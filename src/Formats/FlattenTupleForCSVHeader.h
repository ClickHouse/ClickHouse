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

/// Returns true if the actual CSV-family header (as produced by `getCSVHeaderNamesAndTypes` with the
/// same `flatten`) is not guaranteed to be valid UTF-8: the column names (when `with_names`) or the
/// data type names (when `with_types`) contain bytes that are not valid UTF-8. This mirrors
/// `headerNamesMayProduceRawBytes` but accounts for tuple flattening, so a named Tuple field with a
/// non-UTF-8 element name (a dotted leaf name that never appears in the top-level block names) is
/// detected. Used as a `may_produce_raw_bytes` checker for the CSV-family text output formats.
bool csvHeaderNamesMayProduceRawBytes(const Block & sample, bool flatten, bool with_names, bool with_types);

}
