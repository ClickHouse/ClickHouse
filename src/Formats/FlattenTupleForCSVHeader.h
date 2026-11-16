#pragma once

#include <Core/Names.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

class Block;
class CaseAwareBlockNameMap;
struct FormatSettings;

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

/// The input side of `getCSVHeaderNamesAndTypes` with `flatten`: every run of `names` that is exactly the
/// flattened element names of a bare Tuple column of `header`, none of which is a column itself, is replaced
/// by that column's name, so the column is read from the whole run. `type_names`, when it has the same size,
/// is collapsed in step to the column's type name; with `check_types` a different element type throws.
/// Names are compared like column names, with `input_format_column_name_matching_mode`.
/// With `header_detection` (a first row that may be data) the column check is exact and a run two Tuple
/// columns can claim is left as is, so it never throws.
void collapseCSVHeaderTupleNames(
    const Block & header,
    const CaseAwareBlockNameMap & column_indexes_by_names,
    const FormatSettings & format_settings,
    VectorWithMemoryTracking<String> & names,
    VectorWithMemoryTracking<String> & type_names,
    bool check_types,
    bool header_detection);

}
