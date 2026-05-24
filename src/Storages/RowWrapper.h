#pragma once

#include <Core/Names.h>
#include <Storages/ColumnsDescription.h>

#include <optional>
#include <vector>


namespace DB
{

/// A Row-typed column whose MATERIALIZED expression is tuple(a, b, ...) of plain
/// column references matching the Row's fields 1-for-1, e.g.
/// `combined Row(a String, b UInt32) MATERIALIZED (a, b)` wraps columns {a, b}.
struct RowWrapperInfo
{
    String wrapper_name;
    Names wrapped_columns;  /// In declared order; matches Row(...) fields.
};

/// Returns wrapper info if `column` is such a wrapper, otherwise std::nullopt.
std::optional<RowWrapperInfo> tryDescribeRowWrapper(const ColumnDescription & column);

/// All Row wrappers in `columns`. Throws if a source column is wrapped twice
/// (the optimizer rule assumes wrappers are disjoint).
std::vector<RowWrapperInfo> collectRowWrappers(const ColumnsDescription & columns);

}
