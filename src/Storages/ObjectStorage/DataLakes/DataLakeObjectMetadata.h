#pragma once

#include <base/types.h>

#include <memory>
#include <optional>

namespace DB
{

class ActionsDAG;
class NamesAndTypesList;

template <typename T, UInt8 small_set_size>
class RoaringBitmapWithSmallSet;

struct DataLakeObjectMetadata
{
    std::shared_ptr<ActionsDAG> schema_transform;

    using ExcludedRows = RoaringBitmapWithSmallSet<size_t, 32>;
    using ExcludedRowsPtr = std::shared_ptr<ExcludedRows>;

    /// Excluded rows indexes from selection vector
    ExcludedRowsPtr excluded_rows;

    /// If set, only the rows with these indexes are read, and all other rows of the object are skipped.
    /// Used for the change data feed of Delta Lake, where a pair of deletion vectors selects the changed rows.
    ExcludedRowsPtr selected_rows;

    /// Columns that are read from this object in addition to the columns of the format header.
    /// E.g. a CDC file of the Delta Lake change data feed contains the `_change_type` column, which other
    /// data files do not contain, and which `schema_transform` takes from the file.
    /// Null if there are no such columns.
    std::shared_ptr<const NamesAndTypesList> additional_file_columns;

    /// The number of rows of the object as the table metadata records it (before excluding `excluded_rows`), if known.
    std::optional<UInt64> num_rows;
};

}
