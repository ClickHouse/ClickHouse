#pragma once

#include <Core/Types.h>


namespace DB::PrometheusQueryToSQL
{

/// Represents the sort order of the rows in a prometheus query's result.
struct ResultSorting
{
    enum class Mode
    {
        /// Order of rows is not specified.
        UNORDERED,

        /// Rows are ordered by values of `sorting_tags`, then by all tags.
        /// This mode corresponds to prometheus function sort_by_label().
        ORDERED_BY_TAGS,

        /// Rows are ordered by metric values.
        /// This mode corresponds to prometheus function sort().
        ORDERED_BY_VALUE,
    };

    Mode mode = Mode::UNORDERED;

    /// Tags to sort by - if the mode is `ORDERED_BY_TAGS`.
    Strings sorting_tags;

    /// 1 for ASC, -1 for DESC
    int direction = 0;
};

}
