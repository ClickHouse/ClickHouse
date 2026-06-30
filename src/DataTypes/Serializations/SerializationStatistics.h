#pragma once

#include <cstddef>

namespace DB
{

class IColumn;

/** Lightweight, always-available statistics about a column's content, used to choose the
  * serialization kind of the column (currently sparse) from a sample of its data.
  *
  * Extracted from the former `SerializationInfo::Data`. These statistics are computed cheaply by
  * sampling and are independent of the (optional) column statistics framework, so the serialization
  * kind can be chosen even when statistics are not materialized
  * (e.g. `materialize_statistics_on_insert = 0`).
  *
  * When an equivalent external statistic exists for a column (e.g. `StatisticsBasic` carrying the
  * default count) it can serve as the authoritative source instead, in which case these lightweight
  * statistics do not need to be persisted in `serialization.json`
  * (see `MergeTreeSerializationInfoVersion::WITH_EXTERNAL_STATISTICS`).
  *
  * Currently tracks the number of rows and the (sampled) number of default values. Designed to be
  * extended with a sampled distinct-value estimate (`num_unique`) for automatic `LowCardinality`:
  * such an estimate would be sampled in `add(const IColumn &)` the same way `num_defaults` is, and
  * combined across parts conservatively on merge (it is not additive like `num_defaults`).
  */
struct SerializationStatistics
{
    size_t num_rows = 0;
    size_t num_defaults = 0;

    /// Add information sampled from a column.
    void add(const IColumn & column);
    /// Merge counts from another part/block (additive).
    void add(const SerializationStatistics & other);
    /// Account for `length` rows that are all default (e.g. a column missing from a source part).
    void addDefaults(size_t length);
};

}
