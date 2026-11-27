#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Core/Names.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Common/DataLakeColumnStatistics.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>

#include <memory>

namespace DB
{
class ColumnsDescription;
}

namespace DeltaLake
{

/// Extracts the statistics of the requested columns from the `add` actions of the data files that a read keeps after
/// pruning (`numRecords`, `minValues`, `maxValues`, `nullCount`, partition values and the cardinality of deletion
/// vectors) and merges them with `DataLakeColumnStatistics`.
class LogColumnStatistics
{
public:
    /// Keeps the columns of `column_names` that are physical and not `Array`, `Map`, `Tuple` or `Variant`.
    LogColumnStatistics(
        const DB::Names & column_names,
        const DB::ColumnsDescription & columns,
        const DB::Names & partition_columns,
        const DB::NameToNameMap & physical_names_map,
        DB::ContextPtr context);
    ~LogColumnStatistics();

    /// Returns false for a file without a valid `numRecords`, which leaves the read without a row estimate.
    bool addFile(const TableSnapshot::DataFileStatistics & file);

    [[nodiscard]] bool hasDeletionVectors() const { return has_deletion_vectors; }

    /// Sets the rows of `estimate`, without the rows that deletion vectors delete, and its estimator.
    void finalize(DB::DataLakeReadEstimate & estimate);

private:
    /// Where the statistics of a target are in the log; aligned with the targets of `statistics`.
    struct TargetSource
    {
        /// The key of the column in `minValues`, `maxValues` and `nullCount`.
        String physical_name;
        /// Index in the partition columns, for a partition column: the log has no statistics of it.
        std::optional<size_t> partition_index;
        /// The type can have min/max statistics (numbers and dates).
        bool tracks_min_max;
        /// The type the bounds are parsed with: `DateTime64` in UTC, as the ticks in the data files do not depend on a time zone.
        DB::DataTypePtr bound_type;
    };

    struct Parser;

    DB::DataLakeColumnStatistics statistics;
    std::vector<TargetSource> sources;
    /// Bounds of timestamps are ISO 8601 strings.
    DB::FormatSettings format_settings;
    /// The JSON parser of the `stats`, reused over the files.
    std::unique_ptr<Parser> parser;
    UInt64 deleted_rows = 0;
    bool has_deletion_vectors = false;
};

}

#endif
