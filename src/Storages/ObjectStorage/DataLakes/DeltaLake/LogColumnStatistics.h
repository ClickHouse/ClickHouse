#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Core/Names.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Common/DataLakeColumnStatistics.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>

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

    /// Returns false for a file without a valid `numRecords`, which leaves the read without a row estimate.
    bool addFile(const TableSnapshot::DataFileStatistics & file);

    bool hasDeletionVectors() const { return has_deletion_vectors; }

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
    };

    DB::DataLakeColumnStatistics statistics;
    std::vector<TargetSource> sources;
    /// Bounds of timestamps are ISO 8601 strings.
    DB::FormatSettings format_settings;
    UInt64 deleted_rows = 0;
    bool has_deletion_vectors = false;
};

}

#endif
