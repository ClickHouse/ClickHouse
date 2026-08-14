#pragma once

#include "config.h"

#if USE_AVRO

#include <Core/Types.h>
#include <optional>
#include <vector>
#include <Storages/ObjectStorage/DataLakes/Iceberg/DataFileStatistics.h>

namespace DB
{

/// Column-level statistics for a single Iceberg data file stored in Iceberg wire format.
/// Bounds are pre-serialized to bytes so the struct can be persisted to sidecar Avro files
/// and used directly at manifest-commit time without requiring the original ClickHouse
/// DataFileStatistics or a live Block schema.
struct IcebergSerializedFileStats
{
    Int64 record_count = 0;
    Int64 file_size_in_bytes = 0;

    /// field_id → compressed byte size of column in the file
    std::vector<std::pair<Int32, Int64>> column_sizes;
    /// field_id → number of null values in the file
    std::vector<std::pair<Int32, Int64>> null_value_counts;
    /// field_id → Iceberg-serialized lower bound (same binary format as manifest)
    std::vector<std::pair<Int32, std::vector<uint8_t>>> lower_bounds;
    /// field_id → Iceberg-serialized upper bound (same binary format as manifest)
    std::vector<std::pair<Int32, std::vector<uint8_t>>> upper_bounds;
};

/// One entry describing a data file that will be registered in an Iceberg manifest.
/// Carries per-file statistics so that each manifest entry gets accurate metadata
/// (column sizes, null counts, min/max bounds, record count, file size).
struct IcebergDataFileEntry
{
    String path;
    Int64 record_count = 0;
    Int64 file_size_in_bytes = 0;

    /// Per-file column statistics (null counts, min/max bounds, column sizes).
    /// Pass std::nullopt when statistics are not available or not yet computed.
    std::optional<DataFileStatistics> statistics;
};

}

#endif
