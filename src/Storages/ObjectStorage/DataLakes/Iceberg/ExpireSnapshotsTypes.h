#pragma once

#include "config.h"

#if USE_AVRO

#include <optional>
#include <vector>

#include <Core/Types.h>

namespace DB::Iceberg
{

struct ExpireSnapshotsResult
{
    Int64 deleted_data_files_count = 0;
    Int64 deleted_position_delete_files_count = 0;
    Int64 deleted_equality_delete_files_count = 0;
    Int64 deleted_manifest_files_count = 0;
    Int64 deleted_manifest_lists_count = 0;
    Int64 deleted_statistics_files_count = 0;
    bool dry_run = false;
};

struct ExpireSnapshotsOptions
{
    std::optional<Int64> expire_before_ms;
    std::optional<Int64> retention_period_ms;
    std::optional<Int32> retain_last;
    std::optional<std::vector<Int64>> snapshot_ids;
    bool dry_run = false;
};

}

#endif
