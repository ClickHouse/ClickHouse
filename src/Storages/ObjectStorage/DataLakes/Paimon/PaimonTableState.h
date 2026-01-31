#pragma once
#include <config.h>

#if USE_AVRO

#include <memory>
#include <optional>
#include <base/types.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

/// Immutable table state snapshot for Paimon.
/// Once created, it should never be modified.
/// Used for snapshot isolation and lock-free reads.
struct PaimonTableState
{
    const Int64 snapshot_id;
    const Int64 schema_id;
    const String base_manifest_list_path;
    const String delta_manifest_list_path;
    const Int64 commit_time_millis;

    /// Optional pre-computed values for quick checks
    const std::optional<Int64> total_record_count;
    const std::optional<Int64> delta_record_count;
    const std::optional<Int64> changelog_record_count;
    const std::optional<Int64> watermark;

    PaimonTableState(
        Int64 snapshot_id_,
        Int64 schema_id_,
        String base_manifest_list_path_,
        String delta_manifest_list_path_,
        Int64 commit_time_millis_,
        std::optional<Int64> total_record_count_ = std::nullopt,
        std::optional<Int64> delta_record_count_ = std::nullopt,
        std::optional<Int64> changelog_record_count_ = std::nullopt,
        std::optional<Int64> watermark_ = std::nullopt)
        : snapshot_id(snapshot_id_)
        , schema_id(schema_id_)
        , base_manifest_list_path(std::move(base_manifest_list_path_))
        , delta_manifest_list_path(std::move(delta_manifest_list_path_))
        , commit_time_millis(commit_time_millis_)
        , total_record_count(total_record_count_)
        , delta_record_count(delta_record_count_)
        , changelog_record_count(changelog_record_count_)
        , watermark(watermark_)
    {
    }

    bool operator==(const PaimonTableState & other) const
    {
        return snapshot_id == other.snapshot_id && schema_id == other.schema_id;
    }

    bool operator!=(const PaimonTableState & other) const { return !(*this == other); }
};

using PaimonTableStatePtr = std::shared_ptr<const PaimonTableState>;

/// Type alias for DataLake table state variant
using PaimonTableStateSnapshot = PaimonTableState;

}

#endif

