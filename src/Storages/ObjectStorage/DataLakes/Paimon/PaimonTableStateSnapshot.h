#pragma once
#include <config.h>

#if USE_AVRO

#include <memory>
#include <optional>
#include <base/types.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

namespace Paimon
{

/// Immutable table state snapshot for Paimon.
/// Lives in the top-level ::Paimon namespace (same as other Paimon types),
/// following the same pattern as DeltaLake::TableStateSnapshot.
struct TableStateSnapshot
{
    Int64 snapshot_id{-1};
    Int64 schema_id{-1};
    String base_manifest_list_path;
    String delta_manifest_list_path;
    String commit_kind;
    Int64 commit_time_millis{0};

    std::optional<Int64> total_record_count;
    std::optional<Int64> delta_record_count;
    std::optional<Int64> changelog_record_count;
    std::optional<Int64> watermark;

    TableStateSnapshot() = default;

    TableStateSnapshot(
        Int64 snapshot_id_,
        Int64 schema_id_,
        String base_manifest_list_path_,
        String delta_manifest_list_path_,
        String commit_kind_,
        Int64 commit_time_millis_,
        std::optional<Int64> total_record_count_ = std::nullopt,
        std::optional<Int64> delta_record_count_ = std::nullopt,
        std::optional<Int64> changelog_record_count_ = std::nullopt,
        std::optional<Int64> watermark_ = std::nullopt)
        : snapshot_id(snapshot_id_)
        , schema_id(schema_id_)
        , base_manifest_list_path(std::move(base_manifest_list_path_))
        , delta_manifest_list_path(std::move(delta_manifest_list_path_))
        , commit_kind(std::move(commit_kind_))
        , commit_time_millis(commit_time_millis_)
        , total_record_count(total_record_count_)
        , delta_record_count(delta_record_count_)
        , changelog_record_count(changelog_record_count_)
        , watermark(watermark_)
    {
    }

    bool isCompact() const { return commit_kind == "COMPACT"; }

    bool operator==(const TableStateSnapshot & other) const
    {
        return snapshot_id == other.snapshot_id && schema_id == other.schema_id;
    }

    bool operator!=(const TableStateSnapshot & other) const { return !(*this == other); }

    void serialize(DB::WriteBuffer & out) const;
    static TableStateSnapshot deserialize(DB::ReadBuffer & in, int datalake_state_protocol_version);
};

using TableStateSnapshotPtr = std::shared_ptr<const TableStateSnapshot>;

}

namespace DB
{

/// Backward-compatible aliases so that PaimonMetadata.h/cpp can continue
/// using the old names without mass-renaming every occurrence.
using PaimonTableState = Paimon::TableStateSnapshot;
using PaimonTableStatePtr = Paimon::TableStateSnapshotPtr;

}

#endif
