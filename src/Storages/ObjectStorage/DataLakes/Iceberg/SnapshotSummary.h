#pragma once

#include "config.h"

#if USE_AVRO

#include <optional>
#include <variant>
#include <base/types.h>
#include <Poco/JSON/Object.h>

namespace DB::Iceberg
{

struct SnapshotSummaryUpdateAppend
{
    Int64 added_files = 0;
    Int64 added_records = 0;
    Int64 added_files_size = 0;
    Int64 num_partitions = 0;
};

/// An Iceberg `overwrite` can either rewrite data (engines like Spark: add data files and
/// remove old ones) or add position-delete files (how ClickHouse expresses row deletes), so
/// this struct holds both sets of deltas; the irrelevant ones stay zero.
struct SnapshotSummaryUpdateOverwrite
{
    Int64 added_files = 0;
    Int64 added_records = 0;
    Int64 added_files_size = 0;
    Int64 added_delete_files = 0;
    Int64 added_position_deletes = 0;
    Int64 deleted_data_files = 0;
    Int64 removed_records = 0;
    Int64 removed_files_size = 0;
    Int64 num_partitions = 0;
};

struct SnapshotSummaryUpdateDelete
{
    Int64 deleted_data_files = 0;
    Int64 removed_records = 0;
    Int64 removed_files_size = 0;
    Int64 removed_position_delete_files = 0;
    Int64 removed_position_deletes = 0;
    Int64 num_partitions = 0;
};

struct SnapshotSummaryUpdateReplace
{
    Int64 added_files = 0;
    Int64 added_records = 0;
    Int64 added_files_size = 0;
    Int64 deleted_data_files = 0;
    Int64 removed_records = 0;
    Int64 removed_files_size = 0;
    Int64 num_partitions = 0;
};

using SnapshotSummaryUpdate = std::variant<
    std::monostate, /// UNKNOWN
    SnapshotSummaryUpdateAppend,
    SnapshotSummaryUpdateOverwrite,
    SnapshotSummaryUpdateDelete,
    SnapshotSummaryUpdateReplace>;

struct SnapshotSummaryTotals
{
    Int64 records = 0;
    Int64 files_size = 0;
    Int64 data_files = 0;
    Int64 delete_files = 0;
    Int64 position_deletes = 0;
    Int64 equality_deletes = 0;
};

/// Free-form markers the producing engine writes into the summary; carried through verbatim
/// so `system.iceberg_history` shows who wrote a snapshot. Empty fields are omitted from JSON.
struct SummarySnapshotExtraInfo
{
    String app_id;
    String iceberg_version;
    String engine_name;
    String engine_version;
};

enum class SnapshotSummaryOperation
{
    UNKNOWN,
    APPEND,
    OVERWRITE,
    DELETE,
    REPLACE
};

/// summary from Iceberg's spec
/// https://iceberg.apache.org/spec/?h=snapshot#snapshots
/// https://iceberg.apache.org/spec/?h=snapshot#optional-snapshot-summary-fields
class SnapshotSummary
{
public:
    SnapshotSummary() = default;

    explicit SnapshotSummary(
        SnapshotSummaryUpdate update_,
        std::optional<SnapshotSummaryTotals> parent_totals = std::nullopt,
        SummarySnapshotExtraInfo extra_info_ = {});

    template <typename UpdateType>
    UpdateType * getUpdate()
    {
        return std::get_if<UpdateType>(&update);
    }

    template <typename UpdateType>
    const UpdateType * getUpdate() const
    {
        return std::get_if<UpdateType>(&update);
    }

    Iceberg::SnapshotSummaryOperation getOperation() const;
    SnapshotSummaryTotals getTotals() const;
    SummarySnapshotExtraInfo getExtraInfo() const;

    Poco::JSON::Object::Ptr toJSON() const;
    static SnapshotSummary fromJSON(const Poco::JSON::Object & obj);

private:
    SnapshotSummaryUpdate update;
    SnapshotSummaryTotals totals;
    SummarySnapshotExtraInfo extra_info;
};

}

#endif
