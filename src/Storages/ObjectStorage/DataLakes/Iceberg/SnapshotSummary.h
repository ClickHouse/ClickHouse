#pragma once

#include "config.h"

#if USE_AVRO

#include <optional>
#include <variant>
#include <Poco/JSON/Object.h>
#include <base/types.h>


namespace DB::Iceberg
{

struct SnapshotSummaryUpdateAppend
{
    Int64 added_files = 0;
    Int64 added_records = 0;
    Int64 added_files_size = 0;
    Int64 num_partitions = 0;
};

struct SnapshotSummaryUpdateOverwrite
{
    Int64 added_delete_files = 0;
    Int64 added_files_size = 0;
    Int64 num_partitions = 0;
    Int64 num_deleted_rows = 0;
};

struct SnapshotSummaryUpdateDelete
{
    Int64 removed_data_files = 0;
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
    Int64 removed_data_files = 0;
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

enum class SnapshotSummaryOperation
{
    UNKNOWN,
    APPEND,
    OVERWRITE,
    DELETE,
    REPLACE
};

/// getHistory() -> snapshot_summary.getOperation() -> std::optional<operation>
struct SnapshotSummary
{
    template <typename UpdateType>
    UpdateType * asUpdate()
    {
        return std::get_if<UpdateType>(&update);
    }

    template <typename UpdateType>
    const UpdateType * asUpdate() const
    {
        return std::get_if<UpdateType>(&update);
    }

    /// The operation this summary describes, derived from the active `update` alternative.
    Iceberg::SnapshotSummaryOperation getOperation() const;

    void applyTotals(std::optional<SnapshotSummaryTotals> other_totals);

    Poco::JSON::Object::Ptr toJSON() const;

    static SnapshotSummary fromJSON(const Poco::JSON::Object & obj);

    SnapshotSummary() = default;

    explicit SnapshotSummary(SnapshotSummaryUpdate update_)
        : update(std::move(update_))
    {
    }

    SnapshotSummaryUpdate update;
    std::optional<SnapshotSummaryTotals> totals;
};

}

#endif
