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

class SnapshotSummary
{
public:
    SnapshotSummary() = default;

    explicit SnapshotSummary(SnapshotSummaryUpdate update_, std::optional<SnapshotSummaryTotals> parent_totals = std::nullopt);

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

    Poco::JSON::Object::Ptr toJSON() const;
    static SnapshotSummary fromJSON(const Poco::JSON::Object & obj);

private:
    SnapshotSummaryUpdate update;
    SnapshotSummaryTotals totals;
};

}

#endif
