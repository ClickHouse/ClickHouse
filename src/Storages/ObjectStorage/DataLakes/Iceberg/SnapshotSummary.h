#pragma once

#include "config.h"

#if USE_AVRO

#include <expected>
#include <functional>
#include <optional>
#include <unordered_map>
#include <variant>
#include <Core/Field.h>
#include <base/types.h>
#include <Poco/JSON/Object.h>

namespace DB::Iceberg
{

struct SnapshotSummaryUpdateAppend
{
    UInt64 added_files = 0;
    UInt64 added_records = 0;
    UInt64 added_files_size = 0;
    UInt64 num_partitions = 0;
};

/// An Iceberg `overwrite` can either rewrite data (engines like Spark: add data files and
/// remove old ones) or add position-delete files (how ClickHouse expresses row deletes), so
/// this struct holds both sets of deltas; the irrelevant ones stay zero.
struct SnapshotSummaryUpdateOverwrite
{
    UInt64 added_files = 0;
    UInt64 added_records = 0;
    UInt64 added_files_size = 0;
    UInt64 added_delete_files = 0;
    UInt64 added_position_delete_files = 0;
    UInt64 added_position_deletes = 0;
    UInt64 added_equality_delete_files = 0;
    UInt64 added_equality_deletes = 0;
    UInt64 deleted_data_files = 0;
    UInt64 removed_records = 0;
    UInt64 removed_files_size = 0;
    UInt64 num_partitions = 0;
};

struct SnapshotSummaryUpdateDelete
{
    UInt64 deleted_data_files = 0;
    UInt64 removed_records = 0;
    UInt64 removed_files_size = 0;
    UInt64 removed_position_delete_files = 0;
    UInt64 removed_position_deletes = 0;
    UInt64 removed_equality_delete_files = 0;
    UInt64 removed_equality_deletes = 0;
    UInt64 num_partitions = 0;
};

struct SnapshotSummaryUpdateReplace
{
    UInt64 added_files = 0;
    UInt64 added_records = 0;
    UInt64 added_files_size = 0;
    UInt64 added_delete_files = 0;
    UInt64 added_position_delete_files = 0;
    UInt64 added_position_deletes = 0;
    UInt64 added_equality_delete_files = 0;
    UInt64 added_equality_deletes = 0;
    UInt64 deleted_data_files = 0;
    UInt64 removed_records = 0;
    UInt64 removed_files_size = 0;
    UInt64 removed_delete_files = 0;
    UInt64 removed_position_delete_files = 0;
    UInt64 removed_position_deletes = 0;
    UInt64 removed_equality_delete_files = 0;
    UInt64 removed_equality_deletes = 0;
    UInt64 num_partitions = 0;
};

using SnapshotSummaryUpdate = std::variant<
    SnapshotSummaryUpdateAppend,
    SnapshotSummaryUpdateOverwrite,
    SnapshotSummaryUpdateDelete,
    SnapshotSummaryUpdateReplace>;

struct SnapshotSummaryTotals
{
    UInt64 records = 0;
    UInt64 files_size = 0;
    UInt64 data_files = 0;
    UInt64 delete_files = 0;
    UInt64 position_deletes = 0;
    UInt64 equality_deletes = 0;
};

enum class SnapshotSummaryOperation : int8_t
{
    /// UNKNOWN = -1,
    APPEND = 0,
    OVERWRITE = 1,
    DELETE = 2,
    REPLACE = 3
};

using SnapshotSummaryExtraFields = std::unordered_map<String, String>;

/// summary from Iceberg's spec
/// https://iceberg.apache.org/spec/?h=snapshot#snapshots
/// https://iceberg.apache.org/spec/?h=snapshot#optional-snapshot-summary-fields
class SnapshotSummary
{
public:
    explicit SnapshotSummary(
        SnapshotSummaryUpdate update_,
        std::optional<SnapshotSummaryTotals> parent_totals = std::nullopt,
        SnapshotSummaryExtraFields extra_fields_ = {});

    template <typename UpdateType>
    UpdateType & getUpdate()
    {
        return std::get<UpdateType>(update);
    }

    template <typename UpdateType>
    const UpdateType & getUpdate() const
    {
        return std::get<UpdateType>(update);
    }

    Iceberg::SnapshotSummaryOperation getOperation() const;
    SnapshotSummaryTotals getTotals() const;
    const SnapshotSummaryExtraFields & getExtraFields() const;

    Poco::JSON::Object::Ptr toJSON() const;
    Map toMap() const;

    using Expected = std::expected<SnapshotSummary, std::string>;
    static Expected fromJSON(const Poco::JSON::Object & obj, bool with_extra_fields = false);

private:
    SnapshotSummary() = default;

    void forEachField(std::function<void(std::string_view, std::string)> && fn, bool with_extra_fields = true) const;

    SnapshotSummaryUpdate      update;
    SnapshotSummaryTotals      totals;
    SnapshotSummaryExtraFields extra_fields;
};

}

#endif
