#include <Storages/ObjectStorage/DataLakes/Iceberg/SnapshotSummary.h>

#if USE_AVRO

#include <type_traits>
#include <unordered_map>
#include <IO/ReadHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <base/EnumReflection.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace DB::Iceberg
{

SnapshotSummaryOperation SnapshotSummary::getOperation() const
{
    return std::visit(
        [&]<typename T>(const T &)
        {
            if constexpr (std::is_same_v<SnapshotSummaryUpdateAppend, T>)
                return SnapshotSummaryOperation::APPEND;
            else if constexpr (std::is_same_v<SnapshotSummaryUpdateOverwrite, T>)
                return SnapshotSummaryOperation::OVERWRITE;
            else if constexpr (std::is_same_v<SnapshotSummaryUpdateDelete, T>)
                return SnapshotSummaryOperation::DELETE;
            else if constexpr (std::is_same_v<SnapshotSummaryUpdateReplace, T>)
                return SnapshotSummaryOperation::REPLACE;
        },
        update);
}

SnapshotSummaryTotals SnapshotSummary::getTotals() const
{
    return totals;
}

const SnapshotSummaryExtraFields & SnapshotSummary::getExtraFields() const
{
    return extra_fields;
}

SnapshotSummary::SnapshotSummary(
    SnapshotSummaryUpdate update_, std::optional<SnapshotSummaryTotals> parent_totals, SnapshotSummaryExtraFields extra_fields_)
    : update(std::move(update_))
    , extra_fields(std::move(extra_fields_))
{
    if (parent_totals)
        totals = *parent_totals;
    else if (getOperation() != SnapshotSummaryOperation::APPEND)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "No parent snapshot for DELETE/OVERWRITE/REPLACE");

    switch (getOperation())
    {
        case SnapshotSummaryOperation::APPEND:
        {
            const auto & u = std::get<SnapshotSummaryUpdateAppend>(update);
            totals.records += u.added_records;
            totals.files_size += u.added_files_size;
            totals.data_files += u.added_files;
            break;
        }
        case SnapshotSummaryOperation::OVERWRITE:
        {
            const auto & u = std::get<SnapshotSummaryUpdateOverwrite>(update);
            totals.records += u.added_records - u.removed_records;
            totals.files_size += u.added_files_size - u.removed_files_size;
            totals.data_files += u.added_files - u.deleted_data_files;
            totals.delete_files += u.added_delete_files;
            totals.position_deletes += u.added_position_deletes;
            totals.equality_deletes += u.added_equality_deletes;
            break;
        }
        case SnapshotSummaryOperation::DELETE:
        {
            const auto & u = std::get<SnapshotSummaryUpdateDelete>(update);
            totals.records -= u.removed_records;
            totals.files_size -= u.removed_files_size;
            totals.data_files -= u.deleted_data_files;
            totals.delete_files -= u.removed_position_delete_files + u.removed_equality_delete_files;
            totals.position_deletes -= u.removed_position_deletes;
            totals.equality_deletes -= u.removed_equality_deletes;
            break;
        }
        case SnapshotSummaryOperation::REPLACE:
        {
            const auto & u = std::get<SnapshotSummaryUpdateReplace>(update);
            totals.records += u.added_records - u.removed_records;
            totals.files_size += u.added_files_size - u.removed_files_size;
            totals.data_files += u.added_files - u.deleted_data_files;
            totals.delete_files += u.added_delete_files - u.removed_delete_files;
            totals.position_deletes += u.added_position_deletes - u.removed_position_deletes;
            totals.equality_deletes += u.added_equality_deletes - u.removed_equality_deletes;
            break;
        }
    }
}

Poco::JSON::Object::Ptr SnapshotSummary::toJSON() const
{
    Poco::JSON::Object::Ptr obj = new Poco::JSON::Object;
    forEachField(
        [&obj](std::string_view key, std::string value)
        {
            std::string key_str(key);

            if (obj->has(key_str))
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Duplicate key in SnapshotSummary: {}", key);

            obj->set(key_str, value);
        },
        /*with_extra_fields=*/true);
    return obj;
}

Map SnapshotSummary::toMap() const
{
    Map result;
    forEachField(
        [&result](std::string_view key, std::string value) { result.emplace_back(Tuple{std::string(key), value}); },
        /*with_extra_fields=*/true);
    return result;
}

SnapshotSummary::Expected SnapshotSummary::fromJSON(const Poco::JSON::Object & obj, bool with_extra_fields)
{
    std::unordered_set<std::string_view> parsed;

    /// https://iceberg.apache.org/spec/?h=summary#optional-snapshot-summary-fields
    /// Snapshot summary can include metrics fields to track numeric stats of the snapshot (see Metrics) and operational details (see Other Fields).
    /// The value of these fields should be of string type (e.g., "120").
    auto get_string = [&](const char * field) -> String
    {
        if (with_extra_fields)
            parsed.emplace(field);

        return obj.getValue<String>(field);
    };

    auto get_optional_uint64 = [&](const char * field) -> UInt64
    {
        if (!obj.has(field))
            return 0;

        return DB::parse<UInt64>(get_string(field));
    };

    /// `deleted-data-files` is the canonical Iceberg field; fall back to the legacy
    /// `removed-data-files` when an engine wrote only the latter.
    auto get_deleted_data_files = [&]() -> UInt64
    {
        return obj.has(Iceberg::f_deleted_data_files)
            ? get_optional_uint64(Iceberg::f_deleted_data_files)
            : get_optional_uint64(Iceberg::f_removed_data_files);
    };

    SnapshotSummary result;

    const auto operation_str = get_string(Iceberg::f_operation);
    if (operation_str == Iceberg::f_append)
        result.update = SnapshotSummaryUpdateAppend{
            .added_files = get_optional_uint64(Iceberg::f_added_data_files),
            .added_records = get_optional_uint64(Iceberg::f_added_records),
            .added_files_size = get_optional_uint64(Iceberg::f_added_files_size),
            .num_partitions = get_optional_uint64(Iceberg::f_changed_partition_count),
        };
    else if (operation_str == Iceberg::f_overwrite)
        result.update = SnapshotSummaryUpdateOverwrite{
            .added_files = get_optional_uint64(Iceberg::f_added_data_files),
            .added_records = get_optional_uint64(Iceberg::f_added_records),
            .added_files_size = get_optional_uint64(Iceberg::f_added_files_size),
            .added_delete_files = get_optional_uint64(Iceberg::f_added_delete_files),
            .added_position_delete_files = get_optional_uint64(Iceberg::f_added_position_delete_files),
            .added_position_deletes = get_optional_uint64(Iceberg::f_added_position_deletes),
            .added_equality_delete_files = get_optional_uint64(Iceberg::f_added_equality_delete_files),
            .added_equality_deletes = get_optional_uint64(Iceberg::f_added_equality_deletes),
            .deleted_data_files = get_deleted_data_files(),
            .removed_records = get_optional_uint64(Iceberg::f_deleted_records),
            .removed_files_size = get_optional_uint64(Iceberg::f_removed_files_size),
            .num_partitions = get_optional_uint64(Iceberg::f_changed_partition_count),
        };
    else if (operation_str == Iceberg::f_delete)
        result.update = SnapshotSummaryUpdateDelete{
            .deleted_data_files = get_deleted_data_files(),
            .removed_records = get_optional_uint64(Iceberg::f_deleted_records),
            .removed_files_size = get_optional_uint64(Iceberg::f_removed_files_size),
            .removed_position_delete_files = get_optional_uint64(Iceberg::f_removed_position_delete_files),
            .removed_position_deletes = get_optional_uint64(Iceberg::f_removed_position_deletes),
            .removed_equality_delete_files = get_optional_uint64(Iceberg::f_removed_equality_delete_files),
            .removed_equality_deletes = get_optional_uint64(Iceberg::f_removed_equality_deletes),
            .num_partitions = get_optional_uint64(Iceberg::f_changed_partition_count),
        };
    else if (operation_str == Iceberg::f_replace)
        result.update = SnapshotSummaryUpdateReplace{
            .added_files = get_optional_uint64(Iceberg::f_added_data_files),
            .added_records = get_optional_uint64(Iceberg::f_added_records),
            .added_files_size = get_optional_uint64(Iceberg::f_added_files_size),
            .added_delete_files = get_optional_uint64(Iceberg::f_added_delete_files),
            .added_position_delete_files = get_optional_uint64(Iceberg::f_added_position_delete_files),
            .added_position_deletes = get_optional_uint64(Iceberg::f_added_position_deletes),
            .added_equality_delete_files = get_optional_uint64(Iceberg::f_added_equality_delete_files),
            .added_equality_deletes = get_optional_uint64(Iceberg::f_added_equality_deletes),
            .deleted_data_files = get_deleted_data_files(),
            .removed_records = get_optional_uint64(Iceberg::f_deleted_records),
            .removed_files_size = get_optional_uint64(Iceberg::f_removed_files_size),
            .removed_delete_files = get_optional_uint64(Iceberg::f_removed_delete_files),
            .removed_position_delete_files = get_optional_uint64(Iceberg::f_removed_position_delete_files),
            .removed_position_deletes = get_optional_uint64(Iceberg::f_removed_position_deletes),
            .removed_equality_delete_files = get_optional_uint64(Iceberg::f_removed_equality_delete_files),
            .removed_equality_deletes = get_optional_uint64(Iceberg::f_removed_equality_deletes),
            .num_partitions = get_optional_uint64(Iceberg::f_changed_partition_count),
        };
    else
        return std::unexpected(fmt::format("Unexpected operation '{}'", operation_str));

    result.totals.records = get_optional_uint64(Iceberg::f_total_records);
    result.totals.files_size = get_optional_uint64(Iceberg::f_total_files_size);
    result.totals.data_files = get_optional_uint64(Iceberg::f_total_data_files);
    result.totals.delete_files = get_optional_uint64(Iceberg::f_total_delete_files);
    result.totals.position_deletes = get_optional_uint64(Iceberg::f_total_position_deletes);
    result.totals.equality_deletes = get_optional_uint64(Iceberg::f_total_equality_deletes);

    if (with_extra_fields)
    {
        for (const auto & [key, value] : obj)
        {
            if (parsed.contains(key))
                continue;

            /// We don't access the legacy fields here, but write it
            if (key == Iceberg::f_removed_data_files)
                continue;

            result.extra_fields.emplace(key, value);
        }
    }

    return result;
}

void SnapshotSummary::forEachField(std::function<void(std::string_view, std::string)> && fn, bool with_extra_fields) const
{
    /// https://iceberg.apache.org/spec/?h=summary#optional-snapshot-summary-fields
    /// Snapshot summary can include metrics fields to track numeric stats of the snapshot (see Metrics) and operational details (see Other Fields).
    /// The value of these fields should be of string type (e.g., "120").
    auto set_as_string = [&](const char * field, Int64 val)
    {
        fn(field, std::to_string(val));
    };

    switch (getOperation())
    {
        case SnapshotSummaryOperation::APPEND:
        {
            const auto & u = std::get<SnapshotSummaryUpdateAppend>(update);
            fn(Iceberg::f_operation, Iceberg::f_append);
            set_as_string(Iceberg::f_added_data_files, u.added_files);
            set_as_string(Iceberg::f_added_records, u.added_records);
            set_as_string(Iceberg::f_added_files_size, u.added_files_size);
            set_as_string(Iceberg::f_changed_partition_count, u.num_partitions);
            break;
        }
        case SnapshotSummaryOperation::OVERWRITE:
        {
            const auto & u = std::get<SnapshotSummaryUpdateOverwrite>(update);
            fn(Iceberg::f_operation, Iceberg::f_overwrite);
            set_as_string(Iceberg::f_added_data_files, u.added_files);
            set_as_string(Iceberg::f_added_records, u.added_records);
            set_as_string(Iceberg::f_added_files_size, u.added_files_size);
            set_as_string(Iceberg::f_added_delete_files, u.added_delete_files);
            if (u.added_position_delete_files > 0)
                set_as_string(Iceberg::f_added_position_delete_files, u.added_position_delete_files);
            set_as_string(Iceberg::f_added_position_deletes, u.added_position_deletes);
            if (u.added_equality_delete_files > 0)
                set_as_string(Iceberg::f_added_equality_delete_files, u.added_equality_delete_files);
            if (u.added_equality_deletes > 0)
                set_as_string(Iceberg::f_added_equality_deletes, u.added_equality_deletes);
            set_as_string(Iceberg::f_deleted_data_files, u.deleted_data_files);
            /// `removed-data-files` is a legacy alias of `deleted-data-files`, kept for Spark compatibility.
            set_as_string(Iceberg::f_removed_data_files, u.deleted_data_files);
            set_as_string(Iceberg::f_deleted_records, u.removed_records);
            set_as_string(Iceberg::f_removed_files_size, u.removed_files_size);
            set_as_string(Iceberg::f_changed_partition_count, u.num_partitions);
            break;
        }
        case SnapshotSummaryOperation::DELETE:
        {
            const auto & u = std::get<SnapshotSummaryUpdateDelete>(update);
            fn(Iceberg::f_operation, Iceberg::f_delete);
            set_as_string(Iceberg::f_deleted_data_files, u.deleted_data_files);
            set_as_string(Iceberg::f_removed_data_files, u.deleted_data_files);
            set_as_string(Iceberg::f_deleted_records, u.removed_records);
            set_as_string(Iceberg::f_removed_files_size, u.removed_files_size);
            if (u.removed_position_delete_files > 0)
                set_as_string(Iceberg::f_removed_position_delete_files, u.removed_position_delete_files);
            if (u.removed_position_deletes > 0)
                set_as_string(Iceberg::f_removed_position_deletes, u.removed_position_deletes);
            if (u.removed_equality_delete_files > 0)
                set_as_string(Iceberg::f_removed_equality_delete_files, u.removed_equality_delete_files);
            if (u.removed_equality_deletes > 0)
                set_as_string(Iceberg::f_removed_equality_deletes, u.removed_equality_deletes);
            set_as_string(Iceberg::f_changed_partition_count, u.num_partitions);
            break;
        }
        case SnapshotSummaryOperation::REPLACE:
        {
            const auto & u = std::get<SnapshotSummaryUpdateReplace>(update);
            fn(Iceberg::f_operation, Iceberg::f_replace);
            set_as_string(Iceberg::f_added_data_files, u.added_files);
            set_as_string(Iceberg::f_added_records, u.added_records);
            set_as_string(Iceberg::f_added_files_size, u.added_files_size);
            if (u.added_delete_files > 0)
                set_as_string(Iceberg::f_added_delete_files, u.added_delete_files);
            if (u.added_position_delete_files > 0)
                set_as_string(Iceberg::f_added_position_delete_files, u.added_position_delete_files);
            if (u.added_position_deletes > 0)
                set_as_string(Iceberg::f_added_position_deletes, u.added_position_deletes);
            if (u.added_equality_delete_files > 0)
                set_as_string(Iceberg::f_added_equality_delete_files, u.added_equality_delete_files);
            if (u.added_equality_deletes > 0)
                set_as_string(Iceberg::f_added_equality_deletes, u.added_equality_deletes);
            set_as_string(Iceberg::f_deleted_data_files, u.deleted_data_files);
            set_as_string(Iceberg::f_removed_data_files, u.deleted_data_files);
            set_as_string(Iceberg::f_deleted_records, u.removed_records);
            set_as_string(Iceberg::f_removed_files_size, u.removed_files_size);
            if (u.removed_delete_files > 0)
                set_as_string(Iceberg::f_removed_delete_files, u.removed_delete_files);
            if (u.removed_position_delete_files > 0)
                set_as_string(Iceberg::f_removed_position_delete_files, u.removed_position_delete_files);
            if (u.removed_position_deletes > 0)
                set_as_string(Iceberg::f_removed_position_deletes, u.removed_position_deletes);
            if (u.removed_equality_delete_files > 0)
                set_as_string(Iceberg::f_removed_equality_delete_files, u.removed_equality_delete_files);
            if (u.removed_equality_deletes > 0)
                set_as_string(Iceberg::f_removed_equality_deletes, u.removed_equality_deletes);
            set_as_string(Iceberg::f_changed_partition_count, u.num_partitions);
            break;
        }
    }

    set_as_string(Iceberg::f_total_records, totals.records);
    set_as_string(Iceberg::f_total_files_size, totals.files_size);
    set_as_string(Iceberg::f_total_data_files, totals.data_files);
    set_as_string(Iceberg::f_total_delete_files, totals.delete_files);
    set_as_string(Iceberg::f_total_position_deletes, totals.position_deletes);
    set_as_string(Iceberg::f_total_equality_deletes, totals.equality_deletes);

    if (with_extra_fields)
        for (const auto & [key, value] : extra_fields)
            fn(key, value);
}
}

#endif
