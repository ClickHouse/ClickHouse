#include <type_traits>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SnapshotSummary.h>

#if USE_AVRO

#include <base/EnumReflection.h>

#include <IO/ReadHelpers.h>
#include <Common/Exception.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
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
            else
                return SnapshotSummaryOperation::UNKNOWN;
        },
        update);
}

SnapshotSummaryTotals SnapshotSummary::getTotals() const
{
    return totals;
}

SnapshotSummary::SnapshotSummary(SnapshotSummaryUpdate update_, std::optional<SnapshotSummaryTotals> parent_totals)
    : update(std::move(update_))
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
            totals.equality_deletes = 0;
            break;
        }
        case SnapshotSummaryOperation::OVERWRITE:
        {
            const auto & u = std::get<SnapshotSummaryUpdateOverwrite>(update);
            totals.files_size += u.added_files_size;
            totals.delete_files += u.added_delete_files;
            /// FI->ME: this is correct only while we don't support equality deletes
            totals.position_deletes += u.num_deleted_rows;
            totals.equality_deletes = 0;
            break;
        }
        case SnapshotSummaryOperation::DELETE:
        {
            const auto & u = std::get<SnapshotSummaryUpdateDelete>(update);
            totals.records -= u.removed_records;
            totals.files_size -= u.removed_files_size;
            totals.data_files -= u.removed_data_files;
            totals.delete_files -= u.removed_position_delete_files;
            /// FI->ME: this is correct only while we don't support equality deletes
            totals.position_deletes -= u.removed_position_deletes;
            totals.equality_deletes = 0;
            break;
        }
        case SnapshotSummaryOperation::REPLACE:
        {
            const auto & u = std::get<SnapshotSummaryUpdateReplace>(update);
            totals.records += u.added_records - u.removed_records;
            totals.files_size += u.added_files_size - u.removed_files_size;
            totals.data_files += u.added_files - u.removed_data_files;
            break;
        }
        default:
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unexpected operation enum {}", getOperation());
    }
}

Poco::JSON::Object::Ptr SnapshotSummary::toJSON() const
{
    Poco::JSON::Object::Ptr obj = new Poco::JSON::Object;

    /// https://iceberg.apache.org/spec/?h=summary#optional-snapshot-summary-fields
    /// Snapshot summary can include metrics fields to track numeric stats of the snapshot (see Metrics) and operational details (see Other Fields).
    /// The value of these fields should be of string type (e.g., "120").
    auto set_as_string = [&](const char * field, Int64 val)
    {
        obj->set(field, std::to_string(val));
    };

    switch (getOperation())
    {
        case SnapshotSummaryOperation::APPEND:
        {
            const auto & u = std::get<SnapshotSummaryUpdateAppend>(update);
            obj->set(Iceberg::f_operation, Iceberg::f_append);
            set_as_string(Iceberg::f_added_data_files, u.added_files);
            set_as_string(Iceberg::f_added_records, u.added_records);
            set_as_string(Iceberg::f_added_files_size, u.added_files_size);
            set_as_string(Iceberg::f_changed_partition_count, u.num_partitions);
            break;
        }
        case SnapshotSummaryOperation::OVERWRITE:
        {
            const auto & u = std::get<SnapshotSummaryUpdateOverwrite>(update);
            if (u.num_deleted_rows == 0)
                throw DB::Exception(
                    DB::ErrorCodes::LOGICAL_ERROR,
                    "SnapshotSummary with operation=OVERWRITE must have num_deleted_rows>0, got 0");
            obj->set(Iceberg::f_operation, Iceberg::f_overwrite);
            set_as_string(Iceberg::f_added_delete_files, u.added_delete_files);
            set_as_string(Iceberg::f_added_position_delete_files, u.added_delete_files);
            set_as_string(Iceberg::f_added_files_size, u.added_files_size);
            set_as_string(Iceberg::f_added_position_deletes, u.num_deleted_rows);
            set_as_string(Iceberg::f_changed_partition_count, u.num_partitions);
            break;
        }
        case SnapshotSummaryOperation::DELETE:
        {
            const auto & u = std::get<SnapshotSummaryUpdateDelete>(update);
            obj->set(Iceberg::f_operation, Iceberg::f_delete);
            set_as_string(Iceberg::f_removed_data_files, u.removed_data_files);
            set_as_string(Iceberg::f_deleted_data_files, u.removed_data_files);
            set_as_string(Iceberg::f_deleted_records, u.removed_records);
            set_as_string(Iceberg::f_removed_files_size, u.removed_files_size);
            if (u.removed_position_delete_files > 0)
                set_as_string(Iceberg::f_removed_position_delete_files, u.removed_position_delete_files);
            if (u.removed_position_deletes > 0)
                set_as_string(Iceberg::f_removed_position_deletes, u.removed_position_deletes);
            set_as_string(Iceberg::f_changed_partition_count, u.num_partitions);
            break;
        }
        case SnapshotSummaryOperation::REPLACE:
        {
            const auto & u = std::get<SnapshotSummaryUpdateReplace>(update);
            obj->set(Iceberg::f_operation, Iceberg::f_replace);
            set_as_string(Iceberg::f_added_data_files, u.added_files);
            set_as_string(Iceberg::f_added_records, u.added_records);
            set_as_string(Iceberg::f_added_files_size, u.added_files_size);
            set_as_string(Iceberg::f_removed_data_files, u.removed_data_files);
            set_as_string(Iceberg::f_deleted_data_files, u.removed_data_files);
            set_as_string(Iceberg::f_deleted_records, u.removed_records);
            set_as_string(Iceberg::f_removed_files_size, u.removed_files_size);
            set_as_string(Iceberg::f_changed_partition_count, u.num_partitions);
            break;
        }
        default:
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unexpected operation enum {}", getOperation());
    }

    set_as_string(Iceberg::f_total_records, totals.records);
    set_as_string(Iceberg::f_total_files_size, totals.files_size);
    set_as_string(Iceberg::f_total_data_files, totals.data_files);
    set_as_string(Iceberg::f_total_delete_files, totals.delete_files);
    set_as_string(Iceberg::f_total_position_deletes, totals.position_deletes);
    set_as_string(Iceberg::f_total_equality_deletes, totals.equality_deletes);

    return obj;
}

SnapshotSummary SnapshotSummary::fromJSON(const Poco::JSON::Object & obj)
{
    SnapshotSummary result;

    /// Iceberg spec stores all summary metric values as strings (e.g., "120").
    auto get_optional_int = [&](const char * field) -> Int64
    {
        if (!obj.has(field))
            return 0;
        return DB::parse<Int64>(obj.getValue<String>(field));
    };

    const auto operation_str = obj.getValue<String>(Iceberg::f_operation);
    if (operation_str == Iceberg::f_append)
        result.update = SnapshotSummaryUpdateAppend{
            .added_files = get_optional_int(Iceberg::f_added_data_files),
            .added_records = get_optional_int(Iceberg::f_added_records),
            .added_files_size = get_optional_int(Iceberg::f_added_files_size),
            .num_partitions = get_optional_int(Iceberg::f_changed_partition_count),
        };
    else if (operation_str == Iceberg::f_overwrite)
        result.update = SnapshotSummaryUpdateOverwrite{
            .added_delete_files = get_optional_int(Iceberg::f_added_delete_files),
            .added_files_size = get_optional_int(Iceberg::f_added_files_size),
            .num_partitions = get_optional_int(Iceberg::f_changed_partition_count),
            .num_deleted_rows = get_optional_int(Iceberg::f_added_position_deletes),
        };
    else if (operation_str == Iceberg::f_delete)
        result.update = SnapshotSummaryUpdateDelete{
            .removed_data_files = get_optional_int(Iceberg::f_removed_data_files),
            .removed_records = get_optional_int(Iceberg::f_deleted_records),
            .removed_files_size = get_optional_int(Iceberg::f_removed_files_size),
            .removed_position_delete_files = get_optional_int(Iceberg::f_removed_position_delete_files),
            .removed_position_deletes = get_optional_int(Iceberg::f_removed_position_deletes),
            .num_partitions = get_optional_int(Iceberg::f_changed_partition_count),
        };
    else if (operation_str == Iceberg::f_replace)
        result.update = SnapshotSummaryUpdateReplace{
            .added_files = get_optional_int(Iceberg::f_added_data_files),
            .added_records = get_optional_int(Iceberg::f_added_records),
            .added_files_size = get_optional_int(Iceberg::f_added_files_size),
            .removed_data_files = get_optional_int(Iceberg::f_removed_data_files),
            .removed_records = get_optional_int(Iceberg::f_deleted_records),
            .removed_files_size = get_optional_int(Iceberg::f_removed_files_size),
            .num_partitions = get_optional_int(Iceberg::f_changed_partition_count),
        };
    else
        /// Other Iceberg engines may write operations we don't model.
        /// We don't reject them — `system.iceberg_history` needs to read them, and
        /// `MetadataGenerator::finalize` only consults parent `total_*` fields.
        result.update = std::monostate{};

    result.totals.records = get_optional_int(Iceberg::f_total_records);
    result.totals.files_size = get_optional_int(Iceberg::f_total_files_size);
    result.totals.data_files = get_optional_int(Iceberg::f_total_data_files);
    result.totals.delete_files = get_optional_int(Iceberg::f_total_delete_files);
    result.totals.position_deletes = get_optional_int(Iceberg::f_total_position_deletes);
    result.totals.equality_deletes = get_optional_int(Iceberg::f_total_equality_deletes);

    return result;
}

}

#endif
