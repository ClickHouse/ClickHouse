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

void SnapshotSummary::finalize(std::optional<SnapshotSummary> parent)
{
    if (finalized)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "SnapshotSummary::finalize called twice -- totals would be double-applied");

    if (parent)
    {
        total_records = parent->total_records;
        total_files_size = parent->total_files_size;
        total_data_files = parent->total_data_files;
        total_delete_files = parent->total_delete_files;
        total_position_deletes = parent->total_position_deletes;
        total_equality_deletes = parent->total_equality_deletes;
    }
    else if (operation == Operation::DELETE || operation == Operation::OVERWRITE)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "No parent snapshot for DELETE/OVERWRITE");

    switch (operation)
    {
        case Operation::APPEND:
        case Operation::OVERWRITE:
            total_records += added_records;
            total_files_size += added_files_size;
            total_data_files += added_files;
            total_delete_files += added_delete_files;
            /// FIXME: this is correct only while we don't support equality deletes
            total_position_deletes += num_deleted_rows;
            total_equality_deletes = 0;
            break;
        case Operation::DELETE:
            total_records -= removed_records;
            total_files_size -= removed_files_size;
            total_data_files -= removed_data_files;
            total_delete_files -= removed_position_delete_files;
            /// FIXME: this is correct only while we don't support equality deletes
            total_position_deletes -= removed_position_deletes;
            total_equality_deletes = 0;
            break;
        default:
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unexpected operation enum {}", operation);
    }

    finalized = true;
}

Poco::JSON::Object::Ptr SnapshotSummary::toJSON() const
{
    if (!finalized)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "SnapshotSummary wasn't finalized");

    Poco::JSON::Object::Ptr obj = new Poco::JSON::Object;

    /// https://iceberg.apache.org/spec/?h=summary#optional-snapshot-summary-fields
    /// Snapshot summary can include metrics fields to track numeric stats of the snapshot (see Metrics) and operational details (see Other Fields).
    /// The value of these fields should be of string type (e.g., "120").
    auto set_as_string = [&](const char * field, Int64 val)
    {
        obj->set(field, std::to_string(val));
    };

    switch (operation)
    {
        case Operation::APPEND:
        {
            if (num_deleted_rows != 0)
                throw DB::Exception(
                    DB::ErrorCodes::LOGICAL_ERROR,
                    "SnapshotSummary with operation=APPEND must have num_deleted_rows=0, got {}",
                    num_deleted_rows);
            obj->set(Iceberg::f_operation, Iceberg::f_append);
            set_as_string(Iceberg::f_added_data_files, added_files);
            set_as_string(Iceberg::f_added_records, added_records);
            set_as_string(Iceberg::f_added_files_size, added_files_size);
            set_as_string(Iceberg::f_changed_partition_count, num_partitions);
            break;
        }
        case Operation::OVERWRITE:
        {
            if (num_deleted_rows == 0)
                throw DB::Exception(
                    DB::ErrorCodes::LOGICAL_ERROR,
                    "SnapshotSummary with operation=OVERWRITE must have num_deleted_rows>0, got 0");
            obj->set(Iceberg::f_operation, Iceberg::f_overwrite);
            set_as_string(Iceberg::f_added_delete_files, added_delete_files);
            set_as_string(Iceberg::f_added_position_delete_files, added_delete_files);
            set_as_string(Iceberg::f_added_files_size, added_files_size);
            set_as_string(Iceberg::f_added_position_deletes, num_deleted_rows);
            set_as_string(Iceberg::f_changed_partition_count, num_partitions);
            break;
        }
        case Operation::DELETE:
        {
            obj->set(Iceberg::f_operation, Iceberg::f_delete);
            set_as_string(Iceberg::f_removed_data_files, removed_data_files);
            set_as_string(Iceberg::f_deleted_data_files, removed_data_files);
            set_as_string(Iceberg::f_deleted_records, removed_records);
            set_as_string(Iceberg::f_removed_files_size, removed_files_size);
            if (removed_position_delete_files > 0)
                set_as_string(Iceberg::f_removed_position_delete_files, removed_position_delete_files);
            set_as_string(Iceberg::f_changed_partition_count, num_partitions);
            break;
        }
        default:
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unexpected operation enum {}", operation);
    }

    set_as_string(Iceberg::f_total_records, total_records);
    set_as_string(Iceberg::f_total_files_size, total_files_size);
    set_as_string(Iceberg::f_total_data_files, total_data_files);
    set_as_string(Iceberg::f_total_delete_files, total_delete_files);
    set_as_string(Iceberg::f_total_position_deletes, total_position_deletes);
    set_as_string(Iceberg::f_total_equality_deletes, total_equality_deletes);

    return obj;
}

SnapshotSummary SnapshotSummary::createAppend(
    Int64 added_files, Int64 added_records, Int64 added_files_size, Int64 num_partitions)
{
    SnapshotSummary result;
    result.operation = Operation::APPEND;
    result.added_files = added_files;
    result.added_records = added_records;
    result.added_files_size = added_files_size;
    result.num_partitions = num_partitions;
    return result;
}

SnapshotSummary SnapshotSummary::createOverwrite(
    Int64 added_delete_files, Int64 added_files_size, Int64 num_partitions, Int64 num_deleted_rows)
{
    SnapshotSummary result;
    result.operation = Operation::OVERWRITE;
    result.added_delete_files = added_delete_files;
    result.added_files_size = added_files_size;
    result.num_partitions = num_partitions;
    result.num_deleted_rows = num_deleted_rows;
    return result;
}

SnapshotSummary SnapshotSummary::createDelete(
    Int64 removed_data_files,
    Int64 removed_records,
    Int64 removed_files_size,
    Int64 removed_position_delete_files,
    Int64 removed_position_deletes,
    Int64 num_partitions)
{
    SnapshotSummary result;
    result.operation = Operation::DELETE;
    result.removed_data_files = removed_data_files;
    result.removed_records = removed_records;
    result.removed_files_size = removed_files_size;
    result.removed_position_delete_files = removed_position_delete_files;
    result.removed_position_deletes = removed_position_deletes;
    result.num_partitions = num_partitions;
    return result;
}

SnapshotSummary SnapshotSummary::fromJSON(const Poco::JSON::Object & obj)
{
    SnapshotSummary result;

    const auto operation_str = obj.getValue<String>(Iceberg::f_operation);
    if (operation_str == Iceberg::f_append)
        result.operation = Operation::APPEND;
    else if (operation_str == Iceberg::f_overwrite)
        result.operation = Operation::OVERWRITE;
    else if (operation_str == Iceberg::f_delete)
        result.operation = Operation::DELETE;
    else
        /// Other Iceberg engines may write operations we don't model (e.g. "replace").
        /// We don't reject them — `system.iceberg_history` needs to read them, and
        /// `MetadataGenerator::finalize` only consults parent `total_*` fields.
        result.operation = Operation::UNKNOWN;

    /// Iceberg spec stores all summary metric values as strings (e.g., "120").
    auto get_optional_int = [&](const char * field) -> Int64
    {
        if (!obj.has(field))
            return 0;
        return DB::parse<Int64>(obj.getValue<String>(field));
    };

    result.added_files = get_optional_int(Iceberg::f_added_data_files);
    result.added_records = get_optional_int(Iceberg::f_added_records);
    result.added_files_size = get_optional_int(Iceberg::f_added_files_size);
    result.num_partitions = get_optional_int(Iceberg::f_changed_partition_count);
    result.added_delete_files = get_optional_int(Iceberg::f_added_delete_files);
    result.num_deleted_rows = get_optional_int(Iceberg::f_added_position_deletes);
    result.removed_data_files = get_optional_int(Iceberg::f_removed_data_files);
    result.removed_records = get_optional_int(Iceberg::f_deleted_records);
    result.removed_files_size = get_optional_int(Iceberg::f_removed_files_size);
    result.removed_position_delete_files = get_optional_int(Iceberg::f_removed_position_delete_files);

    result.total_records = get_optional_int(Iceberg::f_total_records);
    result.total_files_size = get_optional_int(Iceberg::f_total_files_size);
    result.total_data_files = get_optional_int(Iceberg::f_total_data_files);
    result.total_delete_files = get_optional_int(Iceberg::f_total_delete_files);
    result.total_position_deletes = get_optional_int(Iceberg::f_total_position_deletes);
    result.total_equality_deletes = get_optional_int(Iceberg::f_total_equality_deletes);

    result.finalized = true;

    return result;
}

}

#endif
