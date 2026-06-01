#pragma once

#include "config.h"

#if USE_AVRO

#include <optional>
#include <Poco/JSON/Object.h>
#include <base/types.h>


namespace DB::Iceberg
{

struct SnapshotSummary
{
    enum class Operation
    {
        UNKNOWN,
        APPEND,
        OVERWRITE,
        DELETE
    };

    Operation operation = Operation::UNKNOWN;

    Int64 added_files = 0;
    Int64 added_records = 0;
    Int64 added_files_size = 0;
    Int64 num_partitions = 0;
    Int64 added_delete_files = 0;
    Int64 num_deleted_rows = 0;
    Int64 removed_data_files = 0;
    Int64 removed_records = 0;
    Int64 removed_files_size = 0;
    Int64 removed_position_delete_files = 0;
    Int64 removed_position_deletes = 0;

    Int64 total_records = 0;
    Int64 total_files_size = 0;
    Int64 total_data_files = 0;
    Int64 total_delete_files = 0;
    Int64 total_position_deletes = 0;
    Int64 total_equality_deletes = 0;

    bool finalized = false;

    void finalize(std::optional<SnapshotSummary> parent);

    Poco::JSON::Object::Ptr toJSON() const;

    static SnapshotSummary fromJSON(const Poco::JSON::Object & obj);

    static SnapshotSummary createAppend(
        Int64 added_files,
        Int64 added_records,
        Int64 added_files_size,
        Int64 num_partitions);

    static SnapshotSummary createOverwrite(
        Int64 added_delete_files,
        Int64 added_files_size,
        Int64 num_partitions,
        Int64 num_deleted_rows);

    static SnapshotSummary createDelete(
        Int64 removed_data_files,
        Int64 removed_records,
        Int64 removed_files_size,
        Int64 removed_position_delete_files,
        Int64 removed_position_deletes,
        Int64 num_partitions);
};

}

#endif
