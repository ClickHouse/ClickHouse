#pragma once

#include <base/types.h>
#include <Common/Exception.h>
#include <Interpreters/StorageID.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Storages/MergeTree/MergeTreePartExportManifest.h>
#include <optional>

namespace DB
{

struct ExportReplicatedMergeTreePartitionProcessingPartEntry
{

    enum class Status
    {
        PENDING,
        COMPLETED,
        FAILED
    };

    String part_name;
    Status status;
    String finished_by;

    std::string toJsonString() const
    {
        Poco::JSON::Object json;

        json.set("part_name", part_name);
        json.set("status", String(magic_enum::enum_name(status)));
        json.set("finished_by", finished_by);
        std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        oss.exceptions(std::ios::failbit);
        Poco::JSON::Stringifier::stringify(json, oss);

        return oss.str();
    }

    static ExportReplicatedMergeTreePartitionProcessingPartEntry fromJsonString(const std::string & json_string)
    {
        Poco::JSON::Parser parser;
        auto json = parser.parse(json_string).extract<Poco::JSON::Object::Ptr>();
        chassert(json);

        ExportReplicatedMergeTreePartitionProcessingPartEntry entry;

        entry.part_name = json->getValue<String>("part_name");
        entry.status = magic_enum::enum_cast<Status>(json->getValue<String>("status")).value();
        if (json->has("finished_by"))
        {
            entry.finished_by = json->getValue<String>("finished_by");
        }
        return entry;
    }
};

/// Per-task "last exception" record persisted at <export-entry>/last_exception.
///
/// Single znode per export task. Updated atomically with the surrounding state
/// transition (status flip / lock release / retry counter bump) via a single
/// `tryMulti` Set op. The `count` field is best-effort and non-atomic: writers
/// `tryGet` the current value and write `count + 1` back without a version
/// check, so concurrent writers may under-count. This matches the semantics
/// used by `commit_attempts` and is documented in the system table.
struct LastExceptionEntry
{
    String message;
    String part;     /// empty for task-level exceptions (commit failure, timeout)
    String replica;
    time_t time = 0;
    size_t count = 0;

    std::string toJsonString() const
    {
        Poco::JSON::Object json;
        json.set("message", message);
        json.set("part", part);
        json.set("replica", replica);
        json.set("time", time);
        json.set("count", count);
        std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        oss.exceptions(std::ios::failbit);
        Poco::JSON::Stringifier::stringify(json, oss);
        return oss.str();
    }

    static LastExceptionEntry fromJsonString(const std::string & json_string)
    {
        LastExceptionEntry entry;
        if (json_string.empty())
            return entry;

        Poco::JSON::Parser parser;
        auto json = parser.parse(json_string).extract<Poco::JSON::Object::Ptr>();
        chassert(json);

        if (json->has("message"))
            entry.message = json->getValue<String>("message");
        if (json->has("part"))
            entry.part = json->getValue<String>("part");
        if (json->has("replica"))
            entry.replica = json->getValue<String>("replica");
        if (json->has("time"))
            entry.time = json->getValue<time_t>("time");
        if (json->has("count"))
            entry.count = json->getValue<size_t>("count");
        return entry;
    }
};

struct ExportReplicatedMergeTreePartitionProcessedPartEntry
{
    String part_name;
    std::vector<String> paths_in_destination;
    String finished_by;

    std::string toJsonString() const
    {
        Poco::JSON::Object json;
        json.set("part_name", part_name);
        json.set("paths_in_destination", paths_in_destination);
        json.set("finished_by", finished_by);
        std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        oss.exceptions(std::ios::failbit);
        Poco::JSON::Stringifier::stringify(json, oss);
        return oss.str();
    }

    static ExportReplicatedMergeTreePartitionProcessedPartEntry fromJsonString(const std::string & json_string)
    {
        Poco::JSON::Parser parser;
        auto json = parser.parse(json_string).extract<Poco::JSON::Object::Ptr>();
        chassert(json);

        ExportReplicatedMergeTreePartitionProcessedPartEntry entry;

        entry.part_name = json->getValue<String>("part_name");

        const auto paths_in_destination_array = json->getArray("paths_in_destination");
        for (size_t i = 0; i < paths_in_destination_array->size(); ++i)
            entry.paths_in_destination.emplace_back(paths_in_destination_array->getElement<String>(static_cast<unsigned int>(i)));

        entry.finished_by = json->getValue<String>("finished_by");

        return entry;
    }
};

/// Per-task "commit info" record persisted at <export-entry>/commit_info.
///
/// Written exactly once, atomically with the status -> COMPLETED transition
/// (see ExportPartitionUtils::commit). Captures the metadata-layer file paths
/// produced by the destination storage during commit so they can be surfaced in
/// system.replicated_partition_exports for debugging.
///
/// All Iceberg fields are empty for non-Iceberg destinations. They may also be
/// empty for an Iceberg destination if the committing replica crashed between
/// writing the object-storage files and writing this znode; in that case the
/// task still transitions to COMPLETED via the recovery path but commit_info
/// remains absent. This is best-effort observability and acceptable.
struct ExportReplicatedMergeTreePartitionCommitInfoEntry
{
    /// Iceberg: path (in destination object storage) of the new vN.metadata.json
    /// written by the commit.
    String iceberg_metadata_file;

    /// Iceberg: path of the snap-<id>-<format_version>-<uuid>.avro manifest list
    /// referenced by the new snapshot.
    String iceberg_manifest_list;

    /// Iceberg: path of the manifest entry file (*.avro) referenced by the
    /// manifest list.
    String iceberg_manifest_file;

    /// Plain object storage: path of the commit marker file written by
    /// StorageObjectStorage::commitExportPartitionTransaction. Empty for Iceberg.
    String commit_marker_file;

    std::string toJsonString() const
    {
        Poco::JSON::Object json;
        json.set("iceberg_metadata_file", iceberg_metadata_file);
        json.set("iceberg_manifest_list", iceberg_manifest_list);
        json.set("iceberg_manifest_file", iceberg_manifest_file);
        json.set("commit_marker_file", commit_marker_file);

        std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        oss.exceptions(std::ios::failbit);
        Poco::JSON::Stringifier::stringify(json, oss);
        return oss.str();
    }

    static ExportReplicatedMergeTreePartitionCommitInfoEntry fromJsonString(const std::string & json_string)
    {
        ExportReplicatedMergeTreePartitionCommitInfoEntry entry;
        if (json_string.empty())
            return entry;

        Poco::JSON::Parser parser;
        auto json = parser.parse(json_string).extract<Poco::JSON::Object::Ptr>();

        if (json->has("iceberg_metadata_file"))
            entry.iceberg_metadata_file = json->getValue<String>("iceberg_metadata_file");
        if (json->has("iceberg_manifest_list"))
            entry.iceberg_manifest_list = json->getValue<String>("iceberg_manifest_list");

        if (json->has("iceberg_manifest_file"))
            entry.iceberg_manifest_file = json->getValue<String>("iceberg_manifest_file");

        if (json->has("commit_marker_file"))
            entry.commit_marker_file = json->getValue<String>("commit_marker_file");

        return entry;
    }
};

struct ExportReplicatedMergeTreePartitionManifest
{
    String transaction_id;
    String query_id;
    String partition_id;
    String destination_database;
    String destination_table;
    String source_replica;
    size_t number_of_parts;
    std::vector<String> parts;
    time_t create_time;
    size_t retry_initial_backoff_seconds = 5;
    size_t retry_max_backoff_seconds = 300;
    size_t task_timeout_seconds;
    size_t max_threads;
    bool parallel_formatting;
    bool parquet_parallel_encoding;
    size_t max_bytes_per_file;
    size_t max_rows_per_file;
    MergeTreePartExportManifest::FileAlreadyExistsPolicy file_already_exists_policy;
    String filename_pattern;
    bool write_full_path_in_iceberg_metadata = false;
    bool allow_lossy_cast = false;
    String iceberg_metadata_json;

    /// Optional because of backwards compatibility
    std::optional<String> parquet_compression_method;
    std::optional<UInt64> output_format_compression_level;
    std::optional<UInt64> parquet_row_group_size;
    std::optional<UInt64> parquet_row_group_size_bytes;

    std::string toJsonString() const
    {
        Poco::JSON::Object json;
        json.set("transaction_id", transaction_id);
        json.set("query_id", query_id);
        json.set("partition_id", partition_id);
        json.set("destination_database", destination_database);
        json.set("destination_table", destination_table);
        json.set("source_replica", source_replica);
        json.set("number_of_parts", number_of_parts);

        if (!iceberg_metadata_json.empty())
        {
            json.set("iceberg_metadata_json", iceberg_metadata_json);
        }

        Poco::JSON::Array::Ptr parts_array = new Poco::JSON::Array();
        for (const auto & part : parts)
            parts_array->add(part);
        json.set("parts", parts_array);
        json.set("parallel_formatting", parallel_formatting);
        json.set("max_threads", max_threads);
        json.set("parquet_parallel_encoding", parquet_parallel_encoding);
        json.set("max_bytes_per_file", max_bytes_per_file);
        json.set("max_rows_per_file", max_rows_per_file);
        json.set("file_already_exists_policy", String(magic_enum::enum_name(file_already_exists_policy)));
        json.set("filename_pattern", filename_pattern);
        json.set("create_time", create_time);
        json.set("retry_initial_backoff_seconds", retry_initial_backoff_seconds);
        json.set("retry_max_backoff_seconds", retry_max_backoff_seconds);
        json.set("task_timeout_seconds", task_timeout_seconds);
        json.set("write_full_path_in_iceberg_metadata", write_full_path_in_iceberg_metadata);
        json.set("allow_lossy_cast", allow_lossy_cast);
        if (parquet_compression_method)
            json.set("parquet_compression_method", *parquet_compression_method);
        if (output_format_compression_level)
            json.set("output_format_compression_level", *output_format_compression_level);
        if (parquet_row_group_size)
            json.set("parquet_row_group_size", *parquet_row_group_size);
        if (parquet_row_group_size_bytes)
            json.set("parquet_row_group_size_bytes", *parquet_row_group_size_bytes);
        std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        oss.exceptions(std::ios::failbit);
        Poco::JSON::Stringifier::stringify(json, oss);
        return oss.str();
    }

    static ExportReplicatedMergeTreePartitionManifest fromJsonString(const std::string & json_string)
    {
        Poco::JSON::Parser parser;
        auto json = parser.parse(json_string).extract<Poco::JSON::Object::Ptr>();
        chassert(json);

        ExportReplicatedMergeTreePartitionManifest manifest;
        manifest.transaction_id = json->getValue<String>("transaction_id");
        manifest.query_id = json->getValue<String>("query_id");
        manifest.partition_id = json->getValue<String>("partition_id");
        manifest.destination_database = json->getValue<String>("destination_database");
        manifest.destination_table = json->getValue<String>("destination_table");
        manifest.source_replica = json->getValue<String>("source_replica");
        manifest.number_of_parts = json->getValue<size_t>("number_of_parts");

        if (json->has("retry_initial_backoff_seconds"))
        {
            manifest.retry_initial_backoff_seconds = json->getValue<size_t>("retry_initial_backoff_seconds");
        }

        if (json->has("retry_max_backoff_seconds"))
        {
            manifest.retry_max_backoff_seconds = json->getValue<size_t>("retry_max_backoff_seconds");
        }

        if (json->has("iceberg_metadata_json"))
        {
            manifest.iceberg_metadata_json = json->getValue<String>("iceberg_metadata_json");
        }

        auto parts_array = json->getArray("parts");
        for (size_t i = 0; i < parts_array->size(); ++i)
            manifest.parts.push_back(parts_array->getElement<String>(static_cast<unsigned int>(i)));
        
        manifest.create_time = json->getValue<time_t>("create_time");
        manifest.task_timeout_seconds = json->getValue<size_t>("task_timeout_seconds");
        manifest.max_threads = json->getValue<size_t>("max_threads");
        manifest.parallel_formatting = json->getValue<bool>("parallel_formatting");
        manifest.parquet_parallel_encoding = json->getValue<bool>("parquet_parallel_encoding");
        manifest.max_bytes_per_file = json->getValue<size_t>("max_bytes_per_file");
        manifest.max_rows_per_file = json->getValue<size_t>("max_rows_per_file");
        manifest.filename_pattern = json->getValue<String>("filename_pattern");

        if (json->has("file_already_exists_policy"))
        {
            const auto file_already_exists_policy = magic_enum::enum_cast<MergeTreePartExportManifest::FileAlreadyExistsPolicy>(json->getValue<String>("file_already_exists_policy"));
            if (file_already_exists_policy)
            {
                manifest.file_already_exists_policy = file_already_exists_policy.value();
            }

            /// what to do if it's not a valid value?
        }

        manifest.write_full_path_in_iceberg_metadata = json->getValue<bool>("write_full_path_in_iceberg_metadata");

        /// Default to true for tasks created before this field existed, so an in-flight
        /// export scheduled with the old permissive worker behavior is not wrongly rejected
        /// on upgrade. New tasks always persist the initiator's actual choice.
        manifest.allow_lossy_cast = json->has("allow_lossy_cast") ? json->getValue<bool>("allow_lossy_cast") : true;


        if (json->has("parquet_compression_method"))
        {
            manifest.parquet_compression_method = json->getValue<String>("parquet_compression_method");
        }

        if (json->has("output_format_compression_level"))
        {
            manifest.output_format_compression_level = json->getValue<UInt64>("output_format_compression_level");
        }

        if (json->has("parquet_row_group_size"))
        {
            manifest.parquet_row_group_size = json->getValue<UInt64>("parquet_row_group_size");
        }

        if (json->has("parquet_row_group_size_bytes"))
        {
            manifest.parquet_row_group_size_bytes = json->getValue<UInt64>("parquet_row_group_size_bytes");
        }

        return manifest;
    }
};

}
