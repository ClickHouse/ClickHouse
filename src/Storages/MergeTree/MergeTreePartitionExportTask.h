#pragma once

#include <base/types.h>
#include <ctime>
#include <optional>
#include <vector>
#include <Common/Exception.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Storages/ExportPartitionCommitInfoEntry.h>
#include <Storages/MergeTree/MergeTreePartExportManifest.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

/// On-disk descriptor for a plain (non-replicated) `MergeTree` partition export task.
struct MergeTreePartitionExportTask
{
    using FileAlreadyExistsPolicy = MergeTreePartExportManifest::FileAlreadyExistsPolicy;

    enum class Status
    {
        PENDING,
        COMPLETED,
        FAILED,
        KILLED,
    };

    struct PartProgress
    {
        String part_name;
        bool done = false;
        std::vector<String> paths_in_destination;
    };

    /// Best-effort record of the most recent failure observed for this task. `count` is a running
    /// total of failures and matches the semantics documented for `system.partition_exports`.
    struct LastException
    {
        String message;
        String part;    /// empty for task-level exceptions (commit failure)
        time_t time = 0;
        size_t count = 0;
    };

    /// Identity
    String transaction_id;
    String query_id;
    String partition_id;
    String source_database;
    String source_table;
    String destination_database;
    String destination_table;
    time_t create_time = 0;

    /// Work + progress
    std::vector<PartProgress> parts;
    Status status = Status::PENDING;
    LastException last_exception;

    std::optional<ExportPartitionCommitInfoEntry> commit_info;

    size_t retry_initial_backoff_seconds = 5;
    size_t retry_max_backoff_seconds = 300;
    size_t task_timeout_seconds = 86400;

    /// Export settings. Field names are intentionally identical to
    /// `ExportReplicatedMergeTreePartitionManifest` so that
    /// `ExportPartitionUtils::getContextCopyWithTaskSettings` works for both descriptors.
    size_t max_threads = 0;
    bool parallel_formatting = false;
    bool parquet_parallel_encoding = false;
    size_t max_bytes_per_file = 0;
    size_t max_rows_per_file = 0;
    FileAlreadyExistsPolicy file_already_exists_policy = FileAlreadyExistsPolicy::skip;
    String filename_pattern;
    bool write_full_path_in_iceberg_metadata = false;
    bool allow_lossy_cast = false;
    String iceberg_metadata_json;

    /// Optional for backwards compatibility with descriptors written before these
    /// settings were persisted (same shape as ExportReplicatedMergeTreePartitionManifest).
    std::optional<String> parquet_compression_method;
    std::optional<UInt64> output_format_compression_level;
    std::optional<UInt64> parquet_row_group_size;
    std::optional<UInt64> parquet_row_group_size_bytes;
    std::optional<MergeTreePartExportSchemaMatchMode> schema_match_mode;
    std::optional<bool> ignore_extra_source_columns;
    std::optional<String> iceberg_partition_timezone;

    size_t partsCount() const { return parts.size(); }

    size_t partsToDo() const
    {
        size_t to_do = 0;
        for (const auto & part : parts)
            if (!part.done)
                ++to_do;
        return to_do;
    }

    bool allPartsDone() const
    {
        for (const auto & part : parts)
            if (!part.done)
                return false;
        return true;
    }

    std::vector<String> partNames() const
    {
        std::vector<String> names;
        names.reserve(parts.size());
        for (const auto & part : parts)
            names.push_back(part.part_name);
        return names;
    }

    /// Flattened list of every destination path produced by the already-exported parts.
    std::vector<String> collectExportedPaths() const
    {
        std::vector<String> paths;
        for (const auto & part : parts)
            for (const auto & path : part.paths_in_destination)
                paths.push_back(path);
        return paths;
    }

    PartProgress * findPart(const String & part_name)
    {
        for (auto & part : parts)
            if (part.part_name == part_name)
                return &part;
        return nullptr;
    }

    std::string toJsonString() const
    {
        Poco::JSON::Object json;
        json.set("transaction_id", transaction_id);
        json.set("query_id", query_id);
        json.set("partition_id", partition_id);
        json.set("source_database", source_database);
        json.set("source_table", source_table);
        json.set("destination_database", destination_database);
        json.set("destination_table", destination_table);
        json.set("create_time", create_time);
        json.set("status", String(magic_enum::enum_name(status)));

        Poco::JSON::Array::Ptr parts_array = new Poco::JSON::Array();
        for (const auto & part : parts)
        {
            Poco::JSON::Object::Ptr part_object = new Poco::JSON::Object();
            part_object->set("part_name", part.part_name);
            part_object->set("done", part.done);
            Poco::JSON::Array::Ptr paths_array = new Poco::JSON::Array();
            for (const auto & path : part.paths_in_destination)
                paths_array->add(path);
            part_object->set("paths_in_destination", paths_array);
            parts_array->add(part_object);
        }
        json.set("parts", parts_array);

        Poco::JSON::Object::Ptr exception_object = new Poco::JSON::Object();
        exception_object->set("message", last_exception.message);
        exception_object->set("part", last_exception.part);
        exception_object->set("time", last_exception.time);
        exception_object->set("count", last_exception.count);
        json.set("last_exception", exception_object);

        if (commit_info)
            json.set("commit_info", commit_info->toJsonObject());

        json.set("retry_initial_backoff_seconds", retry_initial_backoff_seconds);
        json.set("retry_max_backoff_seconds", retry_max_backoff_seconds);
        json.set("task_timeout_seconds", task_timeout_seconds);
        json.set("max_threads", max_threads);
        json.set("parallel_formatting", parallel_formatting);
        json.set("parquet_parallel_encoding", parquet_parallel_encoding);
        json.set("max_bytes_per_file", max_bytes_per_file);
        json.set("max_rows_per_file", max_rows_per_file);
        json.set("file_already_exists_policy", String(magic_enum::enum_name(file_already_exists_policy)));
        json.set("filename_pattern", filename_pattern);
        json.set("write_full_path_in_iceberg_metadata", write_full_path_in_iceberg_metadata);
        json.set("allow_lossy_cast", allow_lossy_cast);
        if (!iceberg_metadata_json.empty())
            json.set("iceberg_metadata_json", iceberg_metadata_json);
        if (parquet_compression_method)
            json.set("parquet_compression_method", *parquet_compression_method);
        if (output_format_compression_level)
            json.set("output_format_compression_level", *output_format_compression_level);
        if (parquet_row_group_size)
            json.set("parquet_row_group_size", *parquet_row_group_size);
        if (parquet_row_group_size_bytes)
            json.set("parquet_row_group_size_bytes", *parquet_row_group_size_bytes);
        if (iceberg_partition_timezone)
            json.set("iceberg_partition_timezone", *iceberg_partition_timezone);
        if (schema_match_mode)
            json.set("schema_match_mode", String(magic_enum::enum_name(*schema_match_mode)));
        if (ignore_extra_source_columns)
            json.set("ignore_extra_source_columns", *ignore_extra_source_columns);

        std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        oss.exceptions(std::ios::failbit);
        Poco::JSON::Stringifier::stringify(json, oss);
        return oss.str();
    }

    static MergeTreePartitionExportTask fromJsonString(const std::string & json_string)
    {
        Poco::JSON::Parser parser;
        auto json = parser.parse(json_string).extract<Poco::JSON::Object::Ptr>();
        chassert(json);

        MergeTreePartitionExportTask task;
        task.transaction_id = json->getValue<String>("transaction_id");
        task.query_id = json->getValue<String>("query_id");
        task.partition_id = json->getValue<String>("partition_id");
        task.source_database = json->getValue<String>("source_database");
        task.source_table = json->getValue<String>("source_table");
        task.destination_database = json->getValue<String>("destination_database");
        task.destination_table = json->getValue<String>("destination_table");
        task.create_time = json->getValue<time_t>("create_time");

        const auto status_str = json->getValue<String>("status");
        if (const auto status = magic_enum::enum_cast<Status>(status_str))
            task.status = status.value();
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown status '{}' in partition export descriptor", status_str);

        const auto parts_array = json->getArray("parts");
        for (size_t i = 0; i < parts_array->size(); ++i)
        {
            const auto part_object = parts_array->getObject(static_cast<unsigned int>(i));
            PartProgress part;
            part.part_name = part_object->getValue<String>("part_name");
            part.done = part_object->getValue<bool>("done");
            const auto paths_array = part_object->getArray("paths_in_destination");
            for (size_t j = 0; j < paths_array->size(); ++j)
                part.paths_in_destination.push_back(paths_array->getElement<String>(static_cast<unsigned int>(j)));
            task.parts.push_back(std::move(part));
        }

        if (json->has("last_exception"))
        {
            const auto exception_object = json->getObject("last_exception");
            task.last_exception.message = exception_object->getValue<String>("message");
            task.last_exception.part = exception_object->getValue<String>("part");
            task.last_exception.time = exception_object->getValue<time_t>("time");
            task.last_exception.count = exception_object->getValue<size_t>("count");
        }

        if (json->has("commit_info"))
        {
            const auto commit_info_object = json->getObject("commit_info");
            if (!commit_info_object)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Field 'commit_info' of a partition export descriptor is not an object");
            task.commit_info = ExportPartitionCommitInfoEntry::fromJsonObject(commit_info_object);
        }

        task.retry_initial_backoff_seconds = json->getValue<size_t>("retry_initial_backoff_seconds");
        task.retry_max_backoff_seconds = json->getValue<size_t>("retry_max_backoff_seconds");
        task.task_timeout_seconds = json->getValue<size_t>("task_timeout_seconds");
        task.max_threads = json->getValue<size_t>("max_threads");
        task.parallel_formatting = json->getValue<bool>("parallel_formatting");
        task.parquet_parallel_encoding = json->getValue<bool>("parquet_parallel_encoding");
        task.max_bytes_per_file = json->getValue<size_t>("max_bytes_per_file");
        task.max_rows_per_file = json->getValue<size_t>("max_rows_per_file");

        const auto policy_str = json->getValue<String>("file_already_exists_policy");
        if (const auto policy = magic_enum::enum_cast<FileAlreadyExistsPolicy>(policy_str))
            task.file_already_exists_policy = policy.value();
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown file_already_exists_policy '{}' in partition export descriptor", policy_str);

        task.filename_pattern = json->getValue<String>("filename_pattern");
        task.write_full_path_in_iceberg_metadata = json->getValue<bool>("write_full_path_in_iceberg_metadata");
        task.allow_lossy_cast = json->getValue<bool>("allow_lossy_cast");
        if (json->has("iceberg_metadata_json"))
            task.iceberg_metadata_json = json->getValue<String>("iceberg_metadata_json");
        if (json->has("parquet_compression_method"))
            task.parquet_compression_method = json->getValue<String>("parquet_compression_method");
        if (json->has("output_format_compression_level"))
            task.output_format_compression_level = json->getValue<UInt64>("output_format_compression_level");
        if (json->has("parquet_row_group_size"))
            task.parquet_row_group_size = json->getValue<UInt64>("parquet_row_group_size");
        if (json->has("parquet_row_group_size_bytes"))
            task.parquet_row_group_size_bytes = json->getValue<UInt64>("parquet_row_group_size_bytes");
        if (json->has("iceberg_partition_timezone"))
            task.iceberg_partition_timezone = json->getValue<String>("iceberg_partition_timezone");
        /// Left unset (nullopt) for tasks created before these fields existed - such tasks were
        /// always scheduled under the old, strict column-matching check, so callers should treat
        /// an absent value as `POSITION` with `ignore_extra_source_columns = false`.
        if (json->has("schema_match_mode"))
        {
            const auto mode_str = json->getValue<String>("schema_match_mode");
            if (const auto mode = magic_enum::enum_cast<MergeTreePartExportSchemaMatchMode>(mode_str))
                task.schema_match_mode = mode;
            else
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown schema_match_mode '{}' in partition export descriptor", mode_str);
        }
        if (json->has("ignore_extra_source_columns"))
            task.ignore_extra_source_columns = json->getValue<bool>("ignore_extra_source_columns");

        return task;
    }
};

}
