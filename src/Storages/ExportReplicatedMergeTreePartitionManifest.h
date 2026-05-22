#pragma once

#include <base/types.h>
#include <Common/Exception.h>
#include <Interpreters/StorageID.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Storages/MergeTree/MergeTreePartExportManifest.h>

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
    size_t retry_count;
    String finished_by;

    std::string toJsonString() const
    {
        Poco::JSON::Object json;

        json.set("part_name", part_name);
        json.set("status", String(magic_enum::enum_name(status)));
        json.set("retry_count", retry_count);
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
        entry.retry_count = json->getValue<size_t>("retry_count");
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
    size_t max_retries;
    size_t ttl_seconds;
    size_t task_timeout_seconds;
    size_t max_threads;
    bool parallel_formatting;
    bool parquet_parallel_encoding;
    size_t max_bytes_per_file;
    size_t max_rows_per_file;
    MergeTreePartExportManifest::FileAlreadyExistsPolicy file_already_exists_policy;
    String filename_pattern;
    bool write_full_path_in_iceberg_metadata = false;
    String iceberg_metadata_json;

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
        json.set("max_retries", max_retries);
        json.set("ttl_seconds", ttl_seconds);
        json.set("task_timeout_seconds", task_timeout_seconds);
        json.set("write_full_path_in_iceberg_metadata", write_full_path_in_iceberg_metadata);
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
        manifest.max_retries = json->getValue<size_t>("max_retries");

        if (json->has("iceberg_metadata_json"))
        {
            manifest.iceberg_metadata_json = json->getValue<String>("iceberg_metadata_json");
        }

        auto parts_array = json->getArray("parts");
        for (size_t i = 0; i < parts_array->size(); ++i)
            manifest.parts.push_back(parts_array->getElement<String>(static_cast<unsigned int>(i)));
        
        manifest.create_time = json->getValue<time_t>("create_time");
        manifest.ttl_seconds = json->getValue<size_t>("ttl_seconds");
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

        return manifest;
    }
};

}
