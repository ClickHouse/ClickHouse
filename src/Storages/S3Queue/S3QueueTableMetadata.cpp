#include <config.h>

#if USE_AWS_S3

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Storages/S3Queue/S3QueueSettings.h>
#include <Storages/S3Queue/S3QueueTableMetadata.h>
#include <Storages/StorageS3.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int METADATA_MISMATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    S3QueueMode modeFromString(const std::string & mode)
    {
        if (mode == "ordered")
            return S3QueueMode::ORDERED;
        if (mode == "unordered")
            return S3QueueMode::UNORDERED;
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected S3Queue mode: {}", mode);
    }
}


S3QueueTableMetadata::S3QueueTableMetadata(
    const StorageS3::Configuration & configuration,
    const S3QueueSettings & engine_settings,
    const StorageInMemoryMetadata & storage_metadata)
{
    format_name = configuration.format;
    after_processing = engine_settings.after_processing.toString();
    mode = engine_settings.mode.toString();
    s3queue_tracked_files_limit = engine_settings.s3queue_tracked_files_limit;
    s3queue_tracked_file_ttl_sec = engine_settings.s3queue_tracked_file_ttl_sec;
    s3queue_total_shards_num = engine_settings.s3queue_total_shards_num;
    s3queue_processing_threads_num = engine_settings.s3queue_processing_threads_num;
    columns = storage_metadata.getColumns().toString();
}

String S3QueueTableMetadata::toString() const
{
    Poco::JSON::Object json;
    json.set("after_processing", after_processing);
    json.set("mode", mode);
    json.set("s3queue_tracked_files_limit", s3queue_tracked_files_limit);
    json.set("s3queue_tracked_file_ttl_sec", s3queue_tracked_file_ttl_sec);
    json.set("s3queue_total_shards_num", s3queue_total_shards_num);
    json.set("s3queue_processing_threads_num", s3queue_processing_threads_num);
    json.set("format_name", format_name);
    json.set("columns", columns);

    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);
    return oss.str();
}

void S3QueueTableMetadata::read(const String & metadata_str)
{
    Poco::JSON::Parser parser;
    auto json = parser.parse(metadata_str).extract<Poco::JSON::Object::Ptr>();

    after_processing = json->getValue<String>("after_processing");
    mode = json->getValue<String>("mode");
    s3queue_tracked_files_limit = json->getValue<UInt64>("s3queue_tracked_files_limit");
    s3queue_tracked_file_ttl_sec = json->getValue<UInt64>("s3queue_tracked_file_ttl_sec");
    format_name = json->getValue<String>("format_name");
    columns = json->getValue<String>("columns");

    if (json->has("s3queue_total_shards_num"))
        s3queue_total_shards_num = json->getValue<UInt64>("s3queue_total_shards_num");
    else
        s3queue_total_shards_num = 1;

    if (json->has("s3queue_processing_threads_num"))
        s3queue_processing_threads_num = json->getValue<UInt64>("s3queue_processing_threads_num");
    else
        s3queue_processing_threads_num = 1;
}

S3QueueTableMetadata S3QueueTableMetadata::parse(const String & metadata_str)
{
    S3QueueTableMetadata metadata;
    metadata.read(metadata_str);
    return metadata;
}

void S3QueueTableMetadata::checkImmutableFieldsEquals(const S3QueueTableMetadata & from_zk) const
{
    if (after_processing != from_zk.after_processing)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs "
            "in action after processing. Stored in ZooKeeper: {}, local: {}",
            DB::toString(from_zk.after_processing),
            DB::toString(after_processing));

    if (mode != from_zk.mode)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in engine mode. "
            "Stored in ZooKeeper: {}, local: {}",
            from_zk.mode,
            mode);

    if (s3queue_tracked_files_limit != from_zk.s3queue_tracked_files_limit)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in max set size. "
            "Stored in ZooKeeper: {}, local: {}",
            from_zk.s3queue_tracked_files_limit,
            s3queue_tracked_files_limit);

    if (s3queue_tracked_file_ttl_sec != from_zk.s3queue_tracked_file_ttl_sec)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in max set age. "
            "Stored in ZooKeeper: {}, local: {}",
            from_zk.s3queue_tracked_file_ttl_sec,
            s3queue_tracked_file_ttl_sec);

    if (format_name != from_zk.format_name)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in format name. "
            "Stored in ZooKeeper: {}, local: {}",
            from_zk.format_name,
            format_name);

    if (modeFromString(mode) == S3QueueMode::ORDERED)
    {
        if (s3queue_processing_threads_num != from_zk.s3queue_processing_threads_num)
        {
            throw Exception(
                ErrorCodes::METADATA_MISMATCH,
                "Existing table metadata in ZooKeeper differs in s3queue_processing_threads_num setting. "
                "Stored in ZooKeeper: {}, local: {}",
                from_zk.s3queue_processing_threads_num,
                s3queue_processing_threads_num);
        }
        if (s3queue_total_shards_num != from_zk.s3queue_total_shards_num)
        {
            throw Exception(
                ErrorCodes::METADATA_MISMATCH,
                "Existing table metadata in ZooKeeper differs in s3queue_total_shards_num setting. "
                "Stored in ZooKeeper: {}, local: {}",
                from_zk.s3queue_total_shards_num,
                s3queue_total_shards_num);
        }
    }
}

void S3QueueTableMetadata::checkEquals(const S3QueueTableMetadata & from_zk) const
{
    checkImmutableFieldsEquals(from_zk);
}

}

#endif
