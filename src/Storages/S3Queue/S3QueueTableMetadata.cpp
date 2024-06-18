#include <config.h>

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Storages/S3Queue/S3QueueSettings.h>
#include <Storages/S3Queue/S3QueueTableMetadata.h>
#include <Storages/S3Queue/S3QueueMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>


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
    const StorageObjectStorage::Configuration & configuration,
    const S3QueueSettings & engine_settings,
    const StorageInMemoryMetadata & storage_metadata)
{
    format_name = configuration.format;
    after_processing = engine_settings.after_processing.toString();
    mode = engine_settings.mode.toString();
    tracked_files_limit = engine_settings.s3queue_tracked_files_limit;
    tracked_file_ttl_sec = engine_settings.s3queue_tracked_file_ttl_sec;
    buckets = engine_settings.s3queue_buckets;
    processing_threads_num = engine_settings.s3queue_processing_threads_num;
    columns = storage_metadata.getColumns().toString();
}

String S3QueueTableMetadata::toString() const
{
    Poco::JSON::Object json;
    json.set("after_processing", after_processing);
    json.set("mode", mode);
    json.set("tracked_files_limit", tracked_files_limit);
    json.set("tracked_file_ttl_sec", tracked_file_ttl_sec);
    json.set("processing_threads_num", processing_threads_num);
    json.set("buckets", buckets);
    json.set("format_name", format_name);
    json.set("columns", columns);
    json.set("last_processed_file", last_processed_path);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
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

    format_name = json->getValue<String>("format_name");
    columns = json->getValue<String>("columns");

    /// Check with "s3queue_" prefix for compatibility.
    {
        if (json->has("s3queue_tracked_files_limit"))
            tracked_files_limit = json->getValue<UInt64>("s3queue_tracked_files_limit");
        if (json->has("s3queue_tracked_file_ttl_sec"))
            tracked_file_ttl_sec = json->getValue<UInt64>("s3queue_tracked_file_ttl_sec");
        if (json->has("s3queue_processing_threads_num"))
            processing_threads_num = json->getValue<UInt64>("s3queue_processing_threads_num");
    }

    if (json->has("tracked_files_limit"))
        tracked_files_limit = json->getValue<UInt64>("tracked_files_limit");

    if (json->has("tracked_file_ttl_sec"))
        tracked_file_ttl_sec = json->getValue<UInt64>("tracked_file_ttl_sec");

    if (json->has("last_processed_file"))
        last_processed_path = json->getValue<String>("last_processed_file");

    if (json->has("processing_threads_num"))
        processing_threads_num = json->getValue<UInt64>("processing_threads_num");

    if (json->has("buckets"))
        buckets = json->getValue<UInt64>("buckets");
}

S3QueueTableMetadata S3QueueTableMetadata::parse(const String & metadata_str)
{
    S3QueueTableMetadata metadata;
    metadata.read(metadata_str);
    return metadata;
}

void S3QueueTableMetadata::checkEquals(const S3QueueTableMetadata & from_zk) const
{
    checkImmutableFieldsEquals(from_zk);
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

    if (tracked_files_limit != from_zk.tracked_files_limit)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in max set size. "
            "Stored in ZooKeeper: {}, local: {}",
            from_zk.tracked_files_limit,
            tracked_files_limit);

    if (tracked_file_ttl_sec != from_zk.tracked_file_ttl_sec)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in max set age. "
            "Stored in ZooKeeper: {}, local: {}",
            from_zk.tracked_file_ttl_sec,
            tracked_file_ttl_sec);

    if (format_name != from_zk.format_name)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in format name. "
            "Stored in ZooKeeper: {}, local: {}",
            from_zk.format_name,
            format_name);

    if (last_processed_path != from_zk.last_processed_path)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in last processed path. "
            "Stored in ZooKeeper: {}, local: {}",
            from_zk.last_processed_path,
            last_processed_path);

    if (modeFromString(mode) == S3QueueMode::ORDERED)
    {
        if (buckets != from_zk.buckets)
        {
            throw Exception(
                ErrorCodes::METADATA_MISMATCH,
                "Existing table metadata in ZooKeeper differs in s3queue_buckets setting. "
                "Stored in ZooKeeper: {}, local: {}",
                from_zk.buckets, buckets);
        }

        if (S3QueueMetadata::getBucketsNum(*this) != S3QueueMetadata::getBucketsNum(from_zk))
        {
            throw Exception(
                ErrorCodes::METADATA_MISMATCH,
                "Existing table metadata in ZooKeeper differs in processing buckets. "
                "Stored in ZooKeeper: {}, local: {}",
                S3QueueMetadata::getBucketsNum(*this), S3QueueMetadata::getBucketsNum(from_zk));
        }
    }
}

void S3QueueTableMetadata::checkEquals(const S3QueueSettings & current, const S3QueueSettings & expected)
{
    if (current.after_processing != expected.after_processing)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs "
            "in action after processing. Stored in ZooKeeper: {}, local: {}",
            expected.after_processing.toString(),
            current.after_processing.toString());

    if (current.mode != expected.mode)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in engine mode. "
            "Stored in ZooKeeper: {}, local: {}",
            expected.mode.toString(),
            current.mode.toString());

    if (current.s3queue_tracked_files_limit != expected.s3queue_tracked_files_limit)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in max set size. "
            "Stored in ZooKeeper: {}, local: {}",
            expected.s3queue_tracked_files_limit,
            current.s3queue_tracked_files_limit);

    if (current.s3queue_tracked_file_ttl_sec != expected.s3queue_tracked_file_ttl_sec)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in max set age. "
            "Stored in ZooKeeper: {}, local: {}",
            expected.s3queue_tracked_file_ttl_sec,
            current.s3queue_tracked_file_ttl_sec);

    if (current.s3queue_last_processed_path.value != expected.s3queue_last_processed_path.value)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in last_processed_path. "
            "Stored in ZooKeeper: {}, local: {}",
            expected.s3queue_last_processed_path.value,
            current.s3queue_last_processed_path.value);

    if (current.mode == S3QueueMode::ORDERED)
    {
        if (current.s3queue_buckets != expected.s3queue_buckets)
        {
            throw Exception(
                ErrorCodes::METADATA_MISMATCH,
                "Existing table metadata in ZooKeeper differs in s3queue_buckets setting. "
                "Stored in ZooKeeper: {}, local: {}",
                expected.s3queue_buckets, current.s3queue_buckets);
        }

        if (S3QueueMetadata::getBucketsNum(current) != S3QueueMetadata::getBucketsNum(expected))
        {
            throw Exception(
                ErrorCodes::METADATA_MISMATCH,
                "Existing table metadata in ZooKeeper differs in processing buckets. "
                "Stored in ZooKeeper: {}, local: {}",
                S3QueueMetadata::getBucketsNum(current), S3QueueMetadata::getBucketsNum(expected));
        }
    }
}
}
