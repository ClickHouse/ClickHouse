#include <config.h>

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSettings.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueTableMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
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
    ObjectStorageQueueMode modeFromString(const std::string & mode)
    {
        if (mode == "ordered")
            return ObjectStorageQueueMode::ORDERED;
        if (mode == "unordered")
            return ObjectStorageQueueMode::UNORDERED;
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected ObjectStorageQueue mode: {}", mode);
    }
}


ObjectStorageQueueTableMetadata::ObjectStorageQueueTableMetadata(
    const StorageObjectStorage::Configuration & configuration,
    const ObjectStorageQueueSettings & engine_settings,
    const StorageInMemoryMetadata & storage_metadata)
{
    format_name = configuration.format;
    after_processing = engine_settings.after_processing.toString();
    mode = engine_settings.mode.toString();
    tracked_files_limit = engine_settings.tracked_files_limit;
    tracked_file_ttl_sec = engine_settings.tracked_file_ttl_sec;
    buckets = engine_settings.buckets;
    processing_threads_num = engine_settings.processing_threads_num;
    columns = storage_metadata.getColumns().toString();
}

String ObjectStorageQueueTableMetadata::toString() const
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

void ObjectStorageQueueTableMetadata::read(const String & metadata_str)
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

ObjectStorageQueueTableMetadata ObjectStorageQueueTableMetadata::parse(const String & metadata_str)
{
    ObjectStorageQueueTableMetadata metadata;
    metadata.read(metadata_str);
    return metadata;
}

void ObjectStorageQueueTableMetadata::checkEquals(const ObjectStorageQueueTableMetadata & from_zk) const
{
    checkImmutableFieldsEquals(from_zk);
}

void ObjectStorageQueueTableMetadata::checkImmutableFieldsEquals(const ObjectStorageQueueTableMetadata & from_zk) const
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

    if (modeFromString(mode) == ObjectStorageQueueMode::ORDERED)
    {
        if (buckets != from_zk.buckets)
        {
            throw Exception(
                ErrorCodes::METADATA_MISMATCH,
                "Existing table metadata in ZooKeeper differs in buckets setting. "
                "Stored in ZooKeeper: {}, local: {}",
                from_zk.buckets, buckets);
        }

        if (ObjectStorageQueueMetadata::getBucketsNum(*this) != ObjectStorageQueueMetadata::getBucketsNum(from_zk))
        {
            throw Exception(
                ErrorCodes::METADATA_MISMATCH,
                "Existing table metadata in ZooKeeper differs in processing buckets. "
                "Stored in ZooKeeper: {}, local: {}",
                ObjectStorageQueueMetadata::getBucketsNum(*this), ObjectStorageQueueMetadata::getBucketsNum(from_zk));
        }
    }
}

void ObjectStorageQueueTableMetadata::checkEquals(const ObjectStorageQueueSettings & current, const ObjectStorageQueueSettings & expected)
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

    if (current.tracked_files_limit != expected.tracked_files_limit)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in max set size. "
            "Stored in ZooKeeper: {}, local: {}",
            expected.tracked_files_limit,
            current.tracked_files_limit);

    if (current.tracked_file_ttl_sec != expected.tracked_file_ttl_sec)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in max set age. "
            "Stored in ZooKeeper: {}, local: {}",
            expected.tracked_file_ttl_sec,
            current.tracked_file_ttl_sec);

    if (current.last_processed_path.value != expected.last_processed_path.value)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in last_processed_path. "
            "Stored in ZooKeeper: {}, local: {}",
            expected.last_processed_path.value,
            current.last_processed_path.value);

    if (current.mode == ObjectStorageQueueMode::ORDERED)
    {
        if (current.buckets != expected.buckets)
        {
            throw Exception(
                ErrorCodes::METADATA_MISMATCH,
                "Existing table metadata in ZooKeeper differs in buckets setting. "
                "Stored in ZooKeeper: {}, local: {}",
                expected.buckets, current.buckets);
        }

        if (ObjectStorageQueueMetadata::getBucketsNum(current) != ObjectStorageQueueMetadata::getBucketsNum(expected))
        {
            throw Exception(
                ErrorCodes::METADATA_MISMATCH,
                "Existing table metadata in ZooKeeper differs in processing buckets. "
                "Stored in ZooKeeper: {}, local: {}",
                ObjectStorageQueueMetadata::getBucketsNum(current), ObjectStorageQueueMetadata::getBucketsNum(expected));
        }
    }
}
}
