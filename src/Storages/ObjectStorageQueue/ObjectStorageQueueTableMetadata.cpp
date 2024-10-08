#include <config.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSettings.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueTableMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Common/getNumberOfCPUCoresToUse.h>


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

    void validateMode(const std::string & mode)
    {
        if (mode != "ordered" && mode != "unordered")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected ObjectStorageQueue mode: {}", mode);
    }
}


ObjectStorageQueueTableMetadata::ObjectStorageQueueTableMetadata(
    const ObjectStorageQueueSettings & engine_settings,
    const ColumnsDescription & columns_,
    const std::string & format_)
    : format_name(format_)
    , columns(columns_.toString())
    , after_processing(engine_settings.after_processing.toString())
    , mode(engine_settings.mode.toString())
    , tracked_files_limit(engine_settings.tracked_files_limit)
    , tracked_files_ttl_sec(engine_settings.tracked_file_ttl_sec)
    , buckets(engine_settings.buckets)
    , last_processed_path(engine_settings.last_processed_path)
    , loading_retries(engine_settings.loading_retries)
{
    processing_threads_num_changed = engine_settings.processing_threads_num.changed;
    if (!processing_threads_num_changed && engine_settings.processing_threads_num <= 1)
        processing_threads_num = std::max<uint32_t>(getNumberOfCPUCoresToUse(), 16);
    else
        processing_threads_num = engine_settings.processing_threads_num;
}

String ObjectStorageQueueTableMetadata::toString() const
{
    Poco::JSON::Object json;
    json.set("after_processing", after_processing);
    json.set("mode", mode);
    json.set("tracked_files_limit", tracked_files_limit);
    json.set("tracked_files_ttl_sec", tracked_files_ttl_sec);
    json.set("processing_threads_num", processing_threads_num);
    json.set("buckets", buckets);
    json.set("format_name", format_name);
    json.set("columns", columns);
    json.set("last_processed_file", last_processed_path);
    json.set("loading_retries", loading_retries);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);
    return oss.str();
}

ObjectStorageQueueMode ObjectStorageQueueTableMetadata::getMode() const
{
    return modeFromString(mode);
}

template <typename T>
static auto getOrDefault(
    const Poco::JSON::Object::Ptr & json,
    const std::string & setting,
    const std::string & compatibility_prefix,
    const T & default_value)
{
    if (!compatibility_prefix.empty() && json->has(compatibility_prefix + setting))
        return json->getValue<T>(compatibility_prefix + setting);

    if (json->has(setting))
        return json->getValue<T>(setting);

    return default_value;
}

ObjectStorageQueueTableMetadata::ObjectStorageQueueTableMetadata(const Poco::JSON::Object::Ptr & json)
    : format_name(json->getValue<String>("format_name"))
    , columns(json->getValue<String>("columns"))
    , after_processing(json->getValue<String>("after_processing"))
    , mode(json->getValue<String>("mode"))
    , tracked_files_limit(getOrDefault(json, "tracked_files_limit", "s3queue_", 0))
    , tracked_files_ttl_sec(getOrDefault(json, "tracked_files_ttl_sec", "", getOrDefault(json, "tracked_file_ttl_sec", "s3queue_", 0)))
    , buckets(getOrDefault(json, "buckets", "", 0))
    , last_processed_path(getOrDefault<String>(json, "last_processed_file", "s3queue_", ""))
    , loading_retries(getOrDefault(json, "loading_retries", "", 10))
    , processing_threads_num(getOrDefault(json, "processing_threads_num", "s3queue_", 1))
{
    validateMode(mode);
}

ObjectStorageQueueTableMetadata ObjectStorageQueueTableMetadata::parse(const String & metadata_str)
{
    Poco::JSON::Parser parser;
    auto json = parser.parse(metadata_str).extract<Poco::JSON::Object::Ptr>();
    return ObjectStorageQueueTableMetadata(json);
}

void ObjectStorageQueueTableMetadata::adjustFromKeeper(const ObjectStorageQueueTableMetadata & from_zk)
{
    if (processing_threads_num != from_zk.processing_threads_num)
    {
        auto log = getLogger("ObjectStorageQueueTableMetadata");
        const std::string message = fmt::format(
            "Using `processing_threads_num` from keeper: {} (local: {})",
            from_zk.processing_threads_num, processing_threads_num);

        if (processing_threads_num_changed)
            LOG_WARNING(log, "{}", message);
        else
            LOG_TRACE(log, "{}", message);

        processing_threads_num = from_zk.processing_threads_num;
    }
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
            "Existing table metadata in ZooKeeper differs in `tracked_files_limit`. "
            "Stored in ZooKeeper: {}, local: {}",
            from_zk.tracked_files_limit,
            tracked_files_limit);

    if (tracked_files_ttl_sec != from_zk.tracked_files_ttl_sec)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in `tracked_files_ttl_sec`. "
            "Stored in ZooKeeper: {}, local: {}",
            from_zk.tracked_files_ttl_sec,
            tracked_files_ttl_sec);

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
                ObjectStorageQueueMetadata::getBucketsNum(from_zk), ObjectStorageQueueMetadata::getBucketsNum(*this));
        }
    }

    if (columns != from_zk.columns)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in columns. "
            "Stored in ZooKeeper: {}, local: {}",
            from_zk.columns,
            columns);
}

}
