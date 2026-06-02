#pragma once
#include <config.h>

#if USE_AVRO

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>
#include <Core/Field.h>
#include <Core/TypeId.h>
#include <Disks/IStoragePolicy.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <IO/ReadSettings.h>
#include <Interpreters/Context_fwd.h>
#include <base/Decimal.h>
#include <base/types.h>

#include <Storages/ObjectStorage/DataLakes/Common/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Types.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Utils.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

using namespace Paimon;


struct PaimonSnapshot
{
    Int64 id{-1};
    Int64 schema_id{};
    String base_manifest_list;
    String delta_manifest_list;
    String commit_user;
    Int64 commit_identifier{};
    String commit_kind;
    Int64 time_millis{};

    /// nullable
    std::optional<Int32> version;
    std::optional<String> index_manifest;
    std::optional<Int64> base_manifest_list_size;
    std::optional<Int64> delta_manifest_list_size;
    std::optional<String> changelog_manifest_list;
    std::optional<Int64> changelog_manifest_list_size;
    std::optional<std::unordered_map<Int32, Int64>> log_offsets;
    std::optional<Int64> total_record_count;
    std::optional<Int64> delta_record_count;
    std::optional<Int64> changelog_record_count;
    std::optional<Int64> watermark;
    std::optional<String> statistics;

    bool operator==(const PaimonSnapshot & other) const
    {
        return version == other.version && id == other.id && schema_id == other.schema_id;
    }

    explicit PaimonSnapshot(const Poco::JSON::Object::Ptr & json_object);
};
using PaimonSnapshotPtr = std::shared_ptr<PaimonSnapshot>;

struct SimpleStats
{
    struct MemoryEstimator
    {
        static size_t getSizeInMemory(const Field & value)
        {
            size_t size = 0;

            switch (value.getType())
            {
                case Field::Types::String:
                    size += value.safeGet<String>().capacity();
                    break;
                case Field::Types::Array:
                    size += getFieldContainerSizeInMemory(value.safeGet<Array>());
                    break;
                case Field::Types::Tuple:
                    size += getFieldContainerSizeInMemory(value.safeGet<Tuple>());
                    break;
                case Field::Types::Map:
                    size += getFieldContainerSizeInMemory(value.safeGet<Map>());
                    break;
                case Field::Types::Object:
                    size += getObjectSizeInMemory(value.safeGet<Object>());
                    break;
                case Field::Types::AggregateFunctionState:
                {
                    const auto & data = value.safeGet<AggregateFunctionStateData>();
                    size += data.name.capacity();
                    size += data.data.capacity();
                    break;
                }
                default:
                    break;
            }

            return size;
        }

        static size_t getSizeInMemory(const Array & values)
        {
            return getFieldContainerSizeInMemory(values);
        }

    private:
        template <typename TFieldContainer>
        static size_t getFieldContainerSizeInMemory(const TFieldContainer & values)
        {
            size_t size = values.capacity() * sizeof(Field);
            for (const auto & element : values)
                size += getSizeInMemory(element);
            return size;
        }

        static size_t getObjectSizeInMemory(const Object & object)
        {
            size_t size = 0;
            for (const auto & [key, value] : object)
            {
                size += sizeof(std::pair<const String, Field>);
                size += key.capacity();
                size += getSizeInMemory(value);
            }
            return size;
        }
    };

    String min_values;
    String max_values;
    Array null_counts;

    SimpleStats(const Iceberg::AvroForIcebergDeserializer & avro_deserializer, const String & root_path, const size_t row_num)
    {
        max_values
            = avro_deserializer.getValueFromRowByName(row_num, concatPath({root_path, COLUMN_SIMPLE_STATS_MAX_VALUES}), TypeIndex::String)
                  .safeGet<std::string>();
        min_values
            = avro_deserializer.getValueFromRowByName(row_num, concatPath({root_path, COLUMN_SIMPLE_STATS_MIN_VALUES}), TypeIndex::String)
                  .safeGet<std::string>();
        null_counts
            = avro_deserializer.getValueFromRowByName(row_num, concatPath({root_path, COLUMN_SIMPLE_STATS_NULL_COUNTS}), TypeIndex::Array)
                  .safeGet<Array>();
    }

    size_t getSizeInMemory() const
    {
        size_t size = 0;
        size += min_values.capacity();
        size += max_values.capacity();
        size += MemoryEstimator::getSizeInMemory(null_counts);
        return size;
    }
};

struct PaimonManifestFileMeta
{
    String file_name;
    Int64 file_size;
    Int64 num_added_files;
    Int64 num_deleted_files;
    SimpleStats partition_stats;
    Int64 schema_id;

    PaimonManifestFileMeta(const Iceberg::AvroForIcebergDeserializer & avro_deserializer, const String & root_path, const size_t row_num)
        : partition_stats(avro_deserializer, concatPath({root_path, COLUMN_PAIMON_MANIFEST_LIST_PARTITION_STATS}), row_num)
    {
        file_name = avro_deserializer
                        .getValueFromRowByName(row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_LIST_FILE_NAME}), TypeIndex::String)
                        .safeGet<std::string>();
        file_size = avro_deserializer
                        .getValueFromRowByName(row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_LIST_FILE_SIZE}), TypeIndex::Int64)
                        .safeGet<Int64>();
        num_added_files
            = avro_deserializer
                  .getValueFromRowByName(row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_LIST_NUM_ADDED_FILES}), TypeIndex::Int64)
                  .safeGet<Int64>();
        num_deleted_files
            = avro_deserializer
                  .getValueFromRowByName(row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_LIST_NUM_DELETED_FILES}), TypeIndex::Int64)
                  .safeGet<Int64>();
        schema_id = avro_deserializer.getValueFromRowByName(row_num, COLUMN_PAIMON_MANIFEST_SCHEMA_ID, TypeIndex::Int64).safeGet<Int64>();
    }

    size_t getSizeInMemory() const
    {
        size_t size = 0;
        size += file_name.capacity();
        size += partition_stats.getSizeInMemory();
        return size;
    }
};

struct PaimonManifestEntry
{
    enum class Kind : int8_t
    {
        ADD = 0,
        DELETE = 1,
    };
    enum class FileSource : int8_t
    {
        APPEND = 0,
        COMPACT = 1,
    };
    static PaimonManifestEntry::Kind toKind(int8_t value)
    {
        if (value < static_cast<int8_t>(PaimonManifestEntry::Kind::ADD) || value > static_cast<int8_t>(PaimonManifestEntry::Kind::DELETE))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected value for PaimonManifestEntry::Kind {}", value);
        return static_cast<PaimonManifestEntry::Kind>(value);
    }

    static PaimonManifestEntry::FileSource toFileSource(int8_t value)
    {
        if (value < static_cast<int8_t>(PaimonManifestEntry::FileSource::APPEND)
            || value > static_cast<int8_t>(PaimonManifestEntry::FileSource::COMPACT))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected value for PaimonManifestEntry::FileSource {}", value);
        return static_cast<PaimonManifestEntry::FileSource>(value);
    }

    struct DataFileMeta
    {
    private:
        template <typename T>
        void getNullableValueFromRowByName(
            std::optional<T> & res,
            const Iceberg::AvroForIcebergDeserializer & avro_deserializer,
            const size_t row_num,
            const String & path)
        {
            auto field = avro_deserializer.getValueFromRowByName(row_num, path);
            if (!field.isNull())
                res = field.safeGet<T>();
        }

    public:
        String file_name;
        String bucket_path;
        Int64 file_size;
        Int64 row_count;
        String min_key;
        String max_key;
        SimpleStats key_stats;
        SimpleStats value_stats;
        Int64 min_sequence_number;
        Int64 max_sequence_number;
        Int64 schema_id;
        Int32 level;
        Array extra_files;
        std::optional<DateTime64> creation_time;
        std::optional<Int64> delete_row_count;
        std::optional<String> embedded_file_index;
        std::optional<FileSource> file_source;
        std::optional<Array> value_stats_cols;

        DataFileMeta(
            const Iceberg::AvroForIcebergDeserializer & avro_deserializer,
            const String & root_path,
            const size_t row_num,
            const String & partition_,
            Int32 bucket_,
            const PaimonTableSchema & table_schema,
            const String & partition_default_name)
            : key_stats(avro_deserializer, concatPath({root_path, COLUMN_PAIMON_MANIFEST_FILE_KEY_STATS}), row_num)
            , value_stats(avro_deserializer, concatPath({root_path, COLUMN_PAIMON_MANIFEST_FILE_VALUE_STATS}), row_num)
        {
            file_name
                = avro_deserializer
                      .getValueFromRowByName(row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_FILE_FILE_NAME}), TypeIndex::String)
                      .safeGet<std::string>();
            bucket_path = Paimon::getBucketPath(partition_, bucket_, table_schema, partition_default_name);
            LOG_TEST(&Poco::Logger::get("DataFileMeta"), "bucket_path: {}", bucket_path);
            file_size
                = avro_deserializer
                      .getValueFromRowByName(row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_FILE_FILE_SIZE}), TypeIndex::Int64)
                      .safeGet<Int64>();
            row_count
                = avro_deserializer
                      .getValueFromRowByName(row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_FILE_ROW_COUNT}), TypeIndex::Int64)
                      .safeGet<Int64>();
            min_key = avro_deserializer
                          .getValueFromRowByName(row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_FILE_MIN_KEY}), TypeIndex::String)
                          .safeGet<String>();
            max_key = avro_deserializer
                          .getValueFromRowByName(row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_FILE_MAX_KEY}), TypeIndex::String)
                          .safeGet<String>();
            min_sequence_number
                = avro_deserializer
                      .getValueFromRowByName(
                          row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_FILE_MIN_SEQUENCE_NUMBER}), TypeIndex::Int64)
                      .safeGet<Int64>();
            max_sequence_number
                = avro_deserializer
                      .getValueFromRowByName(
                          row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_FILE_MAX_SEQUENCE_NUMBER}), TypeIndex::Int64)
                      .safeGet<Int64>();
            schema_id
                = avro_deserializer
                      .getValueFromRowByName(row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_FILE_SCHEMA_ID}), TypeIndex::Int64)
                      .safeGet<Int64>();
            level = static_cast<Int32>(
                avro_deserializer
                    .getValueFromRowByName(row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_FILE_LEVEL}), TypeIndex::Int32)
                    .safeGet<Int32>());
            extra_files = avro_deserializer
                              .getValueFromRowByName(row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_EXTRA_FILES}), TypeIndex::Array)
                              .safeGet<Array>();

            getNullableValueFromRowByName(
                creation_time, avro_deserializer, row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_CREATION_TIME}));
            getNullableValueFromRowByName(
                delete_row_count, avro_deserializer, row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_DELETE_ROW_COUNT}));
            getNullableValueFromRowByName(
                embedded_file_index, avro_deserializer, row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_EMBEDDED_FILE_INDEX}));
            {
                std::optional<Int8> file_source_value;
                getNullableValueFromRowByName(
                    file_source_value, avro_deserializer, row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_FILE_SOURCE}));
                if (file_source_value.has_value())
                    file_source = toFileSource(file_source_value.value());
            }
            getNullableValueFromRowByName(
                value_stats_cols, avro_deserializer, row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_VALUE_STATS_COLS}));
        }

        size_t getSizeInMemory() const
        {
            size_t size = 0;
            size += file_name.capacity();
            size += bucket_path.capacity();
            size += min_key.capacity();
            size += max_key.capacity();
            size += key_stats.getSizeInMemory();
            size += value_stats.getSizeInMemory();
            size += SimpleStats::MemoryEstimator::getSizeInMemory(extra_files);

            if (embedded_file_index.has_value())
                size += embedded_file_index->capacity();

            if (value_stats_cols.has_value())
                size += SimpleStats::MemoryEstimator::getSizeInMemory(value_stats_cols.value());

            return size;
        }
    };
    Kind kind;
    String partition;
    Int32 bucket;
    Int32 total_buckets;
    DataFileMeta file;

    PaimonManifestEntry(
        const Iceberg::AvroForIcebergDeserializer & avro_deserializer,
        const String & root_path,
        const size_t row_num,
        const PaimonTableSchema & table_schema_,
        const String & partition_default_name_)
        : kind(toKind(
              static_cast<int8_t>(
                  avro_deserializer.getValueFromRowByName(row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_KIND}), TypeIndex::Int32)
                      .safeGet<Int8>())))
        , partition(
              avro_deserializer.getValueFromRowByName(row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_PARTITION}), TypeIndex::String)
                  .safeGet<String>())
        , bucket(
              static_cast<Int32>(
                  avro_deserializer.getValueFromRowByName(row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_BUCKET}), TypeIndex::Int32)
                      .safeGet<Int32>()))
        , total_buckets(
              static_cast<Int32>(
                  avro_deserializer
                      .getValueFromRowByName(row_num, concatPath({root_path, COLUMN_PAIMON_MANIFEST_TOTAL_BUCKETS}), TypeIndex::Int32)
                      .safeGet<Int32>()))
        , file(
              avro_deserializer,
              concatPath({root_path, COLUMN_PAIMON_MANIFEST_FILE}),
              row_num,
              partition,
              bucket,
              table_schema_,
              partition_default_name_)
    {
    }

    size_t getSizeInMemory() const
    {
        size_t size = 0;
        size += partition.capacity();
        size += file.getSizeInMemory();
        return size;
    }
};


struct PaimonManifest
{
    std::vector<PaimonManifestEntry> entries;

    size_t getSizeInMemory() const
    {
        size_t size = sizeof(PaimonManifest);
        size += entries.capacity() * sizeof(PaimonManifestEntry);
        for (const auto & entry : entries)
            size += entry.getSizeInMemory();
        return size;
    }
};

class PaimonTableClient : private WithContext
{
public:
    PaimonTableClient(ObjectStoragePtr object_storage_, const String & table_location_, const DB::ContextPtr & context_);

    Poco::JSON::Object::Ptr getTableSchemaJSON(const std::pair<Int32, String> & schema_meta_info);
    /// Get schema meta info (id, path) for the latest schema file.
    std::pair<Int32, String> getLatestTableSchemaInfo();
    /// Get schema meta info for a specific schema_id.
    std::pair<Int32, String> getTableSchemaInfoById(Int32 schema_id) const;
    std::optional<std::pair<Int64, String>> getLatestTableSnapshotInfo();
    PaimonSnapshot getSnapshot(const std::pair<Int64, String> & snapshot_meta_info);
    PaimonManifest getDataManifest(String manifest_path, const PaimonTableSchema & table_schema, const String & partition_default_name, bool disable_filesystem_cache = false);
    std::vector<PaimonManifestFileMeta> getManifestMeta(String manifest_list_path, bool disable_filesystem_cache = false);
private:
    /// Build ReadSettings for metadata reads.  When the caller knows that the
    /// in-memory Paimon metadata cache is active it passes disable_filesystem_cache=true
    /// so the same bytes are not stored twice (once raw in fs cache, once parsed in memory).
    ReadSettings getPaimonMetadataReadSettings(bool disable_filesystem_cache) const;

    const ObjectStoragePtr object_storage;
    const String table_location;
    LoggerPtr log;
};
using PaimonTableClientPtr = std::shared_ptr<PaimonTableClient>;

}

#endif
