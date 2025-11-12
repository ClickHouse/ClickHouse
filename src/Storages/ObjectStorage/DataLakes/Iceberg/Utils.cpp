
#include <memory>
#include <sstream>
#include <config.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <base/getThreadId.h>
#include <base/types.h>
#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/UUID.h>
#include <Poco/UUIDGenerator.h>
#include <Common/DateLUT.h>

#if USE_AVRO

#include <Processors/Formats/Impl/AvroRowInputFormat.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <IO/ReadHelpers.h>
#include <filesystem>

#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/Utils.h>

using namespace DB;


#include <Columns/IColumn.h>

namespace DB::ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int BAD_ARGUMENTS;
extern const int ICEBERG_SPECIFICATION_VIOLATION;
}

namespace DB::DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsString iceberg_metadata_file_path;
    extern const DataLakeStorageSettingsString iceberg_metadata_table_uuid;
    extern const DataLakeStorageSettingsBool iceberg_recent_metadata_file_by_last_updated_ms_field;
    extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
    extern const DataLakeStorageSettingsNonZeroUInt64 iceberg_format_version;
}

namespace ProfileEvents
{
    extern const Event IcebergVersionHintUsed;
}

namespace DB::Setting
{
    extern const SettingsUInt64 output_format_compression_level;
}

/// Hard to imagine a hint file larger than 10 MB
static constexpr size_t MAX_HINT_FILE_SIZE = 10 * 1024 * 1024;
static constexpr auto MAX_TRANSACTION_RETRIES = 100;

namespace DB::Iceberg
{

using namespace DB;
static CompressionMethod getCompressionMethodFromMetadataFile(const String & path)
{
    constexpr std::string_view metadata_suffix = ".metadata.json";

    auto compression_method = chooseCompressionMethod(path, "auto");

    /// NOTE you will be surprised, but some metadata files store compression not in the end of the file name,
    /// but somewhere in the middle of the file name, before metadata.json suffix.
    /// Maybe history of Iceberg metadata files is not so long, but it is already full of surprises.
    /// Example of weird engineering decisions: 00000-85befd5a-69c7-46d4-bca6-cfbd67f0f7e6.gz.metadata.json
    if (compression_method == CompressionMethod::None && path.ends_with(metadata_suffix))
        compression_method = chooseCompressionMethod(path.substr(0, path.size() - metadata_suffix.size()), "auto");

    return compression_method;
}


static bool isTemporaryMetadataFile(const String & file_name)
{
    String substring = String(file_name.begin(), file_name.begin() + file_name.find_first_of('.'));
    return Poco::UUID{}.tryParse(substring);
}

static Iceberg::MetadataFileWithInfo getMetadataFileAndVersion(const std::string & path)
{
    String file_name = std::filesystem::path(path).filename();
    if (isTemporaryMetadataFile(file_name))
    {
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Temporary metadata file '{}' should not be used for reading. It is created during commit operation and should be ignored",
            path);
    }
    String version_str;
    /// v<V>.metadata.json
    if (file_name.starts_with('v'))
        version_str = String(file_name.begin() + 1, file_name.begin() + file_name.find_first_of('.'));
    /// <V>-<random-uuid>.metadata.json
    else
        version_str = String(file_name.begin(), file_name.begin() + file_name.find_first_of('-'));

    if (!std::all_of(version_str.begin(), version_str.end(), isdigit))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Bad metadata file name: '{}'. Expected vN.metadata.json where N is a number", file_name);

    return MetadataFileWithInfo{
        .version = std::stoi(version_str),
        .path = path,
        .compression_method = getCompressionMethodFromMetadataFile(path)};
}


void writeMessageToFile(
    const String & data,
    const String & filename,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    const std::string & write_if_none_match,
    const std::string & write_if_match,
    CompressionMethod compression_method)
{
    auto write_settings = context->getWriteSettings();
    write_settings.object_storage_write_if_none_match = write_if_none_match;
    write_settings.object_storage_write_if_match = write_if_match;
    auto buffer_metadata = object_storage->writeObject(
        StoredObject(filename), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, write_settings);
    if (compression_method != CompressionMethod::None)
    {
        auto settings = context->getSettingsRef();
        auto compressed_buffer_metadata = wrapWriteBufferWithCompressionMethod(std::move(buffer_metadata), compression_method, static_cast<int>(settings[Setting::output_format_compression_level]));
        compressed_buffer_metadata->write(data.data(), data.size());
        compressed_buffer_metadata->finalize();
    }
    else
    {
        buffer_metadata->write(data.data(), data.size());
        buffer_metadata->finalize();
    }
}

bool writeMetadataFileAndVersionHint(
    const std::string & metadata_file_path,
    const std::string & metadata_file_content,
    const std::string & version_hint_path,
    std::string version_hint_content,
    DB::ObjectStoragePtr object_storage,
    DB::ContextPtr context,
    DB::CompressionMethod compression_method,
    bool try_write_version_hint)
{
    try
    {
        if (object_storage->exists(StoredObject(metadata_file_path)))
            return false;

        Iceberg::writeMessageToFile(metadata_file_content, metadata_file_path, object_storage, context, /* write-if-none-match */ "*", "", compression_method);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return false;
    }

    if (try_write_version_hint)
    {
        if (version_hint_content.starts_with('/'))
            version_hint_content = version_hint_content.substr(1);

        size_t i = 0;
        while (i < MAX_TRANSACTION_RETRIES)
        {
            StoredObject object_info(version_hint_path);
            std::string version_hint_value;
            std::string etag;
            std::string write_if_none_match = "*";
            if (object_storage->exists(object_info))
            {
                auto [object_data, object_metadata] = object_storage->readSmallObjectAndGetObjectMetadata(object_info, context->getReadSettings(), MAX_HINT_FILE_SIZE);
                version_hint_value = object_data;
                etag = object_metadata.etag;
                write_if_none_match.clear();
            }

            auto [old_version, _1, _2] = getMetadataFileAndVersion(version_hint_value);
            auto [new_version, _3, _4] = getMetadataFileAndVersion(version_hint_content);
            if (old_version < new_version)
            {
                try
                {
                    Iceberg::writeMessageToFile(version_hint_content, version_hint_path, object_storage, context, write_if_none_match, /* write-if-match */ etag);
                    break;
                }
                catch (...)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
            }
            else
            {
                break;
            }
            ++i;
        }
    }

    return true;
}


std::optional<TransformAndArgument> parseTransformAndArgument(const String & transform_name_src)
{
    std::string transform_name = Poco::toLower(transform_name_src);

    if (transform_name == "year" || transform_name == "years")
        return TransformAndArgument{"toYearNumSinceEpoch", std::nullopt};

    if (transform_name == "month" || transform_name == "months")
        return TransformAndArgument{"toMonthNumSinceEpoch", std::nullopt};

    if (transform_name == "day" || transform_name == "date" || transform_name == "days" || transform_name == "dates")
        return TransformAndArgument{"toRelativeDayNum", std::nullopt};

    if (transform_name == "hour" || transform_name == "hours")
        return TransformAndArgument{"toRelativeHourNum", std::nullopt};

    if (transform_name == "identity")
        return TransformAndArgument{"identity", std::nullopt};

    if (transform_name == "void")
        return TransformAndArgument{"tuple", std::nullopt};

    if (transform_name.starts_with("truncate") || transform_name.starts_with("bucket"))
    {
        /// should look like transform[N] or bucket[N]

        if (transform_name.back() != ']')
            return std::nullopt;

        auto argument_start = transform_name.find('[');

        if (argument_start == std::string::npos)
            throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Incorrect transform name {}", transform_name);

        auto argument_width = transform_name.length() - 2 - argument_start;
        std::string argument_string_representation = transform_name.substr(argument_start + 1, argument_width);
        size_t argument;
        bool parsed = DB::tryParse<size_t>(argument, argument_string_representation);

        if (!parsed)
            return std::nullopt;

        if (transform_name.starts_with("truncate"))
        {
            return TransformAndArgument{"icebergTruncate", argument};
        }
        else if (transform_name.starts_with("bucket"))
        {
            return TransformAndArgument{"icebergBucket", argument};
        }
    }
    return std::nullopt;
}

// This function is used to get the file path inside the directory which corresponds to iceberg table from the full blob path which is written in manifest and metadata files.
// For example, if the full blob path is s3://bucket/table_name/data/00000-1-1234567890.avro, the function will return table_name/data/00000-1-1234567890.avro
// Common path should end with "<table_name>" or "<table_name>/".
std::string getProperFilePathFromMetadataInfo(std::string_view data_path, std::string_view common_path, std::string_view table_location)
{
    auto trim_backward_slash = [](std::string_view str) -> std::string_view
    {
        if (str.ends_with('/'))
        {
            return str.substr(0, str.size() - 1);
        }
        return str;
    };
    auto trim_forward_slash = [](std::string_view str) -> std::string_view
    {
        if (str.starts_with('/'))
        {
            return str.substr(1);
        }
        return str;
    };
    common_path = trim_backward_slash(common_path);
    table_location = trim_backward_slash(table_location);

    if (data_path.starts_with(table_location) && table_location.ends_with(common_path))
    {
        return std::filesystem::path{common_path} / trim_forward_slash(data_path.substr(table_location.size()));
    }


    auto pos = data_path.find(common_path);
    /// Valid situation when data and metadata files are stored in different directories.
    if (pos == std::string::npos)
    {
        /// connection://bucket
        auto prefix = table_location.substr(0, table_location.size() - common_path.size());
        return std::string{data_path.substr(prefix.size())};
    }

    size_t good_pos = std::string::npos;
    while (pos != std::string::npos)
    {
        auto potential_position = pos + common_path.size();
        if ((std::string_view(data_path.data() + potential_position, 6) == "/data/")
            || (std::string_view(data_path.data() + potential_position, 10) == "/metadata/"))
        {
            good_pos = pos;
            break;
        }
        size_t new_pos = data_path.find(common_path, pos + 1);
        if (new_pos == std::string::npos)
        {
            break;
        }
        pos = new_pos;
    }


    if (good_pos != std::string::npos)
    {
        return std::string{data_path.substr(good_pos)};
    }
    else if (pos != std::string::npos)
    {
        return std::string{data_path.substr(pos)};
    }
    else
    {
        throw ::DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Expected to find '{}' in data path: '{}'", common_path, data_path);
    }
}

enum class MostRecentMetadataFileSelectionWay
{
    BY_LAST_UPDATED_MS_FIELD,
    BY_METADATA_FILE_VERSION
};

struct ShortMetadataFileInfo
{
    Int32 version;
    UInt64 last_updated_ms;
    String path;
};

std::string normalizeUuid(const std::string & uuid)
{
    std::string result;
    result.reserve(uuid.size());
    for (char c : uuid)
    {
        if (std::isalnum(c))
        {
            result.push_back(std::tolower(c));
        }
    }
    return result;
}

Poco::JSON::Object::Ptr getMetadataJSONObject(
    const String & metadata_file_path,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration_ptr,
    IcebergMetadataFilesCachePtr cache_ptr,
    const ContextPtr & local_context,
    LoggerPtr log,
    CompressionMethod compression_method)
{
    auto create_fn = [&]()
    {
        ObjectInfo object_info(metadata_file_path);

        auto read_settings = local_context->getReadSettings();
        /// Do not utilize filesystem cache if more precise cache enabled
        if (cache_ptr)
            read_settings.enable_filesystem_cache = false;

        auto source_buf = createReadBuffer(object_info.relative_path_with_metadata, object_storage, local_context, log, read_settings);

        std::unique_ptr<ReadBuffer> buf;
        if (compression_method != CompressionMethod::None)
            buf = wrapReadBufferWithCompressionMethod(std::move(source_buf), compression_method);
        else
            buf = std::move(source_buf);

        String json_str;
        readJSONObjectPossiblyInvalid(json_str, *buf);
        return json_str;
    };

    String metadata_json_str;
    if (cache_ptr)
        metadata_json_str = cache_ptr->getOrSetTableMetadata(IcebergMetadataFilesCache::getKey(configuration_ptr, metadata_file_path), create_fn);
    else
        metadata_json_str = create_fn();

    Poco::JSON::Parser parser; /// For some reason base/base/JSON.h can not parse this json file
    Poco::Dynamic::Var json = parser.parse(metadata_json_str);
    return json.extract<Poco::JSON::Object::Ptr>();
}

/// Returns type and required
std::pair<Poco::Dynamic::Var, bool> getIcebergType(DataTypePtr type, Int32 & iter)
{
    switch (type->getTypeId())
    {
        case TypeIndex::UInt32:
        case TypeIndex::Int32:
            return {"int", true};
        case TypeIndex::UInt64:
        case TypeIndex::Int64:
            return {"long", true};
        case TypeIndex::Float32:
            return {"float", true};
        case TypeIndex::Float64:
            return {"double", true};
        case TypeIndex::Date:
        case TypeIndex::Date32:
            return {"date", true};
        case TypeIndex::DateTime:
        case TypeIndex::DateTime64:
            return {"timestamp", true};
        case TypeIndex::Time:
            return {"time", true};
        case TypeIndex::String:
            return {"string", true};
        case TypeIndex::UUID:
            return {"uuid", true};
        case TypeIndex::Tuple:
        {
            auto type_tuple = std::static_pointer_cast<const DataTypeTuple>(type);
            Poco::JSON::Object::Ptr result = new Poco::JSON::Object;
            result->set(Iceberg::f_type, "struct");
            Poco::JSON::Array::Ptr fields = new Poco::JSON::Array;
            size_t iter_names = 1;
            size_t iter_fields = iter;
            iter += type_tuple->getElements().size();
            for (const auto & element : type_tuple->getElements())
            {
                Poco::JSON::Object::Ptr field = new Poco::JSON::Object;
                field->set(Iceberg::f_id, ++iter_fields);
                field->set(Iceberg::f_name, type_tuple->getNameByPosition(iter_names));
                auto child_type = getIcebergType(element->getNormalizedType(), iter);
                field->set(Iceberg::f_required, child_type.second);
                field->set(Iceberg::f_type, child_type.first);
                fields->add(field);
                ++iter_names;
            }
            result->set(Iceberg::f_fields, fields);
            return {result, true};
        }
        case TypeIndex::Array:
        {
            auto type_array = std::static_pointer_cast<const DataTypeArray>(type);
            Poco::JSON::Object::Ptr field = new Poco::JSON::Object;

            field->set(Iceberg::f_type, "list");
            field->set(Iceberg::f_element_id, ++iter);
            auto child_type = getIcebergType(type_array->getNestedType(), iter);
            field->set(Iceberg::f_required, false);
            field->set(Iceberg::f_element, child_type.first);
            field->set(Iceberg::f_element_required, child_type.second);
            return {field, true};
        }
        case TypeIndex::Map:
        {
            auto type_map = std::static_pointer_cast<const DataTypeMap>(type);
            Poco::JSON::Object::Ptr field = new Poco::JSON::Object;

            field->set(Iceberg::f_type, "map");
            field->set(Iceberg::f_key_id, ++iter);
            field->set(Iceberg::f_value_id, ++iter);

            field->set(Iceberg::f_key, getIcebergType(type_map->getKeyType(), iter).first);
            auto value_type = getIcebergType(type_map->getValueType(), iter);
            field->set(Iceberg::f_value, value_type.first);
            field->set(Iceberg::f_value_required, value_type.second);
            return {field, true};
        }
        case TypeIndex::Nullable:
        {
            auto type_nullable = std::static_pointer_cast<const DataTypeNullable>(type);
            return {getIcebergType(type_nullable->getNestedType(), iter).first, false};
        }
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type for iceberg {}", type->getName());
    }
}

Poco::Dynamic::Var getAvroType(DataTypePtr type)
{
    switch (type->getTypeId())
    {
        case TypeIndex::UInt32:
        case TypeIndex::Int32:
        case TypeIndex::Date:
        case TypeIndex::Date32:
        case TypeIndex::Time:
            return "int";
        case TypeIndex::UInt64:
        case TypeIndex::Int64:
        case TypeIndex::DateTime:
        case TypeIndex::DateTime64:
            return "long";
        case TypeIndex::Float32:
            return "float";
        case TypeIndex::Float64:
            return "double";
        case TypeIndex::String:
        case TypeIndex::UUID:
            return "string";
        case TypeIndex::Nullable:
        {
            auto type_nullable = std::static_pointer_cast<const DataTypeNullable>(type);
            return getAvroType(type_nullable->getNestedType());
        }
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type for iceberg {}", type->getName());
    }
}

Poco::JSON::Object::Ptr getPartitionField(
    ASTPtr partition_by_element,
    const std::unordered_map<String, Int32> & column_name_to_source_id,
    Int32 & partition_iter)
{
    const auto * partition_function = partition_by_element->as<ASTFunction>();
    if (!partition_function)
    {
        const auto * ast_identifier = partition_by_element->as<ASTIdentifier>();
        if (!ast_identifier)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown expression for partitioning: {}", partition_by_element->formatForLogging());

        Poco::JSON::Object::Ptr result = new Poco::JSON::Object;
        auto field = ast_identifier->name();
        auto it = column_name_to_source_id.find(field);
        if (it == column_name_to_source_id.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown field to partition {}", field);
        result->set(Iceberg::f_name, field);
        result->set(Iceberg::f_source_id, it->second);
        result->set(Iceberg::f_field_id, ++partition_iter);
        result->set(Iceberg::f_transform, "identity");
        return result;
    }

    std::optional<String> field;
    std::optional<Int64> param;
    for (const auto & child : partition_function->children)
    {
        const auto * expression_list = child->as<ASTExpressionList>();
        if (!expression_list)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported partitioning for Iceberg table.");

        for (const auto & expression_list_child : expression_list->children)
        {
            const auto * identifier = expression_list_child->as<ASTIdentifier>();
            if (identifier)
            {
                if (field.has_value())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Functions with multiple arguments are not supported in Iceberg.");
                field = identifier->name();
            }
            const auto * literal = expression_list_child->as<ASTLiteral>();
            if (literal)
            {
                param = literal->value.safeGet<Int64>();
            }
        }
    }
    if (!field)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Functions with no arguments are not supported in Iceberg.");

    Poco::JSON::Object::Ptr result = new Poco::JSON::Object;
    result->set(Iceberg::f_name, field.value());

    if (!column_name_to_source_id.contains(*field))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown field to partition {}", *field);
    result->set(Iceberg::f_source_id, column_name_to_source_id.at(*field));
    result->set(Iceberg::f_field_id, ++partition_iter);

    if (partition_function->name == "identity")
    {
        result->set(Iceberg::f_transform, "identity");
        return result;
    }
    else if (partition_function->name == "toYearNumSinceEpoch")
    {
        result->set(Iceberg::f_transform, "year");
        return result;
    }
    else if (partition_function->name == "toMonthNumSinceEpoch")
    {
        result->set(Iceberg::f_transform, "month");
        return result;
    }
    else if (partition_function->name == "toRelativeDayNum")
    {
        result->set(Iceberg::f_transform, "days");
        return result;
    }
    else if (partition_function->name == "toRelativeHourNum")
    {
        result->set(Iceberg::f_transform, "hours");
        return result;
    }
    else if (partition_function->name == "icebergTruncate")
    {
        if (!param.has_value())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "TRUNCATE function for iceberg partitioning requires one integer parameter");
        result->set(Iceberg::f_transform, fmt::format("truncate[{}]", *param));
        return result;
    }
    else if (partition_function->name == "icebergBucket")
    {
        if (!param.has_value())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "BUCKET function for iceberg partitioning requires one integer parameter");
        result->set(Iceberg::f_transform, fmt::format("bucket[{}]", *param));
        return result;
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported function for iceberg partitioning {}", partition_function->name);
}

std::pair<Poco::JSON::Object::Ptr, Int32> getPartitionSpec(
    ASTPtr partition_by,
    const std::unordered_map<String, Int32> & column_name_to_source_id)
{
    Poco::JSON::Object::Ptr result = new Poco::JSON::Object;
    result->set(Iceberg::f_spec_id, 0);

    Poco::JSON::Array::Ptr fields = new Poco::JSON::Array;
    Int32 partition_iter = 1000;
    if (partition_by)
    {
        if (const auto * partition_function = partition_by->as<ASTFunction>(); partition_function && partition_function->name == "tuple")
        {
            for (const auto & child : partition_function->children)
            {
                const auto * expression_list = child->as<ASTExpressionList>();
                for (const auto & expression_list_child : expression_list->children)
                {
                    auto partition_field = getPartitionField(expression_list_child, column_name_to_source_id, partition_iter);
                    fields->add(partition_field);
                }
            }
        }
        else
        {
            auto partition_field = getPartitionField(partition_by, column_name_to_source_id, partition_iter);
            fields->add(partition_field);
        }
    }
    else
        partition_iter = 0;

    result->set(Iceberg::f_fields, fields);
    return {result, partition_iter};
}

std::pair<Poco::JSON::Object::Ptr, String> createEmptyMetadataFile(
    String path_location,
    const ColumnsDescription & columns,
    ASTPtr partition_by,
    UInt64 format_version)
{
    std::unordered_map<String, Int32> column_name_to_source_id;
    static Poco::UUIDGenerator uuid_generator;

    Poco::JSON::Object::Ptr new_metadata_file_content = new Poco::JSON::Object;
    new_metadata_file_content->set(Iceberg::f_format_version, format_version);
    new_metadata_file_content->set(Iceberg::f_table_uuid, uuid_generator.createRandom().toString());
    new_metadata_file_content->set(Iceberg::f_location, path_location);
    if (format_version > 1)
        new_metadata_file_content->set(Iceberg::f_last_sequence_number, 0);

    auto now = std::chrono::system_clock::now();
    auto ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    new_metadata_file_content->set(Iceberg::f_last_updated_ms, ms.count());
    new_metadata_file_content->set(Iceberg::f_last_column_id, columns.size());
    new_metadata_file_content->set(Iceberg::f_current_schema_id, 0);

    Poco::JSON::Object::Ptr schema_representation = new Poco::JSON::Object;
    schema_representation->set(Iceberg::f_type, "struct");
    schema_representation->set(Iceberg::f_schema_id, 0);

    Poco::JSON::Array::Ptr schema_fields = new Poco::JSON::Array;
    Int32 iter = static_cast<Int32>(columns.size());
    Int32 iter_for_initial_columns = 0;
    for (const auto & column : columns)
    {
        Poco::JSON::Object::Ptr field = new Poco::JSON::Object;
        field->set(Iceberg::f_id, ++iter_for_initial_columns);
        field->set(Iceberg::f_name, column.name);
        auto type = getIcebergType(column.type, iter);
        field->set(Iceberg::f_required, type.second);
        field->set(Iceberg::f_type, type.first);
        column_name_to_source_id[column.name] = iter_for_initial_columns;
        schema_fields->add(field);
    }
    schema_representation->set(Iceberg::f_fields, schema_fields);
    Poco::JSON::Array::Ptr schema_array = new Poco::JSON::Array;
    schema_array->add(schema_representation);
    new_metadata_file_content->set(Iceberg::f_schemas, schema_array);

    new_metadata_file_content->set(Iceberg::f_default_spec_id, 0);
    Poco::JSON::Object::Ptr partition_spec = new Poco::JSON::Object;
    partition_spec->set(Iceberg::f_spec_id, 0);
    partition_spec->set(Iceberg::f_fields, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    Poco::JSON::Array::Ptr partition_specs = new Poco::JSON::Array;
    const auto & [part_spec, last_partition_id] = getPartitionSpec(partition_by, column_name_to_source_id);
    partition_specs->add(part_spec);
    new_metadata_file_content->set(Iceberg::f_partition_specs, partition_specs);
    new_metadata_file_content->set(Iceberg::f_last_partition_id, last_partition_id);
    new_metadata_file_content->set(Iceberg::f_current_snapshot_id, -1);

    Poco::JSON::Object::Ptr refs = new Poco::JSON::Object;
    Poco::JSON::Object::Ptr main_branch = new Poco::JSON::Object;
    main_branch->set(Iceberg::f_metadata_snapshot_id, -1);
    main_branch->set(Iceberg::f_type, "branch");
    refs->set(Iceberg::f_main, main_branch);

    new_metadata_file_content->set(Iceberg::f_refs, refs);
    new_metadata_file_content->set(Iceberg::f_snapshots, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    new_metadata_file_content->set(Iceberg::f_statistics, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    new_metadata_file_content->set(Iceberg::f_snapshot_log, Poco::JSON::Array::Ptr(new Poco::JSON::Array));
    new_metadata_file_content->set(Iceberg::f_metadata_log, Poco::JSON::Array::Ptr(new Poco::JSON::Array));

    new_metadata_file_content->set(Iceberg::f_default_sort_order_id, 0);
    Poco::JSON::Object::Ptr sort_order = new Poco::JSON::Object;
    sort_order->set(Iceberg::f_order_id, 0);
    sort_order->set(Iceberg::f_fields, Poco::JSON::Array::Ptr(new Poco::JSON::Array));

    Poco::JSON::Array::Ptr sort_orders = new Poco::JSON::Array;
    sort_orders->add(sort_order);
    new_metadata_file_content->set(Iceberg::f_sort_orders, sort_orders);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(new_metadata_file_content, oss, 4);
    return {new_metadata_file_content, removeEscapedSlashes(oss.str())};
}

/**
 * Each version of table metadata is stored in a `metadata` directory and
 * has one of 2 formats:
 *   1) v<V>.metadata.json, where V - metadata version.
 *   2) <V>-<random-uuid>.metadata.json, where V - metadata version
 */
MetadataFileWithInfo getLatestMetadataFileAndVersion(
    const ObjectStoragePtr & object_storage,
    StorageObjectStorageConfigurationPtr configuration_ptr,
    IcebergMetadataFilesCachePtr cache_ptr,
    const ContextPtr & local_context,
    const std::optional<String> & table_uuid)
{
    auto log = getLogger("IcebergMetadataFileResolver");
    MostRecentMetadataFileSelectionWay selection_way
        = configuration_ptr->getDataLakeSettings()[DataLakeStorageSetting::iceberg_recent_metadata_file_by_last_updated_ms_field].value
        ? MostRecentMetadataFileSelectionWay::BY_LAST_UPDATED_MS_FIELD
        : MostRecentMetadataFileSelectionWay::BY_METADATA_FILE_VERSION;
    bool need_all_metadata_files_parsing
        = (selection_way == MostRecentMetadataFileSelectionWay::BY_LAST_UPDATED_MS_FIELD) || table_uuid.has_value();
    const auto metadata_files = listFiles(*object_storage, *configuration_ptr, "metadata", ".metadata.json");
    if (metadata_files.empty())
    {
        throw Exception(
            ErrorCodes::FILE_DOESNT_EXIST, "The metadata file for Iceberg table with path {} doesn't exist", configuration_ptr->getPathForRead().path);
    }
    std::vector<ShortMetadataFileInfo> metadata_files_with_versions;
    metadata_files_with_versions.reserve(metadata_files.size());
    for (const auto & path : metadata_files)
    {
        String filename = std::filesystem::path(path).filename();
        if (isTemporaryMetadataFile(filename))
            continue;
        auto [version, metadata_file_path, compression_method] = getMetadataFileAndVersion(path);

        if (need_all_metadata_files_parsing)
        {
            auto metadata_file_object = getMetadataJSONObject(metadata_file_path, object_storage, configuration_ptr, cache_ptr, local_context, log, compression_method);
            if (table_uuid.has_value())
            {
                if (metadata_file_object->has(Iceberg::f_table_uuid))
                {
                    auto current_table_uuid = metadata_file_object->getValue<String>(Iceberg::f_table_uuid);
                    if (normalizeUuid(table_uuid.value()) == normalizeUuid(current_table_uuid))
                    {
                        metadata_files_with_versions.emplace_back(
                            version, metadata_file_object->getValue<UInt64>(Iceberg::f_last_updated_ms), metadata_file_path);
                    }
                }
                else
                {
                    Int64 format_version = metadata_file_object->getValue<Int64>(Iceberg::f_format_version);
                    throw Exception(
                        format_version == 1 ? ErrorCodes::BAD_ARGUMENTS : ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                        "Table UUID is not specified in some metadata files for table by path {}",
                        metadata_file_path);
                }
            }
            else
            {
                metadata_files_with_versions.emplace_back(version, metadata_file_object->getValue<UInt64>(Iceberg::f_last_updated_ms), metadata_file_path);
            }
        }
        else
        {
            metadata_files_with_versions.emplace_back(version, 0, metadata_file_path);
        }
    }

    /// Get the latest version of metadata file: v<V>.metadata.json
    const ShortMetadataFileInfo & latest_metadata_file_info = [&]()
    {
        if (selection_way == MostRecentMetadataFileSelectionWay::BY_LAST_UPDATED_MS_FIELD)
        {
            return *std::max_element(
                metadata_files_with_versions.begin(),
                metadata_files_with_versions.end(),
                [](const ShortMetadataFileInfo & a, const ShortMetadataFileInfo & b) { return a.last_updated_ms < b.last_updated_ms; });
        }
        else
        {
            return *std::max_element(
                metadata_files_with_versions.begin(),
                metadata_files_with_versions.end(),
                [](const ShortMetadataFileInfo & a, const ShortMetadataFileInfo & b) { return a.version < b.version; });
        }
    }();
    return {latest_metadata_file_info.version, latest_metadata_file_info.path, getCompressionMethodFromMetadataFile(latest_metadata_file_info.path)};
}

MetadataFileWithInfo getLatestOrExplicitMetadataFileAndVersion(
    const ObjectStoragePtr & object_storage,
    StorageObjectStorageConfigurationPtr configuration_ptr,
    IcebergMetadataFilesCachePtr cache_ptr,
    const ContextPtr & local_context,
    Poco::Logger * log)
{
    const auto & data_lake_settings = configuration_ptr->getDataLakeSettings();
    if (data_lake_settings[DataLakeStorageSetting::iceberg_metadata_file_path].changed)
    {
        auto explicit_metadata_path = data_lake_settings[DataLakeStorageSetting::iceberg_metadata_file_path].value;
        try
        {
            LOG_TEST(log, "Explicit metadata file path is specified {}, will read from this metadata file", explicit_metadata_path);
            std::filesystem::path p(explicit_metadata_path);
            auto it = p.begin();
            if (it != p.end())
            {
                if (*it == "." || *it == "..")
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Relative paths are not allowed");
            }
            auto prefix_storage_path = configuration_ptr->getPathForRead().path;
            if (!explicit_metadata_path.starts_with(prefix_storage_path))
                explicit_metadata_path = std::filesystem::path(prefix_storage_path) / explicit_metadata_path;
            return getMetadataFileAndVersion(explicit_metadata_path);
        }
        catch (const std::exception & ex)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid path {} specified for iceberg_metadata_file_path: '{}'", explicit_metadata_path, ex.what());
        }
    }
    else if (data_lake_settings[DataLakeStorageSetting::iceberg_metadata_table_uuid].changed)
    {
        std::optional<String> table_uuid = data_lake_settings[DataLakeStorageSetting::iceberg_metadata_table_uuid].value;
        return getLatestMetadataFileAndVersion(object_storage, configuration_ptr, cache_ptr, local_context, table_uuid);
    }
    else if (data_lake_settings[DataLakeStorageSetting::iceberg_use_version_hint].value)
    {
        auto prefix_storage_path = configuration_ptr->getPathForRead().path;
        auto version_hint_path = std::filesystem::path(prefix_storage_path) / "metadata" / "version-hint.text";
        std::string metadata_file;
        StoredObject version_hint(version_hint_path);
        auto buf = object_storage->readObject(version_hint, ReadSettings{});
        readString(metadata_file, *buf);
        if (!metadata_file.ends_with(".metadata.json"))
        {
            if (std::all_of(metadata_file.begin(), metadata_file.end(), isdigit))
                metadata_file = "v" + metadata_file + ".metadata.json";
            else
                metadata_file = metadata_file + ".metadata.json";
        }
        LOG_TEST(log, "Version hint file points to {}, will read from this metadata file", metadata_file);
        ProfileEvents::increment(ProfileEvents::IcebergVersionHintUsed);

        return getMetadataFileAndVersion(std::filesystem::path(prefix_storage_path) / "metadata" / fs::path(metadata_file).filename());
    }
    else
    {
        return getLatestMetadataFileAndVersion(object_storage, configuration_ptr, cache_ptr, local_context, std::nullopt);
    }
}

}

#endif
