#include <Analyzer/FunctionNode.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn_fwd.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Databases/DataLake/Common.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Formats/FormatFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>
#include <Functions/identity.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaWrites.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/ObjectStorage/Utils.h>
#include <base/defines.h>
#include <base/types.h>
#include "Common/DateLUT.h"
#include <Common/Exception.h>
#include <Common/PODArray_fwd.h>
#include <Common/isValidUTF8.h>
#include <Common/quoteString.h>
#include <Common/randomSeed.h>
#include "Core/Range.h"
#include "Core/TypeId.h"
#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/DataTypesDecimal.h"
#include "Processors/Chunk.h"
#include "base/Decimal_fwd.h"
#include <Columns/IColumn.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <fmt/format.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Storages/ObjectStorage/DataLakes/Common.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>

#include <memory>
#include <sstream>
#include <string>

namespace DB
{

namespace
{

std::pair<String, bool> getTypeAndNullable(DataTypePtr type)
{
    switch (type->getTypeId())
    {
        case TypeIndex::String:
            return {"string", false};
        case TypeIndex::Int64:
            return {"long", false};
        case TypeIndex::Int32:
            return {"integer", false};
        case TypeIndex::Int16:
            return {"integer", false};
        case TypeIndex::Int8:
            return {"byte", false};
        case TypeIndex::UInt8:
            return {"boolean", false};
        case TypeIndex::Float32:
            return {"float", false};
        case TypeIndex::Float64:
            return {"double", false};
        case TypeIndex::Date32:
            return {"date", false};
        case TypeIndex::DateTime64:
            return {"timestamp", false};
        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        {
            return {fmt::format("decimal({}, {})", getDecimalPrecision(*type), getDecimalScale(*type)), false};
        }
        case TypeIndex::Nullable:
        {
            const auto & nullable_type = assert_cast<const DataTypeNullable &>(*type);
            auto child = getTypeAndNullable(nullable_type.getNestedType());
            child.second = true;
            return child;
        }
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported data type for delta lake {}", type->getName());
    }
}

String dumpFieldToDelta(const Field & field)
{
    switch (field.getType())
    {
        case Field::Types::Which::Int64:
            return std::to_string(field.safeGet<Int64>());
        case Field::Types::Which::UInt64:
            return std::to_string(field.safeGet<UInt64>());
        case Field::Types::Which::String:
            return field.safeGet<String>();
        case Field::Types::Which::Float64:
            return std::to_string(field.safeGet<Float64>());
        case Field::Types::Which::Decimal64:
            return std::to_string(field.safeGet<Decimal64>().getValue());
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported data type for dump to delta lake {}", field.getTypeName());
    }
}

String generateCommitInfo(Int64 num_rows, Int64 num_bytes, const std::vector<String> & partition_columns)
{
    static Poco::UUIDGenerator uuid_generator;

    Poco::JSON::Object::Ptr result = new Poco::JSON::Object;

    Poco::JSON::Object::Ptr commit_info = new Poco::JSON::Object;
    auto now = std::chrono::system_clock::now();
    auto ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch());

    commit_info->set("timestamp", ms.count());
    commit_info->set("operation", "WRITE");

    {
        Poco::JSON::Object::Ptr operation_parameters = new Poco::JSON::Object;
        operation_parameters->set("mode", "ErrorIfExists");

        Poco::JSON::Array::Ptr partition_by = new Poco::JSON::Array;
        for (const auto & partition_column : partition_columns)
            partition_by->add(partition_column);

        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::JSON::Stringifier::stringify(partition_by, oss);

        operation_parameters->set("partitionBy", oss.str());
        commit_info->set("operationParameters", operation_parameters);
    }

    commit_info->set("isolationLevel", "Serializable");
    commit_info->set("isBlindAppend", true);

    {
        Poco::JSON::Object::Ptr operation_metrics = new Poco::JSON::Object;
        operation_metrics->set("numFiles", "1");
        operation_metrics->set("numOutputRows", std::to_string(num_rows));
        operation_metrics->set("numOutputBytes", std::to_string(num_bytes));
        commit_info->set("operationMetrics", operation_metrics);
    }
    commit_info->set("engineInfo", "ClickHouse");
    commit_info->set("txnId", uuid_generator.createRandom().toString());
    result->set("commitInfo", commit_info);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(result, oss);
    return oss.str();
}

String generateMetadataInfo(const Block & header, const String & format)
{
    Poco::JSON::Object::Ptr result = new Poco::JSON::Object;

    Poco::JSON::Object::Ptr metadata_info = new Poco::JSON::Object;
    static Poco::UUIDGenerator uuid_generator;

    metadata_info->set("id", uuid_generator.createRandom().toString());

    {
        Poco::JSON::Object::Ptr format_json = new Poco::JSON::Object;
        format_json->set("provider", boost::to_lower_copy(format));
        Poco::JSON::Object::Ptr options = new Poco::JSON::Object;
        format_json->set("options", options);

        metadata_info->set("format", format_json);
    }

    {
        Poco::JSON::Array::Ptr fields = new Poco::JSON::Array;
        for (size_t i = 0; i < header.getColumns().size(); ++i)
        {
            const auto col = header.getColumns()[i];
            const auto column_name = header.getNames()[i];
            Poco::JSON::Object::Ptr field = new Poco::JSON::Object;
            field->set("name", column_name);

            auto type_representation = getTypeAndNullable(header.getDataTypes()[i]);
            field->set("type", type_representation.first);
            field->set("nullable", type_representation.second);

            Poco::JSON::Object::Ptr metadata = new Poco::JSON::Object;
            field->set("metadata", metadata);
            fields->add(field);
        }
        Poco::JSON::Object::Ptr schema_str = new Poco::JSON::Object;
        schema_str->set("type", "struct");
        schema_str->set("fields", fields);
        
        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::JSON::Stringifier::stringify(schema_str, oss);
        metadata_info->set("schemaString", oss.str());
    }

    {
        Poco::JSON::Array::Ptr partition_columns = new Poco::JSON::Array;
        metadata_info->set("partitionColumns", partition_columns);
    }

    {
        Poco::JSON::Object::Ptr configuration = new Poco::JSON::Object;
        metadata_info->set("configuration", configuration);
    }

    {
        auto now = std::chrono::system_clock::now();
        auto ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch());

        metadata_info->set("createdTime", ms.count());
    }

    result->set("metaData", metadata_info);
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(result, oss);

    return oss.str();
}

String generateProtocolInfo()
{
    return R"({"protocol":{"minReaderVersion":1,"minWriterVersion":2}})";
}

String generateRecordInfo(
    const String & data_filename,
    const std::vector<Range> & columns_stats,
    Int64 num_bytes,
    Int64 total_rows,
    const std::vector<Int64> & null_values,
    const std::vector<String> & column_names)
{
    Poco::JSON::Object::Ptr result = new Poco::JSON::Object;
    Poco::JSON::Object::Ptr add_info = new Poco::JSON::Object;
    add_info->set("path", data_filename);
    
    Poco::JSON::Object::Ptr partition_values = new Poco::JSON::Object;
    add_info->set("partitionValues", partition_values);
    add_info->set("size", num_bytes);

    Poco::JSON::Object::Ptr commit_info = new Poco::JSON::Object;
    auto now = std::chrono::system_clock::now();
    auto ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch());

    add_info->set("modificationTime", ms.count());
    add_info->set("dataChange", true);

    {
        Poco::JSON::Object::Ptr stats = new Poco::JSON::Object;
        stats->set("numRecords", total_rows);
        Poco::JSON::Object::Ptr min_values = new Poco::JSON::Object;
        for (size_t i = 0; i < column_names.size(); ++i)
        {
            min_values->set(column_names[i], dumpFieldToDelta(columns_stats[i].left));
        }
        stats->set("minValues", min_values);
    
        Poco::JSON::Object::Ptr max_values = new Poco::JSON::Object;
        for (size_t i = 0; i < column_names.size(); ++i)
        {
            max_values->set(column_names[i], dumpFieldToDelta(columns_stats[i].right));
        }
        stats->set("maxValues", max_values);

        Poco::JSON::Object::Ptr null_counts = new Poco::JSON::Object;
        for (size_t i = 0; i < column_names.size(); ++i)
            null_counts->set(column_names[i], null_values[i]);

        stats->set("nullCount", null_counts);   

        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::JSON::Stringifier::stringify(stats, oss);
        add_info->set("stats", oss.str());
    }

    result->set("add", add_info);
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(result, oss);
    return oss.str();
}

std::vector<String> getPartitionColumns(
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    ContextPtr context)
{
    const auto metadata_files = listFiles(*object_storage, *configuration, "", ".json");
    for (const auto & metadata_file : metadata_files)
    {
        auto read_settings = context->getReadSettings();
        ObjectInfo object_info(metadata_file);
        auto log = getLogger("DeltaWrites");
        auto buf = StorageObjectStorageSource::createReadBuffer(object_info, object_storage, context, log);

        char c;
        while (!buf->eof())
        {
            /// May be some invalid characters before json.
            while (buf->peek(c) && c != '{')
                buf->ignore();

            if (buf->eof())
                break;

            String json_str;
            readJSONObjectPossiblyInvalid(json_str, *buf);
            
            if (auto it = json_str.find("metaData"); it == std::string::npos)
                continue;

            Poco::JSON::Parser parser;
            Poco::Dynamic::Var json = parser.parse(json_str);
            const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

            auto partition_columns = object->getObject("metaData")->getArray("partitionColumns");
            std::vector<String> result;
            for (size_t i = 0; i < partition_columns->size(); ++i)
            {
                result.push_back(partition_columns->getElement<String>(i));
            }
            return result;
        }
    }
    return {};
}



}

namespace Setting
{
extern const SettingsUInt64 output_format_compression_level;
extern const SettingsUInt64 output_format_compression_zstd_window_log;
extern const SettingsBool write_full_path_in_iceberg_metadata;
}

namespace DataLakeStorageSetting
{
extern const DataLakeStorageSettingsString iceberg_metadata_file_path;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

DeltaFileNameGenerator::DeltaFileNameGenerator(const String & path)
    : data_dir(path)
    , metadata_dir(path + "_delta_log/")
{
}

String DeltaFileNameGenerator::generateMetadataPath(Int64 version)
{
    String suffix = std::to_string(version);
    String prefix = std::string(20 - suffix.size(), '0');

    return fmt::format("{}{}{}.json", metadata_dir, prefix, suffix);
}

std::pair<String, String> DeltaFileNameGenerator::generateDataPath(const Row & partition_values, const std::vector<String> & partition_columns)
{
    auto uuid_str = uuid_generator.createRandom().toString();

    String additional_path;
    for (size_t i = 0; i < partition_columns.size(); ++i)
    {
        additional_path += fmt::format("{}={}/", partition_columns[i], dumpFieldToDelta(partition_values[i]));
    }

    return {fmt::format("{}{}part-{}.parquet", data_dir, additional_path, uuid_str), uuid_str};
}

DeltaChunkPartitioner::DeltaChunkPartitioner(
    const std::vector<String> & columns_to_apply_, SharedHeader sample_block_)
    : columns_to_apply(columns_to_apply_)
    , sample_block(sample_block_)
{
    for (size_t i = 0; i < sample_block->columns(); ++i)
    {
        column_name_to_index[sample_block_->getNames()[i]] = i;
    }
}

size_t DeltaChunkPartitioner::PartitionKeyHasher::operator()(const PartitionKey & key) const
{
    size_t result = 0;
    for (const auto & part_key : key)
        result ^= hasher(part_key.dump());
    return result;
}

std::vector<std::pair<DeltaChunkPartitioner::PartitionKey, Chunk>>
DeltaChunkPartitioner::partitionChunk(const Chunk & chunk)
{

    std::vector<DeltaChunkPartitioner::PartitionKey> transform_results(chunk.getNumRows());
    for (const auto & column_name : columns_to_apply)
    {
        for (size_t i = 0; i < chunk.getNumRows(); ++i)
        {
            Field field;
            chunk.getColumns()[column_name_to_index[column_name]]->get(i, field);
            transform_results[i].push_back(field);
        }
    }

    auto get_partition = [&](size_t row_num)
    {
        return transform_results[row_num];
    };

    PODArray<size_t> partition_num_to_first_row;
    IColumn::Selector selector;
    ColumnRawPtrs raw_columns;
    for (const auto & column : chunk.getColumns())
        raw_columns.push_back(column.get());

    buildScatterSelector(raw_columns, partition_num_to_first_row, selector, 0, Context::getGlobalContextInstance());

    size_t partitions_count = partition_num_to_first_row.size();
    std::vector<std::pair<DeltaChunkPartitioner::PartitionKey, MutableColumns>> result_columns;
    result_columns.reserve(partitions_count);

    for (size_t i = 0; i < partitions_count; ++i)
        result_columns.push_back({get_partition(partition_num_to_first_row[i]), chunk.cloneEmptyColumns()});

    for (size_t col = 0; col < chunk.getNumColumns(); ++col)
    {
        if (partitions_count > 1)
        {
            MutableColumns scattered = chunk.getColumns()[col]->scatter(partitions_count, selector);
            for (size_t i = 0; i < partitions_count; ++i)
                result_columns[i].second[col] = std::move(scattered[i]);
        }
        else
        {
            result_columns[0].second[col] = chunk.getColumns()[col]->cloneFinalized();
        }
    }

    std::vector<std::pair<DeltaChunkPartitioner::PartitionKey, Chunk>> result;
    result.reserve(result_columns.size());
    for (auto && [key, partition_columns] : result_columns)
    {
        size_t column_size = partition_columns[0]->size();
        result.push_back({key, Chunk(std::move(partition_columns), column_size)});
    }
    return result;
}


DeltaLakeStorageSink::DeltaLakeStorageSink(
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    const std::optional<FormatSettings> & format_settings_,
    SharedHeader sample_block_,
    ContextPtr context_)
    : SinkToStorage(sample_block_)
    , sample_block(sample_block_)
    , object_storage(object_storage_)
    , context(context_)
    , configuration(configuration_)
    , format_settings(format_settings_)
    , filename_generator(configuration_->getPathForRead().path)
{
}

void DeltaLakeStorageSink::consume(Chunk & chunk)
{
    std::cerr << "DeltaLakeStorageSink::consume\n";
    if (isCancelled())
        return;

    if (!partition_columns)
    {
        partition_columns = getPartitionColumns(object_storage, configuration, context);
        if (partition_columns->size() > 0)
            partitioner = DeltaChunkPartitioner(*partition_columns, sample_block);
    }

    std::unordered_map<DeltaChunkPartitioner::PartitionKey, Chunk, DeltaChunkPartitioner::PartitionKeyHasher> partitioned_chunks;
    if (partitioner)
    {
        auto partition_result = partitioner->partitionChunk(chunk);
        for (auto && [key, part_chunk] : partition_result)
            partitioned_chunks[key] = std::move(part_chunk);
    }
    else
        partitioned_chunks[Row{}] = chunk.clone();

    for (const auto & [partition_key, partition_chunk] : partitioned_chunks)
    {
        if (!data_filename_in_storage.contains(partition_key))
        {
            auto filename_info = filename_generator.generateDataPath(partition_key, partition_columns.value_or(std::vector<String>{}));
            data_filename_in_storage[partition_key] = std::move(filename_info.first);
            uuid_data_file[partition_key] = std::move(filename_info.second);
        }

        if (null_counts[partition_key].empty())
            null_counts[partition_key].resize(chunk.getNumColumns());

        for (size_t i = 0; i < partition_chunk.getNumColumns(); ++i)
        {
            for (size_t j = 0; j < partition_chunk.getNumRows(); ++j)
            {
                null_counts[partition_key][i] += partition_chunk.getColumns()[i]->isNullAt(j);
            }
        }
        for (size_t i = 0; i < partition_chunk.getNumColumns(); ++i)
        {
            if (partition_chunk.getNumRows() > 0 && column_stats.size() < partition_chunk.getNumColumns())
            {
                Field sample;
                partition_chunk.getColumns()[i]->get(0, sample);
                column_stats[partition_key].push_back(Range(sample, true, sample, true));
            }

            for (size_t j = 0; j < partition_chunk.getNumRows(); ++j)
            {
                Field cur_element;
                partition_chunk.getColumns()[i]->get(0, cur_element);
                auto point = Range(cur_element, true, cur_element, true);
                if (column_stats[partition_key][i].leftThan(point))
                    column_stats[partition_key][i].right = cur_element;

                if (column_stats[partition_key][i].rightThan(point))
                    column_stats[partition_key][i].left = cur_element;
            }
        }

        total_rows[partition_key] += partition_chunk.getNumRows();
        if (!data_file_buffer[partition_key])
        {
            data_file_buffer[partition_key] = object_storage->writeObject(
                StoredObject(data_filename_in_storage[partition_key]), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

            data_file_writer[partition_key] = FormatFactory::instance().getOutputFormatParallelIfPossible(
                configuration->format, *data_file_buffer[partition_key], *sample_block, context, format_settings);
        }
        data_file_writer[partition_key]->write(getHeader().cloneWithColumns(partition_chunk.getColumns()));
    }
}

void DeltaLakeStorageSink::onFinish()
{
    if (isCancelled())
        return;

    finalizeBuffers();
    releaseBuffers();
}

void DeltaLakeStorageSink::finalizeBuffers()
{
    for (const auto & [partition_key, _] : data_file_writer)
    {
        try
        {
            data_file_writer[partition_key]->flush();
            data_file_writer[partition_key]->finalize();
        }
        catch (...)
        {
            /// Stop ParallelFormattingOutputFormat correctly.
            cancelBuffers();
            releaseBuffers();
            throw;
        }

        data_file_buffer[partition_key]->finalize();
        total_chunks_size[partition_key] += data_file_buffer[partition_key]->count();
    }

    while (!initializeMetadata())
    {
    }
}

void DeltaLakeStorageSink::releaseBuffers()
{
    for (const auto & [partition_key, _] : data_file_writer)
    {
        data_file_writer[partition_key].reset();
        data_file_buffer[partition_key].reset();
    }
}

void DeltaLakeStorageSink::cancelBuffers()
{
    for (const auto & [partition_key, _] : data_file_writer)
    {
        data_file_writer[partition_key]->cancel();
        data_file_buffer[partition_key]->cancel();
    }
}

bool DeltaLakeStorageSink::initializeMetadata()
{
    std::cerr << "DeltaLakeStorageSink::consume::initializeMetadata\n";
    size_t all_rows_count = 0;
    size_t all_chunks_size = 0;
    for (const auto & [partition_key, _] : total_rows)
    {
        all_rows_count += total_rows[partition_key];
        all_chunks_size += total_chunks_size[partition_key];
    }
    auto commit_info = generateCommitInfo(all_rows_count, all_chunks_size, partition_columns.value_or(std::vector<String>{}));
    auto metadata_ionfo = generateMetadataInfo(getHeader(), configuration->format);
    auto protocol_info = generateProtocolInfo();

    auto metadata_content = removeEscapedSlashes(fmt::format("{}\n{}\n{}\n", commit_info, metadata_ionfo, protocol_info));
    
    for (const auto & [partition_key, _] : total_rows)
    {
        auto filename = data_filename_in_storage[partition_key];
        if (auto pos = filename.find('/'); pos != std::string::npos)
            filename = filename.substr(pos + 1);

        auto record_info = generateRecordInfo(filename, column_stats[partition_key], total_chunks_size[partition_key], total_rows[partition_key], null_counts[partition_key], getHeader().getNames());
        metadata_content += record_info + "\n";
    }
    Int64 latest_version = -1;
    {
        const auto metadata_files = listFiles(*object_storage, *configuration, "", ".json");
        for (const auto & metadata_file : metadata_files)
        {
            std::cerr << "metadata_file " << metadata_file << '\n';
            auto version_str = metadata_file;
            size_t pos = metadata_file.find('.');

            if (pos != std::string::npos)
                version_str = metadata_file.substr(0, pos);

            pos = version_str.rfind('/');
            if (pos != std::string::npos)
                version_str = version_str.substr(pos + 1);

            latest_version = std::max(latest_version, static_cast<Int64>(std::stoll(version_str)));
        }
    }
    auto metadata_filename = filename_generator.generateMetadataPath(latest_version + 1);

    std::cerr << "metadata_filename " << metadata_filename << '\n';
    auto buffer_metadata = object_storage->writeObject(
        StoredObject(metadata_filename), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());
    buffer_metadata->write(metadata_content.data(), metadata_content.size());
    buffer_metadata->finalize();


    const auto metadata_files = listFiles(*object_storage, *configuration, "", ".json");
    std::cerr << "====\n\n\n" << metadata_files.size() << ' ' << metadata_files[0] << "\n\n\n\n=====\n"; 
    return true;
}

}
