#include "config.h"
#if USE_AWS_S3

#    include <Storages/StorageIceberg.h>
#    include <Common/logger_useful.h>

#    include <IO/ReadBufferFromS3.h>
#    include <IO/ReadHelpers.h>
#    include <IO/ReadSettings.h>
#    include <IO/S3Common.h>

#    include <Storages/ExternalDataSourceConfiguration.h>
#    include <Storages/StorageFactory.h>
#    include <Storages/checkAndGetLiteralArgument.h>

#    include <Formats/FormatFactory.h>

#    include <aws/core/auth/AWSCredentials.h>
#    include <aws/s3/S3Client.h>
#    include <aws/s3/model/ListObjectsV2Request.h>

#    include <QueryPipeline/Pipe.h>

#    include <fmt/format.h>
#    include <fmt/ranges.h>
#    include <ranges>

#    include <Processors/Formats/Impl/AvroRowInputFormat.h>

#    include <Poco/JSON/Array.h>
#    include <Poco/JSON/Object.h>
#    include <Poco/JSON/Parser.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_DATA;
    extern const int FILE_DOESNT_EXIST;
}

IcebergMetaParser::IcebergMetaParser(const StorageS3Configuration & configuration_, const String & table_path_, ContextPtr context_)
    : base_configuration(configuration_), table_path(table_path_), context(context_)
{
}

std::vector<String> IcebergMetaParser::getFiles() const
{
    auto metadata = getNewestMetaFile();
    auto manifest_list = getManiFestList(metadata);

    /// When table first created and does not have any data
    if (manifest_list.empty())
    {
        return {};
    }

    auto manifest_files = getManifestFiles(manifest_list);
    return getFilesForRead(manifest_files);
}

String IcebergMetaParser::getNewestMetaFile() const
{
    /// Iceberg stores all the metadata.json in metadata directory, and the
    /// newest version has the max version name, so we should list all of them
    /// then find the newest metadata.
    std::vector<String> metadata_files;

    const auto & client = base_configuration.client;

    Aws::S3::Model::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Outcome outcome;

    bool is_finished{false};
    const auto bucket{base_configuration.uri.bucket};

    request.SetBucket(bucket);

    static constexpr auto metadata_directory = "metadata";
    request.SetPrefix(std::filesystem::path(table_path) / metadata_directory);

    while (!is_finished)
    {
        outcome = client->ListObjectsV2(request);
        if (!outcome.IsSuccess())
            throw Exception(
                ErrorCodes::S3_ERROR,
                "Could not list objects in bucket {} with key {}, S3 exception: {}, message: {}",
                quoteString(bucket),
                quoteString(table_path),
                backQuote(outcome.GetError().GetExceptionName()),
                quoteString(outcome.GetError().GetMessage()));

        const auto & result_batch = outcome.GetResult().GetContents();
        for (const auto & obj : result_batch)
        {
            const auto & filename = obj.GetKey();

            if (std::filesystem::path(filename).extension() == ".json")
                metadata_files.push_back(filename);
        }

        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());

        is_finished = !outcome.GetResult().GetIsTruncated();
    }

    if (metadata_files.empty())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "The metadata file for Iceberg table with path {} doesn't exist", table_path);

    auto it = std::max_element(metadata_files.begin(), metadata_files.end());
    return *it;
}

String IcebergMetaParser::getManiFestList(String metadata_name) const
{
    auto buffer = createS3ReadBuffer(metadata_name, context);
    String json_str;
    readJSONObjectPossiblyInvalid(json_str, file);

    /// Looks like base/base/JSON.h can not parse this json file
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(json_str);
    Poco::JSON::Object::Ptr object = json.extract<Poco::JSON::Object::Ptr>();

    auto current_snapshot_id = object->getValue<Int64>("current-snapshot-id");

    auto snapshots = object->get("snapshots").extract<Poco::JSON::Array::Ptr>();

    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        if (snapshot->getValue<Int64>("snapshot-id") == current_snapshot_id)
            return object->getValue<String>("manifest-list");
    }

    return {};
}

static ColumnPtr
parseAvro(const std::uniq_ptr<avro::DataFileReaderBase> & file_reader, const DataTypePtr & data_type, const String & field_name)
{
    auto deserializer = std::make_unique<AvroDeserializer>(
        Block{{data_type->createColumn(), data_type, field_name}}, file_reader->dataSchema(), true, true);
    file_reader->init();
    MutableColumns columns;
    columns.emplace_back(data_type->createColumn());

    RowReadExtension ext;
    while (file_reader->hasMore())
    {
        file_reader->decr();
        deserializer->deserializeRow(columns, file_reader->decoder, ext);
    }
    return columns.at(0);
}

std::vector<String> IcebergMetaParser::getManifestFiles(const String & manifest_list) const
{
    auto buffer = createS3ReadBuffer(manifest_list, context);

    auto file_reader = std::make_unique<avro::DataFileReaderBase>(std::make_unique<AvroInputStreamReadBufferAdapter>(in));

    static constexpr manifest_path = "manifest_path";

    /// The manifest_path is the first field in manifest list file,
    /// And its have String data type
    /// {'manifest_path': 'xxx', ...}
    auto data_type = AvroSchemaReader::avroNodeToDataType(file_reader->dataSchema().root()->leafAt(0));
    auto col = parseAvro(file_reader, data_type, manifest_path);

    std::vector<String> res;
    if (col->getDataType() == TypeIndex::String)
    {
        const auto * col_str = typeid_cast<ColumnString *>(col.get());
        size_t col_size = col_str->size();
        for (size_t i = 0; i < col_size; ++i)
        {
            auto file_path = col_str[i].safeGet<String>();
            /// We just need obtain the file name
            std::filesystem::path path(file_path);
            res.emplace_back(path.filename());
        }

        return res;
    }
    Throw Exception(
        ErrorCodes::ILLEGAL_COLUMN,
        "The parsed column from Avro file for manifest_path should have data type String, but get {}",
        col->getFamilyName());
}

std::vector<String> IcebergMetaParser::getFilesForRead(const std::vector<String> & manifest_files) const
{
    std::vector<String> keys;
    for (const auto & manifest_file : manifest_files)
    {
        auto buffer = createS3ReadBuffer(manifest_file, context);

        auto file_reader = std::make_unique<avro::DataFileReaderBase>(std::make_unique<AvroInputStreamReadBufferAdapter>(in));

        static constexpr manifest_path = "data_file";

        /// The data_file filed at the 3rd position of the manifest file:
        /// {'status': xx, 'snapshot_id': xx, 'data_file': {'file_path': 'xxx', ...}, ...}
        /// and it's also a nested record, so its result type is a nested Tuple
        auto data_type = AvroSchemaReader::avroNodeToDataType(file_reader->dataSchema().root()->leafAt(2));
        auto col = parseAvro(file_reader, data_type, manifest_path);

        std::vector<String> res;
        if (col->getDataType() == TypeIndex::Tuple)
        {
            auto * col_tuple = typeid_cast<ColumnTuple *>(col.get());
            auto * col_str = col_tuple->getColumnPtr(0);
            if (col_str->getDataType() == TypeIndex::String)
            {
                const auto * str_col = typeid_cast<ColumnString *>(col_str.get());
                size_t col_size = str_col->size();
                for (size_t i = 0; i < col_size; ++i)
                {
                    auto file_path = std_col[i].safeGet<String>();
                    /// We just obtain the parition/file name
                    std::filesystem::path path(file_path);
                    res.emplace_back(path.parent_path().filename() + '/' + path.filename());
                }
            }
            else
            {
                Throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "The parsed column from Avro file for file_path should have data type String, got {}",
                    col_str->getFamilyName());
            }
        }
        else
        {
            Throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "The parsed column from Avro file for data_file field should have data type Tuple, got {}",
                col->getFamilyName());
        }
    }

    return res;
}

std::shared_ptr<ReadBuffer> IcebergMetaParser::createS3ReadBuffer(const String & key, ContextPtr context)
{
    S3Settings::RequestSettings request_settings;
    request_settings.max_single_read_retries = 10;
    return std::make_shared<ReadBufferFromS3>(
        base_configuration.client,
        base_configuration.uri.bucket,
        key,
        base_configuration.uri.version_id,
        request_settings,
        context->getReadSettings());
}

namespace
{

StorageS3::S3Configuration getBaseConfiguration(const StorageS3Configuration & configuration)
{
    return {configuration.url, configuration.auth_settings, configuration.request_settings, configuration.headers};
}

// generateQueryFromKeys constructs query from all parquet filenames
// for underlying StorageS3 engine
String generateQueryFromKeys(const std::vector<String> & keys)
{
    std::string new_query = fmt::format("{{{}}}", fmt::join(keys, ","));
    return new_query;
}


StorageS3Configuration getAdjustedS3Configuration(
    const ContextPtr & context,
    StorageS3::S3Configuration & base_configuration,
    const StorageS3Configuration & configuration,
    const std::string & table_path,
    Poco::Logger * log)
{
    IcebergMetaParser parser{base_configuration, table_path, context};

    auto keys = parser.getFiles();
    static constexpr iceberg_data_directory = "data";
    auto new_uri = std::filesystem::path(base_configuration.uri.uri.toString()) / iceberg_data_directory / generateQueryFromKeys(keys);

    LOG_DEBUG(log, "New uri: {}", new_uri);
    LOG_DEBUG(log, "Table path: {}", table_path);

    // set new url in configuration
    StorageS3Configuration new_configuration;
    new_configuration.url = new_uri;
    new_configuration.auth_settings.access_key_id = configuration.auth_settings.access_key_id;
    new_configuration.auth_settings.secret_access_key = configuration.auth_settings.secret_access_key;
    new_configuration.format = configuration.format;

    return new_configuration;
}

}

StorageIceberg::StorageIceberg(
    const StorageS3Configuration & configuration_,
    const StorageID & table_id_,
    ColumnsDescription columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_)
    : IStorage(table_id_)
    , base_configuration{getBaseConfiguration(configuration_)}
    , log(&Poco::Logger::get("StorageIceberg(" + table_id_.table_name + ")"))
    , table_path(base_configuration.uri.key)
{
    StorageInMemoryMetadata storage_metadata;
    StorageS3::updateS3Configuration(context_, base_configuration);

    auto new_configuration = getAdjustedS3Configuration(context_, base_configuration, configuration_, table_path, log);

    if (columns_.empty())
    {
        columns_ = StorageS3::getTableStructureFromData(
            new_configuration, /*distributed processing*/ false, format_settings_, context_, nullptr);
        storage_metadata.setColumns(columns_);
    }
    else
        storage_metadata.setColumns(columns_);


    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    s3engine = std::make_shared<StorageS3>(
        new_configuration,
        table_id_,
        columns_,
        constraints_,
        comment,
        context_,
        format_settings_,
        /* distributed_processing_ */ false,
        nullptr);
}

Pipe StorageIceberg::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    StorageS3::updateS3Configuration(context, base_configuration);

    return s3engine->read(column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
}

ColumnsDescription StorageIceberg::getTableStructureFromData(
    const StorageS3Configuration & configuration, const std::optional<FormatSettings> & format_settings, ContextPtr ctx)
{
    auto base_configuration = getBaseConfiguration(configuration);
    StorageS3::updateS3Configuration(ctx, base_configuration);
    auto new_configuration = getAdjustedS3Configuration(
        ctx, base_configuration, configuration, base_configuration.uri.key, &Poco::Logger::get("StorageIceberg"));
    return StorageS3::getTableStructureFromData(
        new_configuration, /*distributed processing*/ false, format_settings, ctx, /*object_infos*/ nullptr);
}

void registerStorageIceberg(StorageFactory & factory)
{
    factory.registerStorage(
        "Iceberg",
        [](const StorageFactory::Arguments & args)
        {
            auto & engine_args = args.engine_args;
            if (engine_args.empty() || engine_args.size() < 3)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Storage Iceberg requires 3 to 4 arguments: table_url, access_key, secret_access_key, [format]");

            StorageS3Configuration configuration;

            configuration.url = checkAndGetLiteralArgument<String>(engine_args[0], "url");
            configuration.auth_settings.access_key_id = checkAndGetLiteralArgument<String>(engine_args[1], "access_key_id");
            configuration.auth_settings.secret_access_key = checkAndGetLiteralArgument<String>(engine_args[2], "secret_access_key");

            if (engine_args.size() == 4)
                configuration.format = checkAndGetLiteralArgument<String>(engine_args[3], "format");
            else
            {
                /// Iceberg uses Parquet by default.
                configuration.format = "Parquet";
            }

            return std::make_shared<StorageIceberg>(
                configuration, args.table_id, args.columns, args.constraints, args.comment, args.getContext(), std::nullopt);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });
}

}

#endif
